import os
import threading
import time
import socket
import platform
import json
import uuid
import tempfile
import shutil
from datetime import datetime, timezone
from typing import Optional
from supabase import Client
from ..dao.supabase_client import get_client
from ...main import get_conn
try:
    from ..playwrite.bowser_utils import BowserBrowser
except Exception:
    BowserBrowser = None

class NodeRuntime:
    def __init__(self):
        self.client: Optional[Client] = get_client()
        self.name = os.environ.get("NODE_NAME") or f"node-{socket.gethostname()}"
        self.host = socket.gethostname()
        self.region = os.environ.get("NODE_REGION") or "local"
        self.version = platform.python_version()
        self.status = "online"
        self.node_id: Optional[int] = None
        self._stop = False
        self._paused = False

    def start(self):
        if not self.client:
            return
        self._ensure_node()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._command_loop, daemon=True).start()

    def stop(self):
        self._stop = True

    def _ensure_node(self):
        now = datetime.now(timezone.utc).isoformat()
        self.client.table("runtime_nodes").upsert({
            "name": self.name,
            "host": self.host,
            "region": self.region,
            "version": self.version,
            "status": self.status,
            "current_tasks": 0,
            "queue_size": 0,
            "total_completed": 0,
            "last_seen": now,
        }, on_conflict="name").execute()
        r = self.client.table("runtime_nodes").select("id").eq("name", self.name).limit(1).execute()
        rows = getattr(r, "data", []) or []
        if rows:
            self.node_id = rows[0]["id"]

    def _heartbeat_loop(self):
        while not self._stop:
            self._heartbeat_once()
            time.sleep(5)

    def _heartbeat_once(self):
        if not self.client or not self.node_id:
            return
        conn = get_conn()
        cur = conn.cursor()
        running = cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'running'").fetchone()[0]
        pending = cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'pending'").fetchone()[0]
        completed = cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'completed'").fetchone()[0]
        conn.close()
        now = datetime.now(timezone.utc).isoformat()
        st = "paused" if self._paused else "online"
        self.client.table("runtime_nodes").update({
            "status": st,
            "current_tasks": int(running or 0),
            "queue_size": int(pending or 0),
            "total_completed": int(completed or 0),
            "last_seen": now,
        }).eq("id", self.node_id).execute()

    def _command_loop(self):
        while not self._stop:
            try:
                self._consume_commands()
            except Exception:
                pass
            time.sleep(2)

    def _consume_commands(self):
        if not self.client or not self.node_id:
            return
        r = self.client.table("node_commands").select("id,command,payload").eq("node_id", self.node_id).eq("status", "pending").order("id", desc=False).limit(20).execute()
        rows = getattr(r, "data", []) or []
        for row in rows:
            cmd = row.get("command")
            if cmd == "pause":
                self._paused = True
            if cmd == "resume":
                self._paused = False
            if cmd == "test_crawl":
                try:
                    payload = row.get("payload") or {}
                    url = payload.get("url") or "https://example.com"
                    job_id = payload.get("job_id") or str(uuid.uuid4())
                    self._run_test_crawl(url, job_id)
                except Exception:
                    pass
            self.client.table("node_commands").update({
                "status": "processed",
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", row.get("id")).execute()

    def _log(self, job_id: str, level: str, message: str):
        if not self.client:
            return
        try:
            self.client.table("crawl_logs").insert({
                "job_id": job_id,
                "node_id": self.node_id,
                "level": level,
                "message": message,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
        except Exception:
            pass

    def _run_test_crawl(self, url: str, job_id: str):
        self._log(job_id, "info", f"开始测试爬取: {url}")
        if self._paused:
            self._log(job_id, "warn", "节点处于暂停状态，跳过执行")
            return
        ws = os.environ.get("PLAYWRIGHT_WS_ENDPOINT") or "ws://43.133.224.11:20001/"
        if not self.client:
            self._log(job_id, "error", "Supabase 客户端不可用")
            return
        # 确保存储桶存在
        try:
            bconf = {"public": True}
            getattr(self.client.storage, "create_bucket", lambda *a, **k: None)("artifacts", **bconf)
        except Exception:
            pass
        tmpdir = tempfile.mkdtemp(prefix=f"pm_{job_id}_")
        trace_path = os.path.join(tmpdir, "trace.zip")
        har_path = os.path.join(tmpdir, "har.zip")
        screenshot_path = os.path.join(tmpdir, "screenshot.png")
        video_file = None
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.connect(ws)
                context = browser.new_context(record_video_dir=tmpdir, record_har_path=har_path, record_har_mode="minimal")
                context.tracing.start(screenshots=True, snapshots=True, sources=True)
                page = context.new_page()
                page.set_default_timeout(30000)
                page.goto(url)
                title = page.title()
                self._log(job_id, "info", f"页面标题: {title}")
                body = page.evaluate("document.body.innerText") or ""
                self._log(job_id, "debug", f"正文长度: {len(body)}")
                try:
                    page.screenshot(path=screenshot_path, full_page=True)
                except Exception:
                    pass
                context.tracing.stop(path=trace_path)
                page.close()
                vids = []
                try:
                    vpath = page.video.path()
                    vids.append(vpath)
                except Exception:
                    pass
                video_file = vids[0] if vids else None
                browser.close()
        except Exception as e:
            self._log(job_id, "error", f"浏览器执行失败: {str(e)}")
        # 上传工件
        try:
            bucket = self.client.storage.from_("artifacts")
            if os.path.exists(trace_path):
                with open(trace_path, "rb") as f:
                    bucket.upload(f"{job_id}/trace.zip", f)
                pub = bucket.get_public_url(f"{job_id}/trace.zip")
                urlt = getattr(pub, "data", {}).get("publicUrl") or getattr(pub, "publicURL", None) or pub
                self._log(job_id, "artifact", json.dumps({"type": "trace", "url": urlt}))
            if os.path.exists(har_path):
                with open(har_path, "rb") as f:
                    bucket.upload(f"{job_id}/har.zip", f)
                pubh = bucket.get_public_url(f"{job_id}/har.zip")
                urlh = getattr(pubh, "data", {}).get("publicUrl") or getattr(pubh, "publicURL", None) or pubh
                self._log(job_id, "artifact", json.dumps({"type": "har", "url": urlh}))
            if video_file and os.path.exists(video_file):
                with open(video_file, "rb") as f:
                    bucket.upload(f"{job_id}/video.webm", f)
                pubv = bucket.get_public_url(f"{job_id}/video.webm")
                urlv = getattr(pubv, "data", {}).get("publicUrl") or getattr(pubv, "publicURL", None) or pubv
                self._log(job_id, "artifact", json.dumps({"type": "video", "url": urlv}))
            if os.path.exists(screenshot_path):
                with open(screenshot_path, "rb") as f:
                    bucket.upload(f"{job_id}/screenshot.png", f)
                pubs = bucket.get_public_url(f"{job_id}/screenshot.png")
                urls = getattr(pubs, "data", {}).get("publicUrl") or getattr(pubs, "publicURL", None) or pubs
                self._log(job_id, "artifact", json.dumps({"type": "screenshot", "url": urls}))
        except Exception as e:
            self._log(job_id, "warn", f"工件上传失败: {str(e)}")
        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass
        # 生成模拟价格结果
        try:
            import random
            price = round(random.uniform(50.0, 200.0), 2)
            self._log(job_id, "result", json.dumps({"price": price, "url": url}))
            self._log(job_id, "info", "测试爬取完成")
        except Exception:
            pass
