import os
import threading
import time
import socket
import platform
import json
import uuid
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
        if BowserBrowser is None:
            self._log(job_id, "error", "Playwright 未可用，无法执行")
            return
        try:
            browser = BowserBrowser(ws_endpoint=ws, headless=True)
            def action(page):
                try:
                    page.goto(url)
                    title = page.title()
                    self._log(job_id, "info", f"页面标题: {title}")
                    body = page.evaluate("document.body.innerText") or ""
                    self._log(job_id, "debug", f"正文长度: {len(body)}")
                except Exception as e:
                    self._log(job_id, "error", f"页面操作失败: {str(e)}")
            browser.run_sync(url, action, timeout=30000)
            # 生成模拟价格结果
            import random
            price = round(random.uniform(50.0, 200.0), 2)
            self._log(job_id, "result", json.dumps({"price": price, "url": url}))
            self._log(job_id, "info", "测试爬取完成")
        except Exception as e:
            self._log(job_id, "error", f"浏览器执行失败: {str(e)}")
