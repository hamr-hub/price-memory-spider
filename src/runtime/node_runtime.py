import os
import threading
import time
import socket
import platform
import json
import uuid
import tempfile
import shutil
import subprocess
from datetime import datetime, timezone
from typing import Optional
from supabase import Client
from ..dao.supabase_client import get_client
# remove sqlite dependency; use Supabase for counts
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
        self._proxies = []
        try:
            raw = os.environ.get("HTTP_PROXY_LIST") or os.environ.get("PLAYWRIGHT_PROXIES") or ""
            self._proxies = [x.strip() for x in raw.split(",") if x.strip()]
        except Exception:
            self._proxies = []
        try:
            self._concurrency = int(os.environ.get("NODE_CONCURRENCY") or "1")
        except Exception:
            self._concurrency = 1
        self._running = 0

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
        self._refresh_settings_once()

    def _heartbeat_loop(self):
        while not self._stop:
            self._heartbeat_once()
            time.sleep(5)

    def _heartbeat_once(self):
        if not self.client or not self.node_id:
            return
        try:
            self._refresh_settings_once()
        except Exception:
            pass
        running = int(self._running or 0)
        pending_tasks = 0
        completed_tasks = 0
        try:
            rp = self.client.table("tasks").select("id", count="exact").eq("status", "pending").execute()
            pending_tasks = int(getattr(rp, "count", 0) or 0)
            rc = self.client.table("tasks").select("id", count="exact").eq("status", "completed").execute()
            completed_tasks = int(getattr(rc, "count", 0) or 0)
        except Exception:
            pending_tasks = 0
            completed_tasks = 0
        now = datetime.now(timezone.utc).isoformat()
        st = "paused" if self._paused else "online"
        self.client.table("runtime_nodes").update({
            "status": st,
            "current_tasks": running,
            "queue_size": pending_tasks,
            "total_completed": completed_tasks,
            "last_seen": now,
        }).eq("id", self.node_id).execute()

    def _refresh_settings_once(self):
        if not self.client or not self.node_id:
            return
        try:
            rs = self.client.table("runtime_nodes").select("concurrency").eq("id", self.node_id).limit(1).execute()
            data = getattr(rs, "data", []) or []
            if data:
                c = int((data[0] or {}).get("concurrency") or 0)
                if c > 0:
                    self._concurrency = c
        except Exception:
            pass

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
        now = datetime.now(timezone.utc).isoformat()
        r = (
            self.client
            .table("node_commands")
            .select("id,command,payload,priority,scheduled_at")
            .eq("node_id", self.node_id)
            .eq("status", "pending")
            .or(f"scheduled_at.is.null,scheduled_at.lte.{now}")
            .order("priority", desc=True)
            .order("scheduled_at", desc=False)
            .order("id", desc=False)
            .limit(50)
            .execute()
        )
        rows = getattr(r, "data", []) or []
        for row in rows:
            if self._paused:
                continue
            if self._running >= self._concurrency:
                break
            try:
                sch = row.get("scheduled_at")
                if sch and str(sch) > now:
                    continue
            except Exception:
                pass
            try:
                self.client.table("node_commands").update({
                    "status": "processing",
                }).eq("id", row.get("id")).execute()
            except Exception:
                pass
            threading.Thread(target=self._handle_command, args=(row,), daemon=True).start()
            self._running += 1

    def _handle_command(self, row: dict):
        try:
            cmd = row.get("command")
            payload = row.get("payload") or {}
            if cmd == "pause":
                self._paused = True
            elif cmd == "resume":
                self._paused = False
            elif cmd == "ping":
                try:
                    ws = os.environ.get("PLAYWRIGHT_WS_ENDPOINT") or "ws://43.133.224.11:20001/"
                    from playwright.sync_api import sync_playwright
                    t0 = time.time()
                    with sync_playwright() as p:
                        b = p.chromium.connect(ws)
                        b.close()
                    dt = int((time.time() - t0) * 1000)
                    now = datetime.now(timezone.utc).isoformat()
                    self.client.table("runtime_nodes").update({
                        "latency_ms": dt,
                        "last_seen": now,
                    }).eq("id", self.node_id).execute()
                except Exception:
                    pass
            elif cmd == "test_crawl":
                url = payload.get("url") or "https://example.com"
                job_id = payload.get("job_id") or str(uuid.uuid4())
                timeout_ms = int(payload.get("timeout_ms") or 30000)
                retries = int(payload.get("retries") or 0)
                attempt = 0
                last_err = None
                while attempt <= retries:
                    try:
                        self._run_test_crawl(url, job_id, timeout_ms)
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        self._log(job_id, "warn", f"重试 {attempt+1}/{retries} 失败: {str(e)}")
                        time.sleep(1)
                    finally:
                        attempt += 1
                if last_err:
                    self._log(job_id, "error", f"最终失败: {str(last_err)}")
            elif cmd == "test_steps":
                job_id = payload.get("job_id") or str(uuid.uuid4())
                url = payload.get("url") or "https://example.com"
                steps = payload.get("steps") or []
                timeout_ms = int(payload.get("timeout_ms") or 30000)
                retries = int(payload.get("retries") or 0)
                attempt = 0
                last_err = None
                while attempt <= retries:
                    try:
                        self._run_steps(job_id, url, steps, timeout_ms)
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        self._log(job_id, "warn", f"步骤重试 {attempt+1}/{retries} 失败: {str(e)}")
                        time.sleep(1)
                    finally:
                        attempt += 1
                if last_err:
                    self._log(job_id, "error", f"步骤最终失败: {str(last_err)}")
            elif cmd == "codegen":
                url = payload.get("url") or "https://example.com"
                job_id = payload.get("job_id") or str(uuid.uuid4())
                target = (payload.get("target") or "python").lower()
                duration_sec = int(payload.get("duration_sec") or 10)
                self._run_codegen(url, job_id, target, duration_sec)
            elif cmd == "convert_codegen":
                job_id = payload.get("job_id") or str(uuid.uuid4())
                script_url = payload.get("script_url") or payload.get("url")
                target = (payload.get("target") or "python").lower()
                if script_url:
                    self._run_convert_codegen(script_url, job_id, target)
        finally:
            try:
                self.client.table("node_commands").update({
                    "status": "processed",
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", row.get("id")).execute()
            except Exception:
                pass
            self._running = max(0, self._running - 1)

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

    def _run_test_crawl(self, url: str, job_id: str, timeout_ms: int = 30000):
        self._log(job_id, "info", f"开始测试爬取: {url}")
        if self._paused:
            self._log(job_id, "warn", "节点处于暂停状态，跳过执行")
            return
        ws = os.environ.get("PLAYWRIGHT_WS_ENDPOINT") or "ws://43.133.224.11:20001/"
        mode = os.environ.get("BROWSER_MODE") or "remote"
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
        har_path = os.path.join(tmpdir, "network.har")
        screenshot_path = os.path.join(tmpdir, "screenshot.png")
        video_file = None
        start_ts = time.time()
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = None
                if mode == "local":
                    proxy = self._select_proxy(job_id)
                    launch_args = {"headless": True}
                    if proxy:
                        launch_args["proxy"] = {"server": proxy}
                    browser = p.chromium.launch(**launch_args)
                else:
                    browser = p.chromium.connect(ws)
                context = browser.new_context(record_video_dir=tmpdir, record_har_path=har_path, record_har_mode="minimal")
                context.tracing.start(screenshots=True, snapshots=True, sources=True)
                page = context.new_page()
                page.set_default_timeout(timeout_ms)
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
        duration_ms = int((time.time() - start_ts) * 1000)
        # 上传工件
        try:
            bucket = self.client.storage.from_("artifacts")
            if os.path.exists(trace_path):
                with open(trace_path, "rb") as f:
                    bucket.upload(f"{job_id}/trace.zip", f)
                pub = bucket.get_public_url(f"{job_id}/trace.zip")
                urlt = getattr(pub, "data", {}).get("publicUrl") or getattr(pub, "publicURL", None) or pub
                tsize = os.path.getsize(trace_path)
                self._log(job_id, "artifact", json.dumps({"type": "trace", "url": urlt, "page_url": url, "title": title if 'title' in locals() else None, "duration_ms": duration_ms, "size_bytes": tsize}))
            if os.path.exists(har_path):
                with open(har_path, "rb") as f:
                    bucket.upload(f"{job_id}/network.har", f)
                pubh = bucket.get_public_url(f"{job_id}/network.har")
                urlh = getattr(pubh, "data", {}).get("publicUrl") or getattr(pubh, "publicURL", None) or pubh
                try:
                    with open(har_path, "r", encoding="utf-8", errors="ignore") as hf:
                        har = json.load(hf)
                    entries = (har.get("log", {}).get("entries", []))
                    total = len(entries)
                    status2 = {}
                    bytes_sum = 0
                    for e in entries:
                        st = int((e.get("response", {}) or {}).get("status", 0) or 0)
                        status2[st] = status2.get(st, 0) + 1
                        cont = (e.get("response", {}) or {}).get("content", {}) or {}
                        sz = int(cont.get("size", 0) or 0)
                        bytes_sum += sz
                    self._log(job_id, "metric", json.dumps({"type": "har_summary", "requests": total, "status_counts": status2, "bytes": bytes_sum}))
                except Exception:
                    pass
                self._log(job_id, "artifact", json.dumps({"type": "har", "url": urlh}))
            if video_file and os.path.exists(video_file):
                with open(video_file, "rb") as f:
                    bucket.upload(f"{job_id}/video.webm", f)
                pubv = bucket.get_public_url(f"{job_id}/video.webm")
                urlv = getattr(pubv, "data", {}).get("publicUrl") or getattr(pubv, "publicURL", None) or pubv
                vsize = os.path.getsize(video_file)
                self._log(job_id, "artifact", json.dumps({"type": "video", "url": urlv, "page_url": url, "title": title if 'title' in locals() else None, "duration_ms": duration_ms, "size_bytes": vsize}))
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

    def _select_proxy(self, key: str) -> Optional[str]:
        try:
            if not self._proxies:
                return None
            import hashlib
            idx = int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16) % len(self._proxies)
            return self._proxies[idx]
        except Exception:
            return None

    def _run_steps(self, job_id: str, url: str, steps: list, timeout_ms: int = 30000):
        self._log(job_id, "info", f"执行步骤: {len(steps)} 个, 初始页面: {url}")
        ws = os.environ.get("PLAYWRIGHT_WS_ENDPOINT") or "ws://43.133.224.11:20001/"
        from playwright.sync_api import sync_playwright
        outputs = []
        with sync_playwright() as p:
            browser = p.chromium.connect(ws)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url)
            for idx, step in enumerate(steps or []):
                act = (step or {}).get("action")
                sel = (step or {}).get("selector")
                val = (step or {}).get("value")
                self._log(job_id, "debug", json.dumps({"step": idx+1, "action": act, "selector": sel}))
                try:
                    if act == "goto":
                        page.goto(str(step.get("url") or val or url))
                    elif act == "wait_for_selector":
                        page.wait_for_selector(str(sel))
                    elif act == "click":
                        page.click(str(sel))
                    elif act == "fill":
                        page.fill(str(sel), str(val or ""))
                    elif act == "wait":
                        time.sleep(float(step.get("seconds") or val or 0))
                    elif act == "evaluate_text":
                        txt = page.text_content(str(sel))
                        outputs.append({"type": "text", "selector": sel, "value": txt})
                    elif act == "screenshot":
                        tmpdir = tempfile.mkdtemp(prefix=f"pm_{job_id}_shot_")
                        shot = os.path.join(tmpdir, f"shot_{idx+1}.png")
                        page.screenshot(path=shot)
                        try:
                            bucket = self.client.storage.from_("artifacts")
                            with open(shot, "rb") as f:
                                bucket.upload(f"{job_id}/shot_{idx+1}.png", f)
                            pub = bucket.get_public_url(f"{job_id}/shot_{idx+1}.png")
                            urlp = getattr(pub, "data", {}).get("publicUrl") or getattr(pub, "publicURL", None) or pub
                            outputs.append({"type": "screenshot", "index": idx+1, "url": urlp})
                            self._log(job_id, "artifact", json.dumps({"type": "screenshot", "url": urlp}))
                        finally:
                            shutil.rmtree(tmpdir, ignore_errors=True)
                    else:
                        self._log(job_id, "warn", f"未知动作: {act}")
                except Exception as e:
                    self._log(job_id, "error", f"步骤 {idx+1} 执行失败: {str(e)}")
                    raise
            title = page.title()
            self._log(job_id, "info", f"步骤执行完成, 标题: {title}")
            browser.close()
        self._log(job_id, "result", json.dumps({"url": url, "title": title if 'title' in locals() else None, "outputs": outputs}))

    def _run_codegen(self, url: str, job_id: str, target: str = "python", duration_sec: int = 10):
        if not self.client:
            return
        tmpdir = tempfile.mkdtemp(prefix=f"pm_codegen_{job_id}_")
        script_ext = "py" if target == "python" else "js"
        out_script = os.path.join(tmpdir, f"script.{script_ext}")
        trace_path = os.path.join(tmpdir, "trace.zip")
        cmd = ["python", "-m", "playwright", "codegen", url, "--target", target, "--output", out_script, "--save-trace", trace_path]
        proc = None
        try:
            proc = subprocess.Popen(cmd)
            time.sleep(max(1, duration_sec))
            if proc and proc.poll() is None:
                proc.terminate()
        except Exception as e:
            self._log(job_id, "error", f"codegen 失败: {str(e)}")
        try:
            bucket = self.client.storage.from_("artifacts")
            if os.path.exists(out_script):
                with open(out_script, "rb") as f:
                    bucket.upload(f"{job_id}/script.{script_ext}", f)
                pubs = bucket.get_public_url(f"{job_id}/script.{script_ext}")
                urls = getattr(pubs, "data", {}).get("publicUrl") or getattr(pubs, "publicURL", None) or pubs
                self._log(job_id, "artifact", json.dumps({"type": "script", "url": urls, "target": target}))
            if os.path.exists(trace_path):
                with open(trace_path, "rb") as f:
                    bucket.upload(f"{job_id}/codegen_trace.zip", f)
                pubt = bucket.get_public_url(f"{job_id}/codegen_trace.zip")
                urlt = getattr(pubt, "data", {}).get("publicUrl") or getattr(pubt, "publicURL", None) or pubt
                self._log(job_id, "artifact", json.dumps({"type": "trace", "url": urlt}))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def _run_convert_codegen(self, script_url: str, job_id: str, target: str = "python"):
        import requests
        r = requests.get(script_url, timeout=10)
        txt = r.text
        steps = []
        lines = txt.splitlines()
        if target == "python":
            import re
            pat = {
                "goto": re.compile(r"page\.goto\((['\"])(.+?)\1\)"),
                "click": re.compile(r"page\.click\((['\"])(.+?)\1\)"),
                "fill": re.compile(r"page\.fill\((['\"])(.+?)\1\s*,\s*(['\"])(.+?)\3\)"),
                "wait": re.compile(r"page\.wait_for_selector\((['\"])(.+?)\1\)"),
            }
        else:
            import re
            pat = {
                "goto": re.compile(r"await\s+page\.goto\((['\"])(.+?)\1\)"),
                "click": re.compile(r"await\s+page\.click\((['\"])(.+?)\1\)"),
                "fill": re.compile(r"await\s+page\.fill\((['\"])(.+?)\1\s*,\s*(['\"])(.+?)\3\)"),
                "wait": re.compile(r"await\s+page\.waitForSelector\((['\"])(.+?)\1\)"),
            }
        for ln in lines:
            m = pat["goto"].search(ln)
            if m:
                steps.append({"action": "goto", "url": m.group(2)})
                continue
            m = pat["wait"].search(ln)
            if m:
                steps.append({"action": "wait_for_selector", "selector": m.group(2)})
                continue
            m = pat["click"].search(ln)
            if m:
                steps.append({"action": "click", "selector": m.group(2)})
                continue
            m = pat["fill"].search(ln)
            if m:
                steps.append({"action": "fill", "selector": m.group(2), "value": m.group(4)})
                continue
        import io, json as pyjson
        bio = io.BytesIO(pyjson.dumps({"steps": steps}).encode("utf-8"))
        bucket = self.client.storage.from_("artifacts")
        bucket.upload(f"{job_id}/steps.json", bio)
        pub = bucket.get_public_url(f"{job_id}/steps.json")
        urlp = getattr(pub, "data", {}).get("publicUrl") or getattr(pub, "publicURL", None) or pub
        self._log(job_id, "artifact", pyjson.dumps({"type": "steps", "url": urlp, "count": len(steps)}))
