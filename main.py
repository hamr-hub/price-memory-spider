import os
import sqlite3
import math
import datetime
from typing import Any, List, Optional
from fastapi import FastAPI, APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel
import sys

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "spider.db")

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ok(data: Any, message: str = "操作成功"):
    return {"success": True, "data": data, "message": message, "timestamp": now_iso()}

def error_response(status_code: int, code: str, message: str, details: Optional[List[Any]] = None):
    return {"success": False, "error": {"code": code, "message": message, "details": details or []}, "timestamp": now_iso()}

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, url TEXT NOT NULL, category TEXT, last_updated TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS prices (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL, price REAL NOT NULL, created_at TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE, display_name TEXT, created_at TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS user_follows (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, product_id INTEGER NOT NULL, created_at TEXT NOT NULL, UNIQUE(user_id, product_id))")
    cur.execute("CREATE TABLE IF NOT EXISTS pushes (id INTEGER PRIMARY KEY AUTOINCREMENT, sender_id INTEGER NOT NULL, recipient_id INTEGER NOT NULL, product_id INTEGER NOT NULL, message TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.commit()
    conn.close()

def row_to_product(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "name": r["name"], "url": r["url"], "category": r["category"], "last_updated": r["last_updated"]}

def row_to_price(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "product_id": r["product_id"], "price": r["price"], "created_at": r["created_at"]}

def create_product(name: str, url: str, category: Optional[str] = None) -> int:
    conn = get_conn()
    cur = conn.cursor()
    now = now_iso()
    cur.execute("INSERT INTO products(name, url, category, last_updated) VALUES(?, ?, ?, ?)", (name, url, category, now))
    conn.commit()
    pid = cur.lastrowid
    conn.close()
    return pid

def get_product(product_id: int) -> Optional[dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return row_to_product(r)

class ProductCreate(BaseModel):
    name: str
    url: str
    category: Optional[str] = None

class ListingRequest(BaseModel):
    url: str
    max_items: int = 50

class TaskCreate(BaseModel):
    product_id: Optional[int] = None

class UserCreate(BaseModel):
    username: str
    display_name: Optional[str] = None

class FollowCreate(BaseModel):
    product_id: int

class PushCreate(BaseModel):
    recipient_id: int
    product_id: int
    message: Optional[str] = None

class PushUpdate(BaseModel):
    status: str

app = FastAPI()
router = APIRouter(prefix="/api/v1")

@app.on_event("startup")
def on_startup():
    init_db()
    sys.path.append(os.path.join(BASE_DIR, "src"))

@router.get("/system/status")
def system_status():
    conn = get_conn()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    completed = cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'completed'").fetchone()[0]
    pending = cur.execute("SELECT COUNT(*) FROM tasks WHERE status = 'pending'").fetchone()[0]
    today = datetime.datetime.utcnow().date().isoformat()
    today_count = cur.execute("SELECT COUNT(*) FROM tasks WHERE substr(created_at,1,10) = ?", (today,)).fetchone()[0]
    conn.close()
    return ok({"health": "ok", "today_tasks": today_count, "total_tasks": total, "completed_tasks": completed, "pending_tasks": pending})

@router.get("/products")
def list_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)):
    conn = get_conn()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    offset = (page - 1) * size
    cur.execute("SELECT * FROM products ORDER BY id DESC LIMIT ? OFFSET ?", (size, offset))
    items = [row_to_product(r) for r in cur.fetchall()]
    conn.close()
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.get("/products/search")
def search_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100), search: Optional[str] = None, category: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    where = []
    params: List[Any] = []
    if search:
        where.append("name LIKE ?")
        params.append(f"%{search}%")
    if category:
        where.append("category = ?")
        params.append(category)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = ""
    if sort_by in {"name", "last_updated"}:
        order = "DESC" if sort_order == "desc" else "ASC"
        order_sql = f" ORDER BY {sort_by} {order}"
    total = conn.execute(f"SELECT COUNT(*) FROM products {where_sql}", params).fetchone()[0]
    offset = (page - 1) * size
    cur.execute(f"SELECT * FROM products {where_sql}{order_sql} LIMIT ? OFFSET ?", params + [size, offset])
    items = [row_to_product(r) for r in cur.fetchall()]
    conn.close()
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.post("/products")
def create_product_endpoint(body: ProductCreate):
    pid = create_product(body.name, body.url, body.category)
    p = get_product(pid)
    return ok(p)

@router.get("/products/{product_id}")
def product_detail(product_id: int):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    stats = cur.execute("SELECT COUNT(*) as cnt, MAX(price) as max_price, MIN(price) as min_price, AVG(price) as avg_price FROM prices WHERE product_id = ?", (product_id,)).fetchone()
    conn.close()
    p["stats"] = {"count": stats["cnt"], "max_price": stats["max_price"], "min_price": stats["min_price"], "avg_price": stats["avg_price"]}
    return ok(p)

@router.get("/products/{product_id}/prices")
def product_prices(product_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    where = ["product_id = ?"]
    params: List[Any] = [product_id]
    if start_date:
        where.append("substr(created_at,1,10) >= ?")
        params.append(start_date)
    if end_date:
        where.append("substr(created_at,1,10) <= ?")
        params.append(end_date)
    cur.execute(f"SELECT * FROM prices WHERE {' AND '.join(where)} ORDER BY created_at DESC", params)
    items = [row_to_price(r) for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.get("/products/{product_id}/export")
def export_product_prices(product_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    where = ["product_id = ?"]
    params: List[Any] = [product_id]
    if start_date:
        where.append("substr(created_at,1,10) >= ?")
        params.append(start_date)
    if end_date:
        where.append("substr(created_at,1,10) <= ?")
        params.append(end_date)
    cur.execute(f"SELECT id, price, created_at FROM prices WHERE {' AND '.join(where)} ORDER BY created_at DESC", params)
    rows = cur.fetchall()
    conn.close()
    import csv
    import io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["product_id", "product_name", "url", "category", "price_id", "price", "created_at"])
    for r in rows:
        writer.writerow([product_id, p["name"], p["url"], p["category"], r["id"], r["price"], r["created_at"]])
    csv_content = output.getvalue()
    filename = f"product_{product_id}_prices.csv"
    return Response(content=csv_content, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@router.post("/users")
def create_user(body: UserCreate):
    now = now_iso()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users(username, display_name, created_at) VALUES(?, ?, ?)", (body.username, body.display_name, now))
        conn.commit()
        uid = cur.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        return error_response(400, "VALIDATION_ERROR", "用户名已存在")
    cur.execute("SELECT id, username, display_name, created_at FROM users WHERE id = ?", (uid,))
    r = cur.fetchone()
    conn.close()
    return ok({"id": r[0], "username": r[1], "display_name": r[2], "created_at": r[3]})

@router.get("/users")
def list_users(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100), search: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    where = []
    params: List[Any] = []
    if search:
        where.append("username LIKE ?")
        params.append(f"%{search}%")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    total = conn.execute(f"SELECT COUNT(*) FROM users {where_sql}", params).fetchone()[0]
    offset = (page - 1) * size
    cur.execute(f"SELECT id, username, display_name, created_at FROM users {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?", params + [size, offset])
    items = [{"id": r[0], "username": r[1], "display_name": r[2], "created_at": r[3]} for r in cur.fetchall()]
    conn.close()
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.get("/users/{user_id}")
def user_detail(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username, display_name, created_at FROM users WHERE id = ?", (user_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return error_response(404, "NOT_FOUND", "资源不存在")
    cur.execute("SELECT COUNT(*) FROM user_follows WHERE user_id = ?", (user_id,))
    follows = cur.fetchone()[0]
    conn.close()
    return ok({"id": r[0], "username": r[1], "display_name": r[2], "created_at": r[3], "follows": follows})

@router.get("/products/{product_id}/followers")
def product_followers(product_id: int):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT u.id, u.username, u.display_name FROM user_follows f JOIN users u ON f.user_id = u.id WHERE f.product_id = ? ORDER BY f.id DESC", (product_id,))
    items = [{"id": r[0], "username": r[1], "display_name": r[2]} for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.get("/users/{user_id}/follows")
def list_user_follows(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT p.id, p.name, p.url, p.category, p.last_updated FROM user_follows f JOIN products p ON f.product_id = p.id WHERE f.user_id = ? ORDER BY f.id DESC", (user_id,))
    items = [row_to_product(r) for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.post("/users/{user_id}/follows")
def add_follow(user_id: int, body: FollowCreate):
    now = now_iso()
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO user_follows(user_id, product_id, created_at) VALUES(?, ?, ?)", (user_id, body.product_id, now))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return error_response(400, "VALIDATION_ERROR", "已关注")
    conn.close()
    return ok({"user_id": user_id, "product_id": body.product_id})

@router.delete("/users/{user_id}/follows/{product_id}")
def remove_follow(user_id: int, product_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM user_follows WHERE user_id = ? AND product_id = ?", (user_id, product_id))
    conn.commit()
    conn.close()
    return ok({"user_id": user_id, "product_id": product_id})

@router.post("/users/{sender_id}/pushes")
def create_push(sender_id: int, body: PushCreate):
    now = now_iso()
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO pushes(sender_id, recipient_id, product_id, message, status, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)", (sender_id, body.recipient_id, body.product_id, body.message, "pending", now, now))
    conn.commit()
    pid = cur.lastrowid
    cur.execute("SELECT * FROM pushes WHERE id = ?", (pid,))
    r = cur.fetchone()
    conn.close()
    return ok({"id": r["id"], "sender_id": r["sender_id"], "recipient_id": r["recipient_id"], "product_id": r["product_id"], "message": r["message"], "status": r["status"], "created_at": r["created_at"], "updated_at": r["updated_at"]})

@router.get("/users/{user_id}/pushes")
def list_pushes(user_id: int, box: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    if box == "outbox":
        cur.execute("SELECT * FROM pushes WHERE sender_id = ? ORDER BY id DESC", (user_id,))
    else:
        cur.execute("SELECT * FROM pushes WHERE recipient_id = ? ORDER BY id DESC", (user_id,))
    items = [{"id": r["id"], "sender_id": r["sender_id"], "recipient_id": r["recipient_id"], "product_id": r["product_id"], "message": r["message"], "status": r["status"], "created_at": r["created_at"], "updated_at": r["updated_at"]} for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.post("/pushes/{push_id}/status")
def update_push_status(push_id: int, body: PushUpdate):
    now = now_iso()
    if body.status not in {"accepted", "rejected"}:
        return error_response(400, "VALIDATION_ERROR", "状态无效")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE pushes SET status = ?, updated_at = ? WHERE id = ?", (body.status, now, push_id))
    conn.commit()
    cur.execute("SELECT * FROM pushes WHERE id = ?", (push_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return error_response(404, "NOT_FOUND", "资源不存在")
    conn.close()
    return ok({"id": r["id"], "sender_id": r["sender_id"], "recipient_id": r["recipient_id"], "product_id": r["product_id"], "message": r["message"], "status": r["status"], "created_at": r["created_at"], "updated_at": r["updated_at"]})

@router.get("/spider/tasks")
def list_tasks(status: Optional[str] = None, product_id: Optional[int] = None):
    conn = get_conn()
    cur = conn.cursor()
    where = []
    params: List[Any] = []
    if status:
        where.append("status = ?")
        params.append(status)
    if product_id:
        where.append("product_id = ?")
        params.append(product_id)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    cur.execute(f"SELECT * FROM tasks {where_sql} ORDER BY id DESC", params)
    items = [{"id": r["id"], "product_id": r["product_id"], "status": r["status"], "created_at": r["created_at"], "updated_at": r["updated_at"]} for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.post("/spider/tasks")
def create_task(body: TaskCreate):
    now = now_iso()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO tasks(product_id, status, created_at, updated_at) VALUES(?, ?, ?, ?)", (body.product_id, "pending", now, now))
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return ok({"id": tid, "product_id": body.product_id, "status": "pending", "created_at": now, "updated_at": now})

@router.post("/spider/tasks/{task_id}/execute")
def execute_task(task_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    t = cur.fetchone()
    if not t:
        conn.close()
        return error_response(404, "NOT_FOUND", "资源不存在")
    pid = t["product_id"]
    if pid is None:
        conn.close()
        return error_response(400, "VALIDATION_ERROR", "任务缺少产品ID")
    p = get_product(pid)
    if not p:
        conn.close()
        return error_response(404, "NOT_FOUND", "资源不存在")
    now = now_iso()
    cur.execute("UPDATE tasks SET status = 'completed', updated_at = ? WHERE id = ?", (now, task_id))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 99.0, now))
    conn.commit()
    cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    t2 = cur.fetchone()
    conn.close()
    return ok({"id": t2["id"], "product_id": t2["product_id"], "status": t2["status"], "created_at": t2["created_at"], "updated_at": t2["updated_at"]})

@router.post("/spider/listing")
def listing(body: ListingRequest):
    try:
        src_path = os.path.join(BASE_DIR, "src")
        if src_path not in sys.path:
            sys.path.append(src_path)
        from utils.url_util import is_valid_url, get_base_url
    except Exception:
        return error_response(500, "INTERNAL_ERROR", "依赖导入失败")
    if not is_valid_url(body.url):
        return error_response(400, "VALIDATION_ERROR", "URL无效")
    base = get_base_url(body.url)
    if base == "://":
        return error_response(400, "VALIDATION_ERROR", "URL无效")
    limit = max(1, min(body.max_items, 50))
    items = [{"title": f"Item {i+1}", "url": body.url, "source": base} for i in range(limit)]
    return ok({"count": len(items), "items": items})

app.include_router(router)

def main():
    print("Hello from spider!")

if __name__ == "__main__":
    main()
