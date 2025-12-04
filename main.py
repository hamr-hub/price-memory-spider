import os
import math
import datetime
import random
from typing import Any, List, Optional, Dict
from fastapi import FastAPI, APIRouter, Query, Header
from fastapi.responses import Response
import secrets
from pydantic import BaseModel
import sys

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "spider.db")
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))
sys.path.append(os.path.join(BASE_DIR, "src"))
from src.dao.supabase_client import get_client
SB = get_client()

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ok(data: Any, message: str = "操作成功"):
    return {"success": True, "data": data, "message": message, "timestamp": now_iso()}

def error_response(status_code: int, code: str, message: str, details: Optional[List[Any]] = None):
    return {"success": False, "error": {"code": code, "message": message, "details": details or []}, "timestamp": now_iso()}


def get_user_by_api_key(api_key: Optional[str]) -> Optional[dict]:
    if not api_key:
        return None
    res = SB.table("users").select("*").eq("api_key", api_key).limit(1).execute()
    data = getattr(res, "data", None) or []
    return data[0] if data else None

def reset_user_quota_if_needed(user_id: int):
    today = datetime.datetime.utcnow().date().isoformat()
    res = SB.table("users").select("exports_used_today,last_quota_reset").eq("id", user_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    if not data:
        return
    last = data[0].get("last_quota_reset")
    if last != today:
        SB.table("users").update({"exports_used_today": 0, "last_quota_reset": today}).eq("id", user_id).execute()

def row_to_product(r: dict) -> dict:
    return {"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")}

def row_to_price(r: dict) -> dict:
    return {"id": r.get("id"), "product_id": r.get("product_id"), "price": r.get("price"), "created_at": r.get("created_at")}

def create_product(name: str, url: str, category: Optional[str] = None) -> int:
    now = now_iso()
    res = SB.table("products").insert({"name": name, "url": url, "category": category, "updated_at": now}).select("id").execute()
    data = getattr(res, "data", None) or []
    return int(data[0]["id"]) if data else 0

def get_product(product_id: int) -> Optional[dict]:
    res = SB.table("products").select("*").eq("id", product_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    if not data:
        return None
    r = data[0]
    return {"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")}

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
    email: Optional[str] = None

class FollowCreate(BaseModel):
    product_id: int

class PushCreate(BaseModel):
    recipient_id: int
    product_id: int
    message: Optional[str] = None

class PushUpdate(BaseModel):
    status: str

class PoolAddProduct(BaseModel):
    product_id: int

class SelectFromPoolBody(BaseModel):
    product_id: int

class CollectionCreate(BaseModel):
    name: str
    owner_user_id: int

class CollectionAddProduct(BaseModel):
    product_id: int

class CollectionShare(BaseModel):
    user_id: int
    role: Optional[str] = "editor"

class AlertCreate(BaseModel):
    user_id: int
    product_id: int
    rule_type: str
    threshold: Optional[float] = None
    percent: Optional[float] = None
    channel: Optional[str] = None
    cooldown_minutes: Optional[int] = None

class AlertStatusUpdate(BaseModel):
    status: str

app = FastAPI()
router = APIRouter(prefix="/api/v1")
RATE_LIMIT: Dict[str, float] = {}

@app.on_event("startup")
def on_startup():
    sys.path.append(os.path.join(BASE_DIR, "src"))
    try:
        from runtime.node_runtime import NodeRuntime
        rt = NodeRuntime()
        rt.start()
    except Exception:
        pass

@router.get("/system/status")
def system_status():
    total = getattr(SB.table("tasks").select("id", count="exact").execute(), "count", 0) or 0
    completed = getattr(SB.table("tasks").select("id", count="exact").eq("status", "completed").execute(), "count", 0) or 0
    pending = getattr(SB.table("tasks").select("id", count="exact").eq("status", "pending").execute(), "count", 0) or 0
    today = datetime.datetime.utcnow().date().isoformat()
    today_count = getattr(SB.table("tasks").select("id", count="exact").gte("created_at", today).execute(), "count", 0) or 0
    return ok({"health": "ok", "today_tasks": today_count, "total_tasks": total, "completed_tasks": completed, "pending_tasks": pending})

@router.get("/products")
def list_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)):
    offset = (page - 1) * size
    sel = SB.table("products").select("*", count="exact").order("id", desc=True).range(offset, offset + size - 1).execute()
    items = [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")} for r in (getattr(sel, "data", None) or [])]
    total = getattr(sel, "count", 0) or len(items)
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.get("/products/search")
def search_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100), search: Optional[str] = None, category: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = None):
    q = SB.table("products").select("*", count="exact")
    if search:
        q = q.ilike("name", f"%{search}%")
    if category:
        q = q.eq("category", category)
    if sort_by in {"name", "updated_at"}:
        q = q.order(sort_by, desc=(sort_order == "desc"))
    offset = (page - 1) * size
    res = q.range(offset, offset + size - 1).execute()
    items = [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")} for r in (getattr(res, "data", None) or [])]
    total = getattr(res, "count", 0) or len(items)
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
    stats = SB.rpc("rpc_product_stats", {"product_id": product_id}).execute()
    rows = getattr(stats, "data", None) or []
    if rows:
        r = rows[0]
        p["stats"] = {"count": r.get("count"), "max_price": r.get("max_price"), "min_price": r.get("min_price"), "avg_price": r.get("avg_price")}
    else:
        p["stats"] = {"count": 0, "max_price": None, "min_price": None, "avg_price": None}
    return ok(p)

@router.get("/products/{product_id}/prices")
def product_prices(product_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    q = SB.table("prices").select("*").eq("product_id", product_id).order("created_at", desc=True)
    if start_date:
        q = q.gte("created_at", start_date)
    if end_date:
        q = q.lte("created_at", end_date + " 23:59:59")
    res = q.execute()
    items = [{"id": r.get("id"), "product_id": r.get("product_id"), "price": r.get("price"), "created_at": r.get("created_at")} for r in (getattr(res, "data", None) or [])]
    return ok(items)

@router.get("/products/{product_id}/trend")
def product_trend(product_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None, granularity: Optional[str] = "daily"):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    if granularity not in {"daily", "hourly"}:
        return error_response(400, "VALIDATION_ERROR", "不支持的粒度")
    if granularity == "daily":
        sd = start_date or datetime.datetime.utcnow().date().isoformat()
        ed = end_date or sd
        res = SB.rpc("rpc_product_daily_ohlc", {"product_id": product_id, "start_date": sd, "end_date": ed}).execute()
        rows = getattr(res, "data", None) or []
        series = [{"date": r.get("day"), "open": r.get("open"), "close": r.get("close"), "low": r.get("low"), "high": r.get("high"), "avg": r.get("avg"), "count": r.get("count")} for r in rows]
        return ok({"granularity": granularity, "series": series})
    else:
        sd = start_date or datetime.datetime.utcnow().date().isoformat()
        ed = end_date or sd
        res = SB.rpc("rpc_product_hourly_ohlc", {"product_id": product_id, "start_ts": sd + " 00:00:00", "end_ts": ed + " 23:59:59"}).execute()
        rows = getattr(res, "data", None) or []
        series = [{"date": r.get("hour"), "open": r.get("open"), "close": r.get("close"), "low": r.get("low"), "high": r.get("high"), "avg": r.get("avg"), "count": r.get("count")} for r in rows]
        return ok({"granularity": granularity, "series": series})

@router.get("/products/{product_id}/export")
def export_product_prices(product_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None, api_key: Optional[str] = None):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    user = get_user_by_api_key(api_key)
    if user:
        reset_user_quota_if_needed(int(user["id"]))
        quota = user.get("quota_exports_per_day") or 0
        used = user.get("exports_used_today") or 0
        if quota and used >= quota:
            return error_response(429, "QUOTA_EXCEEDED", "导出额度已用尽")
    q = SB.table("prices").select("id,price,created_at").eq("product_id", product_id).order("created_at", desc=True)
    if start_date:
        q = q.gte("created_at", start_date)
    if end_date:
        q = q.lte("created_at", end_date + " 23:59:59")
    rows = getattr(q.execute(), "data", None) or []
    import csv
    import io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["product_id", "product_name", "url", "category", "price_id", "price", "created_at"])
    for r in rows:
        writer.writerow([product_id, p["name"], p["url"], p["category"], r.get("id"), r.get("price"), r.get("created_at")])
    csv_content = output.getvalue()
    filename = f"product_{product_id}_prices.csv"
    if user:
        SB.table("users").update({"exports_used_today": (used or 0) + 1, "last_quota_reset": datetime.datetime.utcnow().date().isoformat()}).eq("id", int(user["id"])).execute()
    return Response(content=csv_content, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@router.post("/users")
def create_user(body: UserCreate):
    now = now_iso()
    api_key = secrets.token_hex(16)
    plan = "basic"
    quota_exports = 5
    today = datetime.datetime.utcnow().date().isoformat()
    res = SB.table("users").insert({"username": body.username, "display_name": body.display_name, "created_at": now, "email": body.email, "api_key": api_key, "plan": plan, "quota_exports_per_day": quota_exports, "exports_used_today": 0, "last_quota_reset": today}).select("id,username,display_name,created_at,email,api_key,plan,quota_exports_per_day").execute()
    data = getattr(res, "data", None) or []
    if not data:
        return error_response(400, "VALIDATION_ERROR", "用户名已存在")
    r = data[0]
    return ok({"id": r.get("id"), "username": r.get("username"), "display_name": r.get("display_name"), "created_at": r.get("created_at"), "email": r.get("email"), "api_key": r.get("api_key"), "plan": r.get("plan"), "quota_exports_per_day": r.get("quota_exports_per_day")})

@router.get("/users")
def list_users(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100), search: Optional[str] = None):
    q = SB.table("users").select("id,username,display_name,created_at,email", count="exact")
    if search:
        q = q.or_(f"username.ilike.%{search}%,display_name.ilike.%{search}%,email.ilike.%{search}%")
    offset = (page - 1) * size
    res = q.order("id", desc=True).range(offset, offset + size - 1).execute()
    items = getattr(res, "data", None) or []
    total = getattr(res, "count", 0) or len(items)
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.get("/users/{user_id}")
def user_detail(user_id: int):
    ures = SB.table("users").select("id,username,display_name,created_at").eq("id", user_id).limit(1).execute()
    u = (getattr(ures, "data", None) or [])
    if not u:
        return error_response(404, "NOT_FOUND", "资源不存在")
    fres = SB.table("user_follows").select("id", count="exact").eq("user_id", user_id).execute()
    follows = getattr(fres, "count", 0) or 0
    r = u[0]
    return ok({"id": r.get("id"), "username": r.get("username"), "display_name": r.get("display_name"), "created_at": r.get("created_at"), "follows": follows})

@router.get("/products/{product_id}/followers")
def product_followers(product_id: int):
    p = get_product(product_id)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    links = SB.table("user_follows").select("user_id").eq("product_id", product_id).order("id", desc=True).execute()
    ids = [x.get("user_id") for x in (getattr(links, "data", None) or [])]
    if not ids:
        return ok([])
    users = SB.table("users").select("id,username,display_name").in_("id", ids).execute()
    items = getattr(users, "data", None) or []
    return ok(items)

@router.get("/pools/public/products")
def list_public_pool_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100), search: Optional[str] = None, category: Optional[str] = None):
    pres = SB.table("pools").select("id").eq("is_public", True).limit(1).execute()
    pitems = getattr(pres, "data", None) or []
    if not pitems:
        return ok({"items": [], "page": page, "size": size, "total": 0, "pages": 0})
    pool_id = pitems[0]["id"]
    offset = (page - 1) * size
    links_q = SB.table("pool_products").select("product_id", count="exact").eq("pool_id", pool_id).order("id", desc=True)
    links = links_q.range(offset, offset + size - 1).execute()
    ids = [x.get("product_id") for x in (getattr(links, "data", None) or [])]
    total = getattr(links, "count", 0) or 0
    if not ids:
        return ok({"items": [], "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})
    pq = SB.table("products").select("*").in_("id", ids)
    if search:
        pq = pq.ilike("name", f"%{search}%")
    if category:
        pq = pq.eq("category", category)
    products = getattr(pq.execute(), "data", None) or []
    items = [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")} for r in products]
    return ok({"items": items, "page": page, "size": size, "total": total, "pages": math.ceil(total / size) if size else 0})

@router.post("/pools/public/products")
def add_product_to_public_pool(body: PoolAddProduct):
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    pres = SB.table("pools").select("id").eq("is_public", True).limit(1).execute()
    pitems = getattr(pres, "data", None) or []
    if not pitems:
        cres = SB.table("pools").insert({"name": "public", "is_public": True}).select("id").execute()
        pool_id = (getattr(cres, "data", None) or [{}])[0].get("id")
    else:
        pool_id = pitems[0]["id"]
    try:
        SB.table("pool_products").insert({"pool_id": pool_id, "product_id": body.product_id}).execute()
    except Exception:
        return error_response(400, "VALIDATION_ERROR", "已在公共池")
    return ok({"pool": "public", "product_id": body.product_id})

@router.post("/users/{user_id}/select_from_pool")
def user_select_from_pool(user_id: int, body: SelectFromPoolBody):
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    now = now_iso()
    try:
        SB.table("user_follows").insert({"user_id": user_id, "product_id": body.product_id, "created_at": now}).execute()
    except Exception:
        return error_response(400, "VALIDATION_ERROR", "已选择/关注")
    return ok({"user_id": user_id, "product_id": body.product_id})

@router.post("/collections")
def create_collection(body: CollectionCreate):
    now = now_iso()
    ures = SB.table("users").select("id").eq("id", body.owner_user_id).limit(1).execute()
    owner = (getattr(ures, "data", None) or [])
    if not owner:
        return error_response(404, "NOT_FOUND", "用户不存在")
    cres = SB.table("collections").insert({"name": body.name, "owner_user_id": body.owner_user_id, "created_at": now}).select("id").execute()
    cid = (getattr(cres, "data", None) or [{}])[0].get("id")
    SB.table("collection_members").insert({"collection_id": cid, "user_id": body.owner_user_id, "role": "admin"}).execute()
    return ok({"id": cid, "name": body.name, "owner_user_id": body.owner_user_id, "created_at": now})

@router.get("/users/{user_id}/collections")
def list_user_collections(user_id: int):
    links = SB.table("collection_members").select("collection_id").eq("user_id", user_id).execute()
    ids = [x.get("collection_id") for x in (getattr(links, "data", None) or [])]
    if not ids:
        return ok([])
    cols = SB.table("collections").select("id,name,created_at").in_("id", ids).order("id", desc=True).execute()
    items = getattr(cols, "data", None) or []
    return ok(items)

@router.get("/collections/{collection_id}")
def collection_detail(collection_id: int):
    cres = SB.table("collections").select("id,name,owner_user_id,created_at").eq("id", collection_id).limit(1).execute()
    citems = getattr(cres, "data", None) or []
    if not citems:
        return error_response(404, "NOT_FOUND", "资源不存在")
    c = citems[0]
    links = SB.table("collection_products").select("product_id").eq("collection_id", collection_id).order("id", desc=True).execute()
    pids = [x.get("product_id") for x in (getattr(links, "data", None) or [])]
    products = []
    if pids:
        pres = SB.table("products").select("*").in_("id", pids).execute()
        products = [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "last_updated": r.get("updated_at")} for r in (getattr(pres, "data", None) or [])]
    ms = SB.table("collection_members").select("user_id,role").eq("collection_id", collection_id).execute()
    uids = [x.get("user_id") for x in (getattr(ms, "data", None) or [])]
    members = []
    if uids:
        ures = SB.table("users").select("id,username,display_name").in_("id", uids).execute()
        udata = getattr(ures, "data", None) or []
        roles = {x.get("user_id"): x.get("role") for x in (getattr(ms, "data", None) or [])}
        members = [{"id": u.get("id"), "username": u.get("username"), "display_name": u.get("display_name"), "role": roles.get(u.get("id"))} for u in udata]
    return ok({"id": c.get("id"), "name": c.get("name"), "owner_user_id": c.get("owner_user_id"), "created_at": c.get("created_at"), "products": products, "members": members})

@router.post("/collections/{collection_id}/products")
def add_collection_product(collection_id: int, body: CollectionAddProduct):
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    cres = SB.table("collections").select("id").eq("id", collection_id).limit(1).execute()
    if not (getattr(cres, "data", None) or []):
        return error_response(404, "NOT_FOUND", "资源不存在")
    try:
        SB.table("collection_products").insert({"collection_id": collection_id, "product_id": body.product_id}).execute()
    except Exception:
        return error_response(400, "VALIDATION_ERROR", "已在集合")
    return ok({"collection_id": collection_id, "product_id": body.product_id})

@router.delete("/collections/{collection_id}/products/{product_id}")
def remove_collection_product(collection_id: int, product_id: int):
    SB.table("collection_products").delete().eq("collection_id", collection_id).eq("product_id", product_id).execute()
    return ok({"collection_id": collection_id, "product_id": product_id})

@router.post("/collections/{collection_id}/share")
def share_collection(collection_id: int, body: CollectionShare):
    cres = SB.table("collections").select("id").eq("id", collection_id).limit(1).execute()
    if not (getattr(cres, "data", None) or []):
        return error_response(404, "NOT_FOUND", "资源不存在")
    ures = SB.table("users").select("id").eq("id", body.user_id).limit(1).execute()
    if not (getattr(ures, "data", None) or []):
        return error_response(404, "NOT_FOUND", "用户不存在")
    role = body.role if body.role in {"admin", "editor", "viewer"} else "editor"
    try:
        SB.table("collection_members").insert({"collection_id": collection_id, "user_id": body.user_id, "role": role}).execute()
    except Exception:
        return error_response(400, "VALIDATION_ERROR", "成员已存在")
    return ok({"collection_id": collection_id, "user_id": body.user_id, "role": role})

@router.get("/collections/{collection_id}/export.xlsx")
def export_collection_xlsx(collection_id: int, api_key: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    cres = SB.table("collections").select("id,name").eq("id", collection_id).limit(1).execute()
    citems = getattr(cres, "data", None) or []
    if not citems:
        return error_response(404, "NOT_FOUND", "资源不存在")
    user = get_user_by_api_key(api_key)
    if user:
        reset_user_quota_if_needed(int(user["id"]))
        quota = user.get("quota_exports_per_day") or 0
        used = user.get("exports_used_today") or 0
        if quota and used >= quota:
            return error_response(429, "QUOTA_EXCEEDED", "导出额度已用尽")
    links = SB.table("collection_products").select("product_id").eq("collection_id", collection_id).execute()
    pids = [x.get("product_id") for x in (getattr(links, "data", None) or [])]
    pres = SB.table("products").select("id,name,url,category").in_("id", pids).execute()
    products = getattr(pres, "data", None) or []
    try:
        from openpyxl import Workbook
    except Exception:
        conn.close()
        return error_response(501, "DEPENDENCY_MISSING", "缺少openpyxl依赖，无法导出Excel")
    wb = Workbook()
    if wb.active:
        wb.remove(wb.active)
    for p in products:
        ws = wb.create_sheet(title=str(p.get("name"))[:31] or f"P{p.get('id')}")
        ws.append(["product_id", "product_name", "url", "category", "price_id", "price", "created_at"])
        q = SB.table("prices").select("id,price,created_at").eq("product_id", p.get("id")).order("created_at", desc=True)
        if start_date:
            q = q.gte("created_at", start_date)
        if end_date:
            q = q.lte("created_at", end_date + " 23:59:59")
        rows = getattr(q.execute(), "data", None) or []
        for r in rows:
            ws.append([p.get("id"), p.get("name"), p.get("url"), p.get("category"), r.get("id"), r.get("price"), r.get("created_at")])
    import io
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    filename = f"collection_{collection_id}.xlsx"
    if user:
        SB.table("users").update({"exports_used_today": (used or 0) + 1, "last_quota_reset": datetime.datetime.utcnow().date().isoformat()}).eq("id", int(user["id"])).execute()
    return Response(content=bio.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@router.get("/alerts")
def list_alerts(user_id: Optional[int] = None, product_id: Optional[int] = None):
    q = SB.table("alerts").select("*").order("id", desc=True)
    if user_id is not None:
        q = q.eq("user_id", user_id)
    if product_id is not None:
        q = q.eq("product_id", product_id)
    res = q.execute()
    items = getattr(res, "data", None) or []
    return ok(items)

@router.post("/alerts")
def create_alert(body: AlertCreate):
    now = now_iso()
    if body.rule_type not in {"price_below", "price_above", "percent_drop", "percent_rise"}:
        return error_response(400, "VALIDATION_ERROR", "规则类型无效")
    if not get_product(body.product_id):
        return error_response(404, "NOT_FOUND", "资源不存在")
    res = SB.table("alerts").insert({"user_id": body.user_id, "product_id": body.product_id, "rule_type": body.rule_type, "threshold": body.threshold, "percent": body.percent, "status": "active", "created_at": now, "updated_at": now}).select("*").execute()
    data = getattr(res, "data", None) or []
    return ok(data[0] if data else {})

@router.post("/alerts/{alert_id}/status")
def update_alert_status(alert_id: int, body: AlertStatusUpdate):
    if body.status not in {"active", "paused"}:
        return error_response(400, "VALIDATION_ERROR", "状态无效")
    now = now_iso()
    SB.table("alerts").update({"status": body.status, "updated_at": now}).eq("id", alert_id).execute()
    res = SB.table("alerts").select("*").eq("id", alert_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    if not data:
        return error_response(404, "NOT_FOUND", "资源不存在")
    return ok(data[0])

@router.delete("/alerts/{alert_id}")
def delete_alert(alert_id: int):
    SB.table("alerts").delete().eq("id", alert_id).execute()
    return ok({"id": alert_id})

@router.get("/alerts/{alert_id}/events")
def list_alert_events(alert_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM alert_events WHERE alert_id = ? ORDER BY id DESC", (alert_id,))
    items = [{"id": r["id"], "product_id": r["product_id"], "user_id": r["user_id"], "price": r["price"], "created_at": r["created_at"], "message": r["message"], "channel": r["channel"], "push_id": r["push_id"]} for r in cur.fetchall()]
    conn.close()
    return ok(items)

@router.post("/alerts/{alert_id}/update")
def update_alert(alert_id: int, threshold: Optional[float] = None, channel: Optional[str] = None, cooldown_minutes: Optional[int] = None):
    now = now_iso()
    conn = get_conn()
    cur = conn.cursor()
    fields = []
    params: List[Any] = []
    if threshold is not None:
        fields.append("threshold = ?")
        params.append(threshold)
    if channel is not None:
        fields.append("channel = ?")
        params.append(channel)
    if cooldown_minutes is not None:
        fields.append("cooldown_minutes = ?")
        params.append(cooldown_minutes)
    if not fields:
        conn.close()
        return ok({"id": alert_id})
    fields.append("updated_at = ?")
    params.append(now)
    params.append(alert_id)
    cur.execute(f"UPDATE alerts SET {', '.join(fields)} WHERE id = ?", params)
    conn.commit()
    conn.close()
    return ok({"id": alert_id})

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
    res = SB.table("pushes").insert({"sender_id": sender_id, "recipient_id": body.recipient_id, "product_id": body.product_id, "message": body.message, "status": "pending", "created_at": now, "updated_at": now}).select("*").execute()
    data = getattr(res, "data", None) or []
    return ok(data[0] if data else {})

@router.get("/users/{user_id}/pushes")
def list_pushes(user_id: int, box: Optional[str] = None):
    q = SB.table("pushes").select("*").order("id", desc=True)
    if box == "outbox":
        q = q.eq("sender_id", user_id)
    else:
        q = q.eq("recipient_id", user_id)
    res = q.execute()
    items = getattr(res, "data", None) or []
    return ok(items)

@router.post("/pushes/{push_id}/status")
def update_push_status(push_id: int, body: PushUpdate):
    now = now_iso()
    if body.status not in {"accepted", "rejected"}:
        return error_response(400, "VALIDATION_ERROR", "状态无效")
    SB.table("pushes").update({"status": body.status, "updated_at": now}).eq("id", push_id).execute()
    res = SB.table("pushes").select("*").eq("id", push_id).limit(1).execute()
    data = getattr(res, "data", None) or []
    if not data:
        return error_response(404, "NOT_FOUND", "资源不存在")
    return ok(data[0])

@router.get("/spider/tasks")
def list_tasks(status: Optional[str] = None, product_id: Optional[int] = None):
    q = SB.table("tasks").select("*").order("id", desc=True)
    if status:
        q = q.eq("status", status)
    if product_id:
        q = q.eq("product_id", product_id)
    res = q.execute()
    items = getattr(res, "data", None) or []
    return ok(items)

@router.post("/spider/tasks")
def create_task(body: TaskCreate):
    now = now_iso()
    res = SB.table("tasks").insert({"product_id": body.product_id, "status": "pending", "created_at": now, "updated_at": now, "scheduled_at": now}).select("id").execute()
    data = getattr(res, "data", None) or []
    tid = (data[0] or {}).get("id") if data else None
    return ok({"id": tid, "product_id": body.product_id, "status": "pending", "created_at": now, "updated_at": now})

@router.post("/spider/tasks/{task_id}/execute")
def execute_task(task_id: int):
    tres = SB.table("tasks").select("*").eq("id", task_id).limit(1).execute()
    titems = getattr(tres, "data", None) or []
    if not titems:
        return error_response(404, "NOT_FOUND", "资源不存在")
    t = titems[0]
    pid = t.get("product_id")
    if pid is None:
        return error_response(400, "VALIDATION_ERROR", "任务缺少产品ID")
    p = get_product(pid)
    if not p:
        return error_response(404, "NOT_FOUND", "资源不存在")
    now = now_iso()
    SB.table("tasks").update({"status": "running", "updated_at": now, "started_at": now}).eq("id", task_id).execute()
    rlast = SB.table("prices").select("price,created_at").eq("product_id", pid).order("created_at", desc=True).limit(1).execute()
    last_rows = getattr(rlast, "data", None) or []
    last_price = float(last_rows[0]["price"]) if last_rows else None
    fetched = try_fetch_price(p["url"]) if p and p.get("url") else None
    base = fetched if (isinstance(fetched, float) and fetched > 0) else (last_price if last_price is not None else random.uniform(50.0, 200.0))
    price = round(base * (1 + (0 if fetched else random.uniform(-0.03, 0.03))), 2)
    prev = SB.table("prices").select("price,created_at").eq("product_id", pid).order("created_at", desc=True).limit(1).execute()
    prev_rows = getattr(prev, "data", None) or []
    prev_price = float(prev_rows[0]["price"]) if prev_rows else None
    prev_minute = str(prev_rows[0]["created_at"])[:16] if prev_rows else None
    curr_minute = now[:16]
    if not prev_rows or not (prev_price == price and prev_minute == curr_minute):
        SB.table("prices").insert({"product_id": pid, "price": price, "created_at": now}).execute()
        evaluate_alerts_for_product(pid, price, now)
    SB.table("products").update({"updated_at": now}).eq("id", pid).execute()
    SB.table("tasks").update({"status": "completed", "updated_at": now, "completed_at": now}).eq("id", task_id).execute()
    t2 = SB.table("tasks").select("*").eq("id", task_id).limit(1).execute()
    rows = getattr(t2, "data", None) or []
    r = rows[0] if rows else {}
    return ok({"id": r.get("id"), "product_id": r.get("product_id"), "status": r.get("status"), "created_at": r.get("created_at"), "updated_at": r.get("updated_at")})

def try_fetch_price(url: str) -> Optional[float]:
    try:
        import urllib.request
        resp = urllib.request.urlopen(url, timeout=5)
        html = resp.read().decode("utf-8", errors="ignore")
        import re
        # simple patterns for currency prices
        patterns = [r"\$\s*(\d+(?:\.\d+)?)", r"¥\s*(\d+(?:\.\d+)?)", r"CNY\s*(\d+(?:\.\d+)?)"]
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                val = float(m.group(1))
                return val
        return None
    except Exception:
        return None

def evaluate_alerts_for_product(product_id: int, price: float, now: str):
    res = SB.table("alerts").select("id,user_id,rule_type,threshold,percent").eq("product_id", product_id).eq("status", "active").execute()
    for a in getattr(res, "data", None) or []:
        rt = a.get("rule_type")
        th = a.get("threshold")
        uid = a.get("user_id")
        trig = False
        if rt == "price_below" and th is not None and price <= float(th):
            trig = True
        if rt == "price_above" and th is not None and price >= float(th):
            trig = True
        if trig:
            SB.table("pushes").insert({"sender_id": 0, "recipient_id": uid, "product_id": product_id, "message": f"价格触发: {price}", "status": "pending", "created_at": now, "updated_at": now}).execute()

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

@router.get("/export")
def export_products(product_ids: str, api_key: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    ids = [int(x) for x in product_ids.split(",") if x.strip().isdigit()]
    if not ids:
        return error_response(400, "VALIDATION_ERROR", "缺少有效的product_ids")
    user = get_user_by_api_key(api_key)
    if user:
        reset_user_quota_if_needed(int(user["id"]))
        quota = user.get("quota_exports_per_day") or 0
        used = user.get("exports_used_today") or 0
        if quota and used >= quota:
            return error_response(429, "QUOTA_EXCEEDED", "导出额度已用尽")
    import csv
    import io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["product_id", "product_name", "url", "category", "price_id", "price", "created_at"])
    for pid in ids:
        p = get_product(pid)
        if not p:
            continue
        q = SB.table("prices").select("id,price,created_at").eq("product_id", pid).order("created_at", desc=True)
        if start_date:
            q = q.gte("created_at", start_date)
        if end_date:
            q = q.lte("created_at", end_date + " 23:59:59")
        rows = getattr(q.execute(), "data", None) or []
        for r in rows:
            writer.writerow([pid, p["name"], p["url"], p["category"], r.get("id"), r.get("price"), r.get("created_at")])
    csv_content = output.getvalue()
    filename = "products_export.csv"
    if user:
        SB.table("users").update({"exports_used_today": (used or 0) + 1, "last_quota_reset": datetime.datetime.utcnow().date().isoformat()}).eq("id", int(user["id"])).execute()
    return Response(content=csv_content, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

app.include_router(router)

def main():
    print("Hello from spider!")

if __name__ == "__main__":
    main()
