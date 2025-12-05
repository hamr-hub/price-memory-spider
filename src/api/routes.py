"""
API路由模块
包含所有API端点的定义
"""
import os
import math
import datetime
import random
import secrets
from typing import Any, List, Optional, Dict
from fastapi import APIRouter, Query, Header, Body, HTTPException
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

from ..config.config import config
from ..dao.supabase_client import get_client
from ..dao.supabase_repo import SupabaseRepo

# 创建路由器
router = APIRouter()

# 初始化数据库客户端
try:
    supabase_client = get_client()
    repo = SupabaseRepo(supabase_client) if supabase_client else None
except Exception:
    supabase_client = None
    repo = None

# Pydantic模型定义
class ProductCreate(BaseModel):
    name: str
    url: str
    category: Optional[str] = None

class TaskCreate(BaseModel):
    product_id: Optional[int] = None
    priority: Optional[int] = 0

class UserCreate(BaseModel):
    username: str
    display_name: Optional[str] = None
    email: Optional[str] = None

class FollowCreate(BaseModel):
    product_id: int

class AlertCreate(BaseModel):
    user_id: int
    product_id: int
    rule_type: str
    threshold: Optional[float] = None
    percent: Optional[float] = None
    channel: Optional[str] = None
    cooldown_minutes: Optional[int] = None
    target: Optional[str] = None

# 工具函数
def now_iso() -> str:
    """获取当前时间的ISO格式字符串"""
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ok(data: Any, message: str = "操作成功"):
    """成功响应格式"""
    return {"success": True, "data": data, "message": message, "timestamp": now_iso()}

def error_response(status_code: int, code: str, message: str, details: Optional[List[Any]] = None):
    """错误响应格式"""
    if config.STRICT_HTTP:
        raise HTTPException(
            status_code=status_code,
            detail={"code": code, "message": message, "details": details or []}
        )
    return {"success": False, "error": {"code": code, "message": message, "details": details or []}, "timestamp": now_iso()}

def get_user_by_api_key(api_key: Optional[str]) -> Optional[dict]:
    """根据API Key获取用户信息"""
    if not api_key or not isinstance(api_key, str) or not repo:
        return None
    
    try:
        res = supabase_client.table("users").select("*").eq("api_key", api_key).limit(1).execute()
        data = getattr(res, "data", None) or []
        return data[0] if data else None
    except Exception:
        return None

# 系统状态端点
@router.get("/system/status")
def system_status():
    """获取系统状态"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        # 获取任务统计
        total_res = supabase_client.table("tasks").select("id", count="exact").execute()
        total = getattr(total_res, "count", 0) or 0
        
        completed_res = supabase_client.table("tasks").select("id", count="exact").eq("status", "completed").execute()
        completed = getattr(completed_res, "count", 0) or 0
        
        pending_res = supabase_client.table("tasks").select("id", count="exact").eq("status", "pending").execute()
        pending = getattr(pending_res, "count", 0) or 0
        
        # 今日任务统计
        today = datetime.datetime.utcnow().date().isoformat()
        today_res = supabase_client.table("tasks").select("id", count="exact").gte("created_at", today).execute()
        today_count = getattr(today_res, "count", 0) or 0
        
        return ok({
            "health": "ok",
            "today_tasks": today_count,
            "total_tasks": total,
            "completed_tasks": completed,
            "pending_tasks": pending
        })
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

# 权限检查端点
@router.get("/auth/permissions")
def auth_permissions(api_key: Optional[str] = Header(None)):
    """获取用户权限列表"""
    perms: List[Dict[str, str]] = []
    perms.append({"resource": "products", "action": "export"})
    perms.append({"resource": "collections", "action": "share"})
    perms.append({"resource": "collections", "action": "export"})
    perms.append({"resource": "public-pool", "action": "select"})
    perms.append({"resource": "pushes", "action": "update"})
    return ok(perms)

# 商品管理端点
@router.get("/products")
def list_products(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)):
    """获取商品列表"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        offset = (page - 1) * size
        res = supabase_client.table("products").select("*", count="exact").order("id", desc=True).range(offset, offset + size - 1).execute()
        
        items = []
        for r in (getattr(res, "data", None) or []):
            items.append({
                "id": r.get("id"),
                "name": r.get("name"),
                "url": r.get("url"),
                "category": r.get("category"),
                "last_updated": r.get("updated_at")
            })
        
        total = getattr(res, "count", 0) or len(items)
        return ok({
            "items": items,
            "page": page,
            "size": size,
            "total": total,
            "pages": math.ceil(total / size) if size else 0
        })
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

@router.post("/products")
def create_product_endpoint(body: ProductCreate):
    """创建新商品"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        result = repo.upsert_product(
            name=body.name,
            url=body.url,
            category=body.category
        )
        return ok({
            "id": result.get("id"),
            "name": result.get("name"),
            "url": result.get("url"),
            "category": result.get("category")
        })
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

@router.get("/products/{product_id}")
def product_detail(product_id: int):
    """获取商品详情"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        product = repo.get_product(product_id)
        if not product:
            return error_response(404, "NOT_FOUND", "商品不存在")
        
        # 获取价格统计
        stats_res = supabase_client.rpc("rpc_product_stats", {"product_id": product_id}).execute()
        stats_data = getattr(stats_res, "data", None) or []
        
        if stats_data:
            r = stats_data[0]
            product["stats"] = {
                "count": r.get("count"),
                "max_price": r.get("max_price"),
                "min_price": r.get("min_price"),
                "avg_price": r.get("avg_price")
            }
        else:
            product["stats"] = {
                "count": 0,
                "max_price": None,
                "min_price": None,
                "avg_price": None
            }
        
        return ok(product)
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

# 任务管理端点
@router.get("/spider/tasks")
def list_tasks(status: Optional[str] = None, product_id: Optional[int] = None):
    """获取任务列表"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        query = supabase_client.table("tasks").select("*").order("priority", desc=True).order("id", desc=True)
        
        if status:
            query = query.eq("status", status)
        if product_id:
            query = query.eq("product_id", product_id)
        
        res = query.execute()
        items = getattr(res, "data", None) or []
        return ok(items)
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

@router.get("/spider/stats")
def get_spider_stats():
    """获取爬虫统计信息"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        from ..services.task_scheduler import task_scheduler
        
        # 获取调度器统计
        scheduler_stats = task_scheduler.get_stats()
        
        # 获取队列状态
        queue_status = task_scheduler.get_queue_status()
        
        return ok({
            "scheduler": scheduler_stats,
            "queue": queue_status
        })
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))

@router.post("/spider/tasks")
def create_task(body: TaskCreate, api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """创建新任务"""
    if not repo:
        return error_response(500, "DATABASE_ERROR", "数据库连接不可用")
    
    try:
        now = now_iso()
        
        # 检查API Key配额
        created_by = None
        if api_key:
            user = get_user_by_api_key(api_key)
            if user:
                # 检查任务创建配额
                today = datetime.datetime.utcnow().date().isoformat()
                quota_check = supabase_client.table("users").select("quota_tasks_per_day,tasks_created_today,last_tasks_quota_reset").eq("id", user["id"]).limit(1).execute()
                user_data = getattr(quota_check, "data", None) or []
                
                if user_data:
                    limit = int(user_data[0].get("quota_tasks_per_day", 20))
                    used = int(user_data[0].get("tasks_created_today", 0))
                    last_reset = user_data[0].get("last_tasks_quota_reset")
                    
                    # 重置每日配额
                    if last_reset != today:
                        supabase_client.table("users").update({
                            "tasks_created_today": 0,
                            "last_tasks_quota_reset": today
                        }).eq("id", user["id"]).execute()
                        used = 0
                    
                    if used >= limit:
                        return error_response(429, "QUOTA_EXCEEDED", "任务创建配额已用尽")
                    
                    created_by = user["id"]
        
        # 使用任务调度器创建任务
        from ..services.task_scheduler import task_scheduler
        
        task_id = task_scheduler.add_task(
            product_id=body.product_id,
            priority=int(body.priority or 0)
        )
        
        if not task_id:
            return error_response(500, "TASK_CREATE_FAILED", "任务创建失败")
        
        # 更新用户配额
        if created_by:
            supabase_client.table("users").update({
                "tasks_created_today": used + 1,
                "last_tasks_quota_reset": today
            }).eq("id", created_by).execute()
        
        return ok({
            "id": task_id,
            "product_id": body.product_id,
            "status": "pending",
            "created_at": now,
            "updated_at": now
        })
    except Exception as e:
        return error_response(500, "INTERNAL_ERROR", str(e))