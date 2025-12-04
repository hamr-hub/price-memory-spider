import os
from typing import Any, Dict, Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.dao.supabase_client import get_client
SB = get_client()

try:
    from ai.embedding import embed_text, embed_image
except Exception:
    embed_text = None
    embed_image = None

def now_iso() -> str:
    import datetime
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ok(data: Any):
    return {"success": True, "data": data, "timestamp": now_iso()}

def error_response(code: str, message: str):
    return {"success": False, "error": {"code": code, "message": message}, "timestamp": now_iso()}

class AISearchBody(BaseModel):
    text: Optional[str] = None
    image_url: Optional[str] = None
    top_k: Optional[int] = 20
    category: Optional[str] = None

router = APIRouter()

@router.post("/api/v1/products/ai_search")
def ai_search(body: AISearchBody):
    use_img = bool(body.image_url and str(body.image_url).strip())
    use_txt = bool(body.text and str(body.text).strip())
    top_k = max(1, min(int(body.top_k or 20), 100))
    category = body.category
    if use_img and embed_image:
        ivec = embed_image(body.image_url or "")
        if ivec:
            res = SB.rpc("rpc_ai_search_products_with_info", {"q": ivec, "top_k": top_k, "use_image": True, "category": category}).execute()
            rows = getattr(res, "data", None) or []
            return ok({"items": rows, "mode": "image", "total": len(rows)})
    if use_txt and embed_text:
        tvec = embed_text(body.text or "")
        if tvec:
            res = SB.rpc("rpc_ai_search_products_with_info", {"q": tvec, "top_k": top_k, "use_image": False, "category": category}).execute()
            rows = getattr(res, "data", None) or []
            return ok({"items": rows, "mode": "text", "total": len(rows)})
    if use_txt:
        q = SB.table("products").select("*", count="exact")
        q = q.ilike("name", f"%{body.text}%")
        if category:
            q = q.eq("category", category)
        res = q.range(0, top_k - 1).execute()
        items = [{"id": r.get("id"), "name": r.get("name"), "url": r.get("url"), "category": r.get("category"), "score": None} for r in (getattr(res, "data", None) or [])]
        return ok({"items": items, "mode": "fallback", "total": len(items)})
    return error_response("VALIDATION_ERROR", "缺少有效的查询输入")

def _product_text_for_embedding(r: dict) -> str:
    name = r.get("name") or ""
    cat = r.get("category") or ""
    attrs = r.get("attributes") or {}
    parts = [name]
    if cat:
        parts.append(f"类别:{cat}")
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            parts.append(f"{k}:{v}")
    return " \n ".join([str(x) for x in parts if str(x).strip()])

@router.post("/api/v1/ai/index_products")
def ai_index_products(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    pres = SB.table("products").select("*").order("id", desc=True).range(offset, offset + limit - 1).execute()
    rows = getattr(pres, "data", None) or []
    count = 0
    for r in rows:
        pid = int(r.get("id"))
        tvec = embed_text(_product_text_for_embedding(r)) if embed_text else None
        image_url = None
        attrs = r.get("attributes") or {}
        if isinstance(attrs, dict):
            image_url = attrs.get("image") or attrs.get("image_url")
        ivec = embed_image(image_url) if (image_url and embed_image) else None
        payload: Dict[str, Any] = {"product_id": pid, "updated_at": now_iso()}
        if tvec:
            payload["embedding_text"] = tvec
        if ivec:
            payload["embedding_image"] = ivec
        if "embedding_text" in payload or "embedding_image" in payload:
            SB.table("product_embeddings").upsert(payload).execute()
            count += 1
    return ok({"indexed": count, "scanned": len(rows)})

