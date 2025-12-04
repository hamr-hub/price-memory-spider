from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from supabase import Client

from .supabase_client import get_client


class SupabaseRepo:
    def __init__(self, client: Optional[Client] = None):
        self.client: Client = client or get_client()
        if self.client is None:
            raise RuntimeError("Supabase client not configured. Set SUPABASE_URL and SUPABASE_KEY")

    # products
    def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        res = self.client.table("products").select("*").eq("id", product_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        return data[0] if data else None

    def upsert_product(self, name: str, url: str, source_domain: Optional[str] = None, category: Optional[str] = None, attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"name": name, "url": url}
        if source_domain:
            payload["source_domain"] = source_domain
        if category:
            payload["category"] = category
        if attributes:
            payload["attributes"] = attributes
        res = self.client.table("products").upsert(payload, on_conflict="url").select("*").execute()
        return (getattr(res, "data", None) or [])[0]

    # skus
    def upsert_sku(self, product_id: int, asin: Optional[str], name: Optional[str], url: Optional[str], attributes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {"product_id": product_id}
        if asin:
            payload["asin"] = asin
        if name:
            payload["name"] = name
        if url:
            payload["url"] = url
        if attributes:
            payload["attributes"] = attributes
        res = self.client.table("skus").upsert(payload, on_conflict="product_id,asin").select("*").execute()
        return (getattr(res, "data", None) or [])[0]

    # prices
    def insert_price(self, product_id: int, price: float, currency: str = "USD", sku_id: Optional[int] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"product_id": product_id, "price": price, "currency": currency}
        if sku_id is not None:
            payload["sku_id"] = sku_id
        res = self.client.table("prices").insert(payload).select("*").execute()
        return (getattr(res, "data", None) or [])[0]

    # tasks
    def get_pending_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        res = (
            self.client
            .table("tasks")
            .select("*")
            .eq("status", "pending")
            .order("scheduled_at", desc=False)
            .limit(limit)
            .execute()
        )
        return getattr(res, "data", None) or []

    def mark_task_running(self, task_id: int, source_url: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload: Dict[str, Any] = {"status": "running", "started_at": now}
        if source_url:
            payload["source_url"] = source_url
        self.client.table("tasks").update(payload).eq("id", task_id).execute()

    def mark_task_result(self, task_id: int, status: str, message: Optional[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload: Dict[str, Any] = {"status": status, "completed_at": now}
        if message:
            payload["result_message"] = message
        self.client.table("tasks").update(payload).eq("id", task_id).execute()

    def rpc_prices_aggregate(self, product_ids: List[int], interval: str, start_ts: Optional[str], end_ts: Optional[str]) -> List[Dict[str, Any]]:
        res = self.client.rpc("rpc_prices_aggregate", {"product_ids": product_ids, "interval": interval, "start_ts": start_ts, "end_ts": end_ts}).execute()
        return getattr(res, "data", None) or []

    def storage_upload(self, bucket: str, path: str, data: bytes) -> None:
        self.client.storage.from_(bucket).upload(path, data)

    def storage_signed_url(self, bucket: str, path: str, expires: int = 3600) -> Optional[str]:
        res = self.client.storage.from_(bucket).create_signed_url(path, expires)
        url = getattr(res, "signed_url", None) or getattr(res, "data", None)
        return url
