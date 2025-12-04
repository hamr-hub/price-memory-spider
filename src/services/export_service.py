from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import csv
import io

from src.dao.supabase_repo import SupabaseRepo


class ExportService:
    def __init__(self):
        self.repo = SupabaseRepo()

    def aggregate_prices(self, product_ids: List[int], interval: str, start_ts: Optional[str], end_ts: Optional[str]) -> List[Dict[str, Any]]:
        return self.repo.rpc_prices_aggregate(product_ids=product_ids, interval=interval, start_ts=start_ts, end_ts=end_ts)

    def export_to_storage(self, product_ids: List[int], interval: str, start_ts: Optional[str], end_ts: Optional[str], path: Optional[str] = None, bucket: str = "exports") -> Dict[str, Any]:
        rows = self.aggregate_prices(product_ids, interval, start_ts, end_ts)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["bucket", "product_id", "avg_price", "min_price", "max_price", "samples"])
        for r in rows:
            writer.writerow([r.get("bucket"), r.get("product_id"), r.get("avg_price"), r.get("min_price"), r.get("max_price"), r.get("samples")])
        data = buf.getvalue().encode("utf-8")
        if not path:
            now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = f"prices/{now}_{interval}.csv"
        self.repo.storage_upload(bucket=bucket, path=path, data=data)
        url = self.repo.storage_signed_url(bucket=bucket, path=path, expires=3600)
        return {"bucket": bucket, "path": path, "url": url}

