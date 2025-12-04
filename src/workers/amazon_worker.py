import time
from typing import Optional

from src.dao.supabase_repo import SupabaseRepo
from src.playwrite.bowser_utils import BowserBrowser
from src.sites.amazon import extract_spu_and_skus


class AmazonWorker:
    def __init__(self, ws_endpoint: str = "ws://43.133.224.11:20001/", poll_interval_sec: int = 10):
        self.repo = SupabaseRepo()
        self.browser = BowserBrowser(ws_endpoint=ws_endpoint)
        self.poll_interval_sec = poll_interval_sec

    def process_task(self, task: dict) -> None:
        task_id = task["id"]
        product_id = task["product_id"]
        self.repo.mark_task_running(task_id, source_url=url)

        product = self.repo.get_product(product_id)
        if not product or not product.get("url"):
            self.repo.mark_task_result(task_id, "failed", "product or url not found")
            return

        url: str = product["url"]
        page = self.browser.open_page_sync(url)
        try:
            spu, skus = extract_spu_and_skus(page, url)

            # optional: update product name/source_domain/category if missing
            name = spu.get("name") or product.get("name") or ""
            source_domain = spu.get("source_domain") or product.get("source_domain")
            category = product.get("category")
            self.repo.upsert_product(name=name, url=url, source_domain=source_domain, category=category, attributes=spu.get("attributes"))

            inserted_skus = 0
            for sku in skus:
                rec = self.repo.upsert_sku(
                    product_id=product_id,
                    asin=sku.get("asin"),
                    name=sku.get("name"),
                    url=sku.get("url"),
                    attributes=sku.get("attributes"),
                )
                inserted_skus += 1 if rec else 0

            # write price snapshot for currently displayed variant
            price = spu.get("price")
            currency = spu.get("currency") or "USD"
            if price is not None:
                self.repo.insert_price(product_id=product_id, price=float(price), currency=currency)

            msg = f"processed product {product_id}, skus={inserted_skus}, price={'yes' if price is not None else 'no'}"
            self.repo.mark_task_result(task_id, "succeeded", msg)
        except Exception as e:
            self.repo.mark_task_result(task_id, "failed", str(e))
        finally:
            self.browser.close_sync()

    def run_forever(self):
        while True:
            try:
                tasks = self.repo.get_pending_tasks(limit=5)
                for t in tasks:
                    self.process_task(t)
            except Exception:
                pass
            time.sleep(self.poll_interval_sec)


def main():
    worker = AmazonWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()
