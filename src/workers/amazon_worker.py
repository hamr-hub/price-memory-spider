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
        self._last_rates_refresh = 0.0

    def process_task(self, task: dict) -> None:
        task_id = task["id"]
        product_id = task["product_id"]
        print(f"[worker] start task id={task_id} product_id={product_id}")

        product = self.repo.get_product(product_id)
        if not product or not product.get("url"):
            self.repo.mark_task_result(task_id, "failed", "product or url not found")
            return

        url: str = product["url"]
        retries = int(__import__("os").environ.get("WORKER_TASK_RETRIES", "2"))
        attempt = 0
        last_err = None
        self.repo.mark_task_running(task_id, source_url=url)
        while attempt <= retries:
            attempt += 1
            page = self.browser.open_page_sync(url)
            try:
                print(f"[worker] attempt {attempt} open {url}")
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
            currency = spu.get("currency")
            if not currency:
                # fallback to site default currency
                site = self.repo.get_site_by_domain(source_domain or "") if source_domain else None
                currency = (site or {}).get("currency") or "USD"
            if price is not None:
                self.repo.insert_price(product_id=product_id, price=float(price), currency=currency)

                msg = f"processed product {product_id}, skus={inserted_skus}, price={'yes' if price is not None else 'no'}"
                print(f"[worker] success {msg}")
                self.repo.mark_task_result(task_id, "succeeded", msg)
                break
            except Exception as e:
                last_err = e
                print(f"[worker] attempt {attempt} failed: {e}")
            finally:
                self.browser.close_sync()
        if last_err is not None and attempt > retries:
            self.repo.mark_task_result(task_id, "failed", str(last_err))

    def run_forever(self):
        while True:
            try:
                # periodic exchange rates refresh (optional, external source)
                self.refresh_exchange_rates_if_needed()
                tasks = self.repo.get_pending_tasks(limit=5)
                for t in tasks:
                    self.process_task(t)
            except Exception:
                pass
            time.sleep(self.poll_interval_sec)

    def refresh_exchange_rates_if_needed(self):
        import time as _t, os, json, urllib.request
        now = _t.time()
        interval = float(os.environ.get("EXCHANGE_RATES_REFRESH_SEC", "3600"))
        if now - self._last_rates_refresh < interval:
            return
        source = os.environ.get("EXCHANGE_RATES_SOURCE")  # e.g., https://api.exchangerate.host/latest?base=USD
        if not source:
            self._last_rates_refresh = now
            return
        try:
            with urllib.request.urlopen(source, timeout=10) as resp:
                j = json.loads(resp.read().decode("utf-8"))
                rates = j.get("rates") or {}
                for cur, rate in rates.items():
                    try:
                        self.repo.upsert_exchange_rate(cur, float(rate))
                    except Exception:
                        pass
        except Exception:
            pass
        finally:
            self._last_rates_refresh = now


def main():
    worker = AmazonWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()
