"""
通用电商网站爬虫工作器
支持Amazon、淘宝、京东等多个电商平台
"""
import time
import os
from typing import Optional, Dict, Any

from ..dao.supabase_repo import SupabaseRepo
from ..playwrite.bowser_utils import BowserBrowser
from ..sites.universal import extract_product_data, is_supported_ecommerce_site
from ..config.config import config


class UniversalWorker:
    """通用电商爬虫工作器"""
    
    def __init__(self, ws_endpoint: Optional[str] = None, poll_interval_sec: int = 10):
        self.repo = SupabaseRepo()
        self.browser = BowserBrowser(ws_endpoint=ws_endpoint or config.PLAYWRIGHT_WS_ENDPOINT)
        self.poll_interval_sec = poll_interval_sec
        self._last_rates_refresh = 0.0
        self._retry_count = config.WORKER_TASK_RETRIES

    def process_task(self, task: dict) -> None:
        """
        务
        
        Args:
            task: 任务信息字典
        """
        task_id = task["id"]
        product_id = task["product_id"]
        print(f"[worker] 开始处理任务 id={task_id} product_id={product_id}")

        # 获取商品信息
        product = self.repo.get_product(product_id)
        if not product or not product.get("url"):
            self.repo.mark_task_result(task_id, "failed", "商品或URL不存在")
            return

        url: str = product["url"]
        
        # 检查是否为支持的电商网站
        if not is_supported_ecommerce_site(url):
            print(f"[worker] 不支持的网站: {url}")
            self.repo.mark_task_result(task_id, "failed", "不支持的电商网站")
            return

        # 标记任务为运行中
        self.repo.mark_task_running(task_id, source_url=url)
        
        # 重试机制
        attempt = 0
        last_err = None
        
        while attempt <= self._retry_count:
            attempt += 1
            page = None
            
            try:
                print(f"[worker] 第 {attempt} 次尝试访问 {url}")
                
                # 打开页面
                page = self.browser.open_page_sync(url)
                
                # 提取商品数据
                spu, skus = extract_product_data(page, url)
                
                # 更新商品信息
                name = spu.get("name") or product.get("name") or ""
                source_domain = spu.get("source_domain") or product.get("source_domain")
                category = spu.get("category") or product.get("category")
                
                updated_product = self.repo.upsert_product(
                    name=name, 
                    url=url, 
                    source_domain=source_domain, 
                    category=category, 
                    attributes=spu.get("attributes")
                )
                
                # 处理SKU信息
                inserted_skus = 0
                for sku in skus:
                    try:
                        rec = self.repo.upsert_sku(
                            product_id=product_id,
                            asin=sku.get("asin"),
                            name=sku.get("name"),
                            url=sku.get("url"),
                            attributes=sku.get("attributes"),
                        )
                        if rec:
                            inserted_skus += 1
                    except Exception as e:
                        print(f"[worker] SKU插入失败: {e}")
                        continue

                # 处理价格信息
                price = spu.get("price")
                currency = spu.get("currency")
                
                if not currency:
                    # 根据网站域名获取默认货币
                    site = self.repo.get_site_by_domain(source_domain or "") if source_domain else None
                    currency = (site or {}).get("currency") or "USD"
                
                if price is not None and price > 0:
                    # 插入价格记录
                    self.repo.insert_price(
                        product_id=product_id, 
                        price=float(price), 
                        currency=currency
                    )
                    
                    msg = f"成功处理商品 {product_id}, SKU数量={inserted_skus}, 价格={price} {currency}"
                    print(f"[worker] {msg}")
                    self.repo.mark_task_result(task_id, "succeeded", msg)
                    
                    # 触发价格变化检查
                    try:
                        from ..services.price_monitor import check_and_send_price_alerts
                        check_and_send_price_alerts(product_id, price, currency)
                    except Exception as e:
                        print(f"[worker] 价格告警检查失败: {e}")
                    
                    break
                else:
                    msg = f"未能获取到有效价格，商品={product_id}"
                    print(f"[worker] {msg}")
                    if attempt > self._retry_count:
                        self.repo.mark_task_result(task_id, "failed", msg)
                    
            except Exception as e:
                last_err = e
                print(f"[worker] 第 {attempt} 次尝试失败: {e}")
                
                if attempt > self._retry_count:
                    self.repo.mark_task_result(task_id, "failed", str(last_err))
                else:
                    # 等待一段时间后重试
                    time.sleep(min(attempt * 2, 10))
                    
            finally:
                # 确保浏览器页面被关闭
                if page:
                    try:
                        self.browser.close_sync()
                    except Exception:
                        pass

    

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
