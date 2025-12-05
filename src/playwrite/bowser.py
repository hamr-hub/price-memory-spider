"""
Playwright浏览器工具类
提供统一的浏览器操作接口
"""
import os
import time
import tempfile
import shutil
import json
import asyncio
from typing import Optional, Callable, Any, Dict, List
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
from playwright.async_api import async_playwright

try:
    from ..config.config import config
except ImportError:
    # 兼容性处理
    class Config:
        PLAYWRIGHT_WS_ENDPOINT = "ws://43.133.224.11:20001/"
        BROWSER_MODE = "remote"
        @classmethod
        def get_proxy_list(cls):
            return []
    config = Config()

try:
    from ..utils.url_util import get_link_latency
except ImportError:
    def get_link_latency(browser, page, url):
        return 0.0


class BowserBrowser:
    """浏览器管理类"""
    
    def __init__(self, ws_endpoint: Optional[str] = None, headless: bool = True):
        self.ws_endpoint = ws_endpoint or config.PLAYWRIGHT_WS_ENDPOINT
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
    
    def open_page_sync(self, url: str, timeout: int = 30000) -> Page:
        """同步方式打开页面"""
        with sync_playwright() as p:
            if config.BROWSER_MODE == "local":
                # 本地模式
                proxy_config = self._get_proxy_config()
                launch_args = {"headless": self.headless}
                if proxy_config:
                    launch_args["proxy"] = proxy_config
                
                self.browser = p.chromium.launch(**launch_args)
            else:
                # 远程模式
                self.browser = p.chromium.connect(self.ws_endpoint)
            
            self.context = self.browser.new_context()
            self.page = self.context.new_page()
            self.page.set_default_timeout(timeout)
            self.page.goto(url)
            
            return self.page
    
    def close_sync(self):
        """同步关闭浏览器"""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        finally:
            self.page = None
            self.context = None
            self.browser = None
    
    def _get_proxy_config(self) -> Optional[Dict[str, str]]:
        """获取代理配置"""
        proxies = config.get_proxy_list()
        if not proxies:
            return None
        
        # 简单轮询选择代理
        import hashlib
        import time
        key = str(int(time.time() / 60))  # 每分钟轮换
        idx = int(hashlib.sha1(key.encode()).hexdigest(), 16) % len(proxies)
        proxy_url = proxies[idx]
        
        return {"server": proxy_url}


def run(url: str, func: Callable[[Any], None], headless: bool = False, timeout: int = 30000) -> None:
    """
    同步方式运行Playwright浏览器操作
    
    Args:
        url: 要访问的URL
        func: 回调函数，接收page参数进行页面操作
        headless: 是否以无头模式运行浏览器
        timeout: 页面加载超时时间（毫秒）
    
    Returns:
        None
    """
    browser = BowserBrowser(headless=headless)
    try:
        page = browser.open_page_sync(url, timeout)
        func(page)
    except Exception as e:
        print(f"同步执行失败: {e}")
        raise
    finally:
        browser.close_sync()


async def run_async(url: str, func: Callable[[Any], Any], headless: bool = False, timeout: int = 30000) -> None:
    """
    异步方式运行Playwright浏览器操作
    
    Args:
        url: 要访问的URL
        func: 回调函数，接收page参数进行页面操作，可以是异步函数
        headless: 是否以无头模式运行浏览器
        timeout: 页面加载超时时间（毫秒）
    
    Returns:
        None
    """
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_async(config.PLAYWRIGHT_WS_ENDPOINT)
            context = await browser.new_context()
            page = await context.new_page()
            await page.set_default_timeout(timeout)
            await page.goto(url)
            
            # 判断func是否为异步函数
            if asyncio.iscoroutinefunction(func):
                await func(page)
            else:
                await asyncio.to_thread(func, page)
            
            await browser.close()
        except Exception as e:
            print(f"异步执行失败: {e}")
            raise


def get_amazon_domain(url: str) -> str:
    """
    从Amazon链接中提取域名
    
    Args:
        url: Amazon链接
    
    Returns:
        域名字符串
    """
    parsed = urlparse(url)
    return parsed.netloc


if __name__ == "__main__":
    # 同步示例
    def sync_example(page):
        page.goto("https://www.amazon.com/customer-preferences/country")
        page.locator("#icp-dropdown > span > span").click()
        page.wait_for_selector("#a-popover-1 > div > div > ul a")
        for a_elem in page.locator("#a-popover-1 > div > div > ul a").all():
            link = json.loads(a_elem.get_attribute("data-value")).get("stringVal")
            text = a_elem.text_content()
            print(f"链接: {get_amazon_domain(link)}, 文本: {text}")
            print(f"延迟: {get_link_latency(None, page, link)} 秒")
    
    # 异步示例
    async def async_example(page):
        await page.goto("https://www.amazon.com/customer-preferences/country")
        await page.locator("#icp-dropdown > span > span").click()
        await page.wait_for_selector("#a-popover-1 > div > div > ul a")
        for a_elem in await page.locator("#a-popover-1 > div > div > ul a").all():
            link = json.loads(await a_elem.get_attribute("data-value")).get("stringVal")
            text = await a_elem.text_content()
            print(f"链接: {get_amazon_domain(link)}, 文本: {text}")
            print(f"延迟: {get_link_latency(None, page, link)} 秒")
    
    # 运行同步版本
    print("=== 同步执行 ===")
    run("https://www.amazon.com/customer-preferences/country", sync_example)
    
    # 运行异步版本
    print("\n=== 异步执行 ===")
    asyncio.run(run_async("https://www.amazon.com/customer-preferences/country", async_example))

