"""
Playwright Bowser 工具类
提供更高级的浏览器操作封装
"""

from playwright.sync_api import sync_playwright, Page as SyncPage
from playwright.async_api import async_playwright, Page as AsyncPage
from typing import Callable, Any, Optional, Union, Dict, List
import asyncio
import json
from contextlib import contextmanager


class BowserBrowser:
    """
    Playwright 浏览器操作工具类
    支持同步和异步两种模式
    """
    
    def __init__(self, ws_endpoint: str = "ws://43.133.224.11:20001/", headless: bool = False):
        """
        初始化浏览器工具
        
        Args:
            ws_endpoint: Playwright WebSocket连接地址
            headless: 是否无头模式
        """
        self.ws_endpoint = ws_endpoint
        self.headless = headless
        self._sync_playwright = None
        self._async_playwright = None
        self._browser = None
        self._page = None
        self._user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        ]
        self._locales = ["zh-CN", "en-US"]
        self._timezones = ["Asia/Shanghai", "UTC"]
        self._max_retries = 2
    
    # ========== 同步方法 ==========
    
    def run_sync(self, url: str, func: Callable[[SyncPage], None], timeout: int = 30000) -> None:
        """
        同步执行浏览器操作
        
        Args:
            url: 目标URL
            func: 操作函数
            timeout: 超时时间（毫秒）
        """
        with sync_playwright() as p:
            try:
                browser = p.chromium.connect(self.ws_endpoint)
                context = browser.new_context(**self._build_context_args())
                page = context.new_page()
                page.set_default_timeout(timeout)
                self._apply_stealth(page)
                self._navigate_with_retry(page, url, timeout)
                func(page)
                browser.close()
            except Exception as e:
                print(f"同步执行失败: {e}")
                raise
    
    def open_page_sync(self, url: str, timeout: int = 30000) -> SyncPage:
        """
        同步打开页面（用于链式调用）
        
        Args:
            url: 目标URL
            timeout: 超时时间（毫秒）
        
        Returns:
            Page对象
        """
        if not self._sync_playwright:
            self._sync_playwright = sync_playwright().start()
        
        if not self._browser:
            self._browser = self._sync_playwright.chromium.connect(self.ws_endpoint)
        
        if not self._page:
            context = self._browser.new_context(**self._build_context_args())
            self._page = context.new_page()
        
        self._page.set_default_timeout(timeout)
        self._apply_stealth(self._page)
        self._navigate_with_retry(self._page, url, timeout)
        return self._page
    
    def close_sync(self):
        """关闭同步浏览器"""
        if self._page:
            self._page.close()
            self._page = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._sync_playwright:
            self._sync_playwright.stop()
            self._sync_playwright = None
    
    # ========== 异步方法 ==========
    
    async def run_async(self, url: str, func: Callable[[AsyncPage], Any], timeout: int = 30000) -> None:
        """
        异步执行浏览器操作
        
        Args:
            url: 目标URL
            func: 操作函数（可以是异步函数）
            timeout: 超时时间（毫秒）
        """
        async with async_playwright() as p:
            try:
                browser = await p.chromium.connect_async(self.ws_endpoint)
                context = await browser.new_context(**self._build_context_args())
                page = await context.new_page()
                await page.set_default_timeout(timeout)
                await self._apply_stealth_async(page)
                await self._navigate_with_retry_async(page, url, timeout)
                
                if asyncio.iscoroutinefunction(func):
                    await func(page)
                else:
                    await asyncio.to_thread(func, page)
                
                await browser.close()
            except Exception as e:
                print(f"异步执行失败: {e}")
                raise
    
    async def open_page_async(self, url: str, timeout: int = 30000) -> AsyncPage:
        """
        异步打开页面（用于链式调用）
        
        Args:
            url: 目标URL
            timeout: 超时时间（毫秒）
        
        Returns:
            Page对象
        """
        if not self._async_playwright:
            self._async_playwright = await async_playwright().start()
        
        if not self._browser:
            self._browser = await self._async_playwright.chromium.connect_async(self.ws_endpoint)
        
        if not self._page:
            context = await self._browser.new_context(**self._build_context_args())
            self._page = await context.new_page()
        
        await self._page.set_default_timeout(timeout)
        await self._apply_stealth_async(self._page)
        await self._navigate_with_retry_async(self._page, url, timeout)
        return self._page
    
    async def close_async(self):
        """关闭异步浏览器"""
        if self._page:
            await self._page.close()
            self._page = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._async_playwright:
            await self._async_playwright.stop()
            self._async_playwright = None
    
    # ========== 便捷方法 ==========
    
    def wait_for_element(self, page: Union[SyncPage, AsyncPage], selector: str, timeout: int = 5000) -> Union[SyncPage, AsyncPage]:
        """等待元素出现"""
        if hasattr(page, 'wait_for_selector'):
            # 异步模式
            return page.wait_for_selector(selector, timeout=timeout)
        else:
            # 同步模式
            return page.wait_for_selector(selector, timeout=timeout)
    
    def click_element(self, page: Union[SyncPage, AsyncPage], selector: str) -> None:
        """点击元素"""
        if hasattr(page, 'click'):
            # 异步模式
            return page.click(selector)
        else:
            # 同步模式
            return page.click(selector)
    
    def fill_input(self, page: Union[SyncPage, AsyncPage], selector: str, value: str) -> None:
        """填写输入框"""
        if hasattr(page, 'fill'):
            # 异步模式
            return page.fill(selector, value)
        else:
            # 同步模式
            return page.fill(selector, value)
    
    def get_text(self, page: Union[SyncPage, AsyncPage], selector: str) -> str:
        """获取元素文本"""
        if hasattr(page, 'text_content'):
            # 异步模式
            return page.text_content(selector)
        else:
            # 同步模式
            return page.text_content(selector)
    
    def take_screenshot(self, page: Union[SyncPage, AsyncPage], path: str) -> None:
        """截图"""
        if hasattr(page, 'screenshot'):
            # 异步模式
            return page.screenshot(path=path)
        else:
            # 同步模式
            return page.screenshot(path=path)

    def _build_context_args(self) -> Dict[str, Any]:
        import random
        ua = random.choice(self._user_agents)
        loc = random.choice(self._locales)
        tz = random.choice(self._timezones)
        headers = {"Accept-Language": loc}
        return {
            "user_agent": ua,
            "locale": loc,
            "timezone_id": tz,
            "extra_http_headers": headers,
        }

    def _apply_stealth(self, page: SyncPage):
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

    async def _apply_stealth_async(self, page: AsyncPage):
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

    def _navigate_with_retry(self, page: SyncPage, url: str, timeout: int):
        import random, time as _t
        tries = 0
        last = None
        while tries <= self._max_retries:
            try:
                page.goto(url, timeout=timeout)
                return
            except Exception as e:
                last = e
                _t.sleep(0.2 + random.random())
                tries += 1
        if last:
            raise last

    async def _navigate_with_retry_async(self, page: AsyncPage, url: str, timeout: int):
        import random, asyncio as _a
        tries = 0
        last = None
        while tries <= self._max_retries:
            try:
                await page.goto(url, timeout=timeout)
                return
            except Exception as e:
                last = e
                await _a.sleep(0.2 + random.random())
                tries += 1
        if last:
            raise last


# ========== 装饰器 ==========
def with_browser(ws_endpoint: str = "ws://43.133.224.11:20001/"):
    """
    装饰器：自动管理浏览器生命周期
    
    使用示例:
    @with_browser()
    def my_task(page):
        page.goto("https://example.com")
        return page.title()
    """
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            # 异步函数
            async def async_wrapper(*args, **kwargs):
                async with async_playwright() as p:
                    browser = await p.chromium.connect_async(ws_endpoint)
                    context = await browser.new_context()
                    page = await context.new_page()
                    try:
                        return await func(page, *args, **kwargs)
                    finally:
                        await browser.close()
            return async_wrapper
        else:
            # 同步函数
            def sync_wrapper(*args, **kwargs):
                with sync_playwright() as p:
                    browser = p.chromium.connect(ws_endpoint)
                    context = browser.new_context()
                    page = context.new_page()
                    try:
                        return func(page, *args, **kwargs)
                    finally:
                        browser.close()
            return sync_wrapper
    return decorator


# ========== 上下文管理器 ==========
@contextmanager
def browser_context(ws_endpoint: str = "ws://43.133.224.11:20001/"):
    """
    上下文管理器：自动管理同步浏览器生命周期
    
    使用示例:
    with browser_context() as page:
        page.goto("https://example.com")
        print(page.title())
    """
    with sync_playwright() as p:
        browser = p.chromium.connect(ws_endpoint)
        context = browser.new_context()
        page = context.new_page()
        try:
            yield page
        finally:
            browser.close()


@contextmanager  
async def async_browser_context(ws_endpoint: str = "ws://43.133.224.11:20001/"):
    """
    异步上下文管理器：自动管理异步浏览器生命周期
    
    使用示例:
    async with async_browser_context() as page:
        await page.goto("https://example.com")
        print(await page.title())
    """
    async with async_playwright() as p:
        browser = await p.chromium.connect_async(ws_endpoint)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            yield page
        finally:
            await browser.close()


# ========== 实用函数 ==========
def batch_process_urls(urls: List[str], func: Callable, sync: bool = True, **kwargs) -> Dict[str, Any]:
    """
    批量处理URL列表
    
    Args:
        urls: URL列表
        func: 处理函数
        sync: 是否同步执行
        **kwargs: 传递给run或run_async的额外参数
    
    Returns:
        结果字典，key为URL，value为处理结果
    """
    results = {}
    
    if sync:
        # 同步批量处理
        for url in urls:
            try:
                browser = BowserBrowser(kwargs.get('ws_endpoint', "ws://43.133.224.11:20001/"))
                browser.run_sync(url, func, kwargs.get('timeout', 30000))
                results[url] = "success"
            except Exception as e:
                results[url] = f"error: {str(e)}"
    else:
        # 异步批量处理
        async def process_batch():
            for url in urls:
                try:
                    browser = BowserBrowser(kwargs.get('ws_endpoint', "ws://43.133.224.11:20001/"))
                    await browser.run_async(url, func, kwargs.get('timeout', 30000))
                    results[url] = "success"
                except Exception as e:
                    results[url] = f"error: {str(e)}"
        
        asyncio.run(process_batch())
    
    return results
