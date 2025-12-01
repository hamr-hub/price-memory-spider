from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright
import json
import asyncio
from urllib.parse import urlparse
from typing import Callable, Any
from src.utils.url_util import get_link_latency



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
    with sync_playwright() as p:
        try:
            browser = p.chromium.connect("ws://43.133.224.11:20001/")
            page = browser.new_page()
            page.set_default_timeout(timeout)
            page.goto(url)
            func(page)
            browser.close()
        except Exception as e:
            print(f"同步执行失败: {e}")
            raise


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
            browser = await p.chromium.connect_async("ws://43.133.224.11:20001/")
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

