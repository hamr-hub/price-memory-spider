from playwright.sync_api import sync_playwright
import json
from urllib.parse import urlparse
from src.utils.url_util import get_link_latency



def run(url, func):
    with (sync_playwright() as p):
        browser = p.chromium.connect("ws://43.133.224.11:20001/")
        page = browser.new_page()
        page.goto(url)
        func(page)

def run_async(url,)

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.connect("ws://43.133.224.11:20001/")
        page = browser.new_page()
        page.goto("https://www.amazon.com/customer-preferences/country")
        page.locator("#icp-dropdown > span > span").click()
        page.wait_for_selector("#a-popover-1 > div > div > ul a")
        for a_elem in page.locator("#a-popover-1 > div > div > ul a").all():
            # 提取a标签的href属性（链接）
            link = json.loads(a_elem.get_attribute("data-value")).get("stringVal")
            text = a_elem.text_content()
            print(f"链接: {get_amazon_domain(link)}, 文本: {text}")
            print(f"延迟: {get_link_latency(browser, page, link)} 秒")
        browser.close()

