from urllib.parse import urlparse
import json
from playwright.sync_api import Page

from src.utils.url_util import get_link_latency

def get_amazon_domain(url: str):
    url_info = urlparse(url)
    return f"{url_info.scheme}://{url_info.netloc}"


def test_amazon_country_link(page: Page):
    page.goto("https://www.amazon.com/customer-preferences/country")
    page.locator("#icp-dropdown > span > span").click()
    page.wait_for_selector("#a-popover-1 > div > div > ul a")
    for a_elem in page.locator("#a-popover-1 > div > div > ul a").all():
        # 提取a标签的href属性（链接）
        link = json.loads(a_elem.get_attribute("data-value")).get("stringVal")
        text = a_elem.text_content()
        print(f"链接: {get_amazon_domain(link)}, 文本: {text}")
        get_link_latency(page, link)

