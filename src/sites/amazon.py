from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page


def parse_price_text(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    s = text.strip()
    currency = None
    if s.startswith("$"):
        currency = "USD"
        s = s[1:]
    elif s.startswith("£"):
        currency = "GBP"
        s = s[1:]
    elif s.startswith("€"):
        currency = "EUR"
        s = s[1:]
    elif s.startswith("￥") or s.startswith("¥"):
        currency = "CNY"
        s = s[1:]
    s = s.replace(",", "").strip()
    try:
        return float(s), currency
    except Exception:
        return None, currency


def extract_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    page.goto(url)
    title = page.locator('#productTitle').inner_text() if page.locator('#productTitle').count() else None
    asin = page.locator('input#ASIN').get_attribute('value') if page.locator('input#ASIN').count() else None

    # price candidates
    price_text = None
    for sel in ['#priceblock_ourprice', '#priceblock_dealprice', 'span.a-price span.a-price-whole']:
        if page.locator(sel).count():
            price_text = page.locator(sel).first.inner_text()
            break
    price, currency = parse_price_text(price_text)

    domain = urlparse(url).netloc
    spu: Dict[str, Any] = {
        "name": title or "",
        "url": url,
        "source_domain": domain,
        "category": None,
        "price": price,
        "currency": currency or "USD",
        "attributes": {"asin": asin} if asin else None,
    }

    skus: List[Dict[str, Any]] = []
    # collect variations from twister
    if page.locator('#twister').count():
        items = page.locator('#twister [data-asin]')
        for i in range(items.count()):
            el = items.nth(i)
            sku_asin = el.get_attribute('data-asin') or el.get_attribute('data-defaultasin')
            text = el.text_content() or el.get_attribute('title')
            href = el.get_attribute('href')
            attrs: Dict[str, Any] = {}
            if el.get_attribute('data-csa-c-type'):
                attrs['csa_type'] = el.get_attribute('data-csa-c-type')
            skus.append({
                "asin": sku_asin,
                "name": (text or '').strip(),
                "url": href,
                "attributes": attrs or None,
            })

    return spu, skus
