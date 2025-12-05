"""
Amazon网站价格抓取模块
支持多个Amazon区域站点的价格抓取
"""
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page


def parse_price_text(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    解析价格文本，提取价格和货币
    
    Args:
        text: 价格文本
    
    Returns:
        Tuple[价格, 货币代码]
    """
    if not text:
        return None, None
    
    # 清理文本
    s = text.strip().replace('\n', ' ').replace('\t', ' ')
    s = re.sub(r'\s+', ' ', s)  # 合并多个空格
    
    currency = None
    
    # 检测货币符号
    if '$' in s:
        currency = "USD"
        s = re.sub(r'[^\d.,]', '', s)  # 只保留数字、逗号和点
    elif '£' in s:
        currency = "GBP"
        s = re.sub(r'[^\d.,]', '', s)
    elif '€' in s:
        currency = "EUR"
        s = re.sub(r'[^\d.,]', '', s)
    elif '￥' in s or '¥' in s:
        currency = "CNY"
        s = re.sub(r'[^\d.,]', '', s)
    elif 'CAD' in s.upper():
        currency = "CAD"
        s = re.sub(r'[^\d.,]', '', s)
    elif 'AUD' in s.upper():
        currency = "AUD"
        s = re.sub(r'[^\d.,]', '', s)
    else:
        # 尝试提取纯数字
        s = re.sub(r'[^\d.,]', '', s)
    
    if not s:
        return None, currency
    
    # 处理千分位分隔符
    s = s.replace(',', '')
    
    # 处理小数点
    if '.' in s:
        parts = s.split('.')
        if len(parts) == 2:
            s = parts[0] + '.' + parts[1][:2]  # 只保留两位小数
    
    try:
        price = float(s)
        return price, currency
    except (ValueError, TypeError):
        return None, currency


def detect_amazon_region(url: str) -> str:
    """
    根据URL检测Amazon区域
    
    Args:
        url: Amazon URL
    
    Returns:
        区域代码
    """
    domain = urlparse(url).netloc.lower()
    
    region_map = {
        'amazon.com': 'US',
        'amazon.co.uk': 'UK',
        'amazon.de': 'DE',
        'amazon.fr': 'FR',
        'amazon.it': 'IT',
        'amazon.es': 'ES',
        'amazon.ca': 'CA',
        'amazon.com.au': 'AU',
        'amazon.co.jp': 'JP',
        'amazon.in': 'IN',
        'amazon.com.br': 'BR',
        'amazon.com.mx': 'MX',
        'amazon.cn': 'CN',
        'amazon.sg': 'SG'
    }
    
    for domain_key, region in region_map.items():
        if domain_key in domain:
            return region
    
    return 'US'  # 默认美国


def get_price_selectors() -> List[str]:
    """
    获取价格选择器列表
    按优先级排序
    """
    return [
        # 新版价格选择器
        'span.a-price.a-text-price.a-size-medium.apexPriceToPay span.a-offscreen',
        'span.a-price-whole',
        '.a-price .a-offscreen',
        'span[class*="a-price"] .a-offscreen',
        
        # 旧版价格选择器
        '#priceblock_ourprice',
        '#priceblock_dealprice',
        '#price_inside_buybox',
        '.a-price-current',
        
        # 特殊情况
        'span.a-size-medium.a-color-price',
        '.a-price-range .a-price .a-offscreen',
        'span[data-a-color="price"]',
        
        # 备用选择器
        '[data-testid="price"]',
        '.price',
        '[class*="price"]'
    ]


def extract_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取商品基本信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        商品信息字典
    """
    info = {}
    
    # 商品标题
    title_selectors = [
        '#productTitle',
        'h1[data-automation-id="product-title"]',
        '.product-title',
        'h1'
    ]
    
    for selector in title_selectors:
        try:
            if page.locator(selector).count() > 0:
                title = page.locator(selector).first.inner_text().strip()
                if title:
                    info['title'] = title
                    break
        except Exception:
            continue
    
    # ASIN
    asin_selectors = [
        'input#ASIN',
        '[data-asin]',
        'input[name="ASIN"]'
    ]
    
    for selector in asin_selectors:
        try:
            if page.locator(selector).count() > 0:
                asin = page.locator(selector).first.get_attribute('value') or page.locator(selector).first.get_attribute('data-asin')
                if asin:
                    info['asin'] = asin
                    break
        except Exception:
            continue
    
    # 品牌
    brand_selectors = [
        '#bylineInfo',
        '.a-row .a-size-small span.a-color-secondary',
        '[data-automation-id="brand-name"]'
    ]
    
    for selector in brand_selectors:
        try:
            if page.locator(selector).count() > 0:
                brand = page.locator(selector).first.inner_text().strip()
                if brand and 'brand' not in brand.lower():
                    info['brand'] = brand
                    break
        except Exception:
            continue
    
    # 评分
    try:
        rating_selector = 'span.a-icon-alt'
        if page.locator(rating_selector).count() > 0:
            rating_text = page.locator(rating_selector).first.inner_text()
            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
            if rating_match:
                info['rating'] = float(rating_match.group(1))
    except Exception:
        pass
    
    # 评论数
    try:
        review_selectors = [
            '#acrCustomerReviewText',
            'span[data-hook="total-review-count"]'
        ]
        for selector in review_selectors:
            if page.locator(selector).count() > 0:
                review_text = page.locator(selector).first.inner_text()
                review_match = re.search(r'([\d,]+)', review_text)
                if review_match:
                    info['review_count'] = int(review_match.group(1).replace(',', ''))
                    break
    except Exception:
        pass
    
    return info


def extract_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取商品信息和SKU变体
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        Tuple[商品信息, SKU列表]
    """
    try:
        # 等待页面加载
        page.wait_for_load_state('networkidle', timeout=10000)
        time.sleep(2)  # 额外等待动态内容加载
    except Exception:
        pass
    
    # 提取基本信息
    product_info = extract_product_info(page, url)
    
    # 提取价格
    price = None
    currency = None
    price_selectors = get_price_selectors()
    
    for selector in price_selectors:
        try:
            if page.locator(selector).count() > 0:
                price_text = page.locator(selector).first.inner_text()
                if price_text:
                    extracted_price, extracted_currency = parse_price_text(price_text)
                    if extracted_price is not None:
                        price = extracted_price
                        currency = extracted_currency
                        break
        except Exception:
            continue
    
    # 如果没有检测到货币，根据区域设置默认货币
    if currency is None:
        region = detect_amazon_region(url)
        currency_map = {
            'US': 'USD', 'UK': 'GBP', 'DE': 'EUR', 'FR': 'EUR',
            'IT': 'EUR', 'ES': 'EUR', 'CA': 'CAD', 'AU': 'AUD',
            'JP': 'JPY', 'IN': 'INR', 'BR': 'BRL', 'MX': 'MXN',
            'CN': 'CNY', 'SG': 'SGD'
        }
        currency = currency_map.get(region, 'USD')
    
    domain = urlparse(url).netloc
    
    # 构建SPU信息
    spu: Dict[str, Any] = {
        "name": product_info.get('title', ''),
        "url": url,
        "source_domain": domain,
        "category": None,  # 可以后续扩展分类提取
        "price": price,
        "currency": currency,
        "attributes": {
            "asin": product_info.get('asin'),
            "brand": product_info.get('brand'),
            "rating": product_info.get('rating'),
            "review_count": product_info.get('review_count'),
            "region": detect_amazon_region(url)
        }
    }
    
    # 提取SKU变体
    skus: List[Dict[str, Any]] = []
    
    # 尝试提取颜色/尺寸等变体
    variation_selectors = [
        '#twister [data-asin]',
        '[data-defaultasin]',
        '.swatches li[data-asin]',
        '.variation_color_name [data-asin]',
        '.variation_size_name [data-asin]'
    ]
    
    for selector in variation_selectors:
        try:
            if page.locator(selector).count() > 0:
                items = page.locator(selector)
                for i in range(min(items.count(), 20)):  # 限制最多20个变体
                    try:
                        el = items.nth(i)
                        sku_asin = el.get_attribute('data-asin') or el.get_attribute('data-defaultasin')
                        if not sku_asin:
                            continue
                        
                        # 获取变体名称
                        text = el.text_content() or el.get_attribute('title') or el.get_attribute('alt')
                        
                        # 获取变体链接
                        href = el.get_attribute('href')
                        if href and not href.startswith('http'):
                            href = f"https://{domain}{href}"
                        
                        # 获取其他属性
                        attrs: Dict[str, Any] = {}
                        if el.get_attribute('data-csa-c-type'):
                            attrs['csa_type'] = el.get_attribute('data-csa-c-type')
                        if el.get_attribute('data-dp-url'):
                            attrs['dp_url'] = el.get_attribute('data-dp-url')
                        
                        skus.append({
                            "asin": sku_asin,
                            "name": (text or '').strip(),
                            "url": href,
                            "attributes": attrs if attrs else None,
                        })
                    except Exception:
                        continue
                break  # 找到变体就停止
        except Exception:
            continue
    
    return spu, skus


def is_amazon_product_page(url: str) -> bool:
    """
    检查URL是否为Amazon商品页面
    
    Args:
        url: 要检查的URL
    
    Returns:
        是否为Am
    """
    if not url:
        return False
    
    # 检查域名
    domain = urlparse(url).netloc.lon    amazon_domains = [
        'amazon.com', 'amazon.co.uk', 'amazon.de', 'amazon.fr',
        'amazon.it', 'ama.es', 'amazon.ca', 'amazon.com.au',
        'amazon.jp', 'amazon.in', 'amazon.com.br', 'amazon.com.mx',
      .cn', 'amazon.sg'
    ]
    
    is_n = any(d in domain for d in amazon_domains)
   not is_amazon:
        return False
    
    # 检查路径是否包含商品标识
    path = urlparse(u.lower()
    product_indicators = ['/dp/', '/gp/product/', '/product/', '/asin/']
    
    return any(indicator in path for indicator in product_indicators)


def extract_amazon_asin(url: str) -> Optional[str]:
    """
    从Amazon URL中提取ASIN
    
    Args:
        url: Amazon
    Returns:
        ASIN或None
    """
    if not url:
        return None
    
    # 常见的Amazon ASIN模式
    = [
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'/product/([A-Z0-9]{10})',
        r'asin=([]{10})',
        r'/([A-Z0-9]{10})(?:/|$|\\?)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return None
