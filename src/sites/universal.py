"""
通用网站价格抓取模块
支持通用电商网站的价格抓取
"""
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page
from .amazon import extract_spu_and_skus as extract_amazon, is_amazon_product_page
from .taobao import extract_taobao_spu_and_skus as extract_taobao, is_taobao_product_page
from .jd import extract_jd_spu_and_skus as extract_jd, is_jd_product_page


def detect_site_type(url: str) -> str:
    """
    检测网站类型
    
    Args:
        url: 网站URL
    
    Returns:
        网站类型 (amazon, taobao, jd, generic)
    """
    if is_amazon_product_page(url):
        return "amazon"
    elif is_taobao_product_page(url):
        return "taobao"
    elif is_jd_product_page(url):
        return "jd"
    else:
        return "generic"


def parse_generic_price(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    通用价格解析函数
    
    Args:
        text: 价格文本
    
    Returns:
        Tuple[价格, 货币代码]
    """
    if not text:
        return None, None
    
    # 清理文本
    s = text.strip().replace('\n', ' ').replace('\t', ' ')
    s = re.sub(r'\s+', ' ', s)
    
    currency = None
    
    # 检测货币符号
    if '$' in s:
        currency = "USD"
    elif '£' in s:
        currency = "GBP"
    elif '€' in s:
        currency = "EUR"
    elif '￥' in s or '¥' in s:
        currency = "CNY"
    elif '₹' in s:
        currency = "INR"
    elif '₽' in s:
        currency = "RUB"
    elif '₩' in s:
        currency = "KRW"
    
    # 提取数字
    price_pattern = r'[\d,]+\.?\d*'
    matches = re.findall(price_pattern, s)
    
    if matches:
        # 取第一个匹配的价格
        price_str = matches[0].replace(',', '')
        try:
            price = float(price_str)
            return price, currency
        except (ValueError, TypeError):
            pass
    
    return None, currency


def get_generic_price_selectors() -> List[str]:
    """
    获取通用价格选择器列表
    """
    return [
        # 常见的价格类名
        '.price',
        '.current-price',
        '.sale-price',
        '.product-price',
        '.price-current',
        '.price-now',
        '.final-price',
        '.regular-price',
        
        # 常见的价格ID
        '#price',
        '#current-price',
        '#product-price',
        
        # 数据属性
        '[data-price]',
        '[data-current-price]',
        
        # 包含price的类名
        '[class*="price"]',
        '[class*="Price"]',
        
        # 货币符号附近的元素
        '*:has-text("$")',
        '*:has-text("￥")',
        '*:has-text("¥")',
        '*:has-text("€")',
        '*:has-text("£")'
    ]


def get_generic_title_selectors() -> List[str]:
    """
    获取通用标题选择器列表
    """
    return [
        'h1',
        '.product-title',
        '.product-name',
        '.item-title',
        '.title',
        '[data-testid="product-title"]',
        '.product-info h1',
        '.product-details h1',
        '[class*="title"]',
        '[class*="Title"]'
    ]


def extract_generic_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取通用商品信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        商品信息字典
    """
    info = {}
    
    # 商品标题
    title_selectors = get_generic_title_selectors()
    
    for selector in title_selectors:
        try:
            if page.locator(selector).count() > 0:
                title = page.locator(selector).first.inner_text().strip()
                if title and len(title) > 5:  # 过滤太短的标题
                    info['title'] = title
                    break
        except Exception:
            continue
    
    # 如果没有找到标题，尝试从页面标题获取
    if 'title' not in info:
        try:
            page_title = page.title()
            if page_title:
                info['title'] = page_title
        except Exception:
            pass
    
    # 尝试提取品牌
    brand_selectors = [
        '.brand',
        '.brand-name',
        '.manufacturer',
        '[data-brand]',
        '[class*="brand"]'
    ]
    
    for selector in brand_selectors:
        try:
            if page.locator(selector).count() > 0:
                brand = page.locator(selector).first.inner_text().strip()
                if brand:
                    info['brand'] = brand
                    break
        except Exception:
            continue
    
    # 尝试提取描述
    desc_selectors = [
        '.product-description',
        '.description',
        '.product-summary',
        '[class*="description"]'
    ]
    
    for selector in desc_selectors:
        try:
            if page.locator(selector).count() > 0:
                desc = page.locator(selector).first.inner_text().strip()
                if desc and len(desc) > 10:
                    info['description'] = desc[:500]  # 限制长度
                    break
        except Exception:
            continue
    
    return info


def extract_generic_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取通用网站商品信息和SKU变体
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        Tuple[商品信息, SKU列表]
    """
    try:
        # 等待页面加载
        page.wait_for_load_state('networkidle', timeout=10000)
        time.sleep(2)
    except Exception:
        pass
    
    # 提取基本信息
    product_info = extract_generic_product_info(page, url)
    
    # 提取价格
    price = None
    currency = None
    price_selectors = get_generic_price_selectors()
    
    for selector in price_selectors:
        try:
            if page.locator(selector).count() > 0:
                price_element = page.locator(selector).first
                
                # 尝试从data属性获取价格
                data_price = price_element.get_attribute('data-price')
                if data_price:
                    try:
                        price = float(data_price)
                        break
                    except (ValueError, TypeError):
                        pass
                
                # 从文本内容获取价格
                price_text = price_element.inner_text()
                if price_text:
                    extracted_price, extracted_currency = parse_generic_price(price_text)
                    if extracted_price is not None:
                        price = extracted_price
                        currency = extracted_currency
                        break
        except Exception:
            continue
    
    domain = urlparse(url).netloc
    
    # 构建SPU信息
    spu: Dict[str, Any] = {
        "name": product_info.get('title', ''),
        "url": url,
        "source_domain": domain,
        "category": None,
        "price": price,
        "currency": currency or "USD",  # 默认USD
        "attributes": {
            "brand": product_info.get('brand'),
            "description": product_info.get('description'),
            "platform": "generic"
        }
    }
    
    # 对于通用网站，SKU变体较难提取，这里简化处理
    skus: List[Dict[str, Any]] = []
    
    return spu, skus


def extract_product_data(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    根据网站类型提取商品数据
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        Tuple[商品信息, SKU列表]
    """
    site_type = detect_site_type(url)
    
    try:
        if site_type == "amazon":
            return extract_amazon(page, url)
        elif site_type == "taobao":
            return extract_taobao(page, url)
        elif site_type == "jd":
            return extract_jd(page, url)
        else:
            return extract_generic_spu_and_skus(page, url)
    except Exception as e:
        print(f"提取商品数据失败: {e}")
        # 回退到通用提取
        return extract_generic_spu_and_skus(page, url)


def is_supported_ecommerce_site(url: str) -> bool:
    """
    检查是否为支持的电商网站
    
    Args:
        url: 网站URL
    
    Returns:
        是否为支持的电商网站
    """
    if not url:
        return False
    
    # 检查已知的电商网站
    if (is_amazon_product_page(url) or 
        is_taobao_product_page(url) or 
        is_jd_product_page(url)):
        return True
    
    # 检查其他常见电商域名
    domain = urlparse(url).netloc.lower()
    ecommerce_domains = [
        'ebay.com', 'aliexpress.com', 'shopify.com', 'etsy.com',
        'walmart.com', 'target.com', 'bestbuy.com', 'newegg.com',
        'pdd.com', 'pinduoduo.com', 'suning.com', 'gome.com.cn',
        'dangdang.com', 'yhd.com', 'vip.com'
    ]
    
    return any(d in domain for d in ecommerce_domains)