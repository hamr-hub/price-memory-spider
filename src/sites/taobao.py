"""
淘宝网站价格抓取模块
支持淘宝商品页面的价格抓取
"""
import re
import time
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import Page


def parse_taobao_price(text: Optional[str]) -> Tuple[Optional[float], str]:
    """
    解析淘宝价格文本
    
    Args:
        text: 价格文本
    
    Returns:
        Tuple[价格, 货币代码]
    """
    if not text:
        return None, "CNY"
    
    # 清理文本
    s = text.strip().replace('\n', ' ').replace('\t', ' ')
    s = re.sub(r'\s+', ' ', s)
    
    # 移除货币符号和其他字符
    s = re.sub(r'[￥¥元]', '', s)
    s = re.sub(r'[^\d.,\-]', '', s)
    
    if not s:
        return None, "CNY"
    
    # 处理价格区间（取最低价）
    if '-' in s:
        prices = s.split('-')
        try:
            return float(prices[0].replace(',', '')), "CNY"
        except (ValueError, IndexError):
            pass
    
    # 处理千分位分隔符
    s = s.replace(',', '')
    
    try:
        return float(s), "CNY"
    except (ValueError, TypeError):
        return None, "CNY"


def extract_taobao_item_id(url: str) -> Optional[str]:
    """
    从淘宝URL中提取商品ID
    
    Args:
        url: 淘宝URL
    
    Returns:
        商品ID或None
    """
    if not url:
        return None
    
    # 解析URL参数
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    # 尝试从参数中获取ID
    if 'id' in query_params:
        return query_params['id'][0]
    
    # 尝试从路径中提取
    path_patterns = [
        r'/item/(\d+)\.htm',
        r'/detail/(\d+)\.htm',
        r'item_id=(\d+)',
        r'id=(\d+)'
    ]
    
    for pattern in path_patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def get_taobao_price_selectors() -> List[str]:
    """
    获取淘宝价格选择器列表
    """
    return [
        # 新版淘宝
        '.Price--priceText--rpddjI',
        '.price .number',
        '[data-spm="price"]',
        
        # 旧版淘宝
        '.tb-rmb-num',
        '.tm-price-cur',
        '.tm-price',
        '.price-current',
        
        # 手机版
        '.price-now',
        '.current-price',
        
        # 通用选择器
        '[class*="price"]',
        '[class*="Price"]'
    ]


def extract_taobao_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取淘宝商品信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        商品信息字典
    """
    info = {}
    
    # 商品标题
    title_selectors = [
        '.ItemTitle--mainTitle--ICZjDq',
        '.tb-main-title',
        '.item-title',
        'h1[data-spm="1000983"]',
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
    
    # 商品ID
    item_id = extract_taobao_item_id(url)
    if item_id:
        info['item_id'] = item_id
    
    # 店铺名称
    shop_selectors = [
        '.ShopHeader--shopName--1WJY1Q',
        '.shop-name',
        '.seller-name'
    ]
    
    for selector in shop_selectors:
        try:
            if page.locator(selector).count() > 0:
                shop = page.locator(selector).first.inner_text().strip()
                if shop:
                    info['shop_name'] = shop
                    break
        except Exception:
            continue
    
    # 销量
    try:
        sales_selectors = [
            '[class*="sales"]',
            '.SalesInfo--sales--3aXhJ',
            '.tm-ind-sellCount'
        ]
        for selector in sales_selectors:
            if page.locator(selector).count() > 0:
                sales_text = page.locator(selector).first.inner_text()
                sales_match = re.search(r'(\d+)', sales_text.replace(',', ''))
                if sales_match:
                    info['sales_count'] = int(sales_match.group(1))
                    break
    except Exception:
        pass
    
    return info


def extract_taobao_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取淘宝商品信息和SKU变体
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        Tuple[商品信息, SKU列表]
    """
    try:
        # 等待页面加载
        page.wait_for_load_state('networkidle', timeout=15000)
        time.sleep(3)  # 淘宝页面需要更多时间加载
    except Exception:
        pass
    
    # 提取基本信息
    product_info = extract_taobao_product_info(page, url)
    
    # 提取价格
    price = None
    currency = "CNY"
    price_selectors = get_taobao_price_selectors()
    
    for selector in price_selectors:
        try:
            if page.locator(selector).count() > 0:
                price_text = page.locator(selector).first.inner_text()
                if price_text:
                    extracted_price, extracted_currency = parse_taobao_price(price_text)
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
        "currency": currency,
        "attributes": {
            "item_id": product_info.get('item_id'),
            "shop_name": product_info.get('shop_name'),
            "sales_count": product_info.get('sales_count'),
            "platform": "taobao"
        }
    }
    
    # 提取SKU变体（淘宝的SKU比较复杂，这里简化处理）
    skus: List[Dict[str, Any]] = []
    
    try:
        # 尝试提取规格选项
        sku_selectors = [
            '.SkuSelector--skuItem--3VKQyy',
            '.tb-sku li',
            '.sku-item'
        ]
        
        for selector in sku_selectors:
            if page.locator(selector).count() > 0:
                items = page.locator(selector)
                for i in range(min(items.count(), 10)):  # 限制最多10个变体
                    try:
                        el = items.nth(i)
                        sku_text = el.inner_text().strip()
                        if sku_text:
                            skus.append({
                                "asin": None,  # 淘宝没有ASIN
                                "name": sku_text,
                                "url": url,  # 淘宝SKU通常共享同一URL
                                "attributes": {
                                    "sku_text": sku_text,
                                    "platform": "taobao"
                                }
                            })
                    except Exception:
                        continue
                break
    except Exception:
        pass
    
    return spu, skus


def is_taobao_product_page(url: str) -> bool:
    """
    检查URL是否为淘宝商品页面
    
    Args:
        url: 要检查的URL
    
    Returns:
        是否为淘宝商品页面
    """
    if not url:
        return False
    
    domain = urlparse(url).netloc.lower()
    taobao_domains = ['taobao.com', 'tmall.com', 'detail.tmall.com', 'item.taobao.com']
    
    is_taobao = any(d in domain for d in taobao_domains)
    if not is_taobao:
        return False
    
    # 检查路径
    path = urlparse(url).path.lower()
    return '/item/' in path or '/detail/' in path or 'item_id=' in url