"""
京东网站价格抓取模块
支持京东商品页面的价格抓取
"""
import re
import time
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page


def parse_jd_price(text: Optional[str]) -> Tuple[Optional[float], str]:
    """
    解析京东价格文本
    
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


def extract_jd_sku_id(url: str) -> Optional[str]:
    """
    从京东URL中提取SKU ID
    
    Args:
        url: 京东URL
    
    Returns:
        SKU ID或None
    """
    if not url:
        return None
    
    # 京东商品ID模式
    patterns = [
        r'/(\d+)\.html',
        r'product/(\d+)\.html',
        r'item/(\d+)\.html',
        r'skuId=(\d+)',
        r'/(\d+)$'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def get_jd_price_selectors() -> List[str]:
    """
    获取京东价格选择器列表
    """
    return [
        # 新版京东
        '.price .p-price .price',
        '.summary-price .p-price .price',
        '.price-current',
        
        # 旧版京东
        '.jd-price',
        '.price .price',
        '.p-price .price',
        
        # 手机版
        '.current-price',
        '.price-now',
        
        # 通用选择器
        '[class*="price"]',
        '[data-price]'
    ]


def extract_jd_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取京东商品信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        商品信息字典
    """
    info = {}
    
    # 商品标题
    title_selectors = [
        '.sku-name',
        '.product-intro .name',
        'h1[data-hook="product-title"]',
        '.item-title',
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
    
    # SKU ID
    sku_id = extract_jd_sku_id(url)
    if sku_id:
        info['sku_id'] = sku_id
    
    # 品牌
    brand_selectors = [
        '.parameter2 li:has-text("品牌")',
        '.brand-name',
        '[data-hook="brand"]'
    ]
    
    for selector in brand_selectors:
        try:
            if page.locator(selector).count() > 0:
                brand_text = page.locator(selector).first.inner_text()
                # 提取品牌名称
                brand_match = re.search(r'品牌[：:]\s*(.+)', brand_text)
                if brand_match:
                    info['brand'] = brand_match.group(1).strip()
                    break
        except Exception:
            continue
    
    # 评论数
    try:
        comment_selectors = [
            '.comment-count',
            '.comment-item .count',
            '[data-hook="comment-count"]'
        ]
        for selector in comment_selectors:
            if page.locator(selector).count() > 0:
                comment_text = page.locator(selector).first.inner_text()
                comment_match = re.search(r'(\d+)', comment_text.replace(',', ''))
                if comment_match:
                    info['comment_count'] = int(comment_match.group(1))
                    break
    except Exception:
        pass
    
    # 好评率
    try:
        rating_selectors = [
            '.percent-con',
            '.good-rate'
        ]
        for selector in rating_selectors:
            if page.locator(selector).count() > 0:
                rating_text = page.locator(selector).first.inner_text()
                rating_match = re.search(r'(\d+)%', rating_text)
                if rating_match:
                    info['good_rate'] = int(rating_match.group(1))
                    break
    except Exception:
        pass
    
    return info


def extract_jd_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取京东商品信息和SKU变体
    
    Args:
        page: Playwright页面对象
        url: 商品URL
    
    Returns:
        Tuple[商品信息, SKU列表]
    """
    try:
        # 等待页面加载
        page.wait_for_load_state('networkidle', timeout=15000)
        time.sleep(3)
    except Exception:
        pass
    
    # 提取基本信息
    product_info = extract_jd_product_info(page, url)
    
    # 提取价格
    price = None
    currency = "CNY"
    price_selectors = get_jd_price_selectors()
    
    for selector in price_selectors:
        try:
            if page.locator(selector).count() > 0:
                price_text = page.locator(selector).first.inner_text()
                if price_text:
                    extracted_price, extracted_currency = parse_jd_price(price_text)
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
            "sku_id": product_info.get('sku_id'),
            "brand": product_info.get('brand'),
            "comment_count": product_info.get('comment_count'),
            "good_rate": product_info.get('good_rate'),
            "platform": "jd"
        }
    }
    
    # 提取SKU变体
    skus: List[Dict[str, Any]] = []
    
    try:
        # 提取颜色/规格选项
        sku_selectors = [
            '.choose-attrs .item',
            '.choose-color .item',
            '.choose-version .item',
            '.sku-item'
        ]
        
        for selector in sku_selectors:
            if page.locator(selector).count() > 0:
                items = page.locator(selector)
                for i in range(min(items.count(), 15)):  # 限制最多15个变体
                    try:
                        el = items.nth(i)
                        sku_text = el.inner_text().strip()
                        sku_data_sku = el.get_attribute('data-sku')
                        
                        if sku_text:
                            skus.append({
                                "asin": sku_data_sku,  # 京东使用data-sku
                                "name": sku_text,
                                "url": url,  # 京东SKU通常共享同一URL
                                "attributes": {
                                    "sku_text": sku_text,
                                    "data_sku": sku_data_sku,
                                    "platform": "jd"
                                }
                            })
                    except Exception:
                        continue
                break
    except Exception:
        pass
    
    return spu, skus


def is_jd_product_page(url: str) -> bool:
    """
    检查URL是否为京东商品页面
    
    Args:
        url: 要检查的URL
    
    Returns:
        是否为京东商品页面
    """
    if not url:
        return False
    
    domain = urlparse(url).netloc.lower()
    jd_domains = ['jd.com', 'item.jd.com', 'product.jd.com']
    
    is_jd = any(d in domain for d in jd_domains)
    if not is_jd:
        return False
    
    # 检查路径
    path = urlparse(url).path.lower()
    return '.html' in path or '/product/' in path