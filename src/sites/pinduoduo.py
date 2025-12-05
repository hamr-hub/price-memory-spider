"""
拼多多网站价格抓取模块
支持拼多多主站的价格抓取和商品信息提取
"""
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page


def parse_pinduoduo_price_text(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    解析拼多多价格文本，提取价格和货币
    
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
    
    currency = "CNY"  # 拼多多默认人民币
    
    # 移除货币符号和单位
    s = re.sub(r'[¥￥元]', '', s)
    
    if not s:
        return None, currency
    
    # 处理千分位分隔符和小数点
    s = s.replace(',', '')
    
    # 提取数字
    price_match = re.search(r'(\d+(?:\.\d{1,2})?)', s)
    if not price_match:
        return None, currency
    
    try:
        price = float(price_match.group(1))
        return price, currency
    except (ValueError, TypeError):
        return None, currency


def extract_pinduoduo_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取拼多多商品基本信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
        
    Returns:
        商品信息字典
    """
    info = {}
    
    try:
        # 商品标题
        title_selectors = [
            '.goods-name',
            '.product-title',
            'h1.goods-title',
            '.sku-name',
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
        
        # 商品ID（从URL或页面元素获取）
        try:
            # 从URL提取商品ID
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.split('/')
            for part in path_parts:
                if part.startswith('goods_id=') or part.startswith('goodsId='):
                    info['goods_id'] = part.split('=')[1]
                    break
            
            # 如果URL中没有，尝试从页面获取
            if 'goods_id' not in info:
                id_selectors = [
                    '[data-goods-id]',
                    '[data-product-id]',
                    '.goods-id',
                    '#goods_id'
                ]
                for selector in id_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            goods_id = page.locator(selector).first.get_attribute('data-goods-id') or \
                                     page.locator(selector).first.get_attribute('data-product-id') or \
                                     page.locator(selector).first.inner_text()
                            if goods_id:
                                info['goods_id'] = str(goods_id).strip()
                                break
                    except Exception:
                        continue
        except Exception:
            pass
        
        # 店铺信息
        shop_selectors = [
            '.shop-name',
            '.store-name',
            '.merchant-name',
            '.shop-title'
        ]
        
        for selector in shop_selectors:
            try:
                if page.locator(selector).count() > 0:
                    shop_name = page.locator(selector).first.inner_text().strip()
                    if shop_name and '店铺' not in shop_name.lower():
                        info['shop_name'] = shop_name
                        break
            except Exception:
                continue
        
        # 商品分类（如果有）
        try:
            category_selectors = [
                '.breadcrumb .category',
                '.goods-category',
                '.product-category'
            ]
            for selector in category_selectors:
                if page.locator(selector).count() > 0:
                    category = page.locator(selector).first.inner_text().strip()
                    if category:
                        info['category'] = category
                        break
        except Exception:
            pass
        
        # 商品图片
        try:
            image_selectors = [
                '.goods-image img',
                '.product-image img',
                '.main-image img',
                '#goods-image img'
            ]
            for selector in image_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        image_url = page.locator(selector).first.get_attribute('src') or \
                                  page.locator(selector).first.get_attribute('data-src')
                        if image_url:
                            info['image_url'] = image_url
                            break
                except Exception:
                    continue
        except Exception:
            pass
        
        # 库存信息
        try:
            stock_selectors = [
                '.stock-count',
                '.inventory',
                '.goods-stock',
                '.remaining-stock'
            ]
            for selector in stock_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        stock_text = page.locator(selector).first.inner_text().strip()
                        if stock_text:
                            info['stock'] = stock_text
                            break
                except Exception:
                    continue
        except Exception:
            pass
    
    except Exception as e:
        print(f"提取拼多多商品信息时出错: {e}")
    
    return info


def extract_pinduoduo_price(page: Page) -> Tuple[Optional[float], Optional[str]]:
    """
    从拼多多页面提取价格
    
    Args:
        page: Playwright页面对象
        
    Returns:
        Tuple[价格, 货币代码]
    """
    try:
        # 等待价格元素加载
        page.wait_for_load_state('networkidle', timeout=10000)
        time.sleep(2)  # 额外等待动态内容加载
        
        # 拼多多价格选择器列表（按优先级排序）
        price_selectors = [
            # 主价格显示
            '.p-price .price',
            '.goods-price .price',
            '.m-product-price .price',
            '.current-price',
            '.sale-price',
            
            # 活动价格
            '.activity-price .price',
            '.promotion-price .price',
            '.seckill-price .price',
            
            # 原价/市场价
            '.original-price .price',
            '.market-price .price',
            '.line-through-price',
            
            # 通用价格选择器
            '.price',
            '.amount',
            '.money',
            '[class*="price"]',
            '[data-price]'
        ]
        
        # 尝试提取价格
        for selector in price_selectors:
            try:
                elements = page.locator(selector)
                if elements.count() > 0:
                    for i in range(min(elements.count(), 5)):  # 最多检查前5个元素
                        try:
                            price_text = elements.nth(i).inner_text().strip()
                            if price_text:
                                price, currency = parse_pinduoduo_price_text(price_text)
                                if price is not None:
                                    return price, currency
                        except Exception:
                            continue
            except Exception:
                continue
        
        # 如果上述方法都失败，尝试使用JavaScript提取
        try:
            price_data = page.evaluate("""
                () => {
                    const selectors = [
                        '.p-price', '.goods-price', '.current-price', '.sale-price',
                        '.activity-price', '.promotion-price', '.price', '.amount'
                    ];
                    
                    let price = null;
                    
                    for (const selector of selectors) {
                        const elements = document.querySelectorAll(selector);
                        for (const element of elements) {
                            const text = element.textContent || element.innerText;
                            const match = text.match(/¥\\s*(\\d+(?:\\.\\d{1,2})?)/);
                            if (match) {
                                price = parseFloat(match[1]);
                                break;
                            }
                        }
                        if (price) break;
                    }
                    
                    return price;
                }
            """)
            
            if price_data:
                return float(price_data), "CNY"
        
        except Exception:
            pass
        
    except Exception as e:
        print(f"提取拼多多价格时出错: {e}")
    
    return None, None


def extract_pinduoduo_skus(page: Page) -> List[Dict[str, Any]]:
    """
    提取拼多多SKU变体信息
    
    Args:
        page: Playwright页面对象
        
    Returns:
        SKU列表
    """
    skus = []
    
    try:
        # SKU选择器
        sku_selectors = [
            '.sku-item',
            '.goods-sku',
            '.product-sku',
            '.spec-item',
            '.variation-item'
        ]
        
        for selector in sku_selectors:
            try:
                elements = page.locator(selector)
                if elements.count() > 0:
                    for i in range(min(elements.count(), 20)):  # 限制最多20个SKU
                        try:
                            element = elements.nth(i)
                            sku_info = {}
                            
                            # SKU名称
                            name_selectors = ['span', 'div', '.sku-name', '.spec-name']
                            for name_selector in name_selectors:
                                try:
                                    if element.locator(name_selector).count() > 0:
                                        name = element.locator(name_selector).first.inner_text().strip()
                                        if name:
                                            sku_info['name'] = name
                                            break
                                except Exception:
                                    continue
                            
                            # SKU价格（如果有）
                            price_selectors = ['.sku-price', '.spec-price', '.price']
                            for price_selector in price_selectors:
                                try:
                                    if element.locator(price_selector).count() > 0:
                                        price_text = element.locator(price_selector).first.inner_text().strip()
                                        if price_text:
                                            price, currency = parse_pinduoduo_price_text(price_text)
                                            if price is not None:
                                                sku_info['price'] = price
                                                sku_info['currency'] = currency
                                                break
                                except Exception:
                                    continue
                            
                            # SKU图片（如果有）
                            try:
                                if element.locator('img').count() > 0:
                                    image_url = element.locator('img').first.get_attribute('src') or \
                                              element.locator('img').first.get_attribute('data-src')
                                    if image_url:
                                        sku_info['image_url'] = image_url
                            except Exception:
                                pass
                            
                            if sku_info:
                                skus.append(sku_info)
                    
                    if skus:  # 找到SKU就停止
                        break
            except Exception:
                continue
    
    except Exception as e:
        print(f"提取拼多多SKU时出错: {e}")
    
    return skus


def extract_pinduoduo_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取拼多多商品SPU和SKU信息
    
    Args:
        page: Playwright页面对象
        url: 商品URL
        
    Returns:
        Tuple[SPU信息, SKU列表]
    """
    try:
        # 等待页面加载
        page.wait_for_load_state('networkidle', timeout=15000)
        time.sleep(3)  # 额外等待动态内容加载
    except Exception:
        pass
    
    # 提取基本信息
    product_info = extract_pinduoduo_product_info(page, url)
    
    # 提取价格
    price, currency = extract_pinduoduo_price(page)
    
    # 构建SPU信息
    spu: Dict[str, Any] = {
        "name": product_info.get('title', ''),
        "url": url,
        "source_domain": urlparse(url).netloc,
        "category": product_info.get('category'),
        "price": price,
        "currency": currency,
        "attributes": {
            "goods_id": product_info.get('goods_id'),
            "shop_name": product_info.get('shop_name'),
            "stock": product_info.get('stock'),
            "image_url": product_info.get('image_url'),
            "platform": "pinduoduo"
        }
    }
    
    # 提取SKU变体
    skus = extract_pinduoduo_skus(page)
    
    return spu, skus


def is_pinduoduo_product_page(url: str) -> bool:
    """
    检查URL是否为拼多多商品页面
    
    Args:
        url: 要检查的URL
        
    Returns:
        是否为拼多多商品页面
    """
    if not url:
        return False
    
    # 检查域名
    domain = urlparse(url).netloc.lower()
    
    pinduoduo_domains = [
        'pinduoduo.com', 'yangkeduo.com', 'mobile.yangkeduo.com',
        'duo.com', 'pinduoduo.net', 'pinduoduo.net.cn'
    ]
    
    is_pinduoduo = any(d in domain for d in pinduoduo_domains)
    
    if not is_pinduoduo:
        return False
    
    # 检查路径是否包含商品标识
    path = urlparse(url).path.lower()
    product_indicators = ['/goods.html', '/goods/', '/product/', '/item/', '/detail/']
    
    return any(indicator in path for indicator in product_indicators)


def extract_pinduoduo_goods_id(url: str) -> Optional[str]:
    """
    从拼多多URL中提取商品ID
    
    Args:
        url: 拼多多商品URL
        
    Returns:
        商品ID或None
    """
    if not url:
        return None
    
    try:
        # 从查询参数提取
        parsed_url = urlparse(url)
        query_params = parsed_url.query.split('&')
        
        for param in query_params:
            if 'goods_id=' in param or 'goodsId=' in param:
                return param.split('=')[1]
        
        # 从路径提取
        path_parts = parsed_url.path.split('/')
        for part in path_parts:
            if part.isdigit() and len(part) > 6:  # 商品ID通常是较长的数字
                return part
        
        # 从特定路径格式提取
        goods_match = re.search(r'/goods/(\\d+)/', parsed_url.path)
        if goods_match:
            return goods_match.group(1)
    
    except Exception:
        pass
    
    return None


def detect_pinduoduo_promotion(page: Page) -> Dict[str, Any]:
    """
    检测拼多多促销信息
    
    Args:
        page: Playwright页面对象
        
    Returns:
        促销信息字典
    """
    promotion_info = {}
    
    try:
        # 促销标签
        promotion_selectors = [
            '.promotion-tag',
            '.activity-tag',
            '.coupon-tag',
            '.seckill-tag',
            '.flash-sale-tag'
        ]
        
        for selector in promotion_selectors:
            try:
                elements = page.locator(selector)
                if elements.count() > 0:
                    tags = []
                    for i in range(min(elements.count(), 10)):
                        try:
                            tag_text = elements.nth(i).inner_text().strip()
                            if tag_text:
                                tags.append(tag_text)
                        except Exception:
                            continue
                    if tags:
                        promotion_info['tags'] = tags
                        break
            except Exception:
                continue
        
        # 优惠券信息
        try:
            coupon_selectors = [
                '.coupon-item',
                '.discount-coupon',
                '.promotion-coupon'
            ]
            for selector in coupon_selectors:
                elements = page.locator(selector)
                if elements.count() > 0:
                    coupons = []
                    for i in range(min(elements.count(), 5)):
                        try:
                            coupon_text = elements.nth(i).inner_text().strip()
                            if coupon_text:
                                coupons.append(coupon_text)
                        except Exception:
                            continue
                    if coupons:
                        promotion_info['coupons'] = coupons
                        break
        except Exception:
            pass
        
        # 满减信息
        try:
            manjian_selectors = [
                '.manjian',
                '.full-reduction',
                '.满减',
                '.满减优惠'
            ]
            for selector in manjian_selectors:
                elements = page.locator(selector)
                if elements.count() > 0:
                    manjian_text = elements.first.inner_text().strip()
                    if manjian_text:
                        promotion_info['manjian'] = manjian_text
                        break
        except Exception:
            pass
    
    except Exception as e:
        print(f"检测拼多多促销信息时出错: {e}")
    
    return promotion_info