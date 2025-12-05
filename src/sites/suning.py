"""
苏宁易购网站价格抓取模块
支持苏宁易购主站的价格抓取和商品信息提取
"""
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import Page


def parse_suning_price_text(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    解析苏宁易购价格文本，提取价格和货币
    
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
    
    currency = "CNY"  # 苏宁易购默认人民币
    
    # 移除货币符号和单位
    s = re.sub(r'[¥￥元]', '', s)
    
    if not s:
        return None, currency
    
    # 处理千分位分隔符
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


def extract_suning_product_info(page: Page, url: str) -> Dict[str, Any]:
    """
    提取苏宁易购商品基本信息
    
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
            '.product-title',
            '.sku-name',
            'h1.title',
            '.goods-title',
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
            
            # 苏宁易购商品ID通常在路径中
            for part in path_parts:
                if part.isdigit() and len(part) > 6:
                    info['product_id'] = part
                    break
            
            # 如果URL中没有，尝试从页面获取
            if 'product_id' not in info:
                id_selectors = [
                    '[data-product-id]',
                    '[data-sku-id]',
                    '[data-goods-id]',
                    '.product-id',
                    '#product_id'
                ]
                for selector in id_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            product_id = page.locator(selector).first.get_attribute('data-product-id') or \
                                       page.locator(selector).first.get_attribute('data-sku-id') or \
                                       page.locator(selector).first.get_attribute('data-goods-id') or \
                                       page.locator(selector).first.inner_text()
                            if product_id:
                                info['product_id'] = str(product_id).strip()
                                break
                    except Exception:
                        continue
        except Exception:
            pass
        
        # 店铺/商家信息
        shop_selectors = [
            '.shop-name',
            '.merchant-name',
            '.seller-name',
            '.store-name'
        ]
        
        for selector in shop_selectors:
            try:
                if page.locator(selector).count() > 0:
                    shop_name = page.locator(selector).first.inner_text().strip()
                    if shop_name and '店铺' not in shop_name.lower() and '商家' not in shop_name.lower():
                        info['shop_name'] = shop_name
                        break
            except Exception:
                continue
        
        # 品牌信息
        brand_selectors = [
            '.brand-name',
            '.product-brand',
            '.goods-brand',
            '[itemprop="brand"]'
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
        
        # 商品分类
        try:
            category_selectors = [
                '.breadcrumb .category',
                '.goods-category',
                '.product-category',
                '.crumb-category'
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
                '#goods-image img',
                '.product-img img'
            ]
            for selector in image_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        image_url = page.locator(selector).first.get_attribute('src') or \
                                  page.locator(selector).first.get_attribute('data-src') or \
                                  page.locator(selector).first.get_attribute('data-original')
                        if image_url:
                            info['image_url'] = image_url
                            break
                except Exception:
                    continue
        except Exception:
            pass
        
        # 库存状态
        try:
            stock_selectors = [
                '.stock-status',
                '.inventory-status',
                '.goods-stock',
                '.availability'
            ]
            for selector in stock_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        stock_text = page.locator(selector).first.inner_text().strip()
                        if stock_text:
                            info['stock_status'] = stock_text
                            break
                except Exception:
                    continue
        except Exception:
            pass
        
        # 评价信息
        try:
            review_selectors = [
                '.review-count',
                '.comment-count',
                '.evaluation-count',
                '.goods-comment'
            ]
            for selector in review_selectors:
                try:
                    if page.locator(selector).count() > 0:
                        review_text = page.locator(selector).first.inner_text().strip()
                        if review_text:
                            info['review_count'] = review_text
                            break
                except Exception:
                    continue
        except Exception:
            pass
    
    except Exception as e:
        print(f"提取苏宁易购商品信息时出错: {e}")
    
    return info


def extract_suning_price(page: Page) -> Tuple[Optional[float], Optional[str]]:
    """
    从苏宁易购页面提取价格
    
    Args:
        page: Playwright页面对象
        
    Returns:
        Tuple[价格, 货币代码]
    """
    try:
        # 等待价格元素加载
        page.wait_for_load_state('networkidle', timeout=10000)
        time.sleep(2)  # 额外等待动态内容加载
        
        # 苏宁易购价格选择器列表（按优先级排序）
        price_selectors = [
            # 主价格显示
            '.price .current-price',
            '.goods-price .price',
            '.product-price .current',
            '.sale-price',
            '.selling-price',
            
            # 会员价格
            '.vip-price .price',
            '.member-price .price',
            '.gold-price .price',
            
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
                                price, currency = parse_suning_price_text(price_text)
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
                        '.price', '.current-price', '.sale-price', '.selling-price',
                        '.vip-price', '.member-price', '.activity-price', '.promotion-price'
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
        print(f"提取苏宁易购价格时出错: {e}")
    
    return None, None


def extract_suning_skus(page: Page) -> List[Dict[str, Any]]:
    """
    提取苏宁易购SKU变体信息
    
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
            '.variation-item',
            '.goods-variation'
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
                            name_selectors = ['span', 'div', '.sku-name', '.spec-name', '.variation-name']
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
                            price_selectors = ['.sku-price', '.spec-price', '.variation-price', '.price']
                            for price_selector in price_selectors:
                                try:
                                    if element.locator(price_selector).count() > 0:
                                        price_text = element.locator(price_selector).first.inner_text().strip()
                                        if price_text:
                                            price, currency = parse_suning_price_text(price_text)
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
                                              element.locator('img').first.get_attribute('data-src') or \
                                              element.locator('img').first.get_attribute('data-original')
                                    if image_url:
                                        sku_info['image_url'] = image_url
                            except Exception:
                                pass
                            
                            # SKU库存（如果有）
                            try:
                                if element.locator('.sku-stock').count() > 0:
                                    stock_text = element.locator('.sku-stock').first.inner_text().strip()
                                    if stock_text:
                                        sku_info['stock'] = stock_text
                            except Exception:
                                pass
                            
                            if sku_info:
                                skus.append(sku_info)
                    
                    if skus:  # 找到SKU就停止
                        break
            except Exception:
                continue
    
    except Exception as e:
        print(f"提取苏宁易购SKU时出错: {e}")
    
    return skus


def extract_suning_spu_and_skus(page: Page, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    提取苏宁易购商品SPU和SKU信息
    
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
    product_info = extract_suning_product_info(page, url)
    
    # 提取价格
    price, currency = extract_suning_price(page)
    
    # 构建SPU信息
    spu: Dict[str, Any] = {
        "name": product_info.get('title', ''),
        "url": url,
        "source_domain": urlparse(url).netloc,
        "category": product_info.get('category'),
        "price": price,
        "currency": currency,
        "attributes": {
            "product_id": product_info.get('product_id'),
            "brand": product_info.get('brand'),
            "shop_name": product_info.get('shop_name'),
            "stock_status": product_info.get('stock_status'),
            "review_count": product_info.get('review_count'),
            "image_url": product_info.get('image_url'),
            "platform": "suning"
        }
    }
    
    # 提取SKU变体
    skus = extract_suning_skus(page)
    
    return spu, skus


def is_suning_product_page(url: str) -> bool:
    """
    检查URL是否为苏宁易购商品页面
    
    Args:
        url: 要检查的URL
        
    Returns:
        是否为苏宁易购商品页面
    """
    if not url:
        return False
    
    # 检查域名
    domain = urlparse(url).netloc.lower()
    
    suning_domains = [
        'suning.com', 'suning.cn', 'suning.net',
        'suning.com.cn', 'suning.net.cn'
    ]
    
    is_suning = any(d in domain for d in suning_domains)
    
    if not is_suning:
        return False
    
    # 检查路径是否包含商品标识
    path = urlparse(url).path.lower()
    product_indicators = ['/product/', '/goods/', '/item/', '/detail/', '/item-']
    
    return any(indicator in path for indicator in product_indicators)


def extract_suning_product_id(url: str) -> Optional[str]:
    """
    从苏宁易购URL中提取商品ID
    
    Args:
        url: 苏宁易购商品URL
        
    Returns:
        商品ID或None
    """
    if not url:
        return None
    
    try:
        # 从路径提取
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('/')
        
        for part in path_parts:
            # 苏宁易购商品ID通常是较长的数字
            if part.isdigit() and len(part) > 6:
                return part
        
        # 从特定路径格式提取
        product_match = re.search(r'/product/(\\d+)/', parsed_url.path)
        if product_match:
            return product_match.group(1)
        
        # 从查询参数提取
        query_params = parsed_url.query.split('&')
        for param in query_params:
            if any(key in param for key in ['product_id', 'goods_id', 'item_id', 'sku_id']):
                return param.split('=')[1]
    
    except Exception:
        pass
    
    return None


def detect_suning_promotion(page: Page) -> Dict[str, Any]:
    """
    检测苏宁易购促销信息
    
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
            '.flash-sale-tag',
            '.promotion-badge'
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
                '.promotion-coupon',
                '.user-coupon'
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
                '.满减优惠',
                '.promotion-reduction'
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
        
        # 赠品信息
        try:
            gift_selectors = [
                '.gift-item',
                '.free-gift',
                '.赠品',
                '.gift-info'
            ]
            for selector in gift_selectors:
                elements = page.locator(selector)
                if elements.count() > 0:
                    gifts = []
                    for i in range(min(elements.count(), 5)):
                        try:
                            gift_text = elements.nth(i).inner_text().strip()
                            if gift_text:
                                gifts.append(gift_text)
                        except Exception:
                            continue
                    if gifts:
                        promotion_info['gifts'] = gifts
                        break
        except Exception:
            pass
    
    except Exception as e:
        print(f"检测苏宁易购促销信息时出错: {e}")
    
    return promotion_info