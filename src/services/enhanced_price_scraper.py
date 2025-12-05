"""
增强的价格抓取器
支持多种反爬虫策略和智能价格检测
"""
import asyncio
import json
import re
import time
import random
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright_stealth import stealth_async

from ..config.config import config
from ..utils.url_util import is_valid_url, get_base_url


@dataclass
class PriceResult:
    """价格结果数据类"""
    price: Optional[float]
    currency: Optional[str]
    title: Optional[str]
    availability: Optional[str]
    original_price: Optional[float]
    discount: Optional[float]
    image_url: Optional[str]
    error: Optional[str] = None
    timestamp: Optional[datetime] = None


@dataclass
class ScrapingConfig:
    """抓取配置"""
    timeout: int = 30000
    retry_count: int = 3
    delay_range: Tuple[int, int] = (1, 3)
    use_stealth: bool = True
    use_proxy: bool = False
    headless: bool = True


class EnhancedPriceScraper:
    """增强的价格抓取器"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'average_response_time': 0
        }
    
    async def initialize(self) -> None:
        """初始化浏览器"""
        try:
            self.browser = await async_playwright().start()
            
            # 配置浏览器选项
            browser_options = {
                'headless': self.config.headless,
                'args': [
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu'
                ]
            }
            
            if self.config.use_proxy and config.PROXY_SERVER:
                browser_options['proxy'] = {
                    'server': config.PROXY_SERVER,
                    'username': config.PROXY_USERNAME,
                    'password': config.PROXY_PASSWORD
                }
            
            self.context = await self.browser.new_context(**browser_options)
            
            # 设置用户代理
            await self.context.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'DNT': '1',
                'Connection': 'keep-alive',
            })
            
            # 创建页面
            self.page = await self.context.new_page()
            
            # 应用反检测脚本
            if self.config.use_stealth:
                await stealth_async(self.page)
            
            # 设置超时
            self.page.set_default_timeout(self.config.timeout)
            
            print("浏览器初始化成功")
            
        except Exception as e:
            print(f"浏览器初始化失败: {e}")
            raise
    
    async def close(self) -> None:
        """关闭浏览器"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception as e:
            print(f"关闭浏览器时出错: {e}")
    
    async def scrape_price(self, url: str) -> PriceResult:
        """
        抓取商品价格
        
        Args:
            url: 商品URL
            
        Returns:
            价格结果
        """
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        try:
            # 验证URL
            if not is_valid_url(url):
                return PriceResult(
                    price=None,
                    currency=None,
                    title=None,
                    availability=None,
                    original_price=None,
                    discount=None,
                    image_url=None,
                    error="无效的URL"
                )
            
            # 随机延迟
            await self._random_delay()
            
            # 检测网站类型
            site_type = self._detect_site_type(url)
            
            # 加载页面
            await self.page.goto(url, wait_until='networkidle', timeout=self.config.timeout)
            
            # 处理验证码
            if await self._handle_captcha():
                return PriceResult(
                    price=None,
                    currency=None,
                    title=None,
                    availability=None,
                    original_price=None,
                    discount=None,
                    image_url=None,
                    error="遇到验证码，无法继续"
                )
            
            # 根据网站类型选择抓取策略
            result = await self._scrape_by_site_type(site_type, url)
            
            if result.price is not None:
                self.stats['successful_requests'] += 1
            else:
                self.stats['failed_requests'] += 1
            
            # 更新平均响应时间
            response_time = time.time() - start_time
            self._update_average_response_time(response_time)
            
            return result
            
        except Exception as e:
            self.stats['failed_requests'] += 1
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _scrape_by_site_type(self, site_type: str, url: str) -> PriceResult:
        """
        根据网站类型选择抓取策略
        
        Args:
            site_type: 网站类型
            url: 商品URL
            
        Returns:
            价格结果
        """
        if site_type == 'amazon':
            return await self._scrape_amazon(url)
        elif site_type == 'taobao':
            return await self._scrape_taobao(url)
        elif site_type == 'jd':
            return await self._scrape_jd(url)
        elif site_type == 'pinduoduo':
            return await self._scrape_pinduoduo(url)
        else:
            return await self._scrape_universal(url)
    
    async def _scrape_amazon(self, url: str) -> PriceResult:
        """抓取Amazon价格"""
        try:
            # 等待价格元素加载
            await self.page.wait_for_selector('[data-testid="price-inside-buybox"], .a-price-whole, .a-offscreen', timeout=10000)
            
            # 尝试多种价格选择器
            price_selectors = [
                '[data-testid="price-inside-buybox"] .a-offscreen',
                '.a-price .a-offscreen',
                '#price_inside_buybox',
                '.a-price-whole',
                '#apex_desktop .a-price .a-offscreen'
            ]
            
            price = None
            currency = None
            
            for selector in price_selectors:
                try:
                    price_element = await self.page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        price, currency = self._parse_price_text(price_text)
                        if price is not None:
                            break
                except Exception:
                    continue
            
            # 获取商品标题
            title = await self._get_text_by_selectors([
                '#productTitle',
                'h1.a-size-large'
            ])
            
            # 获取图片
            image_url = await self._get_image_url([
                '#landingImage',
                '.a-dynamic-image'
            ])
            
            # 获取库存状态
            availability = await self._get_text_by_selectors([
                '#availability span',
                '.a-size-medium.a-color-success'
            ])
            
            return PriceResult(
                price=price,
                currency=currency,
                title=title,
                availability=availability,
                image_url=image_url,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _scrape_taobao(self, url: str) -> PriceResult:
        """抓取淘宝价格"""
        try:
            # 等待页面加载
            await self.page.wait_for_timeout(2000)
            
            # 处理登录检测
            if await self.page.query_selector('.login-form'):
                return PriceResult(
                    price=None,
                    currency='CNY',
                    title=None,
                    availability='需要登录',
                    error='需要登录才能查看价格'
                )
            
            # 尝试多种价格选择器
            price_selectors = [
                '.price-current',
                '#J_Price .price',
                '.tm-price .tm-current-price',
                '.tb-rmb-num'
            ]
            
            price = None
            currency = 'CNY'
            
            for selector in price_selectors:
                try:
                    price_element = await self.page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        price, _ = self._parse_price_text(price_text)
                        if price is not None:
                            break
                except Exception:
                    continue
            
            # 获取商品标题
            title = await self._get_text_by_selectors([
                '.tb-main-title',
                'h1'
            ])
            
            return PriceResult(
                price=price,
                currency=currency,
                title=title,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _scrape_jd(self, url: str) -> PriceResult:
        """抓取京东价格"""
        try:
            # 等待价格元素加载
            await self.page.wait_for_selector('.price, #price, .p-price', timeout=10000)
            
            # 尝试多种价格选择器
            price_selectors = [
                '.price .price-now',
                '#price .p-price',
                '.p-price .price'
            ]
            
            price = None
            currency = 'CNY'
            
            for selector in price_selectors:
                try:
                    price_element = await self.page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        price, _ = self._parse_price_text(price_text)
                        if price is not None:
                            break
                except Exception:
                    continue
            
            # 获取商品标题
            title = await self._get_text_by_selectors([
                '.sku-name',
                'h1'
            ])
            
            return PriceResult(
                price=price,
                currency=currency,
                title=title,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _scrape_pinduoduo(self, url: str) -> PriceResult:
        """抓取拼多多价格"""
        try:
            # 等待页面加载
            await self.page.wait_for_timeout(2000)
            
            # 尝试多种价格选择器
            price_selectors = [
                '.p-price .price',
                '.goods-price .price',
                '.m-product-price .price'
            ]
            
            price = None
            currency = 'CNY'
            
            for selector in price_selectors:
                try:
                    price_element = await self.page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        price, _ = self._parse_price_text(price_text)
                        if price is not None:
                            break
                except Exception:
                    continue
            
            # 获取商品标题
            title = await self._get_text_by_selectors([
                '.goods-name',
                'h1',
                '.product-title'
            ])
            
            return PriceResult(
                price=price,
                currency=currency,
                title=title,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _scrape_universal(self, url: str) -> PriceResult:
        """通用价格抓取策略"""
        try:
            # 尝试使用JavaScript检测价格
            price_data = await self.page.evaluate("""
                () => {
                    const selectors = [
                        '[data-price]',
                        '[data-cost]',
                        '.price',
                        '.cost',
                        '.amount',
                        '.money',
                        '.currency'
                    ];
                    
                    let price = null;
                    let currency = null;
                    
                    for (const selector of selectors) {
                        const elements = document.querySelectorAll(selector);
                        for (const element of elements) {
                            const text = element.textContent || element.innerText;
                            const priceMatch = text.match(/\\$\\s*(\\d+(\\.\\d{1,2})?)/);
                            if (priceMatch) {
                                price = parseFloat(priceMatch[1]);
                                currency = 'USD';
                                break;
                            }
                            const cnyMatch = text.match(/￥\\s*(\\d+(\\.\\d{1,2})?)/);
                            if (cnyMatch) {
                                price = parseFloat(cnyMatch[1]);
                                currency = 'CNY';
                                break;
                            }
                        }
                        if (price) break;
                    }
                    
                    return { price, currency };
                }
            """)
            
            # 获取商品标题
            title = await self._get_text_by_selectors([
                'h1',
                '.title',
                '.product-title',
                'title'
            ])
            
            return PriceResult(
                price=price_data.get('price'),
                currency=price_data.get('currency'),
                title=title,
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            return PriceResult(
                price=None,
                currency=None,
                title=None,
                availability=None,
                original_price=None,
                discount=None,
                image_url=None,
                error=str(e)
            )
    
    async def _handle_captcha(self) -> bool:
        """
        处理验证码
        
        Returns:
            是否遇到验证码
        """
        try:
            # 检查常见的验证码页面
            captcha_indicators = [
                'g-recaptcha',
                'hcaptcha',
                'verify',
                'captcha',
                '请稍后重试',
                '访问过于频繁'
            ]
            
            page_text = await self.page.inner_text('body')
            
            for indicator in captcha_indicators:
                if indicator in page_text.lower():
                    print(f"检测到验证码: {indicator}")
                    return True
            
            return False
            
        except Exception:
            return False
    
    async def _get_text_by_selectors(self, selectors: List[str]) -> Optional[str]:
        """
        尝试多个选择器获取文本
        
        Args:
            selectors: 选择器列表
            
        Returns:
            获取到的文本
        """
        for selector in selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    text = await element.inner_text()
                    if text and text.strip():
                        return text.strip()
            except Exception:
                continue
        return None
    
    async def _get_image_url(self, selectors: List[str]) -> Optional[str]:
        """
        尝试多个选择器获取图片URL
        
        Args:
            selectors: 选择器列表
            
        Returns:
            图片URL
        """
        for selector in selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    src = await element.get_attribute('src')
                    if src:
                        return src
            except Exception:
                continue
        return None
    
    def _detect_site_type(self, url: str) -> str:
        """
        检测网站类型
        
        Args:
            url: 商品URL
            
        Returns:
            网站类型
        """
        domain = get_base_url(url).lower()
        
        if 'amazon' in domain:
            return 'amazon'
        elif 'taobao' in domain or 'tmall' in domain:
            return 'taobao'
        elif 'jd' in domain:
            return 'jd'
        elif 'pinduoduo' in domain or 'yangkeduo' in domain:
            return 'pinduoduo'
        else:
            return 'universal'
    
    def _parse_price_text(self, text: str) -> Tuple[Optional[float], Optional[str]]:
        """
        解析价格文本
        
        Args:
            text: 价格文本
            
        Returns:
            (价格, 货币代码)
        """
        if not text:
            return None, None
        
        # 清理文本
        text = text.strip().replace('\n', ' ').replace('\t', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        # 检测货币符号
        currency = None
        if '$' in text:
            currency = "USD"
            # 移除非数字字符（保留数字、小数点、逗号）
            price_text = re.sub(r'[^\d.,]', '', text)
        elif '£' in text:
            currency = "GBP"
            price_text = re.sub(r'[^\d.,]', '', text)
        elif '€' in text:
            currency = "EUR"
            price_text = re.sub(r'[^\d.,]', '', text)
        elif '￥' in text or '¥' in text:
            currency = "CNY"
            price_text = re.sub(r'[^\d.,]', '', text)
        else:
            # 尝试提取纯数字
            price_text = re.sub(r'[^\d.,]', '', text)
            currency = "USD"  # 默认货币
        
        if not price_text:
            return None, currency
        
        # 处理千分位分隔符
        price_text = price_text.replace(',', '')
        
        # 处理小数点
        if '.' in price_text:
            parts = price_text.split('.')
            if len(parts) == 2:
                price_text = parts[0] + '.' + parts[1][:2]  # 只保留两位小数
        
        try:
            price = float(price_text)
            return price, currency
        except (ValueError, TypeError):
            return None, currency
    
    async def _random_delay(self) -> None:
        """随机延迟"""
        min_delay, max_delay = self.config.delay_range
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)
    
    def _update_average_response_time(self, response_time: float) -> None:
        """更新平均响应时间"""
        total = self.stats['total_requests']
        if total > 0:
            current_avg = self.stats['average_response_time']
            self.stats['average_response_time'] = ((current_avg * (total - 1)) + response_time) / total
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        
        # 计算成功率
        total = stats['total_requests']
        if total > 0:
            stats['success_rate'] = (stats['successful_requests'] / total) * 100
        else:
            stats['success_rate'] = 0
        
        return stats