"""
价格历史记录服务
实现完整的价格历史管理、趋势分析和数据统计
"""
import json
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy import stats

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo


@dataclass
class PricePoint:
    """价格点数据类"""
    timestamp: datetime
    price: float
    currency: str
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PriceTrend:
    """价格趋势数据类"""
    start_price: float
    end_price: float
    change_amount: float
    change_percent: float
    highest_price: float
    lowest_price: float
    average_price: float
    volatility: float  # 价格波动率
    trend_direction: str  # 'up', 'down', 'stable'
    data_points: int
    period_days: int


@dataclass
class PriceAlert:
    """价格告警数据类"""
    alert_id: int
    user_id: int
    product_id: int
    rule_type: str
    threshold: Optional[float]
    percent: Optional[float]
    cooldown_minutes: int
    last_triggered_at: Optional[datetime]
    status: str


class PriceHistoryService:
    """价格历史记录服务"""
    
    def __init__(self):
        self.repo = SupabaseRepo()
    
    def record_price(self, product_id: int, price: float, currency: str, 
                    source: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        记录价格数据
        
        Args:
            product_id: 商品ID
            price: 价格
            currency: 货币代码
            source: 数据来源
            metadata: 额外元数据
            
        Returns:
            是否记录成功
        """
        try:
            # 检查是否与上一次价格相同且时间间隔过短
            if self._should_skip_duplicate_price(product_id, price):
                print(f"价格 {price} 与上次相同，跳过记录")
                return True
            
            # 记录价格
            self.repo.insert_price(
                product_id=product_id,
                price=price,
                currency=currency,
                source=source,
                metadata=metadata
            )
            
            # 更新商品最后更新时间
            self.repo.update_product_last_updated(product_id)
            
            # 检查是否触发告警
            self._check_alerts(product_id, price, currency)
            
            return True
            
        except Exception as e:
            print(f"记录价格失败: {e}")
            return False
    
    def get_price_history(self, product_id: int, days: int = 30, 
                         granularity: str = 'daily') -> List[PricePoint]:
        """
        获取价格历史数据
        
        Args:
            product_id: 商品ID
            days: 查询天数
            granularity: 数据粒度 ('hourly', 'daily', 'weekly')
            
        Returns:
            价格点列表
        """
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            if granularity == 'hourly':
                prices = self.repo.get_hourly_price_history(product_id, start_date)
            elif granularity == 'daily':
                prices = self.repo.get_daily_price_history(product_id, start_date)
            elif granularity == 'weekly':
                prices = self.repo.get_weekly_price_history(product_id, start_date)
            else:
                prices = self.repo.get_price_history(product_id, start_date)
            
            price_points = []
            for price_record in prices:
                price_points.append(PricePoint(
                    timestamp=price_record['created_at'],
                    price=float(price_record['price']),
                    currency=price_record['currency'],
                    source=price_record.get('source'),
                    metadata=price_record.get('metadata')
                ))
            
            return sorted(price_points, key=lambda x: x.timestamp)
            
        except Exception as e:
            print(f"获取价格历史失败: {e}")
            return []
    
    def analyze_price_trend(self, product_id: int, days: int = 30) -> Optional[PriceTrend]:
        """
        分析价格趋势
        
        Args:
            product_id: 商品ID
            days: 分析天数
            
        Returns:
            价格趋势数据
        """
        try:
            price_history = self.get_price_history(product_id, days, 'daily')
            
            if len(price_history) < 2:
                return None
            
            prices = [p.price for p in price_history]
            timestamps = [p.timestamp for p in price_history]
            
            # 计算基本统计量
            start_price = prices[0]
            end_price = prices[-1]
            change_amount = end_price - start_price
            change_percent = (change_amount / start_price) * 100 if start_price > 0 else 0
            
            highest_price = max(prices)
            lowest_price = min(prices)
            average_price = statistics.mean(prices)
            
            # 计算波动率（标准差）
            volatility = statistics.stdev(prices) if len(prices) > 1 else 0
            
            # 判断趋势方向
            if change_percent > 2:
                trend_direction = 'up'
            elif change_percent < -2:
                trend_direction = 'down'
            else:
                trend_direction = 'stable'
            
            # 计算数据点数量和周期天数
            data_points = len(prices)
            period_days = (timestamps[-1] - timestamps[0]).days
            
            return PriceTrend(
                start_price=start_price,
                end_price=end_price,
                change_amount=change_amount,
                change_percent=change_percent,
                highest_price=highest_price,
                lowest_price=lowest_price,
                average_price=average_price,
                volatility=volatility,
                trend_direction=trend_direction,
                data_points=data_points,
                period_days=period_days
            )
            
        except Exception as e:
            print(f"分析价格趋势失败: {e}")
            return None
    
    def get_price_statistics(self, product_id: int, days: int = 30) -> Dict[str, Any]:
        """
        获取价格统计信息
        
        Args:
            product_id: 商品ID
            days: 统计天数
            
        Returns:
            统计信息字典
        """
        try:
            price_history = self.get_price_history(product_id, days, 'daily')
            
            if not price_history:
                return {}
            
            prices = [p.price for p in price_history]
            
            # 基础统计
            stats_data = {
                'count': len(prices),
                'min': min(prices),
                'max': max(prices),
                'mean': statistics.mean(prices),
                'median': statistics.median(prices),
                'std': statistics.stdev(prices) if len(prices) > 1 else 0,
                'variance': statistics.variance(prices) if len(prices) > 1 else 0
            }
            
            # 分位数
            if len(prices) > 1:
                prices_sorted = sorted(prices)
                n = len(prices_sorted)
                stats_data['q1'] = prices_sorted[n // 4]
                stats_data['q3'] = prices_sorted[3 * n // 4]
                stats_data['iqr'] = stats_data['q3'] - stats_data['q1']
            
            # 价格分布
            price_ranges = self._calculate_price_distribution(prices)
            stats_data['distribution'] = price_ranges
            
            # 价格变化频率
            price_changes = self._calculate_price_changes(prices)
            stats_data['changes'] = price_changes
            
            return stats_data
            
        except Exception as e:
            print(f"获取价格统计失败: {e}")
            return {}
    
    def detect_price_anomalies(self, product_id: int, days: int = 30, 
                              threshold: float = 2.0) -> List[Dict[str, Any]]:
        """
        检测价格异常
        
        Args:
            product_id: 商品ID
            days: 检测天数
            threshold: 异常阈值（标准差倍数）
            
        Returns:
            异常价格列表
        """
        try:
            price_history = self.get_price_history(product_id, days, 'daily')
            
            if len(price_history) < 10:  # 需要足够的数据点
                return []
            
            prices = [p.price for p in price_history]
            timestamps = [p.timestamp for p in price_history]
            
            # 计算移动平均和标准差
            window_size = 7
            anomalies = []
            
            for i in range(window_size, len(prices) - window_size):
                window = prices[i - window_size:i + window_size]
                mean = statistics.mean(window)
                std = statistics.stdev(window) if len(window) > 1 else 0
                
                # 检测是否为异常值
                if std > 0:
                    z_score = abs((prices[i] - mean) / std)
                    if z_score > threshold:
                        anomalies.append({
                            'timestamp': timestamps[i],
                            'price': prices[i],
                            'expected_price': mean,
                            'z_score': z_score,
                            'type': 'high' if prices[i] > mean else 'low'
                        })
            
            return anomalies
            
        except Exception as e:
            print(f"检测价格异常失败: {e}")
            return []
    
    def predict_price(self, product_id: int, days: int = 7) -> Dict[str, Any]:
        """
        预测未来价格
        
        Args:
            product_id: 商品ID
            days: 预测天数
            
        Returns:
            预测结果
        """
        try:
            price_history = self.get_price_history(product_id, 90, 'daily')  # 使用90天数据
            
            if len(price_history) < 30:
                return {'error': '数据不足，无法进行预测'}
            
            prices = [p.price for p in price_history]
            
            # 简单的线性回归预测
            x = np.arange(len(prices))
            y = np.array(prices)
            
            # 拟合线性模型
            slope, intercept = np.polyfit(x, y, 1)
            
            # 预测未来价格
            future_x = np.arange(len(prices), len(prices) + days)
            predicted_prices = slope * future_x + intercept
            
            # 计算置信区间
            residuals = y - (slope * x + intercept)
            std_error = np.std(residuals)
            
            predictions = []
            for i, price in enumerate(predicted_prices):
                predictions.append({
                    'day': i + 1,
                    'predicted_price': float(price),
                    'lower_bound': float(price - 1.96 * std_error),
                    'upper_bound': float(price + 1.96 * std_error)
                })
            
            return {
                'trend_slope': float(slope),
                'predictions': predictions,
                'confidence': 0.95
            }
            
        except Exception as e:
            print(f"价格预测失败: {e}")
            return {'error': str(e)}
    
    def _should_skip_duplicate_price(self, product_id: int, price: float) -> bool:
        """
        判断是否应该跳过重复价格
        
        Args:
            product_id: 商品ID
            price: 当前价格
            
        Returns:
            是否跳过
        """
        try:
            # 获取最近的价格记录
            recent_prices = self.repo.get_price_history(product_id, limit=5)
            
            if not recent_prices:
                return False
            
            # 检查最近的价格是否相同
            last_price = float(recent_prices[0]['price'])
            last_timestamp = recent_prices[0]['created_at']
            
            # 如果价格相同且时间间隔小于10分钟，则跳过
            if abs(last_price - price) < 0.01:
                time_diff = datetime.utcnow() - last_timestamp
                if time_diff.total_seconds() < 600:  # 10分钟
                    return True
            
            return False
            
        except Exception:
            return False
    
    def _check_alerts(self, product_id: int, price: float, currency: str) -> None:
        """
        检查并触发告警
        
        Args:
            product_id: 商品ID
            price: 当前价格
            currency: 货币代码
        """
        try:
            # 获取该商品的所有活跃告警
            alerts = self.repo.get_product_alerts(product_id)
            
            for alert in alerts:
                if self._should_trigger_alert(alert, price):
                    # 更新告警最后触发时间
                    self.repo.update_alert_last_triggered(alert['id'])
                    
                    # 记录告警事件
                    self.repo.insert_alert_event(
                        alert_id=alert['id'],
                        product_id=product_id,
                        user_id=alert['user_id'],
                        price=price,
                        currency=currency,
                        message=f"价格触发告警: {price} {currency}"
                    )
                    
                    print(f"触发告警: {alert['id']} - 商品 {product_id} 价格 {price}")
        
        except Exception as e:
            print(f"检查告警失败: {e}")
    
    def _should_trigger_alert(self, alert: Dict[str, Any], price: float) -> bool:
        """
        判断是否应该触发告警
        
        Args:
            alert: 告警规则
            price: 当前价格
            
        Returns:
            是否触发
        """
        rule_type = alert.get('rule_type', '')
        
        if rule_type == 'price_below':
            threshold = alert.get('threshold')
            if threshold and price <= threshold:
                return True
        
        elif rule_type == 'price_above':
            threshold = alert.get('threshold')
            if threshold and price >= threshold:
                return True
        
        elif rule_type == 'price_change':
            percent = alert.get('percent')
            if percent:
                # 获取历史价格进行比较
                price_history = self.get_price_history(alert['product_id'], 1, 'daily')
                if len(price_history) > 0:
                    last_price = price_history[0].price
                    if last_price > 0:
                        change_percent = ((price - last_price) / last_price) * 100
                        if abs(change_percent) >= abs(percent):
                            return True
        
        return False
    
    def _calculate_price_distribution(self, prices: List[float]) -> Dict[str, int]:
        """
        计算价格分布
        
        Args:
            prices: 价格列表
            
        Returns:
            价格分布字典
        """
        if not prices:
            return {}
        
        min_price = min(prices)
        max_price = max(prices)
        
        # 如果价格范围太小，不进行分组
        if max_price - min_price < 0.01:
            return {'single': len(prices)}
        
        # 创建5个价格区间
        range_size = (max_price - min_price) / 5
        distribution = defaultdict(int)
        
        for price in prices:
            if price <= min_price + range_size:
                distribution['very_low'] += 1
            elif price <= min_price + 2 * range_size:
                distribution['low'] += 1
            elif price <= min_price + 3 * range_size:
                distribution['medium'] += 1
            elif price <= min_price + 4 * range_size:
                distribution['high'] += 1
            else:
                distribution['very_high'] += 1
        
        return dict(distribution)
    
    def _calculate_price_changes(self, prices: List[float]) -> Dict[str, Any]:
        """
        计算价格变化频率
        
        Args:
            prices: 价格列表
            
        Returns:
            价格变化统计
        """
        if len(prices) < 2:
            return {}
        
        changes = []
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            change_percent = (change / prices[i-1]) * 100 if prices[i-1] > 0 else 0
            changes.append({
                'absolute': change,
                'percent': change_percent
            })
        
        # 统计变化情况
        positive_changes = [c for c in changes if c['absolute'] > 0]
        negative_changes = [c for c in changes if c['absolute'] < 0]
        stable_changes = [c for c in changes if abs(c['absolute']) < 0.01]
        
        avg_positive_change = statistics.mean([c['absolute'] for c in positive_changes]) if positive_changes else 0
        avg_negative_change = statistics.mean([c['absolute'] for c in negative_changes]) if negative_changes else 0
        
        return {
            'total_changes': len(changes),
            'positive': len(positive_changes),
            'negative': len(negative_changes),
            'stable': len(stable_changes),
            'avg_positive_change': avg_positive_change,
            'avg_negative_change': avg_negative_change,
            'largest_increase': max([c['percent'] for c in positive_changes]) if positive_changes else 0,
            'largest_decrease': min([c['percent'] for c in negative_changes]) if negative_changes else 0
        }