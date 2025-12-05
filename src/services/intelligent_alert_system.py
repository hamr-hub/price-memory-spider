"""
智能告警系统
实现多种告警规则、多渠道推送和智能冷却机制
"""
import asyncio
import json
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import hashlib
import hmac
import base64

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo


@dataclass
class AlertRule:
    """告警规则数据类"""
    id: int
    user_id: int
    product_id: int
    rule_type: str  # 'price_drop', 'price_rise', 'price_threshold', 'percent_change', 'anomaly'
    threshold: Optional[float] = None
    percent: Optional[float] = None
    cooldown_minutes: int = 60
    channels: List[str] = None  # ['email', 'webhook', 'sms', 'app']
    targets: Dict[str, str] = None  # {'email': 'user@example.com', 'webhook': 'https://...'}
    status: str = 'active'
    created_at: Optional[datetime] = None
    last_triggered_at: Optional[datetime] = None


@dataclass
class AlertEvent:
    """告警事件数据类"""
    alert_id: int
    product_id: int
    user_id: int
    price: float
    currency: str
    rule_type: str
    message: str
    channels: List[str]
    status: str  # 'pending', 'sent', 'failed'
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class AlertMetrics:
    """告警指标数据类"""
    total_alerts: int
    triggered_alerts: int
    sent_events: int
    failed_events: int
    success_rate: float
    average_response_time: float


class IntelligentAlertSystem:
    """智能告警系统"""
    
    def __init__(self):
        self.repo = SupabaseRepo()
        self.alert_handlers = {
            'price_drop': self._handle_price_drop_alert,
            'price_rise': self._handle_price_rise_alert,
            'price_threshold': self._handle_price_threshold_alert,
            'percent_change': self._handle_percent_change_alert,
            'anomaly': self._handle_anomaly_alert
        }
        self.notification_senders = {
            'email': self._send_email_notification,
            'webhook': self._send_webhook_notification,
            'sms': self._send_sms_notification,
            'app': self._send_app_notification
        }
        
        # 告警缓存（减少数据库查询）
        self.alert_cache: Dict[int, List[AlertRule]] = {}
        self.cache_ttl = 300  # 5分钟缓存
        self.last_cache_update: Dict[int, datetime] = {}
    
    async def process_price_update(self, product_id: int, price: float, 
                                 currency: str, metadata: Optional[Dict[str, Any]] = None) -> List[AlertEvent]:
        """
        处理价格更新并触发相关告警
        
        Args:
            product_id: 商品ID
            price: 当前价格
            currency: 货币代码
            metadata: 额外数据
            
        Returns:
            触发的告警事件列表
        """
        triggered_events = []
        
        try:
            # 获取该商品的所有活跃告警规则
            alert_rules = await self._get_product_alert_rules(product_id)
            
            for rule in alert_rules:
                # 检查冷却时间
                if self._is_in_cooldown(rule):
                    continue
                
                # 检查是否触发告警
                if await self._check_alert_condition(rule, product_id, price, currency):
                    # 创建告警事件
                    event = await self._create_alert_event(rule, price, currency, metadata)
                    
                    # 发送通知
                    sent_channels = await self._send_notifications(event)
                    
                    # 更新告警状态
                    await self._update_alert_status(rule, price, currency)
                    
                    # 记录事件
                    await self._record_alert_event(event, sent_channels)
                    
                    triggered_events.append(event)
        
        except Exception as e:
            print(f"处理价格更新告警失败: {e}")
        
        return triggered_events
    
    async def create_alert_rule(self, user_id: int, product_id: int, rule_type: str,
                               threshold: Optional[float] = None, percent: Optional[float] = None,
                               cooldown_minutes: int = 60, channels: List[str] = None,
                               targets: Dict[str, str] = None) -> Optional[int]:
        """
        创建告警规则
        
        Args:
            user_id: 用户ID
            product_id: 商品ID
            rule_type: 告警类型
            threshold: 阈值
            percent: 百分比
            cooldown_minutes: 冷却时间（分钟）
            channels: 通知渠道列表
            targets: 目标地址
            
        Returns:
            告警规则ID
        """
        try:
            rule_data = {
                'user_id': user_id,
                'product_id': product_id,
                'rule_type': rule_type,
                'threshold': threshold,
                'percent': percent,
                'cooldown_minutes': cooldown_minutes,
                'channels': channels or ['email'],
                'targets': targets or {},
                'status': 'active',
                'created_at': datetime.utcnow()
            }
            
            rule_id = self.repo.create_alert_rule(rule_data)
            
            # 清除缓存
            self._clear_alert_cache(product_id)
            
            return rule_id
            
        except Exception as e:
            print(f"创建告警规则失败: {e}")
            return None
    
    async def update_alert_rule(self, rule_id: int, updates: Dict[str, Any]) -> bool:
        """
        更新告警规则
        
        Args:
            rule_id: 告警规则ID
            updates: 更新数据
            
        Returns:
            是否更新成功
        """
        try:
            success = self.repo.update_alert_rule(rule_id, updates)
            
            if success:
                # 清除相关缓存
                rule = self.repo.get_alert_rule(rule_id)
                if rule:
                    self._clear_alert_cache(rule['product_id'])
            
            return success
            
        except Exception as e:
            print(f"更新告警规则失败: {e}")
            return False
    
    async def delete_alert_rule(self, rule_id: int) -> bool:
        """
        删除告警规则
        
        Args:
            rule_id: 告警规则ID
            
        Returns:
            是否删除成功
        """
        try:
            rule = self.repo.get_alert_rule(rule_id)
            if not rule:
                return False
            
            success = self.repo.delete_alert_rule(rule_id)
            
            if success:
                self._clear_alert_cache(rule['product_id'])
            
            return success
            
        except Exception as e:
            print(f"删除告警规则失败: {e}")
            return False
    
    async def get_alert_metrics(self, user_id: int, days: int = 30) -> AlertMetrics:
        """
        获取告警指标
        
        Args:
            user_id: 用户ID
            days: 统计天数
            
        Returns:
            告警指标
        """
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            # 获取用户的所有告警规则
            total_alerts = self.repo.count_user_alert_rules(user_id)
            
            # 获取触发的告警事件
            triggered_events = self.repo.get_alert_events(user_id, start_date)
            
            # 统计发送状态
            sent_events = len([e for e in triggered_events if e['status'] == 'sent'])
            failed_events = len([e for e in triggered_events if e['status'] == 'failed'])
            
            # 计算成功率
            total_events = len(triggered_events)
            success_rate = (sent_events / total_events * 100) if total_events > 0 else 0
            
            # 计算平均响应时间
            response_times = []
            for event in triggered_events:
                if event.get('created_at') and event.get('sent_at'):
                    response_time = (event['sent_at'] - event['created_at']).total_seconds()
                    response_times.append(response_time)
            
            average_response_time = statistics.mean(response_times) if response_times else 0
            
            return AlertMetrics(
                total_alerts=total_alerts,
                triggered_alerts=total_events,
                sent_events=sent_events,
                failed_events=failed_events,
                success_rate=success_rate,
                average_response_time=average_response_time
            )
            
        except Exception as e:
            print(f"获取告警指标失败: {e}")
            return AlertMetrics(0, 0, 0, 0, 0, 0)
    
    async def _get_product_alert_rules(self, product_id: int) -> List[AlertRule]:
        """
        获取商品的告警规则（带缓存）
        
        Args:
            product_id: 商品ID
            
        Returns:
            告警规则列表
        """
        current_time = datetime.utcnow()
        
        # 检查缓存
        if product_id in self.alert_cache:
            if product_id in self.last_cache_update:
                if (current_time - self.last_cache_update[product_id]).total_seconds() < self.cache_ttl:
                    return self.alert_cache[product_id]
        
        # 从数据库获取
        rules_data = self.repo.get_product_alert_rules(product_id)
        
        # 转换为AlertRule对象
        rules = []
        for rule_data in rules_data:
            rules.append(AlertRule(
                id=rule_data['id'],
                user_id=rule_data['user_id'],
                product_id=rule_data['product_id'],
                rule_type=rule_data['rule_type'],
                threshold=rule_data.get('threshold'),
                percent=rule_data.get('percent'),
                cooldown_minutes=rule_data.get('cooldown_minutes', 60),
                channels=rule_data.get('channels', ['email']),
                targets=rule_data.get('targets', {}),
                status=rule_data.get('status', 'active'),
                created_at=rule_data.get('created_at'),
                last_triggered_at=rule_data.get('last_triggered_at')
            ))
        
        # 更新缓存
        self.alert_cache[product_id] = rules
        self.last_cache_update[product_id] = current_time
        
        return rules
    
    def _is_in_cooldown(self, rule: AlertRule) -> bool:
        """
        检查告警是否在冷却期内
        
        Args:
            rule: 告警规则
            
        Returns:
            是否在冷却期
        """
        if not rule.last_triggered_at:
            return False
        
        cooldown_end = rule.last_triggered_at + timedelta(minutes=rule.cooldown_minutes)
        return datetime.utcnow() < cooldown_end
    
    async def _check_alert_condition(self, rule: AlertRule, product_id: int, 
                                   price: float, currency: str) -> bool:
        """
        检查告警条件是否满足
        
        Args:
            rule: 告警规则
            product_id: 商品ID
            price: 当前价格
            currency: 货币代码
            
        Returns:
            条件是否满足
        """
        handler = self.alert_handlers.get(rule.rule_type)
        if not handler:
            return False
        
        return await handler(rule, product_id, price, currency)
    
    async def _handle_price_drop_alert(self, rule: AlertRule, product_id: int, 
                                     price: float, currency: str) -> bool:
        """处理价格下降告警"""
        if rule.threshold:
            return price <= rule.threshold
        
        if rule.percent:
            # 获取历史价格进行比较
            price_history = self.repo.get_price_history(product_id, limit=10)
            if len(price_history) > 1:
                last_price = float(price_history[0]['price'])
                change_percent = ((price - last_price) / last_price) * 100
                return change_percent <= -abs(rule.percent)
        
        return False
    
    async def _handle_price_rise_alert(self, rule: AlertRule, product_id: int, 
                                     price: float, currency: str) -> bool:
        """处理价格上涨告警"""
        if rule.threshold:
            return price >= rule.threshold
        
        if rule.percent:
            # 获取历史价格进行比较
            price_history = self.repo.get_price_history(product_id, limit=10)
            if len(price_history) > 1:
                last_price = float(price_history[0]['price'])
                change_percent = ((price - last_price) / last_price) * 100
                return change_percent >= abs(rule.percent)
        
        return False
    
    async def _handle_price_threshold_alert(self, rule: AlertRule, product_id: int, 
                                          price: float, currency: str) -> bool:
        """处理价格阈值告警"""
        if rule.threshold:
            return price <= rule.threshold or price >= rule.threshold
        return False
    
    async def _handle_percent_change_alert(self, rule: AlertRule, product_id: int, 
                                         price: float, currency: str) -> bool:
        """处理百分比变化告警"""
        if rule.percent:
            price_history = self.repo.get_price_history(product_id, limit=10)
            if len(price_history) > 1:
                last_price = float(price_history[0]['price'])
                change_percent = abs((price - last_price) / last_price) * 100
                return change_percent >= abs(rule.percent)
        return False
    
    async def _handle_anomaly_alert(self, rule: AlertRule, product_id: int, 
                                  price: float, currency: str) -> bool:
        """处理异常检测告警"""
        # 获取近期价格数据
        price_history = self.repo.get_price_history(product_id, limit=30)
        
        if len(price_history) < 10:
            return False
        
        prices = [float(p['price']) for p in price_history]
        
        # 使用Z-score检测异常
        mean_price = statistics.mean(prices)
        std_price = statistics.stdev(prices) if len(prices) > 1 else 0
        
        if std_price > 0:
            z_score = abs((price - mean_price) / std_price)
            # 默认阈值为3（99.7%置信度）
            threshold = rule.threshold or 3.0
            return z_score > threshold
        
        return False
    
    async def _create_alert_event(self, rule: AlertRule, price: float, 
                                currency: str, metadata: Optional[Dict[str, Any]]) -> AlertEvent:
        """创建告警事件"""
        return AlertEvent(
            alert_id=rule.id,
            product_id=rule.product_id,
            user_id=rule.user_id,
            price=price,
            currency=currency,
            rule_type=rule.rule_type,
            message=self._generate_alert_message(rule, price, currency),
            channels=rule.channels,
            status='pending',
            created_at=datetime.utcnow()
        )
    
    def _generate_alert_message(self, rule: AlertRule, price: float, currency: str) -> str:
        """生成告警消息"""
        message_templates = {
            'price_drop': f"价格下降告警：当前价格 {price} {currency}",
            'price_rise': f"价格上涨告警：当前价格 {price} {currency}",
            'price_threshold': f"价格阈值告警：当前价格 {price} {currency}",
            'percent_change': f"价格变动告警：当前价格 {price} {currency}",
            'anomaly': f"价格异常告警：当前价格 {price} {currency}"
        }
        
        return message_templates.get(rule.rule_type, f"价格告警：当前价格 {price} {currency}")
    
    async def _send_notifications(self, event: AlertEvent) -> List[str]:
        """发送通知"""
        sent_channels = []
        
        for channel in event.channels:
            sender = self.notification_senders.get(channel)
            if sender:
                try:
                    await sender(event)
                    sent_channels.append(channel)
                except Exception as e:
                    print(f"发送 {channel} 通知失败: {e}")
                    event.error_message = str(e)
        
        return sent_channels
    
    async def _send_email_notification(self, event: AlertEvent) -> None:
        """发送邮件通知"""
        if not config.SMTP_HOST or not config.SMTP_USER or not config.SMTP_PASS:
            raise Exception("SMTP配置不完整")
        
        targets = self.repo.get_alert_targets(event.alert_id)
        email_target = targets.get('email')
        
        if not email_target:
            raise Exception("未配置邮件接收地址")
        
        # 获取商品信息
        product = self.repo.get_product(event.product_id)
        product_name = product.get('name', '未知商品') if product else '未知商品'
        
        # 构建邮件内容
        subject = f"价格告警通知 - {product_name}"
        body = f"""
        价格告警通知
        
        商品名称: {product_name}
        当前价格: {event.price} {event.currency}
        告警类型: {event.rule_type}
        触发时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
        
        ---
        Price Memory 智能告警系统
        """
        
        # 发送邮件
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = config.SMTP_FROM or config.SMTP_USER
        msg['To'] = email_target
        
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASS)
            server.send_message(msg)
    
    async def _send_webhook_notification(self, event: AlertEvent) -> None:
        """发送Webhook通知"""
        targets = self.repo.get_alert_targets(event.alert_id)
        webhook_url = targets.get('webhook')
        
        if not webhook_url:
            raise Exception("未配置Webhook地址")
        
        # 构建载荷
        payload = {
            'type': 'price_alert',
            'alert_id': event.alert_id,
            'product_id': event.product_id,
            'user_id': event.user_id,
            'price': event.price,
            'currency': event.currency,
            'rule_type': event.rule_type,
            'message': event.message,
            'timestamp': event.created_at.isoformat()
        }
        
        # 添加签名（如果配置了密钥）
        headers = {'Content-Type': 'application/json'}
        
        if config.ALERT_WEBHOOK_SECRET:
            signature = hmac.new(
                config.ALERT_WEBHOOK_SECRET.encode(),
                json.dumps(payload).encode(),
                hashlib.sha256
            ).hexdigest()
            
            headers['X-Signature'] = f"sha256={signature}"
        
        # 发送请求
        response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
    
    async def _send_sms_notification(self, event: AlertEvent) -> None:
        """发送短信通知"""
        targets = self.repo.get_alert_targets(event.alert_id)
        phone_number = targets.get('sms')
        
        if not phone_number:
            raise Exception("未配置短信接收号码")
        
        # 这里可以集成第三方短信服务（如Twilio、阿里云短信等）
        # 由于短信服务需要额外配置，此处仅作为示例
        raise Exception("短信通知服务未实现")
    
    async def _send_app_notification(self, event: AlertEvent) -> None:
        """发送应用内通知"""
        # 记录应用内消息
        self.repo.insert_app_notification(
            user_id=event.user_id,
            title=f"价格告警 - {event.rule_type}",
            message=event.message,
            data={
                'alert_id': event.alert_id,
                'product_id': event.product_id,
                'price': event.price,
                'currency': event.currency
            }
        )
    
    async def _update_alert_status(self, rule: AlertRule, price: float, currency: str) -> None:
        """更新告警状态"""
        self.repo.update_alert_last_triggered(rule.id, price, currency)
        
        # 清除缓存
        self._clear_alert_cache(rule.product_id)
    
    async def _record_alert_event(self, event: AlertEvent, sent_channels: List[str]) -> None:
        """记录告警事件"""
        event_data = {
            'alert_id': event.alert_id,
            'product_id': event.product_id,
            'user_id': event.user_id,
            'price': event.price,
            'currency': event.currency,
            'rule_type': event.rule_type,
            'message': event.message,
            'channels': sent_channels,
            'status': 'sent' if sent_channels else 'failed',
            'error_message': event.error_message,
            'created_at': event.created_at,
            'sent_at': datetime.utcnow() if sent_channels else None
        }
        
        self.repo.insert_alert_event(event_data)
    
    def _clear_alert_cache(self, product_id: int) -> None:
        """清除告警缓存"""
        if product_id in self.alert_cache:
            del self.alert_cache[product_id]
        if product_id in self.last_cache_update:
            del self.last_cache_update[product_id]