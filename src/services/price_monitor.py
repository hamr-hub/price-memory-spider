"""
价格监控和告警系统
实现价格变化检测和多渠道告警推送
"""
import os
import time
import smtplib
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo


class PriceMonitor:
    """价格监控器"""
    
    def __init__(self):
        self.repo = SupabaseRepo()
    
    def check_price_changes(self, product_id: int, new_price: float, currency: str) -> List[Dict[str, Any]]:
        """
        检查价格变化并触发告警
        
        Args:
            product_id: 商品ID
            new_price: 新价格
            currency: 货币代码
        
        Returns:
            触发的告警列表
        """
        triggered_alerts = []
        
        try:
            # 获取商品的历史价格
            price_history = self.repo.get_price_history(product_id, limit=10)
            if not price_history:
                print(f"商品 {product_id} 没有历史价格记录")
                return triggered_alerts
            
            # 获取最近的价格
            last_price_record = price_history[0]
            last_price = float(last_price_record.get('price', 0))
            
            if last_price <= 0:
                print(f"商品 {product_id} 历史价格无效")
                return triggered_alerts
            
            # 计算价格变化
            price_change = new_price - last_price
            price_change_percent = (price_change / last_price) * 100
            
            print(f"商品 {product_id} 价格变化: {last_price} -> {new_price} ({price_change_percent:.2f}%)")
            
            # 获取该商品的所有告警规则
            alerts = self.repo.get_product_alerts(product_id)
            
            for alert in alerts:
                if self._should_trigger_alert(alert, new_price, last_price, price_change_percent):
                    # 检查冷却时间
                    if self._is_in_cooldown(alert):
                        print(f"告警 {alert['id']} 在冷却期内，跳过")
                        continue
                    
                    # 触发告警
                    alert_data = {
                        'alert_id': alert['id'],
                        'product_id': product_id,
                        'old_price': last_price,
                        'new_price': new_price,
                        'price_change': price_change,
                        'price_change_percent': price_change_percent,
                        'currency': currency,
                        'rule_type': alert['rule_type'],
                        'user_id': alert['user_id']
                    }
                    
                    triggered_alerts.append(alert_data)
                    
                    # 更新告警最后触发时间
                    self.repo.update_alert_last_triggered(alert['id'])
        
        except Exception as e:
            print(f"检查价格变化失败: {e}")
        
        return triggered_alerts
    
    def _should_trigger_alert(self, alert: Dict[str, Any], new_price: float, 
                            last_price: float, price_change_percent: float) -> bool:
        """
        判断是否应该触发告警
        
        Args:
            alert: 告警规则
            new_price: 新价格
            last_price: 旧价格
            price_change_percent: 价格变化百分比
        
        Returns:
            是否应该触发告警
        """
        rule_type = alert.get('rule_type', '')
        
        if rule_type == 'price_drop':
            # 价格下降告警
            threshold = alert.get('threshold')
            if threshold and new_price <= threshold:
                return True
            
            percent = alert.get('percent')
            if percent and price_change_percent <= -abs(percent):
                return True
        
        elif rule_type == 'price_rise':
            # 价格上涨告警
            threshold = alert.get('threshold')
            if threshold and new_price >= threshold:
                return True
            
            percent = alert.get('percent')
            if percent and price_change_percent >= abs(percent):
                return True
        
        elif rule_type == 'price_change':
            # 价格变化告警（上涨或下降）
            percent = alert.get('percent')
            if percent and abs(price_change_percent) >= abs(percent):
                return True
        
        return False
    
    def _is_in_cooldown(self, alert: Dict[str, Any]) -> bool:
        """
        检查告警是否在冷却期内
        
        Args:
            alert: 告警规则
        
        Returns:
            是否在冷却期内
        """
        cooldown_minutes = alert.get('cooldown_minutes', 60)  # 默认60分钟
        last_triggered = alert.get('last_triggered_at')
        
        if not last_triggered:
            return False
        
        try:
            if isinstance(last_triggered, str):
                last_triggered_time = datetime.fromisoformat(last_triggered.replace('Z', '+00:00'))
            else:
                last_triggered_time = last_triggered
            
            cooldown_end = last_triggered_time + timedelta(minutes=cooldown_minutes)
            return datetime.utcnow() < cooldown_end.replace(tzinfo=None)
        
        except Exception as e:
            print(f"检查冷却时间失败: {e}")
            return False


class AlertSender:
    """告警发送器"""
    
    def __init__(self):
        self.repo = SupabaseRepo()
    
    def send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        发送告警
        
        Args:
            alert_data: 告警数据
        
        Returns:
            发送是否成功
        """
        try:
            # 获取告警规则详情
            alert = self.repo.get_alert(alert_data['alert_id'])
            if not alert:
                print(f"告警规则 {alert_data['alert_id']} 不存在")
                return False
            
            # 获取用户信息
            user = self.repo.get_user(alert['user_id'])
            if not user:
                print(f"用户 {alert['user_id']} 不存在")
                return False
            
            # 获取商品信息
            product = self.repo.get_product(alert_data['product_id'])
            if not product:
                print(f"商品 {alert_data['product_id']} 不存在")
                return False
            
            # 构建告警消息
            message = self._build_alert_message(alert_data, product, alert)
            
            # 根据渠道发送告警
            channel = alert.get('channel', 'email')
            target = alert.get('target') or user.get('email')
            
            if channel == 'email' and target:
                return self._send_email_alert(target, message, alert_data)
            elif channel == 'webhook' and target:
                return self._send_webhook_alert(target, message, alert_data)
            elif channel == 'internal':
                return self._send_internal_alert(user['id'], message, alert_data)
            else:
                print(f"不支持的告警渠道: {channel}")
                return False
        
        except Exception as e:
            print(f"发送告警失败: {e}")
            return False
    
    def _build_alert_message(self, alert_data: Dict[str, Any], 
                           product: Dict[str, Any], alert: Dict[str, Any]) -> Dict[str, str]:
        """
        构建告警消息
        
        Args:
            alert_data: 告警数据
            product: 商品信息
            alert: 告警规则
        
        Returns:
            消息字典
        """
        product_name = product.get('name', '未知商品')
        product_url = product.get('url', '')
        old_price = alert_data['old_price']
        new_price = alert_data['new_price']
        currency = alert_data['currency']
        change_percent = alert_data['price_change_percent']
        
        # 判断价格变化方向
        if change_percent > 0:
            change_text = f"上涨了 {change_percent:.2f}%"
            emoji = "📈"
        else:
            change_text = f"下降了 {abs(change_percent):.2f}%"
            emoji = "📉"
        
        subject = f"价格告警: {product_name} 价格{change_text}"
        
        content = f"""
{emoji} 价格告警通知

商品名称: {product_name}
商品链接: {product_url}

价格变化:
• 原价格: {old_price} {currency}
• 现价格: {new_price} {currency}
• 变化幅度: {change_text}

告警规则: {alert.get('rule_type', '未知')}
触发时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

---
Price Memory 价格监控系统
        """.strip()
        
        return {
            'subject': subject,
            'content': content,
            'html_content': content.replace('\n', '<br>')
        }
    
    def _send_email_alert(self, email: str, message: Dict[str, str], 
                         alert_data: Dict[str, Any]) -> bool:
        """
        发送邮件告警
        
        Args:
            email: 邮箱地址
            message: 消息内容
            alert_data: 告警数据
        
        Returns:
            发送是否成功
        """
        if not all([config.SMTP_HOST, config.SMTP_USER, config.SMTP_PASS]):
            print("SMTP配置不完整，无法发送邮件")
            return False
        
        try:
            # 创建邮件
            msg = MimeMultipart('alternative')
            msg['Subject'] = message['subject']
            msg['From'] = config.SMTP_FROM or config.SMTP_USER
            msg['To'] = email
            
            # 添加文本和HTML内容
            text_part = MimeText(message['content'], 'plain', 'utf-8')
            html_part = MimeText(message['html_content'], 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # 发送邮件
            with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
                server.starttls()
                server.login(config.SMTP_USER, config.SMTP_PASS)
                server.send_message(msg)
            
            print(f"邮件告警发送成功: {email}")
            return True
        
        except Exception as e:
            print(f"邮件发送失败: {e}")
            return False
    
    def _send_webhook_alert(self, webhook_url: str, message: Dict[str, str], 
                          alert_data: Dict[str, Any]) -> bool:
        """
        发送Webhook告警
        
        Args:
            webhook_url: Webhook URL
            message: 消息内容
            alert_data: 告警数据
        
        Returns:
            发送是否成功
        """
        try:
            payload = {
                'type': 'price_alert',
                'message': message['content'],
                'data': alert_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            headers = {'Content-Type': 'application/json'}
            
            # 如果配置了webhook密钥，添加签名
            if config.ALERT_WEBHOOK_SECRET:
                import hmac
                import hashlib
                
                signature = hmac.new(
                    config.ALERT_WEBHOOK_SECRET.encode(),
                    json.dumps(payload).encode(),
                    hashlib.sha256
                ).hexdigest()
                
                headers['X-Signature'] = f"sha256={signature}"
            
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            print(f"Webhook告警发送成功: {webhook_url}")
            return True
        
        except Exception as e:
            print(f"Webhook发送失败: {e}")
            return False
    
    def _send_internal_alert(self, user_id: int, message: Dict[str, str], 
                           alert_data: Dict[str, Any]) -> bool:
        """
        发送站内消息告警
        
        Args:
            user_id: 用户ID
            message: 消息内容
            alert_data: 告警数据
        
        Returns:
            发送是否成功
        """
        try:
            # 插入站内消息
            self.repo.insert_internal_message(
                user_id=user_id,
                title=message['subject'],
                content=message['content'],
                message_type='price_alert',
                data=alert_data
            )
            
            print(f"站内消息发送成功: 用户 {user_id}")
            return True
        
        except Exception as e:
            print(f"站内消息发送失败: {e}")
            return False


# 全局实例
price_monitor = PriceMonitor()
alert_sender = AlertSender()


def check_and_send_price_alerts(product_id: int, new_price: float, currency: str) -> None:
    """
    检查并发送价格告警的便捷函数
    
    Args:
        product_id: 商品ID
        new_price: 新价格
        currency: 货币代码
    """
    try:
        # 检查价格变化
        triggered_alerts = price_monitor.check_price_changes(product_id, new_price, currency)
        
        # 发送告警
        for alert_data in triggered_alerts:
            alert_sender.send_alert(alert_data)
    
    except Exception as e:
        print(f"价格告警处理失败: {e}")