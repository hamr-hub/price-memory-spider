"""
WebSocket处理器
处理实时价格更新和任务状态推送
"""
import asyncio
import json
import logging
from typing import Dict, Set, List, Optional
from datetime import datetime
import websockets
from websockets.server import WebSocketServerProtocol

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo
from ..services.price_history_service import PriceHistoryService
from ..services.enhanced_price_scraper import EnhancedPriceScraper, ScrapingConfig


class WebSocketHandler:
    """WebSocket处理器"""
    
    def __init__(self):
        self.repo = SupabaseRepo()
        self.price_service = PriceHistoryService()
        self.connected_clients: Set[WebSocketServerProtocol] = set()
        self.client_subscriptions: Dict[WebSocketServerProtocol, Set[int]] = {}
        self.logger = logging.getLogger(__name__)
        
        # 启动后台任务
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """启动后台任务"""
        # 可以在这里启动定期推送任务
        pass
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """处理客户端连接"""
        self.connected_clients.add(websocket)
        self.client_subscriptions[websocket] = set()
        
        self.logger.info(f"新客户端连接: {websocket.remote_address}")
        
        try:
            async for message in websocket:
                await self.handle_message(websocket, message)
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"客户端断开连接: {websocket.remote_address}")
        finally:
            await self.disconnect_client(websocket)
    
    async def disconnect_client(self, websocket: WebSocketServerProtocol):
        """断开客户端连接"""
        self.connected_clients.discard(websocket)
        if websocket in self.client_subscriptions:
            del self.client_subscriptions[websocket]
        
        self.logger.info(f"客户端已断开: {websocket.remote_address}")
    
    async def handle_message(self, websocket: WebSocketServerProtocol, message: str):
        """处理客户端消息"""
        try:
            data = json.loads(message)
            message_type = data.get("type")
            
            if message_type == "subscribe":
                await self.handle_subscribe(websocket, data)
            elif message_type == "unsubscribe":
                await self.handle_unsubscribe(websocket, data)
            elif message_type == "ping":
                await self.send_pong(websocket)
            else:
                await self.send_error(websocket, f"未知的消息类型: {message_type}")
                
        except json.JSONDecodeError:
            await self.send_error(websocket, "无效的JSON消息")
        except Exception as e:
            self.logger.error(f"处理消息时出错: {e}")
            await self.send_error(websocket, str(e))
    
    async def handle_subscribe(self, websocket: WebSocketServerProtocol, data: Dict):
        """处理订阅请求"""
        product_ids = data.get("product_ids", [])
        
        if not isinstance(product_ids, list):
            await self.send_error(websocket, "product_ids必须是数组")
            return
        
        # 验证商品ID
        valid_product_ids = []
        for product_id in product_ids:
            if isinstance(product_id, int) and self.repo.get_product(product_id):
                valid_product_ids.append(product_id)
        
        # 更新订阅
        self.client_subscriptions[websocket] = set(valid_product_ids)
        
        # 发送确认消息
        await self.send_message(websocket, {
            "type": "subscription_confirmed",
            "product_ids": valid_product_ids,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        self.logger.info(f"客户端订阅商品: {websocket.remote_address} -> {valid_product_ids}")
    
    async def handle_unsubscribe(self, websocket: WebSocketServerProtocol, data: Dict):
        """处理取消订阅请求"""
        product_ids = data.get("product_ids", [])
        
        if not isinstance(product_ids, list):
            await self.send_error(websocket, "product_ids必须是数组")
            return
        
        # 从订阅中移除
        current_subscriptions = self.client_subscriptions.get(websocket, set())
        current_subscriptions.difference_update(product_ids)
        self.client_subscriptions[websocket] = current_subscriptions
        
        # 发送确认消息
        await self.send_message(websocket, {
            "type": "unsubscription_confirmed",
            "product_ids": product_ids,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        self.logger.info(f"客户端取消订阅: {websocket.remote_address} -> {product_ids}")
    
    async def send_pong(self, websocket: WebSocketServerProtocol):
        """发送pong响应"""
        await self.send_message(websocket, {
            "type": "pong",
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def send_error(self, websocket: WebSocketServerProtocol, error_message: str):
        """发送错误消息"""
        await self.send_message(websocket, {
            "type": "error",
            "message": error_message,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def send_message(self, websocket: WebSocketServerProtocol, message: Dict):
        """发送消息给客户端"""
        try:
            await websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            pass  # 客户端已断开
        except Exception as e:
            self.logger.error(f"发送消息失败: {e}")
    
    async def broadcast_price_update(self, product_id: int, price: float, currency: str, change: Optional[float] = None):
        """广播价格更新"""
        message = {
            "type": "price_update",
            "product_id": product_id,
            "price": price,
            "currency": currency,
            "change": change,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # 发送给订阅了该商品的客户端
        for client, subscriptions in self.client_subscriptions.items():
            if product_id in subscriptions:
                try:
                    await self.send_message(client, message)
                except Exception as e:
                    self.logger.error(f"广播价格更新失败: {e}")
    
    async def broadcast_task_update(self, task_id: int, status: str, product_id: int, 
                                  price: Optional[float] = None, error_message: Optional[str] = None):
        """广播任务更新"""
        message = {
            "type": "task_update",
            "task_id": task_id,
            "status": status,
            "product_id": product_id,
            "price": price,
            "error_message": error_message,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # 发送给所有客户端
        for client in self.connected_clients:
            try:
                await self.send_message(client, message)
            except Exception as e:
                self.logger.error(f"广播任务更新失败: {e}")
    
    async def broadcast_system_status(self, status: Dict):
        """广播系统状态"""
        message = {
            "type": "system_status",
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # 发送给所有客户端
        for client in self.connected_clients:
            try:
                await self.send_message(client, message)
            except Exception as e:
                self.logger.error(f"广播系统状态失败: {e}")


# 全局WebSocket处理器实例
websocket_handler = WebSocketHandler()


# WebSocket路由
async def websocket_endpoint(websocket: WebSocketServerProtocol, path: str):
    """WebSocket端点"""
    await websocket_handler.handle_client(websocket, path)


# 启动WebSocket服务器的函数
async def start_websocket_server(host: str = "localhost", port: int = 8001):
    """启动WebSocket服务器"""
    server = await websockets.serve(
        websocket_endpoint,
        host,
        port,
        ping_interval=20,
        ping_timeout=20,
        max_size=1024 * 1024  # 1MB
    )
    
    logging.info(f"WebSocket服务器启动在 {host}:{port}")
    
    await server.wait_closed()


# 便捷函数：启动WebSocket服务器
def run_websocket_server(host: str = "localhost", port: int = 8001):
    """运行WebSocket服务器"""
    try:
        asyncio.run(start_websocket_server(host, port))
    except KeyboardInterrupt:
        logging.info("WebSocket服务器已停止")