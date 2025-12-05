# Price Memory API 文档

## 概述

Price Memory 是一个完整的电商价格监控解决方案，提供商品价格抓取、监控、告警和分析功能。

## 基础信息

- **Base URL**: `http://localhost:8000/api/v1`
- **认证方式**: API Key (Header: `X-API-Key`)
- **数据格式**: JSON
- **时间格式**: ISO 8601

## 认证

### 获取API Key

```bash
POST /users
Content-Type: application/json

{
  "username": "your_username",
  "display_name": "Your Display Name",
  "email": "your@email.com"
}
```

响应：
```json
{
  "success": true,
  "data": {
    "id": 1,
    "username": "your_username",
    "display_name": "Your Display Name",
    "created_at": "2024-01-01T00:00:00Z",
    "email": "your@email.com",
    "api_key": "your_api_key_here",
    "plan": "basic",
    "quota_exports_per_day": 5
  }
}
```

## 商品管理

### 创建商品

```bash
POST /products
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "name": "iPhone 15 Pro",
  "url": "https://www.amazon.com/dp/your-product-id",
  "category": "Electronics"
}
```

### 获取商品列表

```bash
GET /products?page=1&size=20&search=iphone&category=Electronics
Authorization: Bearer your_api_key
```

### 获取商品详情

```bash
GET /products/{product_id}
Authorization: Bearer your_api_key
```

### 更新商品

```bash
PATCH /products/{product_id}
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "name": "Updated Product Name",
  "category": "Updated Category"
}
```

### 删除商品

```bash
DELETE /products/{product_id}
Authorization: Bearer your_api_key
```

## 价格数据

### 获取价格历史

```bash
GET /products/{product_id}/prices?start_date=2024-01-01&end_date=2024-01-31
Authorization: Bearer your_api_key
```

### 获取价格趋势

```bash
GET /products/{product_id}/trend?granularity=daily&start_date=2024-01-01&end_date=2024-01-31
Authorization: Bearer your_api_key
```

参数说明：
- `granularity`: `daily` 或 `hourly`
- `start_date`: 开始日期 (YYYY-MM-DD)
- `end_date`: 结束日期 (YYYY-MM-DD)

### 导出价格数据

```bash
GET /products/{product_id}/export?start_date=2024-01-01&end_date=2024-01-31
Authorization: Bearer your_api_key
Accept: text/csv
```

支持格式：CSV, Excel (.xlsx)

## 任务管理

### 创建价格抓取任务

```bash
POST /spider/tasks
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "product_id": 123,
  "priority": 1
}
```

### 获取任务列表

```bash
GET /spider/tasks?status=pending&product_id=123
Authorization: Bearer your_api_key
```

状态：`pending`, `running`, `completed`, `failed`

### 手动执行任务

```bash
POST /spider/tasks/{task_id}/execute
Authorization: Bearer your_api_key
```

## 告警系统

### 创建告警规则

```bash
POST /alerts
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "user_id": 1,
  "product_id": 123,
  "rule_type": "price_drop",
  "threshold": 999.99,
  "percent": null,
  "cooldown_minutes": 60,
  "channel": "email",
  "target": "your@email.com"
}
```

告警类型：
- `price_drop`: 价格下降
- `price_rise`: 价格上涨
- `price_threshold`: 价格阈值
- `percent_change`: 百分比变化
- `anomaly`: 异常检测

### 获取告警列表

```bash
GET /alerts?user_id=1&product_id=123
Authorization: Bearer your_api_key
```

### 更新告警状态

```bash
POST /alerts/{alert_id}/status
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "status": "paused"
}
```

### 删除告警

```bash
DELETE /alerts/{alert_id}
Authorization: Bearer your_api_key
```

### 获取告警事件

```bash
GET /alerts/{alert_id}/events?page=1&size=20&status=sent
Authorization: Bearer your_api_key
```

状态：`pending`, `sent`, `failed`

## 用户管理

### 获取用户信息

```bash
GET /users/{user_id}
Authorization: Bearer your_api_key
```

### 更新用户偏好

```bash
POST /users/{user_id}/preferences
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "trend_ma_window": 10,
  "trend_bb_on": true
}
```

### 关注商品

```bash
POST /users/{user_id}/follows
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "product_id": 123
}
```

### 取消关注

```bash
DELETE /users/{user_id}/follows/{product_id}
Authorization: Bearer your_api_key
```

## 收藏夹和集合

### 创建收藏夹

```bash
POST /collections
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "name": "My Price Watchlist",
  "owner_user_id": 1
}
```

### 添加商品到收藏夹

```bash
POST /collections/{collection_id}/products
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "product_id": 123
}
```

### 分享收藏夹

```bash
POST /collections/{collection_id}/share
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "user_id": 2,
  "role": "editor"
}
```

角色：`admin`, `editor`, `viewer`

## 公共商品池

### 获取公共商品

```bash
GET /pools/public/products?page=1&size=20&search=phone
Authorization: Bearer your_api_key
```

### 选择公共商品

```bash
POST /users/{user_id}/select_from_pool
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "product_id": 123
}
```

## 推送消息

### 发送推送

```bash
POST /users/{sender_id}/pushes
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "recipient_id": 2,
  "product_id": 123,
  "message": "Check out this price!"
}
```

### 获取推送列表

```bash
GET /users/{user_id}/pushes?box=inbox
Authorization: Bearer your_api_key
```

盒子类型：`inbox`, `outbox`

### 更新推送状态

```bash
POST /pushes/{push_id}/status
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "status": "accepted"
}
```

状态：`pending`, `accepted`, `rejected`

## 批量操作

### 批量导出商品价格

```bash
GET /export?product_ids=1,2,3&start_date=2024-01-01&end_date=2024-01-31
Authorization: Bearer your_api_key
Accept: text/csv
```

### 批量创建任务

```bash
POST /spider/tasks/batch
Authorization: Bearer your_api_key
Content-Type: application/json

{
  "product_ids": [1, 2, 3, 4, 5],
  "priority": 1
}
```

## 系统状态

### 健康检查

```bash
GET /health
```

### 系统状态

```bash
GET /system/status
Authorization: Bearer your_api_key
```

### 获取权限

```bash
GET /auth/permissions
Authorization: Bearer your_api_key
```

## WebSocket 实时更新

### 连接地址

```
ws://localhost:8000/ws/price-updates?api_key=your_api_key
```

### 订阅价格更新

```json
{
  "type": "subscribe",
  "product_ids": [1, 2, 3]
}
```

### 取消订阅

```json
{
  "type": "unsubscribe",
  "product_ids": [1, 2]
}
```

## 错误处理

### 错误响应格式

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input parameters",
    "details": [
      {
        "field": "product_id",
        "message": "Product not found"
      }
    ]
  },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### 常见错误码

- `VALIDATION_ERROR`: 参数验证错误
- `NOT_FOUND`: 资源不存在
- `UNAUTHORIZED`: 未授权
- `FORBIDDEN`: 权限不足
- `QUOTA_EXCEEDED`: 配额超限
- `INTERNAL_ERROR`: 内部服务器错误

## 限流

- API 请求限流：100次/秒
- 导出配额：根据用户计划限制
- 任务创建配额：根据用户计划限制

## 示例代码

### Python

```python
import requests
import json

API_KEY = "your_api_key"
BASE_URL = "http://localhost:8000/api/v1"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# 创建商品
response = requests.post(f"{BASE_URL}/products", 
                        headers=headers,
                        json={
                            "name": "iPhone 15",
                            "url": "https://www.amazon.com/dp/your-product-id",
                            "category": "Electronics"
                        })
print(response.json())

# 获取价格趋势
response = requests.get(f"{BASE_URL}/products/123/trend?granularity=daily")
print(response.json())
```

### JavaScript

```javascript
const API_KEY = "your_api_key";
const BASE_URL = "http://localhost:8000/api/v1";

const headers = {
    "Authorization": `Bearer ${API_KEY}`,
    "Content-Type": "application/json"
};

// 创建告警
fetch(`${BASE_URL}/alerts`, {
    method: "POST",
    headers: headers,
    body: JSON.stringify({
        user_id: 1,
        product_id: 123,
        rule_type: "price_drop",
        threshold: 999.99,
        cooldown_minutes: 60,
        channel: "email",
        target: "your@email.com"
    })
}).then(response => response.json())
  .then(data => console.log(data));

// WebSocket 实时更新
const ws = new WebSocket(`ws://localhost:8000/ws/price-updates?api_key=${API_KEY}`);
ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log("Price update:", data);
};
```

## 最佳实践

1. **错误处理**: �始# 终检查API响应的状态码和错误信息
2. **重试机制**: 对于5xx错误实施指数退避重试
3. **配额管理**: 监控API使用配额，避免超限
4. **缓存策略**: 合理使用缓存减少API调用
5. **并发控制**: 避免同时发送过多请求
6. **数据验证**: 客户端也要进行基本的数据验证

## 版本历史

- v1.0.0: 初始版本，支持基础的价格监控功能
- v1.1.0: 新增告警系统和批量操作
- v1.2.0: 新增WebSocket实时更新
- v1.3.0: 新增收藏夹和共享功能

## 联系支持

如有问题或建议，请联系：support@pricememory.com