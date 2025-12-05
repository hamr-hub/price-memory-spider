"""
FastAPI应用主入口文件
提供完整的价格监控API服务
"""
import os
import sys
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 添加src目录到Python路径
BASE_DIR = os.path.dirname(__file__)
src_path = os.path.join(BASE_DIR, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from src.config.config import config
from src.api.routes import router as api_router
from src.runtime.node_runtime import NodeRuntime
from src.services.task_scheduler import task_scheduler, periodic_scheduler

# 创建FastAPI应用
app = FastAPI(
    title="Price Memory API",
    description="价格记忆 - 商品价格监控与分析API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册API路由
app.include_router(api_router, prefix="/api/v1")

# 启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动时的初始化操作"""
    print("🚀 Price Memory API 启动中...")
    
    # 启动节点运行时
    try:
        if config.SUPABASE_URL and config.SUPABASE_KEY:
            runtime = NodeRuntime()
            runtime.start()
            print("✅ 节点运行时已启动")
            
            # 启动任务调度器
            if config.AUTO_CONSUME_QUEUE:
                task_scheduler.start()
                periodic_scheduler.start()
                print("✅ 任务调度器已启动")
        else:
            print("⚠️  Supabase配置缺失，跳过节点运行时启动")
    except Exception as e:
        print(f"❌ 节点运行时启动失败: {e}")
    
    print("✅ Price Memory API 启动完成")

@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时的清理操作"""
    print("🛑 Price Memory API 正在关闭...")
    
    # 停止任务调度器
    try:
        task_scheduler.stop()
        periodic_scheduler.stop()
        print("✅ 任务调度器已停止")
    except Exception as e:
        print(f"❌ 停止任务调度器失败: {e}")

# 健康检查端点
@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {
        "status": "healthy",
        "service": "Price Memory API",
        "version": "1.0.0"
    }

# 根路径重定向到文档
@app.get("/")
async def root():
    """根路径，重定向到API文档"""
    return {
        "message": "Welcome to Price Memory API",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health"
    }

def main():
    """主函数，用于直接运行应用"""
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=config.DEBUG,
        log_level="info" if not config.DEBUG else "debug"
    )

if __name__ == "__main__":
    main()