import os
from typing import Optional
from configparser import ConfigParser
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class Config:
    """应用配置类"""
    
    # Supabase配置
    SUPABASE_URL: Optional[str] = os.getenv("SUPABASE_URL")
    SUPABASE_KEY: Optional[str] = os.getenv("SUPABASE_KEY")
    
    # 数据库配置
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
    
    # API配置
    API_BASE_URL: str = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
    API_VERSION: str = "v1"
    
    # Playwright配置
    PLAYWRIGHT_WS_ENDPOINT: str = os.getenv("PLAYWRIGHT_WS_ENDPOINT", "ws://43.133.224.11:20001/")
    BROWSER_MODE: str = os.getenv("BROWSER_MODE", "remote")  # local or remote
    
    # 节点配置
    NODE_NAME: Optional[str] = os.getenv("NODE_NAME")
    NODE_REGION: str = os.getenv("NODE_REGION", "local")
    NODE_CONCURRENCY: int = int(os.getenv("NODE_CONCURRENCY", "1"))
    NODE_PAUSED: bool = os.getenv("NODE_PAUSED", "0") == "1"
    
    # 任务配置
    WORKER_TASK_RETRIES: int = int(os.getenv("WORKER_TASK_RETRIES", "2"))
    AUTO_CONSUME_QUEUE: bool = os.getenv("AUTO_CONSUME_QUEUE", "false").lower() in {"1", "true", "yes"}
    
    # 代理配置
    HTTP_PROXY_LIST: str = os.getenv("HTTP_PROXY_LIST", "")
    PLAYWRIGHT_PROXIES: str = os.getenv("PLAYWRIGHT_PROXIES", "")
    
    # 汇率配置
    EXCHANGE_RATES_SOURCE: Optional[str] = os.getenv("EXCHANGE_RATES_SOURCE")
    EXCHANGE_RATES_REFRESH_SEC: int = int(os.getenv("EXCHANGE_RATES_REFRESH_SEC", "3600"))
    
    # SMTP配置
    SMTP_HOST: Optional[str] = os.getenv("SMTP_HOST")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: Optional[str] = os.getenv("SMTP_USER")
    SMTP_PASS: Optional[str] = os.getenv("SMTP_PASS")
    SMTP_FROM: Optional[str] = os.getenv("SMTP_FROM")
    
    # Webhook配置
    ALERT_WEBHOOK_SECRET: Optional[str] = os.getenv("ALERT_WEBHOOK_SECRET")
    
    # 其他配置
    STRICT_HTTP: bool = os.getenv("STRICT_HTTP", "0") == "1"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() in {"1", "true", "yes"}
    
    @classmethod
    def get_proxy_list(cls) -> list[str]:
        """获取代理列表"""
        raw = cls.HTTP_PROXY_LIST or cls.PLAYWRIGHT_PROXIES or ""
        return [x.strip() for x in raw.split(",") if x.strip()]
    
    @classmethod
    def validate_config(cls) -> bool:
        """验证配置是否完整"""
        if not cls.SUPABASE_URL or not cls.SUPABASE_KEY:
            print("警告: Supabase配置缺失，将使用本地SQLite数据库")
            return False
        return True

# 全局配置实例
config = Config()

# 验证配置
config.validate_config()

