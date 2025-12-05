"""
数据库初始化脚本
用于初始化Supabase数据库连接和基础数据
"""
import os
import sys
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

# 添加src目录到Python路径
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
src_path = os.path.join(BASE_DIR, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from src.config.config import config
from src.dao.supabase_client import get_client


def check_database_connection() -> bool:
    """
    检查数据库连接
    
    Returns:
        连接是否成功
    """
    try:
        client = get_client()
        if not client:
            print("❌ 无法创建Supabase客户端")
            return False
        
        # 测试连接
        result = client.table("users").select("id").limit(1).execute()
        print("✅ 数据库连接成功")
        return True
    
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False


def init_default_sites() -> None:
    """初始化默认网站配置"""
    try:
        client = get_client()
        if not client:
            return
        
        default_sites = [
            {
                "domain": "amazon.com",
                "name": "Amazon US",
                "currency": "USD",
                "country": "US",
                "enabled": True
            },
            {
                "domain": "amazon.co.uk",
                "name": "Amazon UK",
                "currency": "GBP",
                "country": "UK",
                "enabled": True
            },
            {
                "domain": "amazon.de",
                "name": "Amazon DE",
                "currency": "EUR",
                "country": "DE",
                "enabled": True
            },
            {
                "domain": "taobao.com",
                "name": "淘宝",
                "currency": "CNY",
                "country": "CN",
                "enabled": True
            },
            {
                "domain": "tmall.com",
                "name": "天猫",
                "currency": "CNY",
                "country": "CN",
                "enabled": True
            },
            {
                "domain": "jd.com",
                "name": "京东",
                "currency": "CNY",
                "country": "CN",
                "enabled": True
            }
        ]
        
        for site in default_sites:
            # 检查是否已存在
            existing = client.table("sites").select("id").eq("domain", site["domain"]).execute()
            if not (existing.data if hasattr(existing, 'data') else []):
                # 插入新站点
                client.table("sites").insert(site).execute()
                print(f"✅ 添加网站: {site['name']}")
                        print(f"⚠️  网站已存在: {site['name']}")
    
    except Exception as e:
        print(f"❌ 初始化网站配置失败: {e}")


def init_default_user() -> Optional[Dict[str, Any]]:
    """
    初始化默认用户
    
    Returns:
        创建的用户信息
    """
    try:
        client = get_client()
        if not client:
            return None
        
        # 检查是否已有用户
        existing_users = client.table("users").select("id").limit(1).execute()
        if existing_users.data if hasattr(existing_users, 'data') else []:
            print("⚠️  已存在用户，跳过默认用户创建")
            return None
        
        # 创建默认用户
        import secrets
        api_key = secrets.token_urlsafe(32)
        
        default_user = {
            "username": "admin",
            "display_name": "管理员",
            "email": "admin@example.com",
            "api_key": api_key,
            "quota_tasks_per_day": 100,
            "tasks_created_today": 0,
            "last_tasks_quota_reset": datetime.utcnow().date().isoformat(),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        result = client.table("users").insert(default_user).select("*").execute()
        user_data = result.data[0] if (hasattr(result, 'data') and result.data) else None
        
        if user_data:
            print(f"✅ 创建默认用户成功")
            print(f"   用户名: {user_['username']}")
            print(f"   API Key: {user_data['api_key']}")
            print(f"   请保存API Key，用于API调用认证")
            return user_data
        
    except Exception as e:
        print(f"❌ 创建默认用户失败: {e}")
        return None


def init_sample_products() -> None:
    """初始化示例商品"""
    try:
        client = get_client()
        if not client:
            return
        
        sample_products = [
            {
                "name": "iPhone 15 Pro",
                "url": "https://www.amazon.com/dp/B0CHX1W1XY",
                "source_domain": "amazon.com",
                "category": "Electronics",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            },
            {
                "name": "MacBook Air M2",
                "url": "https://www.amazon.com/dp/B0B3C2R8MP",
                "source_domain": "amazon.com",
                "category": "Electronics",
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
        ]
        
        for product in sample_products:
            # 检查是否已存在
            existing = client.table("products").select("id").eq("url", product["url"]).execute()
            if not (existing.data if hasattr(existing, 'data') else []):
                # 插入新商品
                result = client.table("products").insert(product).select("*").execute()
                if hasattr(result, 'data') and result.data:
                    print(f"✅ 添加示例商品: {product['name']}")
            else:
                print(f"⚠️  商品已存在: {product['name']}")
    
    except Exception as e:
        print(f"❌ 初始化示例商品失败: {e}")


def check_table_structure() -> bool:
    """
    检查数据库表结构
    
    Returns:
        表结构是否正确
    """
    try:
        client = get_client()
        if not client:
            return False
        
        required_tables = [
            "users", "products", "skus", "prices", "tasks", 
            "alerts", "sites", "collections", "follows", "pushes"
        ]
        
        print("检查数据库表结构...")
        
        for table_name in required_tables:
            try:
                # 尝试查询表（只获取1条记录来测试表是否存在）
                client.table(table_name).select("*").limit(1).execute()
                print(f"✅ 表 {table_name} 存在")
            except Exception as e:
                print(f"❌ 表 {table_name} 不存在或无法访问: {e}")
                return False
        
        return True
    
    except Exception as e:
        print(f"❌ 检查表结构失败: {e}")
        return False


def init_database() -> bool:
    """
    初始化数据库
    
    Returns:
        初始化是否成功
    """
    print("🚀 开始初始化数据库...")
    print("=" * 50)
    
    # 检查配置
    if not config.SUPABASE_URL or not config.SUPABASE_KEY:
        print("❌ Supabase配置缺失")
        print("请在 .env 文件中配置 SUPABASE_URL 和 SUPABASE_KEY")
        return False
    
    print(f"📊 Supabase URL: {config.SUPABASE_URL}")
    
    # 检查数据库连接
    if not check_database_connection():
        return False
    
    # 检查表结构
    if not check_table_structure():
        print("❌ 数据库表结构不完整")
        print("请先在Supabase中执行 schema.sql 和 policies_and_rpc.sql")
        return False
    
    # 初始化基础数据
    print("\n📝 初始化基础数据...")
    init_default_sites()
    
    # 创建默认用户
    print("\n👤 初始化用户...")
    default_user = init_default_user()
    
    # 初始化示例商品
    print("\n🛍️  初始化示例商品...")
    init_sample_products()
    
    print("\n" + "=" * 50)
    print("✅ 数据库初始化完成!")
    
    if default_user:
        print(f"\n🔑 默认用户信息:")
        print(f"   用户名: {default_user['username']}")
        print(f"   API Key: {default_user['api_key']}")
        print(f"\n💡 使用API Key进行API调用:")
        print(f"   curl -H 'X-API-Key: {default_user['api_key']}' {config.API_BASE_URL}/products")
    
    return True


def main():
    """主函数"""
    try:
        success = init_database()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  初始化被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 初始化过程中发生异常: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()