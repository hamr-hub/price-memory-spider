from urllib.parse import urlparse
from typing import NamedTuple, Optional


class URLInfo(NamedTuple):
    """URL解析结果的数据结构"""
    scheme: str
    netloc: str
    hostname: str
    port: Optional[int]
    path: str
    params: str
    query: str
    fragment: str


def parse_url(url: str) -> URLInfo:
    """
    解析URL并返回详细信息
    
    Args:
        url (str): 要解析的URL字符串
        
    Returns:
        URLInfo: 包含URL各部分信息的命名元组
        
    Raises:
        ValueError: 当URL格式无效时抛出异常
    """
    if not url or not isinstance(url, str):
        raise ValueError("URL不能为空且必须是字符串类型")
    
    try:
        parsed = urlparse(url)
        
        # 提取hostname和port
        hostname = parsed.hostname or ""
        port = parsed.port
        
        # 如果没有明确指定端口，根据scheme使用默认端口
        if port is None and parsed.scheme:
            if parsed.scheme.lower() == 'https':
                port = 443
            elif parsed.scheme.lower() == 'http':
                port = 80
        
        return URLInfo(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            hostname=hostname,
            port=port,
            path=parsed.path,
            params=parsed.params,
            query=parsed.query,
            fragment=parsed.fragment
        )
    except Exception as e:
        raise ValueError(f"URL解析失败: {e}")


def get_base_url(url: str) -> str:
    """
    获取URL的基础部分（scheme + netloc）
    
    Args:
        url (str): 完整URL
        
    Returns:
        str: 基础URL（如：https://www.example.com）
    """
    url_info = parse_url(url)
    return f"{url_info.scheme}://{url_info.netloc}"


def get_domain(url: str) -> str:
    """
    获取URL的域名部分
    
    Args:
        url (str): 完整URL
        
    Returns:
        str: 域名
    """
    url_info = parse_url(url)
    return url_info.hostname


def is_valid_url(url: str) -> bool:
    """
    验证URL是否有效
    
    Args:
        url (str): 要验证的URL
        
    Returns:
        bool: URL是否有效
    """
    try:
        parse_url(url)
        return True
    except ValueError:
        return False


def extract_url_components(url: str) -> dict:
    """
    以字典形式返回URL的各个组件
    
    Args:
        url (str): 要解析的URL
        
    Returns:
        dict: URL组件字典
    """
    url_info = parse_url(url)
    return {
        'scheme': url_info.scheme,
        'netloc': url_info.netloc,
        'hostname': url_info.hostname,
        'port': url_info.port,
        'path': url_info.path,
        'params': url_info.params,
        'query': url_info.query,
        'fragment': url_info.fragment,
        'base_url': get_base_url(url)
    }

def get_link_latency(browser, page, link, load_state="load"):
    """
    测试单个链接的访问延迟
    :param page: Playwright的page对象（用于新标签页访问）
    :param link: 要测试的绝对URL
    :param load_state: 延迟结束条件（关键！可选值：domcontentloaded/load/networkidle）
    :return: 延迟时间（秒），失败返回None
    """
    import time
    try:
        # 记录开始时间戳（发起请求前）
        start_time = time.time()
        # 打开新标签页访问链接（避免影响原页面）
        with browser.new_context().new_page() as new_page:
            # 访问链接 + 等待指定加载状态（延迟结束的判断标准）
            new_page.goto(link, wait_until=load_state)
            # 记录结束时间戳
            end_time = time.time()
        # 计算延迟（保留3位小数）
        latency = round(end_time - start_time, 3)
        return latency
    except Exception as e:
        print(f"测试链接失败 [{link}]: {str(e)[:50]}")  # 打印错误（截取前50字）
        return None


# 示例使用
if __name__ == "__main__":
    # 测试URL
    test_urls = [
        "https://www.amazon.co.za/?ref_=icp_country_from_us",
        "http://example.com:8080/path?param=value#fragment",
        "https://subdomain.example.co.uk:443/api/v1/users"
    ]
    
    for url in test_urls:
        print(f"\n=== 解析URL: {url} ===")
        try:
            url_info = parse_url(url)
            print(f"协议 (scheme): {url_info.scheme}")
            print(f"网络位置 (netloc): {url_info.netloc}")
            print(f"主机名 (hostname): {url_info.hostname}")
            print(f"端口 (port): {url_info.port}")
            print(f"路径 (path): {url_info.path}")
            print(f"查询参数 (query): {url_info.query}")
            print(f"基础URL: {get_base_url(url)}")
            print(f"域名: {get_domain(url)}")
            
        except ValueError as e:
            print(f"解析失败: {e}")