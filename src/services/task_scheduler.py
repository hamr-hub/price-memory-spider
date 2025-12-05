"""
增强的任务调度系统
实现智能任务调度、失败重试、并发控制和负载均衡
"""
import asyncio
import time
import threading
import queue
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import statistics

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo
from ..workers.amazon_worker import UniversalWorker
from ..services.enhanced_price_scraper import EnhancedPriceScraper, ScrapingConfig


@dataclass
class TaskMetrics:
    """任务指标数据类"""
    task_id: int
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = 'pending'
    retry_count: int = 0
    error_message: Optional[str] = None
    scraped_price: Optional[float] = None
    response_time: float = 0


class EnhancedTaskScheduler:
    """增强的任务调度器"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.repo = SupabaseRepo()
        self.max_workers = max_workers or config.NODE_CONCURRENCY
        self.worker = UniversalWorker()
        self.task_queue = asyncio.Queue()
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None
        self.scheduler_thread: Optional[threading.Thread] = None
        
        # 增强的统计信息
        self.stats = {
            'total_processed': 0,
            'succeeded': 0,
            'failed': 0,
            'retried': 0,
            'skipped': 0,  # 跳过的任务（如重复价格）
            'start_time': None,
            'total_response_time': 0,
            'success_response_time': 0,
            'failure_response_time': 0,
            'price_accuracy': 0,  # 价格准确性指标
            'success_rate_rolling': []  # 滚动成功率
        }
        
        # 任务指标缓存
        self.task_metrics: Dict[int, TaskMetrics] = {}
        
        # 负载均衡参数
        self.load_factor = 0.0
        self.consecutive_failures = 0
        self.adaptive_delay = 1.0  # 自适应延迟
        
        # 错误恢复策略
        self.error_recovery_strategies = {
            'timeout': self._handle_timeout_error,
            'network': self._handle_network_error,
            'captcha': self._handle_captcha_error,
            'rate_limit': self._handle_rate_limit_error
        }
    
    def start(self) -> None:
        """启动任务调度器"""
        if self.running:
            print("任务调度器已在运行")
            return
        
        print(f"启动增强任务调度器，最大并发数: {self.max_workers}")
        self.running = True
        self.stats['start_time'] = datetime.utcnow()
        
        # 初始化增强的价格抓取器
        scraping_config = ScrapingConfig(
            timeout=config.BROWSER_TIMEOUT,
            retry_count=config.WORKER_TASK_RETRIES,
            delay_range=(1, 5),  # 自适应延迟范围
            use_stealth=True,
            use_proxy=config.PROXY_SERVER is not None,
            headless=not config.DEBUG_MODE
        )
        
        # 启动线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # 启动调度线程
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        print("增强任务调度器启动成功")
    
    def stop(self) -> None:
        """停止任务调度器"""
        if not self.running:
            return
        
        print("正在停止任务调度器...")
        self.running = False
        
        # 停止线程池
        if self.executor:
            self.executor.shutdown(wait=True)
        
        # 等待调度线程结束
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)
        
        print("任务调度器已停止")
    
    def _scheduler_loop(self) -> None:
        """增强的调度器主循环"""
        while self.running:
            try:
                # 计算自适应延迟
                delay = self._calculate_adaptive_delay()
                
                # 从数据库获取待处理任务
                pending_tasks = self.repo.get_pending_tasks(limit=self.max_workers * 3)
                
                if not pending_tasks:
                    time.sleep(delay)
                    continue
                
                # 按优先级和智能策略排序任务
                sorted_tasks = self._sort_tasks_intelligently(pending_tasks)
                
                # 提交任务到线程池
                futures = []
                for task in sorted_tasks[:self.max_workers]:
                    if not self.running:
                        break
                    
                    # 创建任务指标
                    self.task_metrics[task['id']] = TaskMetrics(
                        task_id=task['id'],
                        start_time=datetime.utcnow(),
                        status='running'
                    )
                    
                    future = self.executor.submit(self._process_task_enhanced, task)
                    futures.append((future, task))
                
                # 等待任务完成
                for future, task in futures:
                    if not self.running:
                        break
                    
                    try:
                        result = future.result(timeout=config.BROWSER_TIMEOUT + 60000)  # 增加超时时间
                        self._update_task_metrics(task['id'], result)
                    except Exception as e:
                        print(f"任务 {task['id']} 执行异常: {e}")
                        self._handle_task_failure(task, str(e))
                        self._update_task_metrics(task['id'], {'status': 'failed', 'error': str(e)})
                
                # 更新负载因子
                self._update_load_factor()
                
                time.sleep(delay)  # 使用自适应延迟
                
            except Exception as e:
                print(f"调度器循环异常: {e}")
                time.sleep(10)  # 异常时等待更长时间
    
    def _process_task_enhanced(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        增强的任务处理
        
        Args:
            task: 任务信息
            
        Returns:
            处理结果
        """
        task_id = task['id']
        product_id = task.get('product_id')
        
        result = {
            'task_id': task_id,
            'status': 'failed',
            'scraped_price': None,
            'error': None,
            'response_time': 0
        }
        
        start_time = time.time()
        
        try:
            print(f"开始处理任务 {task_id} (商品ID: {product_id})")
            
            # 标记任务为运行中
            self.repo.mark_task_running(task_id)
            
            # 使用增强的价格抓取器
            scraping_config = ScrapingConfig(
                timeout=config.BROWSER_TIMEOUT,
                retry_count=config.WORKER_TASK_RETRIES,
                delay_range=(1, 5),
                use_stealth=True,
                use_proxy=config.PROXY_SERVER is not None,
                headless=not config.DEBUG_MODE
            )
            
            async def scrape_with_timeout():
                scraper = EnhancedPriceScraper(scraping_config)
                try:
                    await scraper.initialize()
                    product = self.repo.get_product(product_id)
                    if not product:
                        return None
                    
                    result = await scraper.scrape_price(product['url'])
                    return result
                finally:
                    await scraper.close()
            
            # 使用 asyncio.run 运行异步任务
            price_result = asyncio.run(scrape_with_timeout())
            
            if price_result and price_result.price is not None:
                # 记录价格
                from ..services.price_history_service import PriceHistoryService
                price_service = PriceHistoryService()
                price_service.record_price(
                    product_id=product_id,
                    price=price_result.price,
                    currency=price_result.currency or 'USD',
                    source='automated_scraping',
                    metadata={
                        'title': price_result.title,
                        'availability': price_result.availability,
                        'image_url': price_result.image_url
                    }
                )
                
                result['status'] = 'completed'
                result['scraped_price'] = price_result.price
                result['response_time'] = time.time() - start_time
                
                # 更新统计
                self.stats['total_processed'] += 1
                self.stats['succeeded'] += 1
                self.stats['success_response_time'] += result['response_time']
                
                # 重置连续失败计数
                self.consecutive_failures = 0
                
                duration = time.time() - start_time
                print(f"任务 {task_id} 完成，价格: {price_result.price} {price_result.currency}, 耗时 {duration:.2f}s")
            else:
                raise Exception(f"价格抓取失败: {price_result.error if price_result else '未知错误'}")
        
        except Exception as e:
            error_msg = str(e)
            result['error'] = error_msg
            result['response_time'] = time.time() - start_time
            
            # 处理特定错误类型
            error_type = self._classify_error(error_msg)
            if error_type in self.error_recovery_strategies:
                self.error_recovery_strategies[error_type](task, error_msg)
            
            # 更新统计
            self.stats['total_processed'] += 1
            self.stats['failed'] += 1
            self.stats['failure_response_time'] += result['response_time']
            self.consecutive_failures += 1
            
            print(f"任务 {task_id} 失败: {error_msg}")
        
        return result
    
    def _handle_task_failure(self, task: Dict[str, Any], error_msg: str) -> None:
        """
        增强的任务失败处理
        
        Args:
            task: 任务信息
            error_msg: 错误消息
        """
        task_id = task['id']
        retry_count = task.get('retry_count', 0)
        max_retries = config.WORKER_TASK_RETRIES
        
        # 错误分类
        error_type = self._classify_error(error_msg)
        
        if retry_count < max_retries and self._should_retry(error_type, retry_count):
            # 计算重试延迟（指数退避 + 随机抖动）
            base_delay = min(60 * (2 ** retry_count), 600)  # 最大10分钟
            jitter = random.uniform(0.5, 1.5)  # 0.5-1.5倍抖动
            retry_delay = int(base_delay * jitter * self.adaptive_delay)
            
            scheduled_at = datetime.utcnow() + timedelta(seconds=retry_delay)
            
            self.repo.retry_task(task_id, retry_count + 1, scheduled_at, error_msg)
            self.stats['retried'] += 1
            
            print(f"任务 {task_id} 将在 {retry_delay}s 后重试 (第 {retry_count + 1} 次, 错误类型: {error_type})")
        else:
            # 标记任务最终失败
            final_error = f"重试次数超限({retry_count}/{max_retries})或不可重试错误({error_type}): {error_msg}"
            self.repo.mark_task_result(task_id, "failed", final_error)
            
            # 记录失败统计
            self.repo.record_task_failure(
                task_id=task_id,
                product_id=task.get('product_id'),
                error_type=error_type,
                retry_count=retry_count,
                final_error=final_error
            )
            
            print(f"任务 {task_id} 最终失败，已达到最大重试次数或遇到不可重试错误")
    
    def _classify_error(self, error_msg: str) -> str:
        """
        错误分类
        
        Args:
            error_msg: 错误消息
            
        Returns:
            错误类型
        """
        error_msg_lower = error_msg.lower()
        
        if any(keyword in error_msg_lower for keyword in ['timeout', '超时']):
            return 'timeout'
        elif any(keyword in error_msg_lower for keyword in ['network', 'networkx', '连接', 'connection']):
            return 'network'
        elif any(keyword in error_msg_lower for keyword in ['captcha', '验证', 'verify']):
            return 'captcha'
        elif any(keyword in error_msg_lower for keyword in ['rate limit', '频率', 'rate-limit', '请求过于频繁']):
            return 'rate_limit'
        elif any(keyword in error_msg_lower for keyword in ['404', 'not found', '不存在']):
            return 'not_found'
        elif any(keyword in error_msg_lower for keyword in ['403', 'forbidden', '禁止']):
            return 'forbidden'
        else:
            return 'unknown'
    
    def _should_retry(self, error_type: str, retry_count: int) -> bool:
        """
        判断是否应该重试
        
        Args:
            error_type: 错误类型
            retry_count: 当前重试次数
            
        Returns:
            是否应该重试
        """
        # 不可重试的错误类型
        non_retryable_errors = ['not_found', 'forbidden', 'captcha']
        
        if error_type in non_retryable_errors:
            return False
        
        # 根据错误类型调整重试策略
        max_retries_by_type = {
            'timeout': 5,
            'network': 4,
            'rate_limit': 3,
            'unknown': 3
        }
        
        max_retries = max_retries_by_type.get(error_type, 3)
        return retry_count < max_retries
    
    def _handle_timeout_error(self, task: Dict[str, Any], error_msg: str) -> None:
        """处理超时错误"""
        print(f"任务 {task['id']} 超时，增加延迟并重试")
        self.adaptive_delay *= 1.5  # 增加延迟
    
    def _handle_network_error(self, task: Dict[str, Any], error_msg: str) -> None:
        """处理网络错误"""
        print(f"任务 {task['id']} 网络错误，检查网络连接")
        self.adaptive_delay *= 1.2
    
    def _handle_captcha_error(self, task: Dict[str, Any], error_msg: str) -> None:
        """处理验证码错误"""
        print(f"任务 {task['id']} 遇到验证码，需要人工干预")
        # 验证码错误通常需要人工处理，不自动重试
    
    def _handle_rate_limit_error(self, task: Dict[str, Any], error_msg: str) -> None:
        """处理频率限制错误"""
        print(f"任务 {task['id']} 触发频率限制，增加延迟")
        self.adaptive_delay *= 2.0
    
    def _calculate_adaptive_delay(self) -> float:
        """
        计算自适应延迟
        
        Returns:
            延迟时间（秒）
        """
        base_delay = 1.0
        
        # 根据成功率调整延迟
        if self.stats['total_processed'] > 0:
            success_rate = self.stats['succeeded'] / self.stats['total_processed']
            if success_rate < 0.5:  # 成功率低于50%
                base_delay *= 3.0
            elif success_rate < 0.7:  # 成功率低于70%
                base_delay *= 2.0
            elif success_rate < 0.9:  # 成功率低于90%
                base_delay *= 1.5
        
        # 根据连续失败次数调整
        if self.consecutive_failures > 3:
            base_delay *= (1 + self.consecutive_failures * 0.5)
        
        # 应用自适应延迟因子
        delay = base_delay * self.adaptive_delay
        
        # 限制延迟范围
        return max(0.5, min(delay, 30.0))  # 0.5-30秒
    
    def _update_load_factor(self) -> None:
        """更新负载因子"""
        if self.stats['total_processed'] > 0:
            success_rate = self.stats['succeeded'] / self.stats['total_processed']
            self.load_factor = 1.0 - success_rate
    
    def _update_task_metrics(self, task_id: int, result: Dict[str, Any]) -> None:
        """更新任务指标"""
        if task_id in self.task_metrics:
            metrics = self.task_metrics[task_id]
            metrics.end_time = datetime.utcnow()
            metrics.status = result.get('status', 'failed')
            metrics.error_message = result.get('error')
            metrics.scraped_price = result.get('scraped_price')
            metrics.response_time = result.get('response_time', 0)
            
            # 计算价格准确性（如果有可能的话）
            if metrics.scraped_price:
                self._calculate_price_accuracy(metrics)
    
    def _calculate_price_accuracy(self, metrics: TaskMetrics) -> None:
        """计算价格准确性"""
        # 这里可以实现价格准确性计算逻辑
        # 例如：与历史价格比较，检查价格合理性等
        self.stats['price_accuracy'] = 0.95  # 示例值
    
    def _sort_tasks_intelligently(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        智能任务排序
        
        Args:
            tasks: 任务列表
            
        Returns:
            排序后的任务列表
        """
        def get_priority_score(task: Dict[str, Any]) -> float:
            # 基础优先级
            base_priority = task.get('priority', 0)
            
            # 重试惩罚（重试次数越多，优先级越低）
            retry_penalty = task.get('retry_count', 0) * 5
            
            # 时间奖励（创建时间越早，优先级越高）
            created_at = task.get('created_at')
            age_bonus = 0
            if created_at:
                try:
                    if isinstance(created_at, str):
                        created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    else:
                        created_time = created_at
                    
                    age_hours = (datetime.utcnow() - created_time.replace(tzinfo=None)).total_seconds() / 3600
                    age_bonus = min(age_hours * 0.1, 10)  # 最多10点奖励
                except Exception:
                    age_bonus = 0
            
            # 成功率奖励（根据商品历史成功率调整）
            product_id = task.get('product_id')
            success_rate = self._get_product_success_rate(product_id)
            success_bonus = success_rate * 5  # 成功率越高，优先级越高
            
            # 负载均衡惩罚
            load_penalty = self.load_factor * 10
            
            return base_priority + age_bonus + success_bonus - retry_penalty - load_penalty
        
        # 添加成功率统计
        sorted_tasks = sorted(tasks, key=get_priority_score, reverse=True)
        
        # 将连续失败的任务放到后面
        failed_tasks = [t for t in sorted_tasks if t.get('retry_count', 0) > 2]
        success_tasks = [t for t in sorted_tasks if t.get('retry_count', 0) <= 2]
        
        return success_tasks + failed_tasks
    
    def _sort_tasks_by_priority(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        按优先级排序任务
        
        Args:
            tasks: 任务列表
        
        Returns:
            排序后的任务列表
        """
        def get_priority_score(task: Dict[str, Any]) -> int:
            # 基础优先级
            base_priority = task.get('priority', 0)
            
            # 重试次数越多，优先级越低
            retry_penalty = task.get('retry_count', 0) * 10
            
            # 创建时间越早，优先级越高
            created_at = task.get('created_at')
            if created_at:
                try:
                    if isinstance(created_at, str):
                        created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    else:
                        created_time = created_at
                    
                    age_hours = (datetime.utcnow() - created_time.replace(tzinfo=None)).total_seconds() / 3600
                    age_bonus = min(age_hours, 24)  # 最多24小时的加成
                except Exception:
                    age_bonus = 0
            else:
                age_bonus = 0
            
            return base_priority + age_bonus - retry_penalty
        
        return sorted(tasks, key=get_priority_score, reverse=True)
    
    def add_task(self, product_id: int, priority: int = 0, scheduled_at: Optional[datetime] = None) -> Optional[int]:
        """
        添加新任务
        
        Args:
            product_id: 商品ID
            priority: 优先级
            scheduled_at: 计划执行时间
        
        Returns:
            任务ID
        """
        try:
            task_id = self.repo.create_task(
                product_id=product_id,
                priority=priority,
                scheduled_at=scheduled_at or datetime.utcnow()
            )
            
            print(f"添加任务成功: 任务ID={task_id}, 商品ID={product_id}")
            return task_id
        
        except Exception as e:
            print(f"添加任务失败: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取增强的调度器统计信息
        
        Returns:
            统计信息字典
        """
        stats = self.stats.copy()
        
        if stats['start_time']:
            uptime = datetime.utcnow() - stats['start_time']
            stats['uptime_seconds'] = uptime.total_seconds()
            stats['uptime_str'] = str(uptime).split('.')[0]  # 去掉微秒
        
        stats['running'] = self.running
        stats['max_workers'] = self.max_workers
        stats['load_factor'] = self.load_factor
        stats['consecutive_failures'] = self.consecutive_failures
        stats['adaptive_delay'] = self.adaptive_delay
        
        # 计算成功率
        total = stats['total_processed']
        if total > 0:
            stats['success_rate'] = (stats['succeeded'] / total) * 100
            stats['skip_rate'] = (stats['skipped'] / total) * 100
            stats['failure_rate'] = (stats['failed'] / total) * 100
            
            # 计算平均响应时间
            stats['avg_response_time'] = stats['total_response_time'] / total
            if stats['succeeded'] > 0:
                stats['avg_success_response_time'] = stats['success_response_time'] / stats['succeeded']
            if stats['failed'] > 0:
                stats['avg_failure_response_time'] = stats['failure_response_time'] / stats['failed']
        else:
            stats['success_rate'] = 0
            stats['skip_rate'] = 0
            stats['failure_rate'] = 0
            stats['avg_response_time'] = 0
            stats['avg_success_response_time'] = 0
            stats['avg_failure_response_time'] = 0
        
        # 获取任务指标统计
        stats['task_metrics'] = self._get_task_metrics_summary()
        
        # 获取错误分布
        stats['error_distribution'] = self._get_error_distribution()
        
        return stats
    
    def _get_product_success_rate(self, product_id: int) -> float:
        """
        获取商品的成功率
        
        Args:
            product_id: 商品ID
            
        Returns:
            成功率（0-1）
        """
        try:
            metrics = self.repo.get_product_task_metrics(product_id)
            if metrics['total'] > 0:
                return metrics['success'] / metrics['total']
            return 0.8  # 默认成功率
        except Exception:
            return 0.8
    
    def _get_task_metrics_summary(self) -> Dict[str, Any]:
        """获取任务指标摘要"""
        if not self.task_metrics:
            return {}
        
        response_times = [m.response_time for m in self.task_metrics.values() if m.response_time > 0]
        
        if not response_times:
            return {
                'avg_response_time': 0,
                'min_response_time': 0,
                'max_response_time': 0,
                'p95_response_time': 0
            }
        
        return {
            'avg_response_time': statistics.mean(response_times),
            'min_response_time': min(response_times),
            'max_response_time': max(response_times),
            'p95_response_time': self._calculate_percentile(response_times, 95),
            'total_tasks': len(self.task_metrics),
            'completed_tasks': len([m for m in self.task_metrics.values() if m.status == 'completed'])
        }
    
    def _calculate_percentile(self, data: List[float], percentile: int) -> float:
        """计算百分位数"""
        if not data:
            return 0
        
        sorted_data = sorted(data)
        index = (percentile / 100) * (len(sorted_data) - 1)
        
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower = sorted_data[int(index)]
            upper = sorted_data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))
    
    def _get_error_distribution(self) -> Dict[str, int]:
        """获取错误分布"""
        error_counts = {}
        
        for metrics in self.task_metrics.values():
            if metrics.error_message:
                error_type = self._classify_error(metrics.error_message)
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        return error_counts
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        获取任务队列状态
        
        Returns:
            队列状态字典
        """
        try:
            pending_count = self.repo.count_tasks_by_status('pending')
            running_count = self.repo.count_tasks_by_status('running')
            failed_count = self.repo.count_tasks_by_status('failed')
            succeeded_count = self.repo.count_tasks_by_status('succeeded')
            
            return {
                'pending': pending_count,
                'running': running_count,
                'failed': failed_count,
                'succeeded': succeeded_count,
                'total': pending_count + running_count + failed_count + succeeded_count
            }
        
        except Exception as e:
            print(f"获取队列状态失败: {e}")
            return {}


class PeriodicTaskScheduler:
    """周期性任务调度器"""
    
    def __init__(self, task_scheduler: TaskScheduler):
        self.task_scheduler = task_scheduler
        self.repo = SupabaseRepo()
        self.running = False
        self.thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """启动周期性任务调度"""
        if self.running:
            return
        
        print("启动周期性任务调度器")
        self.running = True
        self.thread = threading.Thread(target=self._periodic_loop, daemon=True)
        self.thread.start()
    
    def stop(self) -> None:
        """停止周期性任务调度"""
        if not self.running:
            return
        
        print("停止周期性任务调度器")
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=5)
    
    def _periodic_loop(self) -> None:
        """周期性任务循环"""
        while self.running:
            try:
                # 每小时检查一次需要更新的商品
                self._schedule_product_updates()
                
                # 清理过期任务
                self._cleanup_old_tasks()
                
                # 等待1小时
                for _ in range(3600):  # 3600秒 = 1小时
                    if not self.running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                print(f"周期性任务异常: {e}")
                time.sleep(60)  # 异常时等待1分钟
    
    def _schedule_product_updates(self) -> None:
        """调度商品更新任务"""
        try:
            # 获取需要更新的商品（24小时内没有更新的）
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            products = self.repo.get_products_need_update(cutoff_time)
            
            for product in products:
                # 为每个商品创建更新任务
                self.task_scheduler.add_task(
                    product_id=product['id'],
                    priority=1  # 周期性更新使用较低优先级
                )
            
            if products:
                print(f"调度了 {len(products)} 个商品的更新任务")
        
        except Exception as e:
            print(f"调度商品更新任务失败: {e}")
    
    def _cleanup_old_tasks(self) -> None:
        """清理过期任务"""
        try:
            # 清理7天前的已完成任务
            cutoff_time = datetime.utcnow() - timedelta(days=7)
            deleted_count = self.repo.cleanup_old_tasks(cutoff_time)
            
            if deleted_count > 0:
                print(f"清理了 {deleted_count} 个过期任务")
        
        except Exception as e:
            print(f"清理过期任务失败: {e}")


# 全局调度器实例
task_scheduler = TaskScheduler()
periodic_scheduler = PeriodicTaskScheduler(task_scheduler)