"""
任务调度系统
实现智能任务调度、失败重试和并发控制
"""
import time
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..config.config import config
from ..dao.supabase_repo import SupabaseRepo
from ..workers.amazon_worker import UniversalWorker


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.repo = SupabaseRepo()
        self.max_workers = max_workers or config.NODE_CONCURRENCY
        self.worker = UniversalWorker()
        self.task_queue = queue.PriorityQueue()
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None
        self.scheduler_thread: Optional[threading.Thread] = None
        
        # 统计信息
        self.stats = {
            'total_processed': 0,
            'succeeded': 0,
            'failed': 0,
            'retried': 0,
            'start_time': None
        }
    
    def start(self) -> None:
        """启动任务调度器"""
        if self.running:
            print("任务调度器已在运行")
            return
        
        print(f"启动任务调度器，最大并发数: {self.max_workers}")
        self.running = True
        self.stats['start_time'] = datetime.utcnow()
        
        # 启动线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # 启动调度线程
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        print("任务调度器启动成功")
    
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
        """调度器主循环"""
        while self.running:
            try:
                # 从数据库获取待处理任务
                pending_tasks = self.repo.get_pending_tasks(limit=self.max_workers * 2)
                
                if not pending_tasks:
                    time.sleep(5)  # 没有任务时等待5秒
                    continue
                
                # 按优先级排序任务
                sorted_tasks = self._sort_tasks_by_priority(pending_tasks)
                
                # 提交任务到线程池
                futures = []
                for task in sorted_tasks[:self.max_workers]:
                    if not self.running:
                        break
                    
                    future = self.executor.submit(self._process_task_wrapper, task)
                    futures.append((future, task))
                
                # 等待任务完成
                for future, task in futures:
                    if not self.running:
                        break
                    
                    try:
                        future.result(timeout=300)  # 5分钟超时
                    except Exception as e:
                        print(f"任务 {task['id']} 执行异常: {e}")
                        self._handle_task_failure(task, str(e))
                
                time.sleep(1)  # 短暂休息
                
            except Exception as e:
                print(f"调度器循环异常: {e}")
                time.sleep(10)  # 异常时等待更长时间
    
    def _process_task_wrapper(self, task: Dict[str, Any]) -> None:
        """
        任务处理包装器
        
        Args:
            task: 任务信息
        """
        task_id = task['id']
        start_time = time.time()
        
        try:
            print(f"开始处理任务 {task_id}")
            
            # 标记任务为运行中
            self.repo.mark_task_running(task_id)
            
            # 执行任务
            self.worker.process_task(task)
            
            # 更新统计
            self.stats['total_processed'] += 1
            self.stats['succeeded'] += 1
            
            duration = time.time() - start_time
            print(f"任务 {task_id} 完成，耗时 {duration:.2f}s")
            
        except Exception as e:
            print(f"任务 {task_id} 失败: {e}")
            self._handle_task_failure(task, str(e))
            
            self.stats['total_processed'] += 1
            self.stats['failed'] += 1
    
    def _handle_task_failure(self, task: Dict[str, Any], error_msg: str) -> None:
        """
        处理任务失败
        
        Args:
            task: 任务信息
            error_msg: 错误消息
        """
        task_id = task['id']
        retry_count = task.get('retry_count', 0)
        max_retries = config.WORKER_TASK_RETRIES
        
        if retry_count < max_retries:
            # 重试任务
            retry_count += 1
            retry_delay = min(retry_count * 60, 300)  # 最大延迟5分钟
            scheduled_at = datetime.utcnow() + timedelta(seconds=retry_delay)
            
            self.repo.retry_task(task_id, retry_count, scheduled_at, error_msg)
            self.stats['retried'] += 1
            
            print(f"任务 {task_id} 将在 {retry_delay}s 后重试 (第 {retry_count} 次)")
        else:
            # 标记任务失败
            self.repo.mark_task_result(task_id, "failed", f"重试次数超限: {error_msg}")
            print(f"任务 {task_id} 最终失败，已达到最大重试次数")
    
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
        获取调度器统计信息
        
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
        
        # 计算成功率
        total = stats['total_processed']
        if total > 0:
            stats['success_rate'] = (stats['succeeded'] / total) * 100
        else:
            stats['success_rate'] = 0
        
        return stats
    
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