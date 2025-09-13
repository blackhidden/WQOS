"""
统一进度追踪器 (Unified Progress Tracker)
作者：e.e.
日期：2025.09.08

提供统一的进度追踪功能，包括：
- 单模拟和多模拟进度追踪
- 微信通知机制
- 重试逻辑
- 阶段过滤
"""

import time
from typing import List, Any, Optional
from datetime import datetime


class UnifiedProgressTracker:
    """统一进度追踪器 - 整合单模拟和多模拟的进度追踪逻辑"""
    
    def __init__(self, config_manager, stage: int = 1):
        """初始化进度追踪器
        
        Args:
            config_manager: 配置管理器
            stage: 执行阶段（1=一阶, 2=二阶, 3=三阶）
        """
        self.config_manager = config_manager
        self.stage = stage
        self.logger = None
        
        # 追踪状态
        self.total_tasks = 0
        self.completed_tasks = 0
        self.start_time = None
        
        # 通知状态 - 统一的通知逻辑
        self.notification_sent = False
        self.notification_retry_count = 0
        self.max_retry_attempts = 3
        
        # 通知阈值 - 只在>95%时发送一次
        self.notification_threshold = 95.0
        
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
        
    def start_tracking(self, task_groups: List[Any], stage: int):
        """开始追踪进度
        
        Args:
            task_groups: 任务组列表
            stage: 执行阶段
        """
        self.total_tasks = len(task_groups)
        self.completed_tasks = 0
        self.start_time = time.time()
        self.stage = stage
        
        # 重置通知状态
        self.notification_sent = False
        self.notification_retry_count = 0
        
        if self.logger:
            self.logger.info(f"📊 开始追踪进度: {self.total_tasks} 个任务，阶段 {stage}")
    
    def update_progress(self, completed: int, total: Optional[int] = None):
        """更新进度
        
        Args:
            completed: 已完成任务数
            total: 总任务数（可选）
        """
        if total is not None:
            self.total_tasks = total
        
        self.completed_tasks = completed
        
        if self.total_tasks > 0:
            completion_rate = (completed / self.total_tasks) * 100
            remaining = self.total_tasks - completed
            
            if self.logger:
                self.logger.info(f"📈 进度更新: {completion_rate:.1f}% ({completed}/{self.total_tasks})")
            
            # 检查是否需要发送通知
            self._check_and_send_notification(completion_rate, remaining)
    
    def _check_and_send_notification(self, completion_rate: float, remaining_count: int):
        """检查并发送进度通知
        
        Args:
            completion_rate: 完成率
            remaining_count: 剩余任务数
        """
        # 只在一阶发送通知
        if self.stage != 1:
            return
            
        # 如果已经发送过通知，不再发送
        if self.notification_sent:
            return
            
        # 如果重试次数超限，停止重试
        if self.notification_retry_count >= self.max_retry_attempts:
            if self.logger:
                self.logger.warning(f"⚠️ 统一进度通知已重试{self.notification_retry_count}次，停止重试")
            return
        
        # 只在>95%且<100%时发送通知
        if completion_rate > self.notification_threshold and completion_rate < 100.0:
            try:
                self.notification_retry_count += 1
                
                if self.logger:
                    self.logger.info(f"🔔 触发统一进度通知 (第{self.notification_retry_count}次尝试): "
                                   f"{completion_rate:.2f}% > {self.notification_threshold}%")
                
                success = self._send_progress_notification(completion_rate, remaining_count)
                
                if success:
                    self.notification_sent = True
                    if self.logger:
                        self.logger.info(f"✅ 统一进度通知已发送并标记，不会再次发送")
                else:
                    if self.logger:
                        self.logger.warning(f"❌ 统一进度通知发送失败 "
                                          f"(第{self.notification_retry_count}/{self.max_retry_attempts}次)，"
                                          f"下次进度更新时将重试")
                        
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 发送统一进度通知时出错 "
                                    f"(第{self.notification_retry_count}/{self.max_retry_attempts}次): {e}")
    
    def _send_progress_notification(self, completion_rate: float, remaining_count: int) -> bool:
        """发送进度通知
        
        Args:
            completion_rate: 完成率
            remaining_count: 剩余任务数
            
        Returns:
            bool: 发送是否成功
        """
        try:
            # 获取通知服务
            from digging.services.notification_service import NotificationService
            
            # 获取数据集名称
            dataset_name = self.config_manager.current_dataset
            if hasattr(self.config_manager, 'dataset_mode') and self.config_manager.dataset_mode == 'recommended_fields':
                dataset_name = self.config_manager.current_recommended_field
            
            # 计算耗时
            elapsed_time = time.time() - self.start_time if self.start_time else 0
            
            # 构造通知消息
            message = (f"🎯 统一模拟进度通知\n"
                      f"📊 数据集: {dataset_name}\n"
                      f"📈 进度: {completion_rate:.1f}%\n"
                      f"⏰ 剩余: {remaining_count} 个任务\n"
                      f"🕒 耗时: {elapsed_time:.1f}s")
            
            # 创建通知服务实例
            notification_service = NotificationService(self.config_manager)
            if self.logger:
                notification_service.set_logger(self.logger)
            
            # 发送通知
            return notification_service.send_message(message)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 统一进度通知发送异常: {e}")
            return False
    
    def get_completion_rate(self) -> float:
        """获取当前完成率
        
        Returns:
            float: 完成率百分比
        """
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks / self.total_tasks) * 100
    
    def get_elapsed_time(self) -> float:
        """获取已用时间
        
        Returns:
            float: 已用时间（秒）
        """
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time
    
    def is_complete(self) -> bool:
        """检查是否已完成
        
        Returns:
            bool: 是否已完成
        """
        return self.completed_tasks >= self.total_tasks
