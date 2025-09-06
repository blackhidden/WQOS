"""
基础执行器 (Base Executor)
作者：e.e.
日期：2025.09.05

定义因子挖掘执行器的基础接口和通用功能，包括：
- 抽象执行接口
- 会话管理
- 通用方法
- 错误处理
"""

import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional

try:
    from machine_lib_ee import init_session
    from session_client import get_session, get_session_cookies
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from machine_lib_ee import init_session
    from session_client import get_session, get_session_cookies


class BaseExecutor(ABC):
    """基础执行器 - 定义所有执行器的共同接口和功能"""
    
    def __init__(self, config_manager, simulation_engine, progress_tracker, notification_service):
        """初始化基础执行器
        
        Args:
            config_manager: 配置管理器实例
            simulation_engine: 模拟执行引擎实例
            progress_tracker: 进度跟踪器实例
            notification_service: 通知服务实例
        """
        self.config_manager = config_manager
        self.simulation_engine = simulation_engine
        self.progress_tracker = progress_tracker
        self.notification_service = notification_service
        
        # 初始化会话
        self.session = None
        self.logger = None  # 将在设置时注入
        
        # 执行状态
        self.start_time = None
        self.current_dataset = self.config_manager.current_dataset
    
    def set_logger(self, logger):
        """设置日志记录器并传递给所有服务"""
        self.logger = logger
        
        # 传递给所有服务
        if self.simulation_engine:
            self.simulation_engine.set_logger(logger)
        if self.progress_tracker:
            self.progress_tracker.set_logger(logger)
        if self.notification_service:
            self.notification_service.set_logger(logger)
    
    def initialize_session(self) -> bool:
        """初始化API会话
        
        Returns:
            bool: 初始化是否成功
        """
        if self.logger:
            self.logger.info(f"🔐 正在获取API session...")
        
        try:
            self.session = get_session()
            if self.logger:
                self.logger.info(f"  ✅ 会话获取成功 (使用SessionClient)")
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ SessionClient获取会话失败: {e}")
                self.logger.info(f"  💡 请确保SessionKeeper正在运行并维护有效会话")
                self.logger.info(f"  🔄 将仅使用本地数据集信息")
            self.session = None
            return False
    
    def ensure_session_and_operators(self) -> bool:
        """确保会话和操作符都已正确初始化
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            if hasattr(self, 'session') and self.session:
                # 即使复用session，也需要确保操作符已获取
                init_session()  # 这会获取和过滤操作符，但不会改变现有session
                return True
            else:
                s = init_session()
                self.session = s
                return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 会话和操作符初始化失败: {e}")
            return False
    
    def refresh_session_if_needed(self) -> bool:
        """在需要时刷新会话
        
        Returns:
            bool: 刷新是否成功
        """
        try:
            # 使用SessionClient获取最新会话（SessionKeeper会自动维护）
            if self.logger:
                self.logger.info(f"🔄 获取最新会话...")
            
            new_session = get_session()
            self.session = new_session
            
            if self.logger:
                self.logger.info(f"✅ 会话更新成功 (SessionClient)")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 会话更新失败: {e}")
                self.logger.info(f"  💡 请确保SessionKeeper正在运行并维护有效会话")
            return False
    
    def log_execution_start(self, stage: int):
        """记录执行开始"""
        self.start_time = datetime.now()
        if self.logger:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"🚀 第{stage}阶因子挖掘启动")
            self.logger.info(f"  🎯 数据集: {self.current_dataset}")
            self.logger.info(f"  🌍 地区: {self.config_manager.region}")
            self.logger.info(f"  🎛️ 宇宙: {self.config_manager.universe}")
            self.logger.info(f"  ⚡ 并发数: {self.config_manager.get_n_jobs_config()}")
            self.logger.info(f"{'='*80}")
    
    def log_execution_end(self, stage: int, results: List[Dict], success: bool = True):
        """记录执行结束
        
        Args:
            stage: 执行阶段
            results: 执行结果
            success: 是否成功
        """
        if self.logger:
            self.logger.info(f"\n{'='*80}")
            if success:
                self.logger.info(f"✅ 第{stage}阶因子挖掘完成")
                if results:
                    self.logger.info(f"  📊 处理因子: {len(results)} 个")
                else:
                    self.logger.info(f"  ℹ️ 无需处理的因子（可能已完成或无符合条件的因子）")
            else:
                self.logger.info(f"❌ 第{stage}阶因子挖掘失败")
            
            self.logger.info(f"  🎯 数据集: {self.current_dataset}")
            
            if self.start_time:
                execution_time = (datetime.now() - self.start_time).total_seconds()
                hours, remainder = divmod(int(execution_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                if hours > 0:
                    self.logger.info(f"  ⏱️ 执行时间: {hours}小时{minutes}分{seconds}秒")
                else:
                    self.logger.info(f"  ⏱️ 执行时间: {minutes}分{seconds}秒")
            
            self.logger.info(f"{'='*80}")
    
    def handle_execution_error(self, stage: int, error: Exception):
        """处理执行错误
        
        Args:
            stage: 执行阶段
            error: 异常对象
        """
        if self.logger:
            self.logger.error(f"❌ 第{stage}阶挖掘失败: {error}")
            import traceback
            traceback.print_exc()
        
        # 发送错误通知
        if self.notification_service:
            self.notification_service.send_error_notification(
                error_type=f"第{stage}阶挖掘错误",
                error_message=str(error),
                dataset_id=self.current_dataset,
                stage=stage
            )
    
    def send_completion_notification(self, stage: int, total_factors: int):
        """发送阶段完成通知
        
        Args:
            stage: 完成的阶段
            total_factors: 处理的因子总数
        """
        if self.notification_service and self.start_time:
            execution_time = (datetime.now() - self.start_time).total_seconds()
            self.notification_service.send_stage_completion_notification(
                stage=stage,
                dataset_id=self.current_dataset,
                total_factors=total_factors,
                execution_time=execution_time
            )
    
    @abstractmethod
    async def execute(self) -> List[Dict]:
        """执行挖掘任务（抽象方法，子类必须实现）
        
        Returns:
            List[Dict]: 执行结果列表
        """
        pass
    
    @abstractmethod
    def get_stage_number(self) -> int:
        """获取执行器对应的阶段号（抽象方法，子类必须实现）
        
        Returns:
            int: 阶段号
        """
        pass
