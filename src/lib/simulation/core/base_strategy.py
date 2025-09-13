"""
基础模拟策略抽象类 (Base Simulation Strategy)
作者：e.e.
日期：2025.09.08

定义所有模拟策略的通用接口和流程模板
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime


class BaseSimulationStrategy(ABC):
    """模拟策略基础抽象类 - 定义统一的执行流程"""
    
    def __init__(self, config_manager):
        """初始化策略
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager
        self.logger = None
        
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
        
    @abstractmethod
    def get_strategy_name(self) -> str:
        """获取策略名称"""
        pass
        
    @abstractmethod
    def should_use_strategy(self, alpha_count: int, config_manager) -> bool:
        """判断是否应该使用此策略
        
        Args:
            alpha_count: Alpha数量
            config_manager: 配置管理器
            
        Returns:
            bool: 是否使用此策略
        """
        pass
        
    @abstractmethod
    async def group_tasks(self, alpha_list: List[str], decay_list: List[int]) -> List[Any]:
        """将alpha列表分组为任务
        
        Args:
            alpha_list: Alpha表达式列表
            decay_list: 衰减值列表
            
        Returns:
            List[Any]: 分组后的任务列表
        """
        pass
        
    @abstractmethod
    async def execute_task_group(self, task_group: Any, session_manager, 
                                region: str, universe: str, neut: str, 
                                delay: int, name: str, stage: int) -> List[str]:
        """执行单个任务组
        
        Args:
            task_group: 任务组
            session_manager: 会话管理器
            region: 地区
            universe: universe
            neut: 中性化方式
            delay: 延迟
            name: 名称/标签
            stage: 执行阶段
            
        Returns:
            List[str]: 创建的Alpha ID列表
        """
        pass
    
    async def execute(self, alpha_list: List[str], decay_list: List[int],
                     region: str, universe: str, neut: str, delay: int, 
                     name: str, stage: int = 1, session_manager=None,
                     progress_tracker=None) -> List[str]:
        """执行模拟的统一模板方法
        
        Args:
            alpha_list: Alpha表达式列表
            decay_list: 衰减值列表
            region: 地区
            universe: universe
            neut: 中性化方式
            delay: 延迟
            name: 名称/标签
            stage: 执行阶段
            session_manager: 会话管理器（可选）
            progress_tracker: 进度追踪器（可选）
            
        Returns:
            List[str]: 创建的Alpha ID列表
        """
        start_time = time.time()
        total_alphas = len(alpha_list)
        
        if self.logger:
            self.logger.info(f"🚀 {self.get_strategy_name()}启动: {total_alphas:,}个因子")
        
        try:
            # 1. 初始化会话管理器（如果未提供）
            if session_manager is None:
                from ..core.session_manager import UnifiedSessionManager
                session_manager = UnifiedSessionManager()
                await session_manager.initialize()
            
            # 2. 分组任务
            task_groups = await self.group_tasks(alpha_list, decay_list)
            
            if self.logger:
                self.logger.info(f"📊 任务分组: {len(task_groups)} 个任务组")
            
            # 3. 初始化进度追踪（如果需要）
            if progress_tracker:
                progress_tracker.start_tracking(task_groups, stage)
            
            # 4. 执行任务组
            all_alpha_ids = await self._execute_task_groups(
                task_groups, session_manager, region, universe, 
                neut, delay, name, stage, progress_tracker
            )
            
            # 5. 统计结果
            end_time = time.time()
            duration = end_time - start_time
            
            if self.logger:
                self.logger.info(f"🎉 {self.get_strategy_name()}完成: "
                               f"处理 {total_alphas} 个因子，创建 {len(all_alpha_ids)} 个alpha，"
                               f"耗时 {duration:.1f}s")
            
            return all_alpha_ids
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ {self.get_strategy_name()}执行失败: {e}")
            raise
        finally:
            # 清理资源
            if session_manager and hasattr(session_manager, 'close'):
                await session_manager.close()
    
    async def _execute_task_groups(self, task_groups: List[Any], session_manager,
                                  region: str, universe: str, neut: str, 
                                  delay: int, name: str, stage: int,
                                  progress_tracker=None) -> List[str]:
        """执行所有任务组（子类可以重写此方法实现不同的并发策略）
        
        Args:
            task_groups: 任务组列表
            session_manager: 会话管理器
            region: 地区
            universe: universe
            neut: 中性化方式
            delay: 延迟
            name: 名称
            stage: 执行阶段
            progress_tracker: 进度追踪器
            
        Returns:
            List[str]: 所有创建的Alpha ID
        """
        all_alpha_ids = []
        
        for i, task_group in enumerate(task_groups):
            try:
                alpha_ids = await self.execute_task_group(
                    task_group, session_manager, region, universe, 
                    neut, delay, name, stage
                )
                all_alpha_ids.extend(alpha_ids)
                
                # 更新进度
                if progress_tracker:
                    progress_tracker.update_progress(i + 1, len(task_groups))
                    
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 任务组 {i+1} 执行失败: {e}")
                # 继续执行其他任务组
                continue
        
        return all_alpha_ids
