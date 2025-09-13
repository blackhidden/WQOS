"""
多模拟策略 (Multi Simulation Strategy)
作者：e.e.
日期：2025.09.08

实现多模拟执行策略，每个多模拟包含10个alpha
"""

import asyncio
from typing import List, Tuple, Any
from ..core.base_strategy import BaseSimulationStrategy


class MultiSimulationStrategy(BaseSimulationStrategy):
    """多模拟策略 - 每次提交10个alpha的多模拟"""
    
    def __init__(self, config_manager):
        super().__init__(config_manager)
        self.multi_children_limit = 10  # 每个多模拟的alpha数量限制
        
    def get_strategy_name(self) -> str:
        """获取策略名称"""
        return "多模拟"
    
    def should_use_strategy(self, alpha_count: int, config_manager) -> bool:
        """判断是否应该使用多模拟策略
        
        Args:
            alpha_count: Alpha数量
            config_manager: 配置管理器
            
        Returns:
            bool: 是否使用多模拟策略
        """
        # 如果明确启用了多模拟，且alpha数量足够多，使用多模拟
        if hasattr(config_manager, 'enable_multi_simulation'):
            if config_manager.enable_multi_simulation and alpha_count >= 10:
                return True
        
        return False
    
    async def group_tasks(self, alpha_list: List[str], decay_list: List[int]) -> List[List[Tuple[str, int]]]:
        """将alpha列表分组为多模拟任务组
        
        Args:
            alpha_list: Alpha表达式列表
            decay_list: 衰减值列表
            
        Returns:
            List[List[Tuple[str, int]]]: 多模拟任务组列表，每组包含最多10个alpha
        """
        # 创建alpha-decay对
        alpha_decay_pairs = [(alpha_list[i], decay_list[i]) for i in range(len(alpha_list))]
        
        # 按10个alpha一组分组
        task_groups = []
        for i in range(0, len(alpha_decay_pairs), self.multi_children_limit):
            group = alpha_decay_pairs[i:i + self.multi_children_limit]
            task_groups.append(group)
        
        if self.logger:
            self.logger.info(f"📦 多模拟分组: {len(alpha_decay_pairs)} 个alpha → {len(task_groups)} 个多模拟任务")
        
        return task_groups
    
    async def execute_task_group(self, task_group: List[Tuple[str, int]], session_manager,
                                region: str, universe: str, neut: str, 
                                delay: int, name: str, stage: int) -> List[str]:
        """执行单个多模拟任务组
        
        Args:
            task_group: 包含多个(alpha_expression, decay)的任务组
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
        try:
            # 调用多模拟引擎中的函数
            from lib.multi_simulation_engine import submit_and_monitor_single_multi_simulation
            
            # 获取配置参数
            max_trade = "OFF"
            instrument_type = "EQUITY"
            default_decay = 6
            
            if self.config_manager:
                if hasattr(self.config_manager, 'max_trade'):
                    max_trade = self.config_manager.max_trade
                if hasattr(self.config_manager, 'instrument_type'):
                    instrument_type = self.config_manager.instrument_type
                if hasattr(self.config_manager, 'decay'):
                    default_decay = self.config_manager.decay
            
            # 执行多模拟
            alpha_ids = await submit_and_monitor_single_multi_simulation(
                session_manager=session_manager,
                alpha_task=task_group,  # 任务组包含多个(alpha, decay)对
                region=region,
                universe=universe,
                neut=neut,
                delay=delay,
                name=name,
                tags=[name],
                task_idx=0,  # 这里的索引在上层统一管理
                max_trade=max_trade,
                instrument_type=instrument_type,
                default_decay=default_decay
            )
            
            if alpha_ids:
                if self.logger:
                    self.logger.debug(f"✅ 多模拟成功: 获得 {len(alpha_ids)} 个Alpha ID")
                return alpha_ids
            else:
                if self.logger:
                    self.logger.warning(f"⚠️ 多模拟未返回Alpha ID")
                return []
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 多模拟失败: {e}")
            return []
    
    async def _execute_task_groups(self, task_groups: List[List[Tuple[str, int]]], session_manager,
                                  region: str, universe: str, neut: str, 
                                  delay: int, name: str, stage: int,
                                  progress_tracker=None) -> List[str]:
        """智能调度执行所有多模拟任务
        
        Args:
            task_groups: 多模拟任务组列表
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
        # 获取并发数配置（多模拟的槽位数）
        n_jobs = 5  # 默认值
        if hasattr(self.config_manager, 'get_n_jobs_config'):
            n_jobs = self.config_manager.get_n_jobs_config()
        elif hasattr(self.config_manager, 'n_jobs'):
            n_jobs = self.config_manager.n_jobs
        
        if self.logger:
            self.logger.info(f"🔥 多模拟智能调度: {len(task_groups)} 个任务组，{n_jobs} 个槽位")
            self.logger.info(f"⚡ 理论并发度: {self.multi_children_limit * n_jobs} = "
                           f"{self.multi_children_limit * n_jobs} (vs 单模拟的{n_jobs})")
        
        # 使用现有的多模拟并发控制引擎
        from lib.multi_simulation_engine import async_multi_simulate_with_concurrent_control
        
        # 获取配置参数
        max_trade = "OFF"
        instrument_type = "EQUITY"
        default_decay = 6
        
        if self.config_manager:
            if hasattr(self.config_manager, 'max_trade'):
                max_trade = self.config_manager.max_trade
            if hasattr(self.config_manager, 'instrument_type'):
                instrument_type = self.config_manager.instrument_type
            if hasattr(self.config_manager, 'decay'):
                default_decay = self.config_manager.decay
        
        # 初始化多模拟进度追踪器（仅用于内部统计，不发送微信通知）
        multi_progress_tracker = None
        if progress_tracker:
            from lib.multi_simulation_engine import MultiSimulationProgressTracker
            # 传递stage=0来禁用微信通知，避免与统一进度追踪器重复
            multi_progress_tracker = MultiSimulationProgressTracker(self.config_manager, stage=0)
            multi_progress_tracker.start_tracking(len(task_groups))
        
        # 执行多模拟并发控制
        alpha_ids = await async_multi_simulate_with_concurrent_control(
            session_manager=session_manager,
            multi_sim_tasks=task_groups,
            region=region,
            universe=universe,
            neut=neut,
            delay=delay,
            name=name,
            tags=[name],
            n_jobs=n_jobs,
            progress_tracker=multi_progress_tracker,
            max_trade=max_trade,
            instrument_type=instrument_type,
            default_decay=default_decay
        )
        
        # 同步更新统一进度追踪器
        if progress_tracker:
            progress_tracker.update_progress(len(task_groups), len(task_groups))
        
        if self.logger:
            self.logger.info(f"📊 多模拟完成统计: "
                           f"处理 {len(task_groups)} 个任务组，"
                           f"成功创建 {len(alpha_ids)} 个alpha")
        
        return alpha_ids
