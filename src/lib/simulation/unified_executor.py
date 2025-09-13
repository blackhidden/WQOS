"""
统一模拟执行器 (Unified Simulation Executor)
作者：e.e.
日期：2025.09.08

提供统一的模拟执行接口，自动选择合适的策略（单模拟/多模拟）
并提供向后兼容的接口
"""

import asyncio
from typing import List, Tuple, Optional, Union
from .core.base_strategy import BaseSimulationStrategy
from .core.session_manager import UnifiedSessionManager
from .core.progress_tracker import UnifiedProgressTracker
from .core.result_collector import ResultCollector
from .strategies.single_simulation import SingleSimulationStrategy
from .strategies.multi_simulation import MultiSimulationStrategy


class UnifiedSimulationExecutor:
    """统一模拟执行器 - 替代原有的 SimulationEngine"""
    
    def __init__(self, config_manager):
        """初始化统一模拟执行器
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager
        self.logger = None
        
        # 初始化策略
        self.single_strategy = SingleSimulationStrategy(config_manager)
        self.multi_strategy = MultiSimulationStrategy(config_manager)
        
        # 初始化核心组件
        self.session_manager = UnifiedSessionManager()
        self.result_collector = ResultCollector()
        
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
        
        # 传递给所有组件
        self.single_strategy.set_logger(logger)
        self.multi_strategy.set_logger(logger)
        self.session_manager.set_logger(logger)
        self.result_collector.set_logger(logger)
    
    def _select_strategy(self, alpha_count: int) -> BaseSimulationStrategy:
        """自动选择最适合的模拟策略
        
        Args:
            alpha_count: Alpha数量
            
        Returns:
            BaseSimulationStrategy: 选中的策略
        """
        # 优先检查多模拟策略
        if self.multi_strategy.should_use_strategy(alpha_count, self.config_manager):
            if self.logger:
                self.logger.info(f"🔥 自动选择策略: 多模拟 (alpha数量: {alpha_count})")
            return self.multi_strategy
        
        # 默认使用单模拟策略
        if self.logger:
            self.logger.info(f"🔄 自动选择策略: 单模拟 (alpha数量: {alpha_count})")
        return self.single_strategy
    
    def _force_strategy(self, strategy_name: str) -> BaseSimulationStrategy:
        """强制使用指定策略
        
        Args:
            strategy_name: 策略名称 ("single" 或 "multi")
            
        Returns:
            BaseSimulationStrategy: 指定的策略
        """
        if strategy_name.lower() in ["multi", "multiple", "多模拟"]:
            return self.multi_strategy
        else:
            return self.single_strategy
    
    async def execute_batch(self, alpha_list: List[str], dataset_name: str, 
                           stage: int = 1, decay_list: Optional[List[int]] = None,
                           strategy: Optional[str] = None, tags: Optional[List[str]] = None) -> List[str]:
        """统一执行入口 - 核心方法
        
        Args:
            alpha_list: Alpha表达式列表
            dataset_name: 数据集名称
            stage: 执行阶段
            decay_list: 衰减值列表（可选，默认使用配置中的decay）
            strategy: 强制使用的策略（可选："single"或"multi"）
            tags: 自定义标签列表（可选，默认使用dataset_name）
            
        Returns:
            List[str]: 创建的Alpha ID列表
        """
        if not alpha_list:
            if self.logger:
                self.logger.warning("⚠️ Alpha列表为空，跳过执行")
            return []
        
        # 准备衰减值列表
        if decay_list is None:
            default_decay = getattr(self.config_manager, 'decay', 6)
            decay_list = [default_decay] * len(alpha_list)
        
        # 选择策略
        if strategy:
            selected_strategy = self._force_strategy(strategy)
        else:
            selected_strategy = self._select_strategy(len(alpha_list))
        
        # 准备参数
        region = getattr(self.config_manager, 'region', 'CHN')
        universe = getattr(self.config_manager, 'universe', 'TOP2000U')
        neut = getattr(self.config_manager, 'neutralization', 'SUBINDUSTRY')
        delay = getattr(self.config_manager, 'delay', 1)
        
        # 初始化会话管理器
        await self.session_manager.initialize()
        
        # 初始化进度追踪器
        progress_tracker = UnifiedProgressTracker(self.config_manager, stage)
        progress_tracker.set_logger(self.logger)
        
        try:
            # 确定tag名称 - 使用自定义tags或生成规范tag
            if tags and len(tags) > 0:
                tag_name = tags[0]  # 使用第一个自定义tag
            else:
                # 生成规范tag格式
                tag_name = self.config_manager.generate_tag(dataset_name, stage)
            
            # 执行策略
            alpha_ids = await selected_strategy.execute(
                alpha_list=alpha_list,
                decay_list=decay_list,
                region=region,
                universe=universe,
                neut=neut,
                delay=delay,
                name=tag_name,  # 使用规范tag而不是dataset_name
                stage=stage,
                session_manager=self.session_manager,
                progress_tracker=progress_tracker
            )
            
            # 收集结果
            self.result_collector.add_alpha_ids(alpha_ids)
            
            return alpha_ids
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 统一模拟执行失败: {e}")
            raise
        finally:
            # 清理会话
            await self.session_manager.close()
    
    # ===== 向后兼容接口 =====
    
    async def execute_simulation_batch(self, alpha_list: List[Union[str, Tuple[str, int]]], 
                                      dataset_name: str, stage: int = 1) -> List[str]:
        """向后兼容接口 - 兼容 digging/services/simulation_engine.py 的接口
        
        Args:
            alpha_list: Alpha表达式列表或(alpha, decay)元组列表
            dataset_name: 数据集名称
            stage: 执行阶段
            
        Returns:
            List[str]: 创建的Alpha ID列表
        """
        # 处理混合格式的alpha_list
        if alpha_list and isinstance(alpha_list[0], tuple):
            # 如果是(alpha, decay)元组格式
            alphas = [item[0] for item in alpha_list]
            decays = [item[1] for item in alpha_list]
        else:
            # 如果是纯alpha字符串格式
            alphas = alpha_list
            decays = None
        
        return await self.execute_batch(
            alpha_list=alphas,
            dataset_name=dataset_name,
            stage=stage,
            decay_list=decays
        )
    
    async def simulate_multiple_alphas(self, alpha_list: List[str], region_list: List[Tuple], 
                                     decay_list: List[int], delay_list: List[int], 
                                     name: str, neut: str, stone_bag: List = None, 
                                     n_jobs: int = 5, enable_multi_simulation: bool = None) -> None:
        """向后兼容接口 - 兼容原有的 simulate_multiple_alphas 接口
        
        Args:
            alpha_list: Alpha表达式列表
            region_list: 地区列表
            decay_list: 衰减值列表
            delay_list: 延迟列表
            name: 名称
            neut: 中性化方式
            stone_bag: 石头袋（未使用）
            n_jobs: 并发数
            enable_multi_simulation: 是否启用多模拟
        """
        # 更新配置
        if region_list:
            self.config_manager.region, self.config_manager.universe = region_list[0]
        if delay_list:
            self.config_manager.delay = delay_list[0]
        if n_jobs:
            self.config_manager.n_jobs = n_jobs
        self.config_manager.neutralization = neut
        
        # 强制使用指定的模拟模式
        strategy = None
        if enable_multi_simulation is not None:
            strategy = "multi" if enable_multi_simulation else "single"
        
        # 执行模拟
        await self.execute_batch(
            alpha_list=alpha_list,
            dataset_name=name,
            stage=1,
            decay_list=decay_list,
            strategy=strategy
        )
    
    async def sleep_with_countdown(self, seconds: int, message: str = "休眠中"):
        """向后兼容接口 - 带倒计时的休眠功能
        
        Args:
            seconds: 休眠秒数
            message: 显示消息
        """
        if self.logger:
            self.logger.info(f"⏳ {message}: {seconds}秒倒计时开始...")
        
        for remaining in range(seconds, 0, -1):
            if remaining % 10 == 0 or remaining <= 5:
                if self.logger:
                    self.logger.debug(f"  ⏱️ {message}: 剩余 {remaining} 秒")
            await asyncio.sleep(1)
        
        if self.logger:
            self.logger.info(f"✅ {message}完成")
    
    def get_result_summary(self) -> dict:
        """获取执行结果摘要
        
        Returns:
            dict: 结果摘要
        """
        return self.result_collector.get_summary()
