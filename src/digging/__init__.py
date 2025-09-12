"""
因子挖掘模块 (Digging Module)
作者：e.e.
日期：2025.09.05

重构后的因子挖掘模块，按功能域分层：
- core: 核心功能（配置、数据库等）
- services: 服务类（通知、进度跟踪、模拟引擎等）
- executors: 执行器（各阶段挖掘逻辑）
- utils: 工具函数（日志、通用工具等）
"""

from .core.config_manager import ConfigManager
from .services.notification_service import NotificationService
from .services.progress_tracker import ProgressTracker
from .executors.base_executor import BaseExecutor
from .executors.first_order_executor import FirstOrderExecutor
from .executors.second_order_executor import SecondOrderExecutor
from .executors.third_order_executor import ThirdOrderExecutor

# SimulationEngine已被统一模拟执行器替代，不再提供向后兼容
LEGACY_SIMULATION_ENGINE_AVAILABLE = False

__all__ = [
    'ConfigManager',
    'NotificationService', 
    'ProgressTracker',
    'BaseExecutor',
    'FirstOrderExecutor',
    'SecondOrderExecutor', 
    'ThirdOrderExecutor'
]

# SimulationEngine已被移除，使用 lib.simulation.UnifiedSimulationExecutor 替代
