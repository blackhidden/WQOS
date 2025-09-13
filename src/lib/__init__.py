"""
重构后的machine_lib库
作者：e.e.
日期：2025年1月

模块说明：
- config_utils: 配置管理工具
- operator_manager: 操作符管理
- session_manager: 会话管理
- alpha_manager: Alpha管理
- data_client: 数据获取客户端
- factor_generator: 因子生成工具
- simulation_engine: 模拟执行引擎
- database_utils: 数据库操作工具
"""

# 保持与原machine_lib_ee的兼容性
from .config_utils import load_user_config, load_digging_config
from .operator_manager import get_available_ops, init_session, get_vec_fields, list_chuckation
from .alpha_manager import set_alpha_properties, batch_set_alpha_properties
from .data_client import get_datasets, get_datafields, get_alphas, process_datafields
from .factor_generator import (
    first_order_factory, group_factory, ts_factory, vector_factory, 
    trade_when_factory, ts_comp_factory, prune, transform,
    get_group_second_order_factory
)
from .simulation_engine import simulate_single, async_set_alpha_properties
from .database_utils import _write_to_database, _record_failed_expression

# 多模拟引擎
try:
    from .multi_simulation_engine import simulate_multiple_alphas_with_multi_mode
    MULTI_SIMULATION_AVAILABLE = True
except ImportError:
    simulate_multiple_alphas_with_multi_mode = None
    MULTI_SIMULATION_AVAILABLE = False

# 统一模拟执行器
try:
    from .simulation import UnifiedSimulationExecutor
    UNIFIED_SIMULATION_AVAILABLE = True
except ImportError:
    UnifiedSimulationExecutor = None
    UNIFIED_SIMULATION_AVAILABLE = False

__all__ = [
    # 配置管理
    'load_user_config', 'load_digging_config',
    # 操作符管理
    'get_available_ops', 'init_session', 'get_vec_fields', 'list_chuckation',
    # Alpha管理
    'set_alpha_properties', 'batch_set_alpha_properties',
    # 数据获取
    'get_datasets', 'get_datafields', 'get_alphas', 'process_datafields',
    # 因子生成
    'first_order_factory', 'group_factory', 'ts_factory', 'vector_factory',
    'trade_when_factory', 'ts_comp_factory', 'prune', 'transform',
    'get_group_second_order_factory',
    # 模拟引擎
    'simulate_single', 'async_set_alpha_properties',
    # 多模拟引擎
    'simulate_multiple_alphas_with_multi_mode', 'MULTI_SIMULATION_AVAILABLE',
    # 统一模拟执行器
    'UnifiedSimulationExecutor', 'UNIFIED_SIMULATION_AVAILABLE',
    # 数据库操作
    '_write_to_database', '_record_failed_expression'
]
