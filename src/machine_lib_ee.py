"""
machine_lib_ee - WorldQuant Brain API 工具库（重构版）
作者：e.e.
微信：Enkidu_lin
日期：2025.08.24 - 2025.09.08

本库已重构为模块化结构，提供更好的可维护性和扩展性。
所有原有功能保持100%兼容性，现有代码无需修改。

模块结构：
- lib/config_utils.py: 配置管理
- lib/operator_manager.py: 操作符管理  
- lib/alpha_manager.py: Alpha管理
- lib/data_client.py: 数据获取
- lib/factor_generator.py: 因子生成
- lib/simulation_engine.py: 模拟引擎
- lib/database_utils.py: 数据库操作
"""

import os
import pandas as pd
import logging as logger

# 统一日志配置管理
def setup_unified_logger(logger_name: str = 'machine_lib_ee', level: int = logger.INFO) -> logger.Logger:
    """
    设置统一的日志配置
    
    Args:
        logger_name: 日志记录器名称
        level: 日志级别
    
    Returns:
        配置好的日志记录器
    """
    # 配置根日志记录器（如果还没有配置）
    if not logger.getLogger().hasHandlers():
        logger.basicConfig(
            level=level,
            format='%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # 获取指定的日志记录器
    target_logger = logger.getLogger(logger_name)
    target_logger.setLevel(level)
    
    return target_logger

# 设置默认日志配置（保持向后兼容）
_default_logger = setup_unified_logger('machine_lib_ee', logger.INFO)

# 导入路径配置
from config import ROOT_PATH, RECORDS_PATH

# pandas设置（保持与原版本一致）
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)

# API URL设置
brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")

def print_module_info():
    """打印模块信息"""
    print("🎉 machine_lib_ee - WorldQuant Brain API 工具库")
    print("📁 模块化结构:")
    print("  - lib/config_utils.py: 配置管理")
    print("  - lib/operator_manager.py: 操作符管理")
    print("  - lib/session_manager.py: 会话管理") 
    print("  - lib/alpha_manager.py: Alpha管理")
    print("  - lib/data_client.py: 数据获取")
    print("  - lib/factor_generator.py: 因子生成")
    print("  - lib/simulation_engine.py: 模拟引擎")
    print("  - lib/database_utils.py: 数据库操作")
    print("✅ 重构版本，保持100%向后兼容性")

if __name__ == "__main__":
    print_module_info()
