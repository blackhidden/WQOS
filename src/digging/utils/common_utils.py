"""
通用工具函数 (Common Utils)
作者：e.e.
日期：2025.09.05

提供通用的工具函数，包括：
- 操作符获取
- 公共函数
"""

import os
import sys

try:
    import machine_lib_ee
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    import machine_lib_ee


def get_filtered_operators():
    """获取过滤后的操作符（确保在init_session后调用）
    
    Returns:
        Tuple: (ts_ops, basic_ops, group_ops)
    """
    # 确保操作符已被初始化和过滤
    if not hasattr(machine_lib_ee, 'ts_ops') or not machine_lib_ee.ts_ops:
        # 如果操作符未初始化，返回原始定义的操作符列表
        return (
            ["ts_rank", "ts_zscore", "ts_delta", "ts_sum", "ts_product",
             "ts_ir", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", 
             "ts_min_diff", "ts_max_diff", "ts_returns", "ts_scale", 
             "ts_skewness", "ts_kurtosis", "ts_quantile"],
            ["log", "sqrt", "reverse", "inverse", "rank", "zscore", "log_diff", "s_log_1p",
             'fraction', 'quantile', "normalize", "scale_down"],
            ["group_neutralize", "group_rank", "group_normalize", "group_scale", "group_zscore"]
        )
    return machine_lib_ee.ts_ops, machine_lib_ee.basic_ops, machine_lib_ee.group_ops
