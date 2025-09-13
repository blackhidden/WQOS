"""
操作符管理模块 (Operator Manager)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的操作符相关功能：
- 操作符定义和获取
- 会话初始化
- 操作符过滤
"""

import os
import time
import pandas as pd
import logging as logger

from .config_utils import load_digging_config

# 操作符定义
basic_ops = ["log", "sqrt", "reverse", "inverse", "rank", "zscore", "log_diff", "s_log_1p",
             'fraction', 'quantile', "normalize", "scale_down"]

ts_ops = ["ts_rank", "ts_zscore", "ts_delta", "ts_sum", "ts_product",
          "ts_ir", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", "ts_min_diff",
          "ts_max_diff", "ts_returns", "ts_scale", "ts_skewness", "ts_kurtosis",
          "ts_quantile"]

ts_not_use = ["ts_min", "ts_max", "ts_delay", "ts_median", ]

arsenal = ["ts_moment", "ts_entropy", "ts_min_max_cps", "ts_min_max_diff", "inst_tvr", 'sigmoid',
           "ts_decay_exp_window", "ts_percentage", "vector_neut", "vector_proj", "signed_power"]

twin_field_ops = ["ts_corr", "ts_covariance", "ts_co_kurtosis", "ts_co_skewness", "ts_theilsen"]

group_ops = ["group_neutralize", "group_rank", "group_normalize", "group_scale", "group_zscore"]

group_ac_ops = ["group_sum", "group_max", "group_mean", "group_median", "group_min", "group_std_dev", ]

vec_ops = ["vec_avg", "vec_sum", "vec_ir", "vec_max",
                   "vec_count", "vec_skewness", "vec_stddev", "vec_choose"]

ops_set = basic_ops + ts_ops + arsenal + group_ops

# 全局会话变量（与原machine_lib_ee保持兼容）
s = None


def init_session():
    """初始化session，获取可用的操作符"""
    global s, ts_ops, basic_ops, group_ops, vec_ops
    if s is None:
        try:
            # 使用SessionClient获取会话
            from sessions.session_client import get_session
            s = get_session()
            logger.info("✅ operator_manager 使用SessionClient")
        except Exception as e:
            logger.warning(f"❌ SessionClient不可用: {e}")
            logger.warning("💡 请确保SessionKeeper正在运行并维护有效会话")
            raise
        
        # 保存原始操作符列表
        globals()['ts_ops_original'] = ts_ops.copy()
        globals()['basic_ops_original'] = basic_ops.copy()
        globals()['group_ops_original'] = group_ops.copy()
        globals()['vec_ops_original'] = vec_ops.copy()
        
        # 获取配置中的重试参数
        try:
            config = load_digging_config()
            max_retries = config.get('api_max_retries', 3)
            retry_delay = config.get('api_retry_delay', 5)
            use_backoff = config.get('api_rate_limit_backoff', True)
        except:
            # 如果配置加载失败，使用默认值
            max_retries = 3
            retry_delay = 5
            use_backoff = True
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔍 获取可用操作符 (尝试 {attempt + 1}/{max_retries})...")
                res = s.get("https://api.worldquantbrain.com/operators")
                
                # 检查响应状态
                if res.status_code != 200:
                    logger.warning(f"⚠️  API 请求失败，状态码: {res.status_code}")
                    if attempt < max_retries - 1:
                        logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                        # 确保全局变量被正确设置
                        globals()['vec_ops'] = vec_ops
                        return s
                
                # 解析响应
                response_data = res.json()
                
                # 检查是否是错误响应
                if isinstance(response_data, dict) and 'message' in response_data:
                    error_msg = response_data['message']
                    logger.warning(f"⚠️  API 返回错误: {error_msg}")
                    
                    if 'rate limit' in error_msg.lower():
                        if attempt < max_retries - 1:
                            if use_backoff:
                                wait_time = retry_delay * (2 ** attempt)  # 指数退避
                                logger.info(f"🚫 遇到速率限制，等待 {wait_time} 秒后重试 (指数退避)...")
                            else:
                                wait_time = retry_delay
                                logger.info(f"🚫 遇到速率限制，等待 {wait_time} 秒后重试...")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.warning("❌ 速率限制持续存在，使用默认操作符列表")
                            # 确保全局变量被正确设置
                            globals()['vec_ops'] = vec_ops
                            return s
                    else:
                        logger.warning(f"❌ API 错误: {error_msg}")
                        if attempt < max_retries - 1:
                            logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                            # 确保全局变量被正确设置
                            globals()['vec_ops'] = vec_ops
                            return s
                
                # 正常响应，构造 DataFrame
                if isinstance(response_data, list) and len(response_data) > 0:
                    aval = pd.DataFrame(response_data)['name'].tolist()
                    
                    # 过滤所有操作符类型
                    original_ts = len(ts_ops)
                    original_basic = len(basic_ops)
                    original_group = len(group_ops)
                    original_vec = len(vec_ops)
                    
                    ts_ops = [op for op in ts_ops if op in aval]
                    basic_ops = [op for op in basic_ops if op in aval]
                    group_ops = [op for op in group_ops if op in aval]
                    vec_ops = [op for op in vec_ops if op in aval]
                    
                    # 更新全局变量
                    globals()['ts_ops'] = ts_ops
                    globals()['basic_ops'] = basic_ops  
                    globals()['group_ops'] = group_ops
                    globals()['vec_ops'] = vec_ops
                    
                    logger.info(f"✅ 成功获取 {len(aval)} 个可用操作符")
                    logger.info(f"📊 时间序列操作符: {len(ts_ops)}/{original_ts} 个")
                    logger.info(f"📊 基础操作符: {len(basic_ops)}/{original_basic} 个")
                    logger.info(f"📊 组操作符: {len(group_ops)}/{original_group} 个")
                    logger.info(f"📊 向量操作符: {len(vec_ops)}/{original_vec} 个")
                    
                    # 显示被过滤掉的操作符
                    ts_ops_orig = globals().get('ts_ops_original', [])
                    basic_ops_orig = globals().get('basic_ops_original', [])
                    group_ops_orig = globals().get('group_ops_original', [])
                    vec_ops_orig = globals().get('vec_ops_original', [])
                    
                    filtered_ts = [op for op in ts_ops_orig if op not in aval]
                    filtered_basic = [op for op in basic_ops_orig if op not in aval] 
                    filtered_group = [op for op in group_ops_orig if op not in aval]
                    filtered_vec = [op for op in vec_ops_orig if op not in aval]
                    
                    if filtered_ts or filtered_basic or filtered_group or filtered_vec:
                        logger.info(f"⚠️  过滤掉的不可用操作符:")
                        if filtered_ts:
                            logger.info(f"   ts_ops: {filtered_ts}")
                        if filtered_basic:
                            logger.info(f"   basic_ops: {filtered_basic}")
                        if filtered_group:
                            logger.info(f"   group_ops: {filtered_group}")
                        if filtered_vec:
                            logger.info(f"   vec_ops: {filtered_vec}")
                    
                    break
                else:
                    logger.warning("⚠️  API 返回空数据或格式异常")
                    if attempt < max_retries - 1:
                        logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                        # 确保全局变量被正确设置
                        globals()['vec_ops'] = vec_ops
                        return s
                        
            except Exception as e:
                logger.warning(f"❌ 获取操作符时发生异常: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                    # 确保全局变量被正确设置
                    globals()['vec_ops'] = vec_ops
                    return s
    
    return s


def get_available_ops():
    """获取可用的操作符列表"""
    init_session()
    global group_ops, twin_field_ops, vec_ops, arsenal, aval
    if 'aval' not in globals():
        try:
            res = s.get("https://api.worldquantbrain.com/operators")
            if res.status_code == 200:
                response_data = res.json()
                if isinstance(response_data, list) and len(response_data) > 0:
                    aval = pd.DataFrame(response_data)['name'].tolist()
                    group_ops = [op for op in group_ops if op in aval]
                    twin_field_ops = [op for op in twin_field_ops if op in aval]
                else:
                    logger.warning("⚠️  get_available_ops: API 返回空数据，使用默认操作符")
                    aval = []
            else:
                logger.warning(f"⚠️  get_available_ops: API 请求失败，状态码: {res.status_code}")
                aval = []
        except Exception as e:
            logger.warning(f"⚠️  get_available_ops: 获取操作符异常: {e}")
            aval = []
        arsenal = [op for op in arsenal if op in aval]
        vec_ops = [op for op in vec_ops if op in aval]
        # 更新全局变量
        globals()['vec_ops'] = vec_ops
    return aval


def get_vec_fields(fields):
    """生成向量操作符字段列表"""
    vec_fields = []

    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)" % (vec_op, field))
                vec_fields.append("%s(%s, nth=0)" % (vec_op, field))
            else:
                vec_fields.append("%s(%s)" % (vec_op, field))

    return vec_fields


def list_chuckation(field_list, num):
    """将列表分块"""
    list_chucked = []
    lens = len(field_list)
    i = 0
    while i + num <= lens:
        list_chucked.append(field_list[i:i + num])
        i += num
    list_chucked.append(field_list[i:lens])
    return list_chucked
