"""
作者：e.e. （基于鑫鑫鑫代码拓展补充）
微信：Enkidu_lin
日期：2025.08.24
"""
import os

import requests
import time
import pandas as pd
from itertools import product
from collections import defaultdict
import aiohttp
import asyncio
import logging as logger
import math

# 设置日志级别为 INFO
logger.basicConfig(
    level=logger.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 导入路径配置
from config import ROOT_PATH, RECORDS_PATH

def load_user_config(txt_file=None):
    """从config/user_info.txt加载用户配置"""
    if txt_file is None:
        txt_file = os.path.join(ROOT_PATH, 'config', 'user_info.txt')
    config = {}
    try:
        with open(txt_file, 'r') as f:
            data = f.read().strip().split('\n')
            for line in data:
                if ': ' in line:
                    key, value = line.split(': ', 1)
                    # 移除引号
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    elif value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    config[key] = value
    except FileNotFoundError:
        logger.warning(f"配置文件 {txt_file} 未找到")
    except Exception as e:
        logger.warning(f"读取配置文件时出错: {e}")
    return config

def load_digging_config(config_file=None, for_step=None):
    """
    从config/digging_config.txt加载挖掘配置
    支持分阶段数据集配置，提供更好的灵活性和扩展性
    
    :param config_file: 配置文件路径，如果为None则使用默认路径
    :param for_step: 指定步骤('step1', 'step2', 'step3'等)，用于选择对应的数据集配置
    :return: 配置字典
    """
    if config_file is None:
        config_file = os.path.join(ROOT_PATH, 'config', 'digging_config.txt')
    
    config = {}
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 跳过注释和空行
                if line.startswith('#') or not line or ':' not in line:
                    continue
                
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # 类型转换
                if key in ['delay', 'decay', 'n_jobs', 'api_max_retries', 'api_retry_delay',
                           'check_batch_size', 'check_interval',
                            'max_concurrent_checks', 'cache_cleanup_interval',
                           'exponential_backoff_max', 'daily_submit_limit']:
                    value = int(value)
                elif key in ['api_call_interval', 'api_burst_delay']:
                    value = float(value)
                elif key in ['use_recommended_fields', 'api_rate_limit_backoff', 'enable_smart_delay',
                            'smart_retry_enabled']:
                    value = value.lower() in ['true', 'yes', '1', 'on']
                
                config[key] = value
                
    except FileNotFoundError:
        logger.info(f"配置文件 {config_file} 未找到，使用默认配置")
        # 默认配置
        config = {
            'priority_dataset': 'analyst4',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'n_jobs': 3,
            'use_recommended_fields': False,
            'api_max_retries': 3,
            'api_retry_delay': 5,
            'api_rate_limit_backoff': True,
            'daily_submit_limit': 0,
            'daily_limit_timezone': '-4',

        }
    except Exception as e:
        logger.warning(f"读取挖掘配置文件时出错: {e}")
        logger.info("使用默认配置")
        # 发生任何其他错误时也使用默认配置
        config = {
            'priority_dataset': 'analyst4',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'n_jobs': 3,
            'use_recommended_fields': False,
            'api_max_retries': 3,
            'api_retry_delay': 5,
            'api_rate_limit_backoff': True,
            'daily_submit_limit': 0,
            'daily_limit_timezone': '-4',

        }
    
    return config


def parse_timezone_offset(timezone_str):
    """
    解析时区字符串并返回UTC偏移小时数
    
    :param timezone_str: 时区字符串 ('UTC', 'LOCAL', 'ET', '+8', '-4' 等)
    :return: UTC偏移小时数 (正数表示东时区，负数表示西时区)
    """
    import time
    from datetime import datetime
    
    timezone_str = timezone_str.upper().strip()
    
    if timezone_str == 'UTC':
        return 0
    elif timezone_str == 'LOCAL':
        # 获取本地时区偏移
        if time.daylight:
            return -time.altzone / 3600  # 夏令时偏移
        else:
            return -time.timezone / 3600  # 标准时间偏移
    elif timezone_str == 'ET':
        # 美国东部时间 (UTC-4 夏令时, UTC-5 标准时间)
        # 简化处理，使用UTC-4
        return -4
    elif timezone_str.startswith('+') or timezone_str.startswith('-'):
        # 数字偏移格式，如 +8, -4
        try:
            return int(timezone_str)
        except ValueError:
            return 0
    else:
        logger.warning(f"⚠️  未识别的时区格式: {timezone_str}，使用UTC")
        return 0


def get_current_date_with_timezone(timezone_str='UTC'):
    """
    根据指定时区获取当前日期字符串 (YYYY-MM-DD)
    
    :param timezone_str: 时区字符串
    :return: 日期字符串 YYYY-MM-DD
    """
    from datetime import datetime, timedelta
    
    offset_hours = parse_timezone_offset(timezone_str)
    utc_now = datetime.utcnow()
    local_now = utc_now + timedelta(hours=offset_hours)
    
    return local_now.strftime('%Y-%m-%d')


def login():
    # 从txt文件解密并读取数据
    # txt格式:
    # password: 'password'
    # username: 'username'
    def load_decrypted_data(txt_file=None):
        if txt_file is None:
            txt_file = os.path.join(ROOT_PATH, 'config', 'user_info.txt')
        with open(txt_file, 'r') as f:
            data = f.read()
            data = data.strip().split('\n')

            data = {line.split(': ')[0]: line.split(': ')[1] for line in data if ': ' in line}

        return data['username'][1:-1], data['password'][1:-1]

    username, password = load_decrypted_data()

    # Create a session to persistently store the headers
    s = requests.Session()

    # Save credentials into session
    s.auth = (username, password)

    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')

    info_ = response.content.decode('utf-8')
    logger.info(info_)
    if "INVALID_CREDENTIALS" in info_:
        raise Exception("你的账号密码有误，请在【config/user_info.txt】输入正确的邮箱和密码！\n"
                        "Your username or password is incorrect. Please enter the correct email and password!")
    return s

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)

brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")

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

# 延迟登录，避免模块导入时就登录
s = None

def init_session():
    """初始化session，获取可用的操作符"""
    import time
    global s, ts_ops, basic_ops, group_ops
    if s is None:
        try:
            # 使用SessionClient获取会话
            from session_client import get_session
            s = get_session()
            logger.info("✅ machine_lib_ee 使用SessionClient")
        except Exception as e:
            logger.warning(f"❌ SessionClient不可用: {e}")
            logger.warning("💡 请确保SessionKeeper正在运行并维护有效会话")
            raise
        
        # 保存原始操作符列表
        globals()['ts_ops_original'] = ts_ops.copy()
        globals()['basic_ops_original'] = basic_ops.copy()
        globals()['group_ops_original'] = group_ops.copy()
        
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
                            return s
                    else:
                        logger.warning(f"❌ API 错误: {error_msg}")
                        if attempt < max_retries - 1:
                            logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                            return s
                
                # 正常响应，构造 DataFrame
                if isinstance(response_data, list) and len(response_data) > 0:
                    aval = pd.DataFrame(response_data)['name'].tolist()
                    
                    # 过滤所有操作符类型
                    original_ts = len(ts_ops)
                    original_basic = len(basic_ops)
                    original_group = len(group_ops)
                    
                    ts_ops = [op for op in ts_ops if op in aval]
                    basic_ops = [op for op in basic_ops if op in aval]
                    group_ops = [op for op in group_ops if op in aval]
                    
                    # 更新全局变量
                    globals()['ts_ops'] = ts_ops
                    globals()['basic_ops'] = basic_ops  
                    globals()['group_ops'] = group_ops
                    
                    logger.info(f"✅ 成功获取 {len(aval)} 个可用操作符")
                    logger.info(f"📊 时间序列操作符: {len(ts_ops)}/{original_ts} 个")
                    logger.info(f"📊 基础操作符: {len(basic_ops)}/{original_basic} 个")
                    logger.info(f"📊 组操作符: {len(group_ops)}/{original_group} 个")
                    
                    # 显示被过滤掉的操作符
                    ts_ops_orig = globals().get('ts_ops_original', [])
                    basic_ops_orig = globals().get('basic_ops_original', [])
                    group_ops_orig = globals().get('group_ops_original', [])
                    
                    filtered_ts = [op for op in ts_ops_orig if op not in aval]
                    filtered_basic = [op for op in basic_ops_orig if op not in aval] 
                    filtered_group = [op for op in group_ops_orig if op not in aval]
                    
                    if filtered_ts or filtered_basic or filtered_group:
                        logger.info(f"⚠️  过滤掉的不可用操作符:")
                        if filtered_ts:
                            logger.info(f"   ts_ops: {filtered_ts}")
                        if filtered_basic:
                            logger.info(f"   basic_ops: {filtered_basic}")
                        if filtered_group:
                            logger.info(f"   group_ops: {filtered_group}")
                    
                    break
                else:
                    logger.warning("⚠️  API 返回空数据或格式异常")
                    if attempt < max_retries - 1:
                        logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
                        return s
                        
            except Exception as e:
                logger.warning(f"❌ 获取操作符时发生异常: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"⏳ 等待 {retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.warning("❌ 达到最大重试次数，使用默认操作符列表")
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
    return aval


def list_chuckation(field_list, num):
    list_chucked = []
    lens = len(field_list)
    i = 0
    while i + num <= lens:
        list_chucked.append(field_list[i:i + num])
        i += num
    list_chucked.append(field_list[i:lens])
    return list_chucked


def batch_set_alpha_properties(
        s,
        alpha_data: list,  # [{"id": "alpha_id", "color": "GREEN"}, ...]
        max_batch_size: int = 100
):
    """
    批量设置Alpha颜色（使用新的批量API - PATCH /alphas）
    注意：此API目前只支持批量设置颜色，不支持name、tags等其他属性
    
    Args:
        s: session对象
        alpha_data: Alpha数据列表，每个元素必须包含id和color字段
                   例：[{"id": "ZVO5aLY", "color": "GREEN"}, {"id": "QR09ZpG", "color": "BLUE"}]
        max_batch_size: 最大批次大小
    Returns:
        dict: {"success": int, "failed": int, "details": [...]}
    """
    if not alpha_data:
        return {"success": 0, "failed": 0, "details": []}
    
    success_count = 0
    failed_count = 0
    details = []
    
    # 分批处理，避免单次请求过大
    for i in range(0, len(alpha_data), max_batch_size):
        batch = alpha_data[i:i + max_batch_size]
        batch_num = i//max_batch_size + 1
        
        # 重试机制：处理速率限制
        retry_count = 0
        max_retries = 3
        batch_success = False
        
        while retry_count < max_retries and not batch_success:
            try:
                response = s.patch(
                    "https://api.worldquantbrain.com/alphas",
                    json=batch
                )
                
                if response.status_code == 200:
                    batch_success_count = len(batch)
                    success_count += batch_success_count
                    details.append(f"批次 {batch_num}: 成功设置 {batch_success_count} 个Alpha")
                    batch_success = True
                    
                elif response.status_code == 429:
                    # 速率限制，等待后重试
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = min(2 ** retry_count, 8)  # 指数退避，最大8秒
                        details.append(f"批次 {batch_num}: API速率限制，等待 {wait_time}秒后重试 ({retry_count}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        # 重试次数用完，标记为失败
                        batch_failed = len(batch)
                        failed_count += batch_failed
                        details.append(f"批次 {batch_num}: API速率限制重试失败，已达最大重试次数")
                        
                else:
                    # 其他HTTP错误，不重试
                    batch_failed = len(batch)
                    failed_count += batch_failed
                    error_msg = f"批次 {batch_num}: HTTP {response.status_code}"
                    try:
                        error_data = response.json()
                        if 'message' in error_data:
                            error_msg += f" - {error_data['message']}"
                    except:
                        error_msg += f" - {response.text[:100]}"
                    details.append(error_msg)
                    break
                    
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 8)
                    details.append(f"批次 {batch_num}: 网络异常，等待 {wait_time}秒后重试 ({retry_count}/{max_retries}) - {str(e)}")
                    time.sleep(wait_time)
                else:
                    batch_failed = len(batch)
                    failed_count += batch_failed
                    details.append(f"批次 {batch_num}: 网络异常重试失败 - {str(e)}")
                    
        # 批次间休息，避免API压力（成功的批次也要休息）
        if i + max_batch_size < len(alpha_data):
            time.sleep(1)
    
    return {
        "success": success_count,
        "failed": failed_count,
        "details": details
    }


def set_alpha_properties(
        s,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = None,
        combo_desc: str = None,
        tags: list = None,  # ['tag1', 'tag2']
):
    """
    Function changes alpha's description parameters
    Returns:
    - True: 成功
    - False: 一般失败
    - 'RATE_LIMITED': HTTP 429速率限制，建议重新登录
    """
    params = {
        "category": None,
        "regular": {"description": None},
        "name": alpha_id if alpha_id else name  # 默认使用alpha_id作为name
    }
    if color:
        params["color"] = color
    if tags:
        params["tags"] = tags
    if combo_desc:
        params["combo"] = {"description": combo_desc}
    if selection_desc:
        params["selection"] = {"description": selection_desc}

    try:
        response = s.patch(
            "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
        )
        
        # 检查响应状态
        if response.status_code == 200:
            return True
        else:
            logger.warning(f"❌ Alpha {alpha_id} 属性设置失败: HTTP {response.status_code}")
            
            # 特别处理429错误 - 返回特殊标识
            if response.status_code == 429:
                try:
                    error_text = response.text
                    if error_text:
                        logger.warning(f"错误详情: {error_text}")
                    try:
                        error_data = response.json()
                        if 'message' in error_data:
                            logger.info(f"API速率限制: {error_data['message']}")
                        if 'retry_after' in error_data:
                            logger.info(f"建议等待时间: {error_data['retry_after']}秒")
                    except:
                        logger.info("API速率限制超出，建议重新登录")
                except Exception as parse_error:
                    logger.info(f"解析429错误响应时异常: {parse_error}")
                
                # 返回特殊标识，表示需要重新登录
                return 'RATE_LIMITED'
            
            # 其他HTTP错误
            try:
                error_text = response.text
                if error_text:
                    logger.warning(f"错误详情: {error_text}")
            except Exception as parse_error:
                logger.info(f"解析错误响应时异常: {parse_error}")
            return False
            
    except Exception as e:
        logger.warning(f"❌ Alpha {alpha_id} 属性设置异常: {e}")
        return False


def get_vec_fields(fields):
    vec_fields = []

    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)" % (vec_op, field))
                vec_fields.append("%s(%s, nth=0)" % (vec_op, field))
            else:
                vec_fields.append("%s(%s)" % (vec_op, field))

    return (vec_fields)


def get_datasets(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" + \
          f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000',
        dataset_id: str = '',
        search: str = ''
):
    """健壮获取数据字段列表，支持分页、重试和会话刷新

    变更点：
    - 不再依赖 response.json()['count']，改为基于分页直到返回数量<limit
    - 对 429 速率限制尊重 Retry-After 头并退避重试
    - 对 401/403 或异常响应尝试重新登录
    - 任何异常情况下返回空 DataFrame，而不是抛出 KeyError
    """
    base_url = "https://api.worldquantbrain.com/data-fields"
    limit = 50
    offset = 0
    aggregated_results = []

    # 读取重试配置
    try:
        cfg = load_digging_config()
        max_retries = cfg.get('api_max_retries', 3)
        retry_delay = cfg.get('api_retry_delay', 5)
        call_interval = cfg.get('api_call_interval', 0.0)
        burst_delay = cfg.get('api_burst_delay', 0.0)
    except Exception:
        max_retries = 3
        retry_delay = 5
        call_interval = 0.0
        burst_delay = 0.0

    # 构建固定查询参数
    def make_params(current_offset: int):
        params = {
            'instrumentType': instrument_type,
            'region': region,
            'delay': str(delay),
            'universe': universe,
            'limit': str(limit),
            'offset': str(current_offset),
        }
        if dataset_id and not search:
            params['dataset.id'] = dataset_id
        if search:
            params['search'] = search
        return params

    while True:
        params = make_params(offset)

        # 单次请求的重试（处理429/401/临时错误）
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = s.get(base_url, params=params, timeout=15)

                # 速率限制处理
                if resp.status_code == 429:
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after is not None:
                        time.sleep(float(retry_after))
                    else:
                        time.sleep(retry_delay * attempt)
                    if attempt < max_retries:
                        continue
                    else:
                        logger.info("get_datafields: 连续速率限制，提前结束分页")
                        return pd.DataFrame([])

                # 未授权/禁止，尝试获取最新会话
                if resp.status_code in (401, 403):
                    logger.info(f"get_datafields: {resp.status_code}，尝试获取最新会话后重试")
                    try:
                        # 使用SessionClient获取最新会话（SessionKeeper会自动维护）
                        logger.info(f"get_datafields: 获取最新会话...")
                        from session_client import get_session
                        new_session = get_session()
                        logger.info(f"get_datafields: SessionClient获取成功")
                        
                        # 重要：更新传入的session对象的属性
                        s.cookies.update(new_session.cookies)
                        s.headers.update(new_session.headers)
                        if hasattr(new_session, 'auth'):
                            s.auth = new_session.auth
                        logger.info("get_datafields: session更新成功")
                    except Exception as e:
                        logger.error(f"get_datafields: 会话更新失败({e})，使用SessionClient")
                        from session_client import get_session
                        new_session = get_session()
                        s.cookies.update(new_session.cookies)
                        s.headers.update(new_session.headers)
                        if hasattr(new_session, 'auth'):
                            s.auth = new_session.auth
                        logger.info(f"get_datafields: SessionClient恢复成功")
                    if attempt < max_retries:
                        continue
                    else:
                        return pd.DataFrame([])

                if resp.status_code != 200:
                    logger.info(f"get_datafields: HTTP {resp.status_code}: {resp.text[:200]}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 尝试解析JSON
                try:
                    data = resp.json()
                except Exception as e:
                    logger.info(f"get_datafields: JSON解析失败: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 如果返回的是错误消息格式，尝试退避
                if isinstance(data, dict) and 'message' in data and 'results' not in data:
                    logger.info(f"get_datafields: API错误: {data.get('message')}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 正常处理结果
                results = data.get('results', [])
                aggregated_results.extend(results)

                # 控制请求节奏
                if call_interval > 0:
                    time.sleep(call_interval)

                # 结束条件：返回数量小于limit
                if len(results) < limit:
                    return pd.DataFrame(aggregated_results)

                # 准备下一页
                offset += limit
                # 可选的批次间延迟
                if burst_delay > 0:
                    time.sleep(burst_delay)
                # 跳出重试循环，进行下一页
                break

            except requests.RequestException as e:
                logger.info(f"get_datafields: 请求异常: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    return pd.DataFrame([])
            except Exception as e:
                logger.info(f"get_datafields: 未知异常: {e}")
                return pd.DataFrame([])


def process_datafields(df, data_type):
    """处理数据字段，支持空DataFrame的健壮处理"""
    # 检查DataFrame是否为空或缺少必要列
    if df.empty or 'type' not in df.columns or 'id' not in df.columns:
        logger.info(f"process_datafields: DataFrame为空或缺少必要列(type/id)，返回空字段列表")
        return []
    
    try:
        if data_type == "matrix":
            datafields = df[df['type'] == "MATRIX"]["id"].tolist()
        elif data_type == "vector":
            datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
        else:
            logger.info(f"process_datafields: 未知数据类型: {data_type}")
            return []

        tb_fields = []
        for field in datafields:
            tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)" % field)
        return tb_fields
        
    except Exception as e:
        logger.info(f"process_datafields: 处理{data_type}字段时发生异常: {e}")
        return []


def get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th, region, universe, delay,
               instrumentType, alpha_num, usage, tag: str = '', color_exclude='', s=None):
    # color None, RED, YELLOW, GREEN, BLUE, PURPLE
    if s is None:
        from session_client import get_session
        s = get_session()
    alpha_list = []
    next_alphas = []
    decay_alphas = []
    check_alphas = []
    # 3E large 3C less
    # 正的
    i = 0
    while True:
        # 构建基础URL
        url_e = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                 f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                 f"&settings.region={region}&is.sharpe%3E={sharpe_th}")
        
        # 只有在fitness_th不为None时才添加fitness过滤
        if fitness_th is not None:
            url_e += f"&is.fitness%3E={fitness_th}"
        
        url_e += (f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                 f"T00:00:00-04:00&dateCreated%3C{end_date}T00:00:00-04:00&type=REGULAR&color!={color_exclude}&"
                 f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

        response = s.get(url_e)
        # logger.info(response.json())
        try:
            logger.info(i)
            i += 100
            count = response.json()["count"]
            logger.info("count: %d" % count)
            alpha_list.extend(response.json()["results"])
            if i >= count or i == 9900:
                break
            time.sleep(0.01)
        except Exception as e:
            logger.info(f"Failed to get alphas: {e}")
            i -= 100
            logger.info("%d finished re-login" % i)
            from session_client import get_session
            s = get_session()

    # 负的
    if usage != "submit":
        i = 0
        while True:
            url_c = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                     f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                     f"&settings.region={region}&is.sharpe%3C=-{sharpe_th}&is.fitness%3C=-{fitness_th}"
                     f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                     f"T00:00:00-04:00&dateCreated%3C{end_date}T00:00:00-04:00&type=REGULAR&color!={color_exclude}&"
                     f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

            response = s.get(url_c)
            # logger.info(response.json())
            try:
                count = response.json()["count"]
                if i >= count or i == 9900:
                    break
                alpha_list.extend(response.json()["results"])
                i += 100
            except Exception as e:
                logger.info(f"Failed to get alphas: {e}")
                logger.info("%d finished re-login" % i)
                from session_client import get_session
                s = get_session()

    # logger.info(alpha_list)
    if len(alpha_list) == 0:
        if usage != "submit":
            return {"next": [], "decay": []}
        else:
            return {"check": []}

    # logger.info(response.json())
    if usage != "submit":
        for j in range(len(alpha_list)):
            alpha_id = alpha_list[j]["id"]
            name = alpha_list[j]["name"]
            dateCreated = alpha_list[j]["dateCreated"]
            sharpe = alpha_list[j]["is"]["sharpe"]
            fitness = alpha_list[j]["is"]["fitness"]
            turnover = alpha_list[j]["is"]["turnover"]
            margin = alpha_list[j]["is"]["margin"]
            longCount = alpha_list[j]["is"]["longCount"]
            shortCount = alpha_list[j]["is"]["shortCount"]
            decay = alpha_list[j]["settings"]["decay"]
            exp = alpha_list[j]['regular']['code']
            region = alpha_list[j]["settings"]["region"]

            concentrated_weight = next(
                (check.get('value', 0) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "CONCENTRATED_WEIGHT"), 0)
            sub_universe_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "LOW_SUB_UNIVERSE_SHARPE"), 99)
            two_year_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if check["name"] == "LOW_2Y_SHARPE"),
                99)
            ladder_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "IS_LADDER_SHARPE"), 99)

            conditions = ((longCount > 100 or shortCount > 100) and
                          (concentrated_weight < 0.2) and
                        #   (abs(sub_universe_sharpe) > sharpe_th / 1.66) and
                          (abs(sub_universe_sharpe) > math.sqrt(1000/3000) * sharpe) and
                          (abs(two_year_sharpe) > sharpe_th) and
                          (abs(ladder_sharpe) > sharpe_th) and
                          (not (region == "CHN" and sharpe < 0))
                          )
            # if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
            if conditions:
                if sharpe < 0:
                    exp = "-%s" % exp
                rec = [alpha_id, exp, sharpe, turnover, fitness, margin, longCount, shortCount, dateCreated, decay]
                # logger.info(rec)
                if turnover > 0.7:
                    rec.append(decay * 4)
                    decay_alphas.append(rec)
                elif turnover > 0.6:
                    rec.append(decay * 3 + 3)
                    decay_alphas.append(rec)
                elif turnover > 0.5:
                    rec.append(decay * 3)
                    decay_alphas.append(rec)
                elif turnover > 0.4:
                    rec.append(decay * 2)
                    decay_alphas.append(rec)
                elif turnover > 0.35:
                    rec.append(decay + 4)
                    decay_alphas.append(rec)
                elif turnover > 0.3:
                    rec.append(decay + 2)
                    decay_alphas.append(rec)
                else:
                    next_alphas.append(rec)
        output_dict = {"next": next_alphas, "decay": decay_alphas}
        logger.info("count: %d" % (len(next_alphas) + len(decay_alphas)))
    else:
        for alpha_detail in alpha_list:
            id = alpha_detail["id"]
            type = alpha_detail["type"]
            author = alpha_detail["author"]
            instrumentType = alpha_detail["settings"]["instrumentType"]
            region = alpha_detail["settings"]["region"]
            universe = alpha_detail["settings"]["universe"]
            delay = alpha_detail["settings"]["delay"]
            decay = alpha_detail["settings"]["decay"]
            neutralization = alpha_detail["settings"]["neutralization"]
            truncation = alpha_detail["settings"]["truncation"]
            pasteurization = alpha_detail["settings"]["pasteurization"]
            unitHandling = alpha_detail["settings"]["unitHandling"]
            nanHandling = alpha_detail["settings"]["nanHandling"]
            language = alpha_detail["settings"]["language"]
            visualization = alpha_detail["settings"]["visualization"]
            code = alpha_detail["regular"]["code"]
            description = alpha_detail["regular"]["description"]
            operatorCount = alpha_detail["regular"]["operatorCount"]
            dateCreated = alpha_detail["dateCreated"]
            dateSubmitted = alpha_detail["dateSubmitted"]
            dateModified = alpha_detail["dateModified"]
            name = alpha_detail["name"]
            favorite = alpha_detail["favorite"]
            hidden = alpha_detail["hidden"]
            color = alpha_detail["color"]
            category = alpha_detail["category"]
            tags = alpha_detail["tags"]
            classifications = alpha_detail["classifications"]
            grade = alpha_detail["grade"]
            stage = alpha_detail["stage"]
            status = alpha_detail["status"]
            pnl = alpha_detail["is"]["pnl"]
            bookSize = alpha_detail["is"]["bookSize"]
            longCount = alpha_detail["is"]["longCount"]
            shortCount = alpha_detail["is"]["shortCount"]
            turnover = alpha_detail["is"]["turnover"]
            returns = alpha_detail["is"]["returns"]
            drawdown = alpha_detail["is"]["drawdown"]
            margin = alpha_detail["is"]["margin"]
            fitness = alpha_detail["is"]["fitness"]
            sharpe = alpha_detail["is"]["sharpe"]
            startDate = alpha_detail["is"]["startDate"]
            checks = alpha_detail["is"]["checks"]
            os = alpha_detail["os"]
            train = alpha_detail["train"]
            test = alpha_detail["test"]
            prod = alpha_detail["prod"]
            competitions = alpha_detail["competitions"]
            themes = alpha_detail["themes"]
            team = alpha_detail["team"]
            checks_df = pd.DataFrame(checks)
            pyramids = next(
                ([y['name'] for y in item['pyramids']] for item in checks if item['name'] == 'MATCHES_PYRAMID'), None)

            if any(checks_df["result"] == "FAIL"):
                # 最基础的项目不通过
                set_alpha_properties(s, id, color='RED')
                continue
            else:
                # 通过了最基础的项目
                # 把全部的信息以字典的形式返回
                rec = {"id": id, "type": type, "author": author, "instrumentType": instrumentType, "region": region,
                       "universe": universe, "delay": delay, "decay": decay, "neutralization": neutralization,
                       "truncation": truncation, "pasteurization": pasteurization, "unitHandling": unitHandling,
                       "nanHandling": nanHandling, "language": language, "visualization": visualization, "code": code,
                       "description": description, "operatorCount": operatorCount, "dateCreated": dateCreated,
                       "dateSubmitted": dateSubmitted, "dateModified": dateModified, "name": name, "favorite": favorite,
                       "hidden": hidden, "color": color, "category": category, "tags": tags,
                       "classifications": classifications, "grade": grade, "stage": stage, "status": status, "pnl": pnl,
                       "bookSize": bookSize, "longCount": longCount, "shortCount": shortCount, "turnover": turnover,
                       "returns": returns, "drawdown": drawdown, "margin": margin, "fitness": fitness, "sharpe": sharpe,
                       "startDate": startDate, "checks": checks, "os": os, "train": train, "test": test, "prod": prod,
                       "competitions": competitions, "themes": themes, "team": team, "pyramids": pyramids}
                check_alphas.append(rec)
        output_dict = {"check": check_alphas}

    # 超过了限制
    if usage == 'submit' and count >= 9900:
        if len(output_dict['check']) < len(alpha_list):
            # 那么就再来一遍
            output_dict = get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th,
                                     region, universe, delay, instrumentType, alpha_num, usage, tag, color_exclude)
        else:
            raise Exception("Too many alphas to check!! over 10000, universe: %s, region: %s" % (universe, region))

    return output_dict


#保留，优化挖掘脚本
def prune(next_alpha_recs, prefix, keep_num):
    # prefix is datafield prefix, like fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-field alpha to keep 
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp, decay])
    return output


def transform(next_alpha_recs):
    output = []
    for rec in next_alpha_recs:
        decay = rec[-1]
        exp = rec[1]
        output.append([exp, decay])
    return output


def first_order_factory(fields, ops_set):
    alpha_set = []
    for field in fields:
        # reverse op does the work
        alpha_set.append(field)
        # alpha_set.append("-%s"%field)
        for op in ops_set:

            if op == "ts_percentage":

                # lpha_set += ts_comp_factory(op, field, "percentage", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])


            elif op == "ts_decay_exp_window":

                # alpha_set += ts_comp_factory(op, field, "factor", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])

            elif op == "ts_moment":

                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])

            elif op == "ts_entropy":

                # alpha_set += ts_comp_factory(op, field, "buckets", [5, 10, 15, 20])
                alpha_set += ts_comp_factory(op, field, "buckets", [10])

            elif op.startswith("ts_") or op == "inst_tvr":

                alpha_set += ts_factory(op, field)

            elif op.startswith("group_"):

                alpha_set += group_factory(op, field, "usa")

            elif op.startswith("vector"):

                alpha_set += vector_factory(op, field)

            elif op == "signed_power":

                alpha = "%s(%s, 2)" % (op, field)
                alpha_set.append(alpha)

            else:
                alpha = "%s(%s)" % (op, field)
                alpha_set.append(alpha)

    return alpha_set


def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order


def vector_factory(op, field):
    output = []
    vectors = ["cap"]

    for vector in vectors:
        alpha = "%s(%s, %s)" % (op, field, vector)
        output.append(alpha)

    return output


def trade_when_factory(op, field, region, delay=1):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   # "ts_skewness(returns,120)> 0.7",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3",
                   "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3",
                   "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0" % field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0" % field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]
    if delay==1:
        # exit_events = ["abs(returns) > 0.1", "-1", "days_from_last_change(ern3_pre_reptime) > 20"] # ern3_pre_reptime字段失效
        exit_events = ["abs(returns) > 0.1", "-1"]
    else:
        exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8",
                  "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1", ]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)" % (op, oe, field, ee)
            output.append(alpha)
    return output


def ts_factory(op, field):
    output = []
    # days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]

    for day in days:
        alpha = "%s(%s, %d)" % (op, field, day)
        output.append(alpha)

    return output


def ts_comp_factory(op, field, factor, paras):
    output = []
    # l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 120, 240], paras
    comb = list(product(l1, l2))

    for day, para in comb:

        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)" % (op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)" % (op, field, day, factor, para)

        output.append(alpha)

    return output


def group_factory(op, field, region):
    output = []
    vectors = ["cap"]

    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l',
                    'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']

    chn_group_1 = ['sta1_top3000c30', 'sta1_top3000c20', 'sta1_top3000c10', 'sta1_top3000c2', 'sta1_top3000c5']

    chn_group_2 = ['sta2_top3000_fact4_c10', 'sta2_top2000_fact4_c50', 'sta2_top3000_fact3_c20']

    chn_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector',
                   'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']

    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']

    hkg_group_1 = ['sta1_allc50', 'sta1_allc5', 'sta1_allxjp_513_c20', 'sta1_top2000xjp_513_c5']

    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10', 'sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13', 'sta2_top2000_xjp_513_top2000_fact3_c20']

    hkg_group_8 = ['oth455_relation_n2v_p10_q50_w5_kmeans_cluster_5',
                   'oth455_relation_n2v_p10_q50_w4_kmeans_cluster_10',
                   'oth455_relation_n2v_p10_q50_w1_kmeans_cluster_20',
                   'oth455_partner_n2v_p50_q200_w4_kmeans_cluster_5',
                   'oth455_partner_n2v_p10_q50_w4_pca_fact3_cluster_10',
                   'oth455_customer_n2v_p50_q50_w1_kmeans_cluster_5']

    twn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']

    twn_group_1 = ['sta1_allc50', 'sta1_allxjp_513_c50', 'sta1_allxjp_513_c20', 'sta1_allxjp_513_c2',
                   'sta1_allc20', 'sta1_allxjp_513_c5', 'sta1_allxjp_513_c10', 'sta1_allc2', 'sta1_allc5']

    twn_group_2 = ['sta2_allfactor_xjp_513_0', 'sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20', 'sta2_all_xjp_513_all_fact4_c50']

    twn_group_8 = ['oth455_relation_n2v_p50_q200_w1_pca_fact1_cluster_20',
                   'oth455_relation_n2v_p10_q50_w3_kmeans_cluster_20',
                   'oth455_relation_roam_w3_pca_fact2_cluster_5',
                   'oth455_relation_n2v_p50_q50_w2_pca_fact2_cluster_10',
                   'oth455_relation_n2v_p10_q200_w5_pca_fact2_cluster_20',
                   'oth455_relation_n2v_p50_q50_w5_kmeans_cluster_5']

    usa_group_13 = ['pv13_h_min2_3000_sector', 'pv13_r2_min20_3000_sector', 'pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']

    usa_group_1 = ['sta1_top3000c50', 'sta1_allc20', 'sta1_allc10', 'sta1_top3000c20', 'sta1_allc5']

    usa_group_2 = ['sta2_top3000_fact3_c50', 'sta2_top3000_fact4_c20', 'sta2_top3000_fact4_c10']

    usa_group_3 = ['sta3_2_sector', 'sta3_3_sector', 'sta3_news_sector', 'sta3_peer_sector',
                   'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector', 'sta3_pvgroup3_sector', 'sta3_sec_sector']

    usa_group_4 = ['rsk69_01c_1m', 'rsk69_57c_1m', 'rsk69_02c_2m', 'rsk69_5c_2m', 'rsk69_02c_1m',
                   'rsk69_05c_2m', 'rsk69_57c_2m', 'rsk69_5c_1m', 'rsk69_05c_1m', 'rsk69_01c_2m']

    usa_group_5 = ['anl52_2000_backfill_d1_05c', 'anl52_3000_d1_05c', 'anl52_3000_backfill_d1_02c',
                   'anl52_3000_backfill_d1_5c', 'anl52_3000_backfill_d1_05c', 'anl52_3000_d1_5c']

    usa_group_6 = ['mdl10_group_name']

    usa_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector',
                   'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']

    usa_group_8 = ['oth455_competitor_n2v_p10_q50_w1_kmeans_cluster_10',
                   'oth455_customer_n2v_p10_q50_w5_kmeans_cluster_10',
                   'oth455_relation_n2v_p50_q200_w5_kmeans_cluster_20',
                   'oth455_competitor_n2v_p50_q50_w3_kmeans_cluster_10',
                   'oth455_relation_n2v_p50_q50_w3_pca_fact2_cluster_10',
                   'oth455_partner_n2v_p10_q50_w2_pca_fact2_cluster_5',
                   'oth455_customer_n2v_p50_q50_w3_kmeans_cluster_5',
                   'oth455_competitor_n2v_p50_q200_w5_kmeans_cluster_20']

    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']

    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50', 'sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    asi_group_1 = []

    asi_group_8 = ['oth455_partner_roam_w3_pca_fact1_cluster_5',
                   'oth455_relation_roam_w3_pca_fact1_cluster_20',
                   'oth455_relation_roam_w3_kmeans_cluster_20',
                   'oth455_relation_n2v_p10_q200_w5_pca_fact1_cluster_20',
                   'oth455_relation_n2v_p10_q200_w5_pca_fact1_cluster_20',
                   'oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_10']
    asi_group_8 = []

    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']

    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']

    jpn_group_8 = ['oth455_customer_n2v_p50_q50_w5_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q50_w4_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q50_w3_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q50_w2_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q200_w5_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q200_w5_kmeans_cluster_10']

    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']

    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']

    kor_group_1 = ['sta1_allc20', 'sta1_allc50', 'sta1_allc2', 'sta1_allc10', 'sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']

    kor_group_2 = ['sta2_all_xjp_513_all_fact1_c50', 'sta2_top2000_xjp_513_top2000_fact2_c50',
                   'sta2_all_xjp_513_all_fact4_c50', 'sta2_all_xjp_513_all_fact4_c5']

    kor_group_8 = ['oth455_relation_n2v_p50_q200_w3_pca_fact3_cluster_5',
                   'oth455_relation_n2v_p50_q50_w4_pca_fact2_cluster_10',
                   'oth455_relation_n2v_p50_q200_w5_pca_fact2_cluster_5',
                   'oth455_relation_n2v_p50_q200_w4_kmeans_cluster_10',
                   'oth455_relation_n2v_p10_q50_w1_kmeans_cluster_10',
                   'oth455_relation_n2v_p50_q50_w5_pca_fact1_cluster_20']

    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']

    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']

    eur_group_2 = ['sta2_top1200_fact3_c50', 'sta2_top1200_fact3_c20', 'sta2_top1200_fact4_c50']

    eur_group_3 = ['sta3_6_sector', 'sta3_pvgroup4_sector', 'sta3_pvgroup5_sector']

    # eur_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector',
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']
    eur_group_7 = []

    eur_group_8 = ['oth455_relation_n2v_p50_q200_w3_pca_fact1_cluster_5',
                   'oth455_competitor_n2v_p50_q200_w4_kmeans_cluster_20',
                   'oth455_competitor_n2v_p50_q200_w5_pca_fact1_cluster_10',
                   'oth455_competitor_roam_w4_pca_fact2_cluster_20',
                   'oth455_relation_n2v_p10_q200_w2_pca_fact2_cluster_20',
                   'oth455_competitor_roam_w2_pca_fact3_cluster_20']

    glb_group_13 = ["pv13_10_f2_g3_sector", "pv13_2_f3_g2_sector", "pv13_2_sector", "pv13_52_all_delay_1_sector"]

    glb_group_3 = ['sta3_2_sector', 'sta3_3_sector', 'sta3_news_sector', 'sta3_peer_sector',
                   'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector', 'sta3_pvgroup3_sector', 'sta3_sec_sector']

    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']

    glb_group_2 = ['sta2_all_fact4_c50', 'sta2_all_fact4_c20', 'sta2_all_fact3_c20', 'sta2_all_fact4_c10']

    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector', 'pv13_52_minvol_1m_sector', 'pv13_52_minvol_1m_sector']

    # glb_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector',
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']
    glb_group_7 = []  # 字段消失了

    glb_group_8 = ['oth455_relation_n2v_p10_q200_w5_kmeans_cluster_5',
                   'oth455_relation_n2v_p10_q50_w2_kmeans_cluster_5',
                   'oth455_relation_n2v_p50_q200_w5_kmeans_cluster_5',
                   'oth455_customer_n2v_p10_q50_w4_pca_fact3_cluster_20',
                   'oth455_competitor_roam_w2_pca_fact1_cluster_10',
                   'oth455_relation_n2v_p10_q200_w2_kmeans_cluster_5']

    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']

    amr_group_3 = ['sta3_news_sector', 'sta3_peer_sector', 'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector',
                   'sta3_pvgroup3_sector']

    amr_group_8 = ['oth455_relation_roam_w1_pca_fact2_cluster_10',
                   'oth455_competitor_n2v_p50_q50_w4_kmeans_cluster_10',
                   'oth455_competitor_n2v_p50_q50_w3_kmeans_cluster_10',
                   'oth455_competitor_n2v_p50_q50_w2_kmeans_cluster_10',
                   'oth455_competitor_n2v_p50_q50_w1_kmeans_cluster_10',
                   'oth455_competitor_n2v_p50_q200_w5_kmeans_cluster_10']

    group_3 = ["oth171_region_sector_long_d1_sector", "oth171_region_sector_short_d1_sector",
               "oth171_sector_long_d1_sector", "oth171_sector_short_d1_sector"]

    # bps_group = "bucket(rank(fnd28_value_05480/close), range='0.2, 1, 0.2')"  # 字段不可用，已禁用
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap,sector),range='0,1,0.1')"
    vol_group = "bucket(rank(ts_std_dev(ts_returns(close,1),20)),range = '0.1,1,0.1')"

    # groups = ["market", "sector", "industry", "subindustry", cap_group, sector_cap_group]
    #剔除混信号分组
    groups = ["market","sector", "industry", "subindustry", "country"]

    # if region == "chn" or region.lower() == "chn":
    #     groups += chn_group_13 + chn_group_1 + chn_group_2
    # if region == "twn" or region.lower() == "twn":
    #     groups += twn_group_13 + twn_group_1 + twn_group_2 + twn_group_8
    # if region == "asi" or region.lower() == "asi":
    #     groups += asi_group_13 + asi_group_1 + asi_group_8
    # if region == "usa" or region.lower() == "usa":
    #     groups += usa_group_13 + usa_group_2 + usa_group_4 + usa_group_8
    #     groups += usa_group_5 + usa_group_6
    #     # + usa_group_1 + usa_group_3 + usa_group_7
    # if region == "hkg" or region.lower() == "hkg":
    #     groups += hkg_group_13 + hkg_group_1 + hkg_group_2 + hkg_group_8
    # if region == "kor" or region.lower() == "kor":
    #     groups += kor_group_13 + kor_group_1 + kor_group_2 + kor_group_8
    # if region == "eur" or region.lower() == "eur":
    #     groups += eur_group_13 + eur_group_1 + eur_group_2 + eur_group_3 + eur_group_8 + eur_group_7
    # if region == "glb" or region.lower() == "glb":
    #     # groups += glb_group_13 + glb_group_8 + glb_group_3 + glb_group_1 + glb_group_7
    #     groups += []
    # if region == "amr" or region.lower() == "amr":
    #     groups += amr_group_3 + amr_group_13
    # if region == "jpn" or region.lower() == "jpn":
    #     groups += jpn_group_1 + jpn_group_2 + jpn_group_13 + jpn_group_8

    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))" % (op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)" % (op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))" % (op, field, group)
            output.append(alpha)

    return output


async def async_login():
    """
    从YAML文件加载用户信息并异步登录到指定API
    """
    def load_decrypted_data(txt_file=None):
        if txt_file is None:
            txt_file = os.path.join(ROOT_PATH, 'config', 'user_info.txt')
        with open(txt_file, 'r') as f:
            data = f.read()
            data = data.strip().split('\n')
            data = {line.split(': ')[0]: line.split(': ')[1] for line in data if ': ' in line}

        return data['username'][1:-1], data['password'][1:-1]

    username, password = load_decrypted_data()

    # 创建一个aiohttp的Session
    conn = aiohttp.TCPConnector(ssl=False)
    session = aiohttp.ClientSession(connector=conn)

    try:
        # 发送一个POST请求到/authentication API
        async with session.post('https://api.worldquantbrain.com/authentication',
                                auth=aiohttp.BasicAuth(username, password)) as response:
            # 检查状态码是否为201，确保登录成功
            if response.status == 201:
                logger.info("Login successful!")
            else:
                logger.info(f"Login failed! Status code: {response.status}, Response: {await response.text()}")
                await session.close()
                return None

        return session

    except aiohttp.ClientError as e:
        logger.info(f"Error during login request: {e}")
        await session.close()
    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")
        await session.close()

    return None


async def simulate_single(session_manager, alpha_expression, region_info, name, neut,
                          decay, delay, stone_bag, tags=['None'],
                          semaphore=None, max_trade='OFF'):
    """
    单次模拟一个alpha表达式对应的某个地区的信息
    """
    async with semaphore:
        # 注意：会话管理已由统一会话管理器在后台处理，无需在此检查过期

        region, uni = region_info
        alpha = "%s" % (alpha_expression)

        logger.info("Simulating for alpha: %s, region: %s, universe: %s, decay: %s" % (alpha, region, uni, decay))

        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': delay,
                'decay': decay,
                'neutralization': neut,
                'maxTrade': max_trade,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha
        }

        while True:
            try:
                async with session_manager.post('https://api.worldquantbrain.com/simulations',
                                                json=simulation_data) as resp:
                    # 速率与鉴权处理
                    if resp.status in (401, 403):
                        logger.info("Unauthorized/Forbidden on POST, session issue detected")
                        # 获取最新会话（SessionKeeper会自动维护）
                        try:
                            from session_client import get_session_cookies
                            import yarl
                            
                            # 获取SessionClient维护的cookies
                            current_cookies = get_session_cookies()
                            if current_cookies:
                                logger.info("🔍 检查SessionClient是否已有新的cookies...")
                                
                                # 更新aiohttp的cookies
                                session_manager.cookie_jar.clear()
                                cookie_dict = {}
                                for name, value in current_cookies.items():
                                    cookie_dict[name] = value
                                
                                if cookie_dict:
                                    session_manager.cookie_jar.update_cookies(
                                        cookie_dict, 
                                        response_url=yarl.URL("https://api.worldquantbrain.com")
                                    )
                                    logger.info(f"✅ aiohttp cookies更新完成，包含{len(cookie_dict)}个cookie")
                                else:
                                    logger.warning("⚠️ 当前cookies为空，尝试强制刷新...")
                                    raise Exception("当前cookies为空")
                            else:
                                logger.warning("⚠️ 无法获取当前cookies，尝试强制刷新...")
                                raise Exception("无法获取当前cookies")
                                
                        except Exception as e:
                            # 如果获取现有cookies失败，才进行强制刷新
                            logger.info(f"🔄 获取现有cookies失败({e})，强制刷新会话...")
                    
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                    if resp.status == 429:
                        retry_after_hdr = resp.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        logger.info(f"Rate limited on POST, sleep {wait_s}s then retry")
                        await asyncio.sleep(wait_s)
                        continue
                    simulation_progress_url = resp.headers.get('Location', 0)
                    if simulation_progress_url == 0:
                        # 无 Location，解析错误主体，兼容多种结构
                        try:
                            json_data = await resp.json()
                        except Exception:
                            json_data = await resp.text()

                        def extract_detail(payload):
                            if isinstance(payload, dict):
                                return payload.get('detail') or payload.get('message') or payload.get('error') or ''
                            return str(payload)

                        detail = extract_detail(json_data)
                        detail_str = str(detail)

                        # 并发上限 → 等待重试
                        if 'CONCURRENT_SIMULATION_LIMIT_EXCEEDED' in detail_str:
                            logger.info("Limited by the number of simulations allowed per time")
                            await asyncio.sleep(5)
                            continue

                        # 重复表达式 → 记录并跳过
                        if 'duplicate' in detail_str.lower():
                            logger.info("Alpha expression is duplicated")
                            await asyncio.sleep(1)
                            return 0

                        # 其他错误 → 打印并跳过
                        logger.info(f"detail: {detail_str}")
                        logger.info(f"json_data: {json_data}")
                        await asyncio.sleep(1)
                        return 0
                    else:
                        logger.info(f'simulation_progress_url: {simulation_progress_url}')
                        break
            except KeyError:
                logger.info("Location key error during simulation request")
                await asyncio.sleep(60)
                return
            except Exception as e:
                logger.info(f"An error occurred: {str(e)}")
                await asyncio.sleep(60)
                return

        while True:
            try:
                async with session_manager.get(simulation_progress_url) as resp:
                    # 速率与鉴权处理
                    if resp.status in (401, 403):
                        logger.info("Unauthorized/Forbidden on GET, session issue detected")
                        # 获取最新会话（SessionKeeper会自动维护）
                        try:
                            from session_client import get_session_cookies
                            import yarl
                            
                            # 获取SessionClient维护的cookies
                            current_cookies = get_session_cookies()
                            if current_cookies:
                                logger.info("🔍 检查SessionClient是否已有新的cookies...")
                                
                                # 更新aiohttp的cookies
                                session_manager.cookie_jar.clear()
                                cookie_dict = {}
                                for name, value in current_cookies.items():
                                    cookie_dict[name] = value
                                
                                if cookie_dict:
                                    session_manager.cookie_jar.update_cookies(
                                        cookie_dict, 
                                        response_url=yarl.URL("https://api.worldquantbrain.com")
                                    )
                                    logger.info(f"✅ aiohttp cookies更新完成，包含{len(cookie_dict)}个cookie")
                                else:
                                    logger.warning("⚠️ 当前cookies为空，尝试强制刷新...")
                                    raise Exception("当前cookies为空")
                            else:
                                logger.warning("⚠️ 无法获取当前cookies，尝试强制刷新...")
                                raise Exception("无法获取当前cookies")
                                
                        except Exception as e:
                            # 如果获取现有cookies失败，才进行强制刷新
                            logger.info(f"🔄 获取现有cookies失败({e})，强制刷新会话...")
                        
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                    if resp.status == 429:
                        retry_after_hdr = resp.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        logger.info(f"Rate limited on GET, sleep {wait_s}s then retry")
                        await asyncio.sleep(wait_s)
                        continue
                    
                    # 非 JSON 响应（如 504 HTML 网关页）健壮处理
                    content_type = (resp.headers.get('Content-Type') or '').lower()
                    if 'application/json' not in content_type:
                        try:
                            body_preview = (await resp.text())[:200]
                        except Exception:
                            body_preview = '<non-json>'
                        logger.info(f"Non-JSON progress response: status={resp.status}, content-type={content_type}, body[:200]={body_preview}")
                        if resp.status in (500, 502, 503, 504, 408):
                            await asyncio.sleep(5)
                            continue
                        else:
                            await asyncio.sleep(2)
                            continue

                    json_data = await resp.json()
                    # 获取响应头并处理 Retry-After
                    retry_after_hdr = resp.headers.get('Retry-After')
                    try:
                        retry_after_val = float(retry_after_hdr) if retry_after_hdr is not None else 0.0
                    except Exception:
                        retry_after_val = 0.0
                    if retry_after_val <= 0:
                        break
                    await asyncio.sleep(retry_after_val)
            except Exception as e:
                logger.info(f"Error while checking progress: {str(e)}")
                await asyncio.sleep(60)

        logger.info("%s done simulating, getting alpha details" % (simulation_progress_url))
        try:
            # 首先检查模拟状态
            status = json_data.get("status")
            if status == "ERROR":
                # 模拟失败，记录失败表达式
                message = json_data.get("message", "Unknown error")
                logger.info(f"Simulation failed: {message}")
                
                # 判断是否为真正的表达式错误（不是临时问题）
                message_str = str(message).lower() if message else ""
                is_expression_error = (
                    "end of input" in message_str or
                    "syntax" in message_str or
                    "parse" in message_str or
                    "invalid" in message_str or
                    "undefined" in message_str or
                    "unknown" in message_str or
                    ("error" in message_str and "duplicate" not in message_str)
                )
                
                if is_expression_error:
                    failure_reason = message if message else "Expression error"
                    if "end of input" in message_str:
                        failure_reason = "Unexpected end of input"
                    elif "syntax" in message_str:
                        failure_reason = "Syntax error"
                    elif "parse" in message_str:
                        failure_reason = "Parse error"
                    elif "invalid" in message_str:
                        failure_reason = "Invalid expression"
                    elif "undefined" in message_str or "unknown" in message_str:
                        failure_reason = "Undefined function or field"
                    
                    await _record_failed_expression(
                        alpha_expression=alpha_expression,
                        tag_name=name,
                        failure_reason=failure_reason,
                        error_details=str(json_data)
                    )
                else:
                    logger.info("Skipping record - temporary failure (duplication, rate limit, etc.)")
                
                return 0
            
            alpha_id = json_data.get("alpha")
            if not alpha_id:
                logger.info(f"No alpha_id returned for simulation: {simulation_progress_url}")
                return 0

            # 调用属性设置函数，并传递必要的数据库写入参数
            success = await async_set_alpha_properties(session_manager,
                                             alpha_id,
                                             name="%s" % name,
                                             color=None,
                                             tags=tags,
                                             # 传递数据库写入所需参数
                                             alpha_expression=alpha,
                                             tag_name=name)

            # stone_bag.append(alpha_id)

        except KeyError:
            logger.info("Failed to retrieve alpha ID for: %s" % simulation_progress_url)
        except Exception as e:
            logger.info(f"An error occurred while setting alpha properties: {str(e)}")

        # return stone_bag
        return 0


async def async_set_alpha_properties(
        session,  # aiohttp 的 session
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = None,
        combo_desc: str = None,
        tags: list = None,
        # 新增参数用于数据库写入
        alpha_expression: str = None,
        tag_name: str = None,
):
    """
    异步函数，修改 alpha 的描述参数
    成功后写入数据库
    
    Returns:
        bool: True if successful, False otherwise
    """

    params = {
        "category": None,
        "regular": {"description": None},
        "name": alpha_id  # 使用alpha_id作为name，不使用tag名称
    }
    if color:
        params["color"] = color
    if tags:
        params["tags"] = tags
    if combo_desc:
        params["combo"] = {"description": combo_desc}
    if selection_desc:
        params["selection"] = {"description": selection_desc}

    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"

    try:
        async with session.patch(url, json=params) as response:
            # 检查状态码，确保请求成功
            if response.status == 200:
                logger.info(f"Alpha {alpha_id} properties updated successfully! Tag: {tags}")
                
                # ✅ 属性设置成功后，写入数据库
                if alpha_expression and tag_name:
                    await _write_to_database(alpha_expression, tag_name, alpha_id)
                
                return True
            else:
                logger.info(
                    f"Failed to update alpha {alpha_id}. Status code: {response.status}, Response: {await response.text()}")
                return False

    except aiohttp.ClientError as e:
        logger.info(f"Error during patch request for alpha {alpha_id}: {e}")
        return False
    except Exception as e:
        logger.info(f"An unexpected error occurred for alpha {alpha_id}: {e}")
        return False


async def _write_to_database(alpha_expression: str, tag_name: str, alpha_id: str = None):
    """
    异步写入因子表达式到数据库的辅助函数
    
    Args:
        alpha_expression: 因子表达式
        tag_name: 标签名称，如 "USA_1_EQUITY_TOP3000_fundamental6_step1"
        alpha_id: Alpha ID（用于日志）
    """
    logger.debug(f"🔍 _write_to_database调用参数: alpha_expression='{alpha_expression}', tag_name='{tag_name}', alpha_id='{alpha_id}'")
    try:
        # 导入数据库管理器（需要在函数内导入避免循环导入）
        import sys
        import os
        project_root = os.path.dirname(RECORDS_PATH)
        if project_root not in sys.path:
            sys.path.append(project_root)
        from database.db_manager import FactorDatabaseManager
        from database.partitioned_db_manager import PartitionedFactorManager
        
        # 解析标签信息 - tag_name是新格式tag如"USA_1_EQUITY_TOP3000_fundamental6_step1"
        # 新格式：region_delay_instrumentType_universe_dataset_stepN
        parts = tag_name.split('_')
        
        logger.debug(f"🔍 Tag解析调试: tag_name='{tag_name}', parts={parts}, len={len(parts)}")
        
        if len(parts) >= 6:
            # 新格式tag：USA_1_EQUITY_TOP3000_fundamental6_step1
            region = parts[0].upper()
            delay = parts[1]
            instrument_type = parts[2]
            universe = parts[3]
            dataset_id = parts[4]  # 数据集ID在第5个位置
            step_part = parts[5]   # step1
            
            logger.debug(f"🔍 新格式解析: region={region}, dataset_id={dataset_id}, step_part={step_part}")
            
            try:
                step = int(step_part.replace('step', ''))
            except:
                step = 1
        else:
            # 兼容旧格式tag如"fundamental2_usa_1step"
            region = 'USA'  # 默认
            step = 1      # 默认
            
            # 提取region和step
            for part in parts:
                if part.lower() in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr']:
                    region = part.upper()
                elif 'step' in part:
                    try:
                        step = int(part.replace('step', ''))
                    except:
                        pass
            
            # 构造基础dataset_id（去除region和step部分）
            base_dataset_parts = []
            for part in parts:
                if (part.lower() not in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr'] 
                    and 'step' not in part):
                    base_dataset_parts.append(part)
            
            dataset_id = '_'.join(base_dataset_parts)
        
        # 获取数据库管理器 - 支持分库功能
        db_path_full = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        
        # 检查是否启用分库功能（读取配置）
        try:
            from config import load_digging_config
            config = load_digging_config()
            use_partitioned_db = config.get('use_partitioned_db', True)
            
            if use_partitioned_db:
                db = PartitionedFactorManager(db_path_full)
            else:
                db = FactorDatabaseManager(db_path_full)
        except:
            # 配置读取失败时，默认使用分库功能
            db = PartitionedFactorManager(db_path_full)
        
        # 写入因子表达式到数据库（使用正确解析的dataset_id）
        db.add_factor_expression(
            expression=alpha_expression,
            dataset_id=dataset_id,
            region=region,
            step=step
        )
        
        alpha_preview = alpha_expression[:50] + "..." if len(alpha_expression) > 50 else alpha_expression
        logger.info(f"✅ 数据库写入成功 [{alpha_id}]: tag_name='{tag_name}' -> [dataset_id={dataset_id}, region={region}, step={step}] - {alpha_preview}")
        
    except Exception as e:
        logger.error(f"❌ 数据库写入失败 [{alpha_id}]: {e}")
        # 如果数据库写入失败，回退到文件写入（兼容性保障）
        try:
            import aiofiles
            async with aiofiles.open(os.path.join(RECORDS_PATH, f'{tag_name}_simulated_alpha_expression.txt'), mode='a') as f:
                await f.write(alpha_expression + '\n')
            logger.warning(f"⚠️  已回退到文件写入: {tag_name}_simulated_alpha_expression.txt")
        except Exception as file_error:
            logger.error(f"❌ 文件写入也失败: {file_error}")


async def _record_failed_expression(alpha_expression: str, tag_name: str, 
                                   failure_reason: str = None, error_details: str = None):
    """
    记录失败的因子表达式到数据库（仅记录真正无法模拟的表达式）
    
    Args:
        alpha_expression: 失败的因子表达式
        tag_name: 标签名称，如 "fundamental2_usa_1step"
        failure_reason: 失败原因
        error_details: 详细错误信息
    """
    try:
        # 导入数据库管理器
        import sys
        import os
        project_root = os.path.dirname(RECORDS_PATH)
        if project_root not in sys.path:
            sys.path.append(project_root)
        from database.db_manager import FactorDatabaseManager
        from database.partitioned_db_manager import PartitionedFactorManager
        
        # 解析标签信息 - 与_write_to_database保持一致
        parts = tag_name.split('_')
        
        if len(parts) >= 6:
            # 新格式tag：USA_1_EQUITY_TOP3000_fundamental6_step1
            region = parts[0].upper()
            dataset_id = parts[4]  # 数据集ID在第5个位置
            step_part = parts[5]   # step1
            
            try:
                step = int(step_part.replace('step', ''))
            except:
                step = 1
        else:
            # 兼容旧格式tag如"fundamental2_usa_1step"
            parts_lower = [p.lower() for p in parts]
            region = 'USA'  # 默认
            step = 1      # 默认
            
            # 提取region和step
            for part in parts_lower:
                if part in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr']:
                    region = part.upper()
                elif 'step' in part:
                    try:
                        step = int(part.replace('step', ''))
                    except:
                        pass
            
            # 构造基础dataset_id
            base_dataset_parts = []
            for part in parts_lower:
                if (part not in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr'] 
                    and 'step' not in part):
                    base_dataset_parts.append(part)
            
            dataset_id = '_'.join(base_dataset_parts)
        
        # 获取数据库管理器 - 支持分库功能
        db_path_full = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        
        # 检查是否启用分库功能（读取配置）
        try:
            from config import load_digging_config
            config = load_digging_config()
            use_partitioned_db = config.get('use_partitioned_db', True)
            
            if use_partitioned_db:
                db = PartitionedFactorManager(db_path_full)
            else:
                db = FactorDatabaseManager(db_path_full)
        except:
            # 配置读取失败时，默认使用分库功能
            db = PartitionedFactorManager(db_path_full)
        
        # 写入失败记录（注意：failed_expressions表仍在主数据库中）
        success = db.add_failed_expression(
            expression=alpha_expression,
            dataset_id=dataset_id,
            region=region,
            step=step,
            failure_reason=failure_reason,
            error_details=error_details
        )
        
        if success:
            alpha_preview = alpha_expression[:80] + "..." if len(alpha_expression) > 80 else alpha_expression
            logger.warning(f"📝 🎯 失败记录已保存: {failure_reason} - {alpha_preview}")
        else:
            logger.error(f"❌ 失败记录保存失败: {failure_reason}")
            
    except Exception as e:
        logger.error(f"❌ 记录失败表达式时出错: {e}")
        # 最后兜底：写入临时文件
        try:
            import aiofiles
            from datetime import datetime
            failure_log_path = os.path.join(RECORDS_PATH, 'failed_expressions.log')
            async with aiofiles.open(failure_log_path, mode='a') as f:
                timestamp = datetime.now().isoformat()
                await f.write(f"{timestamp} | {tag_name} | {failure_reason} | {alpha_expression}\n")
        except:
            pass  # 彻底失败也不抛异常

