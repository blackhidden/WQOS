"""
配置管理工具 (Config Utils)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的配置相关功能：
- 用户配置加载
- 挖掘配置加载
- 时区处理
"""

import os
import time
import logging as logger
from datetime import datetime, timedelta

# 导入路径配置
from config import ROOT_PATH


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
    offset_hours = parse_timezone_offset(timezone_str)
    utc_now = datetime.utcnow()
    local_now = utc_now + timedelta(hours=offset_hours)
    
    return local_now.strftime('%Y-%m-%d')
