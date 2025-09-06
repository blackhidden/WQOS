#!/usr/bin/env python3
"""
作者：e.e.
微信：Enkidu_lin
日期：2025.08.24
Alpha记录管理模块 (数据库版本)
提供读取、写入和查询Alpha记录的功能

版本2.0: 从文件存储迁移到SQLite数据库存储
保持完全向后兼容的API接口
"""

import os
import sys
import time
import logging
from typing import Set, List, Dict, Tuple, Optional
from datetime import datetime
from contextlib import contextmanager

# 调整导入路径
if __name__ == "__main__":
    # 当作为主程序运行时，添加项目根目录到路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import RECORDS_PATH
else:
    # 当作为模块导入时
    try:
        from config import RECORDS_PATH  # 先尝试相对导入(在src内导入)
    except ImportError:
        from src.config import RECORDS_PATH  # 如果失败，从项目根目录导入

# 导入数据库管理器
try:
    from database.db_manager import FactorDatabaseManager
except ImportError:
    # 如果直接运行，尝试从项目根目录导入
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from database.db_manager import FactorDatabaseManager

# 配置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('alpha_record_manager')

# 全局数据库管理器实例
_db_manager = None

def get_db_manager():
    """获取数据库管理器单例"""
    global _db_manager
    if _db_manager is None:
        # 数据库文件位置
        db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        _db_manager = FactorDatabaseManager(db_path)
    return _db_manager

def _parse_tag_parts(tag: str, record_type: str = "checked") -> Tuple[str, str, str]:
    """
    解析标签信息为数据库字段
    例: analyst4_usa_1step + checked -> dataset_id=analyst4_usa_1step, region=USA, step=1
    """
    parts = tag.lower().split('_')
    
    # 提取region
    region = 'USA'  # 默认
    for part in parts:
        if part.lower() in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr']:
            region = part.upper()
            break
    
    # 提取step
    step = None
    for part in parts:
        if 'step' in part:
            try:
                step = int(part.replace('step', ''))
            except:
                pass
            break
    
    # 构造dataset_id (去除region和step部分，与其他地方保持一致)
    dataset_parts = []
    for part in parts:
        if (part.lower() not in ['usa', 'chn', 'eur', 'asi', 'hkg', 'twn', 'kor', 'jpn', 'glb', 'amr'] 
            and 'step' not in part):
            dataset_parts.append(part)
    
    dataset_id = '_'.join(dataset_parts)
    
    return dataset_id, region, step

# ============================================================================
# 数据库版本的API实现 - 保持向后兼容
# ============================================================================

def add_alpha_record(tag: str, alpha_id: str, record_type: str = "checked") -> bool:
    """
    添加单个Alpha ID到记录
    
    Args:
        tag: Alpha标签
        alpha_id: Alpha ID
        record_type: 记录类型 (checked, submitted, failed等)
        
    Returns:
        bool: 是否添加成功
    """
    try:
        db = get_db_manager()
        dataset_id, region, step = _parse_tag_parts(tag, record_type)
        
        if record_type == "checked":
            if step is None:
                step = 1  # 默认步数
            
            # 检查是否已存在
            if db.is_alpha_checked(alpha_id, dataset_id, region, step):
                logger.debug(f"Alpha {alpha_id} 已存在于记录中")
                return True
            
            # 添加到checked_alphas表
            result = db.add_checked_alpha(alpha_id, dataset_id, region, step)
            logger.info(f"添加Alpha记录: {alpha_id} -> {tag}.{record_type}")
            return result
        else:
            logger.warning(f"记录类型 '{record_type}' 暂未完全迁移到数据库")
            return False
            
    except Exception as e:
        logger.error(f"添加Alpha记录失败: {e}")
        return False

def is_alpha_in_records(alpha_id: str, tag: str, record_type: str = "checked") -> bool:
    """
    检查Alpha是否在记录中
    
    Args:
        alpha_id: Alpha ID
        tag: Alpha标签
        record_type: 记录类型 (checked, submitted, failed等)
        
    Returns:
        bool: 是否在记录中
    """
    try:
        db = get_db_manager()
        dataset_id, region, step = _parse_tag_parts(tag, record_type)
        
        if record_type == "checked":
            if step is None:
                step = 1  # 默认步数
            return db.is_alpha_checked(alpha_id, dataset_id, region, step)
        else:
            logger.warning(f"记录类型 '{record_type}' 暂未完全迁移到数据库")
            return False
            
    except Exception as e:
        logger.error(f"检查Alpha记录失败: {e}")
        return False
