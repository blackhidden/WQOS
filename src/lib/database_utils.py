"""
数据库操作工具 (Database Utils)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的数据库相关功能：
- 数据库写入
- 失败表达式记录
"""

import os
import logging as logger
from datetime import datetime

from config import RECORDS_PATH


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
            from .config_utils import load_digging_config
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
        logger.debug(f"✅ 数据库写入成功 [{alpha_id}]: tag_name='{tag_name}' -> [dataset_id={dataset_id}, region={region}, step={step}] - {alpha_preview}")
        
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
            from .config_utils import load_digging_config
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
            failure_log_path = os.path.join(RECORDS_PATH, 'failed_expressions.log')
            async with aiofiles.open(failure_log_path, mode='a') as f:
                timestamp = datetime.now().isoformat()
                await f.write(f"{timestamp} | {tag_name} | {failure_reason} | {alpha_expression}\n")
        except:
              pass  # 彻底失败也不抛异常
