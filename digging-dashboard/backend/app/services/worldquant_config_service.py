"""
WorldQuant配置管理服务
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session

from app.db.database import SessionLocal
from app.db.worldquant_config import WorldQuantConfig, WorldQuantSyncLog
from app.services.worldquant_service import WorldQuantService

logger = logging.getLogger(__name__)


class WorldQuantConfigService:
    """WorldQuant配置数据库管理服务"""
    
    @staticmethod
    def sync_from_api() -> Dict[str, Any]:
        """从WorldQuant API同步配置到数据库"""
        start_time = time.time()
        sync_success = False
        sync_message = ""
        total_configs = 0
        
        db = SessionLocal()
        try:
            logger.info("开始同步WorldQuant配置...")
            
            # 获取API配置数据
            api_options = WorldQuantService.get_simulation_options()
            
            # 清除旧的配置数据
            db.query(WorldQuantConfig).filter(WorldQuantConfig.is_active == True).update(
                {WorldQuantConfig.is_active: False}
            )
            
            current_time = datetime.utcnow()
            
            # 保存配置数据
            configs_to_save = []
            
            # 1. 保存instrument_types
            if 'instrument_types' in api_options:
                config = WorldQuantConfig(
                    config_type='instrument_types',
                    config_key=None,
                    config_data='',
                    is_active=True,
                    synced_at=current_time
                )
                config.set_data(api_options['instrument_types'])
                configs_to_save.append(config)
            
            # 2. 保存regions
            if 'regions' in api_options:
                for instrument_type, regions in api_options['regions'].items():
                    config = WorldQuantConfig(
                        config_type='regions',
                        config_key=instrument_type,
                        config_data='',
                        is_active=True,
                        synced_at=current_time
                    )
                    config.set_data(regions)
                    configs_to_save.append(config)
            
            # 3. 保存universes
            if 'universes' in api_options:
                universes_data = api_options['universes']
                if 'instrumentType' in universes_data:
                    for instrument_type, instrument_data in universes_data['instrumentType'].items():
                        if 'region' in instrument_data:
                            for region, universe_list in instrument_data['region'].items():
                                config = WorldQuantConfig(
                                    config_type='universes',
                                    config_key=f"{instrument_type}_{region}",
                                    config_data='',
                                    is_active=True,
                                    synced_at=current_time
                                )
                                config.set_data(universe_list)
                                configs_to_save.append(config)
            
            # 4. 保存delays
            if 'delays' in api_options:
                delays_data = api_options['delays']
                if 'instrumentType' in delays_data:
                    for instrument_type, instrument_data in delays_data['instrumentType'].items():
                        if 'region' in instrument_data:
                            for region, delay_list in instrument_data['region'].items():
                                config = WorldQuantConfig(
                                    config_type='delays',
                                    config_key=f"{instrument_type}_{region}",
                                    config_data='',
                                    is_active=True,
                                    synced_at=current_time
                                )
                                config.set_data(delay_list)
                                configs_to_save.append(config)
            
            # 5. 保存neutralizations
            if 'neutralizations' in api_options:
                neut_data = api_options['neutralizations']
                if 'instrumentType' in neut_data:
                    for instrument_type, instrument_data in neut_data['instrumentType'].items():
                        if 'region' in instrument_data:
                            for region, neut_list in instrument_data['region'].items():
                                config = WorldQuantConfig(
                                    config_type='neutralizations',
                                    config_key=f"{instrument_type}_{region}",
                                    config_data='',
                                    is_active=True,
                                    synced_at=current_time
                                )
                                config.set_data(neut_list)
                                configs_to_save.append(config)
            
            # 6. 保存其他简单配置
            simple_configs = [
                'lookback_days', 'pasteurization', 'unit_handling', 
                'nan_handling', 'selection_handling', 'max_trade', 'language'
            ]
            
            for config_type in simple_configs:
                if config_type in api_options:
                    config = WorldQuantConfig(
                        config_type=config_type,
                        config_key=None,
                        config_data='',
                        is_active=True,
                        synced_at=current_time
                    )
                    config.set_data(api_options[config_type])
                    configs_to_save.append(config)
            
            # 7. 保存范围配置
            range_configs = ['decay_range', 'truncation_range', 'selection_limit_range']
            for config_type in range_configs:
                if config_type in api_options:
                    config = WorldQuantConfig(
                        config_type=config_type,
                        config_key=None,
                        config_data='',
                        is_active=True,
                        synced_at=current_time
                    )
                    config.set_data(api_options[config_type])
                    configs_to_save.append(config)
            
            # 批量保存到数据库
            db.add_all(configs_to_save)
            db.commit()
            
            total_configs = len(configs_to_save)
            sync_success = True
            sync_message = f"成功同步 {total_configs} 个配置项"
            
            logger.info(f"WorldQuant配置同步完成: {sync_message}")
            
        except Exception as e:
            db.rollback()
            sync_message = f"同步失败: {str(e)}"
            logger.error(f"WorldQuant配置同步失败: {e}")
            
        finally:
            # 记录同步日志
            duration = int(time.time() - start_time)
            sync_log = WorldQuantSyncLog(
                success=sync_success,
                message=sync_message,
                total_configs=total_configs,
                duration_seconds=duration
            )
            db.add(sync_log)
            
            try:
                db.commit()
            except Exception as e:
                logger.error(f"保存同步日志失败: {e}")
                db.rollback()
            finally:
                db.close()
        
        return {
            "success": sync_success,
            "message": sync_message,
            "total_configs": total_configs,
            "duration_seconds": duration
        }
    
    @staticmethod
    def get_config_from_db() -> Dict[str, Any]:
        """从数据库获取配置选项"""
        db = SessionLocal()
        try:
            configs = db.query(WorldQuantConfig).filter(
                WorldQuantConfig.is_active == True
            ).all()
            
            result = {}
            
            for config in configs:
                config_data = config.get_data()
                
                if config.config_type in ['instrument_types', 'lookback_days', 'pasteurization', 
                                        'unit_handling', 'nan_handling', 'selection_handling', 
                                        'max_trade', 'language']:
                    # 简单列表配置
                    result[config.config_type] = config_data
                    
                elif config.config_type in ['decay_range', 'truncation_range', 'selection_limit_range']:
                    # 范围配置
                    result[config.config_type] = config_data
                    
                elif config.config_type == 'regions':
                    # 地区配置
                    if 'regions' not in result:
                        result['regions'] = {}
                    result['regions'][config.config_key] = config_data
                    
                elif config.config_type in ['universes', 'delays', 'neutralizations']:
                    # 嵌套配置
                    if config.config_type not in result:
                        result[config.config_type] = {'instrumentType': {}}
                    
                    # 解析config_key (格式: EQUITY_USA)
                    if config.config_key and '_' in config.config_key:
                        instrument_type, region = config.config_key.split('_', 1)
                        
                        if instrument_type not in result[config.config_type]['instrumentType']:
                            result[config.config_type]['instrumentType'][instrument_type] = {'region': {}}
                        
                        result[config.config_type]['instrumentType'][instrument_type]['region'][region] = config_data
            
            return result
            
        except Exception as e:
            logger.error(f"从数据库获取配置失败: {e}")
            return {}
        finally:
            db.close()
    
    @staticmethod
    def get_sync_history(limit: int = 10) -> List[Dict[str, Any]]:
        """获取同步历史记录"""
        db = SessionLocal()
        try:
            logs = db.query(WorldQuantSyncLog).order_by(
                WorldQuantSyncLog.sync_time.desc()
            ).limit(limit).all()
            
            return [
                {
                    "id": log.id,
                    "sync_time": log.sync_time.isoformat(),
                    "success": log.success,
                    "message": log.message,
                    "total_configs": log.total_configs,
                    "duration_seconds": log.duration_seconds
                }
                for log in logs
            ]
            
        except Exception as e:
            logger.error(f"获取同步历史失败: {e}")
            return []
        finally:
            db.close()
    
    @staticmethod
    def get_last_sync_time() -> Optional[datetime]:
        """获取最后同步时间"""
        db = SessionLocal()
        try:
            last_log = db.query(WorldQuantSyncLog).filter(
                WorldQuantSyncLog.success == True
            ).order_by(WorldQuantSyncLog.sync_time.desc()).first()
            
            return last_log.sync_time if last_log else None
            
        except Exception as e:
            logger.error(f"获取最后同步时间失败: {e}")
            return None
        finally:
            db.close()
    
    @staticmethod
    def is_config_available() -> bool:
        """检查数据库中是否有可用的配置"""
        db = SessionLocal()
        try:
            count = db.query(WorldQuantConfig).filter(
                WorldQuantConfig.is_active == True
            ).count()
            return count > 0
        except Exception as e:
            logger.error(f"检查配置可用性失败: {e}")
            return False
        finally:
            db.close()
