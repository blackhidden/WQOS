"""
WorldQuant Brain API 服务
"""

import sys
import os
import logging
from typing import Dict, List, Any, Optional

# 添加项目根目录到Python路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 添加src目录到路径
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

try:
    # 首先确保当前目录是项目根目录
    original_cwd = os.getcwd()
    os.chdir(project_root)
    
    from session_manager import get_session
    
    # 恢复原始工作目录
    os.chdir(original_cwd)
except ImportError as e:
    logging.error(f"Failed to import session_manager: {e}")
    # 提供一个fallback实现
    import requests
    def get_session():
        """Fallback session implementation"""
        session = requests.Session()
        # 这里应该添加基本的认证逻辑
        return session

logger = logging.getLogger(__name__)


class WorldQuantService:
    """WorldQuant Brain API 服务类"""
    
    @staticmethod
    def get_simulation_options(force_api: bool = False) -> Dict[str, Any]:
        """获取仿真配置选项
        
        Args:
            force_api: 是否强制从API获取，默认优先从数据库读取
            
        Returns:
            解析后的配置选项字典
        """
        # 优先从数据库读取，除非强制使用API
        if not force_api:
            try:
                from app.services.worldquant_config_service import WorldQuantConfigService
                
                # 检查数据库中是否有配置
                if WorldQuantConfigService.is_config_available():
                    logger.info("从数据库获取WorldQuant配置")
                    return WorldQuantConfigService.get_config_from_db()
                else:
                    logger.info("数据库中无配置，从API获取")
            except ImportError:
                logger.warning("WorldQuantConfigService不可用，从API获取")
        
        try:
            # 使用统一会话管理器获取会话
            session = get_session()
            
            # 调用WorldQuant Brain API (使用OPTIONS获取配置信息)
            response = session.options('https://api.worldquantbrain.com/simulations', timeout=30)
            
            if response.status_code != 200:
                raise Exception(f"API请求失败: HTTP {response.status_code} - {response.text}")
            
            data = response.json()
            
            # 解析配置选项
            return WorldQuantService._parse_simulation_options(data)
            
        except Exception as e:
            logger.error(f"获取仿真配置选项失败: {e}")
            raise
    
    @staticmethod
    def _parse_simulation_options(data: Dict[str, Any]) -> Dict[str, Any]:
        """解析WorldQuant API响应数据
        
        Args:
            data: API响应的原始数据
            
        Returns:
            解析后的配置选项
        """
        try:
            # 提取settings部分
            settings = data.get('actions', {}).get('POST', {}).get('settings', {}).get('children', {})
            
            if not settings:
                raise Exception("无法从API响应中找到settings配置")
            
            # 解析各个配置项
            parsed_options = {
                'instrument_types': WorldQuantService._extract_simple_choices(
                    settings.get('instrumentType', {})
                ),
                'regions': WorldQuantService._extract_conditional_choices(
                    settings.get('region', {}), 
                    base_key='instrumentType'
                ),
                'universes': WorldQuantService._extract_nested_choices(
                    settings.get('universe', {}),
                    keys=['instrumentType', 'region']
                ),
                'delays': WorldQuantService._extract_nested_choices(
                    settings.get('delay', {}),
                    keys=['instrumentType', 'region']
                ),
                'neutralizations': WorldQuantService._extract_nested_choices(
                    settings.get('neutralization', {}),
                    keys=['instrumentType', 'region']
                ),
                'decay_range': WorldQuantService._extract_integer_range(
                    settings.get('decay', {})
                ),
                'truncation_range': WorldQuantService._extract_float_range(
                    settings.get('truncation', {})
                ),
                'lookback_days': WorldQuantService._extract_simple_choices(
                    settings.get('lookbackDays', {})
                ),
                'pasteurization': WorldQuantService._extract_simple_choices(
                    settings.get('pasteurization', {})
                ),
                'unit_handling': WorldQuantService._extract_simple_choices(
                    settings.get('unitHandling', {})
                ),
                'nan_handling': WorldQuantService._extract_simple_choices(
                    settings.get('nanHandling', {})
                ),
                'selection_handling': WorldQuantService._extract_simple_choices(
                    settings.get('selectionHandling', {})
                ),
                'selection_limit_range': WorldQuantService._extract_integer_range(
                    settings.get('selectionLimit', {})
                ),
                'max_trade': WorldQuantService._extract_simple_choices(
                    settings.get('maxTrade', {})
                ),
                'language': WorldQuantService._extract_simple_choices(
                    settings.get('language', {})
                )
            }
            
            logger.info("配置选项解析成功")
            return parsed_options
            
        except Exception as e:
            logger.error(f"解析配置选项失败: {e}")
            raise
    
    @staticmethod
    def _extract_simple_choices(config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取简单的选择项列表"""
        choices = config.get('choices', [])
        if isinstance(choices, list):
            return choices
        return []
    
    @staticmethod
    def _extract_conditional_choices(config: Dict[str, Any], base_key: str) -> Dict[str, List[Dict[str, Any]]]:
        """提取条件选择项（基于单个条件）"""
        choices = config.get('choices', {})
        if isinstance(choices, dict) and base_key in choices:
            return choices[base_key]
        return {}
    
    @staticmethod
    def _extract_nested_choices(config: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """提取嵌套的选择项"""
        choices = config.get('choices', {})
        
        # 如果choices本身就是嵌套结构，返回整个结构让调用者处理
        if isinstance(choices, dict) and keys:
            # 检查是否包含第一个key
            if keys[0] in choices:
                return choices
        
        return {}
    
    @staticmethod
    def _extract_integer_range(config: Dict[str, Any]) -> Dict[str, int]:
        """提取整数范围"""
        return {
            'min': config.get('minValue', 0),
            'max': config.get('maxValue', 100)
        }
    
    @staticmethod
    def _extract_float_range(config: Dict[str, Any]) -> Dict[str, float]:
        """提取浮点数范围"""
        return {
            'min': config.get('minValue', 0.0),
            'max': config.get('maxValue', 1.0)
        }
    
    @staticmethod
    def get_regions_for_instrument(instrument_type: str) -> List[Dict[str, Any]]:
        """根据工具类型获取可用的地区列表"""
        try:
            options = WorldQuantService.get_simulation_options()
            regions = options.get('regions', {})
            return regions.get(instrument_type, [])
        except Exception as e:
            logger.error(f"获取地区列表失败: {e}")
            return []
    
    @staticmethod
    def get_universes_for_region(instrument_type: str, region: str) -> List[Dict[str, Any]]:
        """根据工具类型和地区获取可用的股票池列表"""
        try:
            options = WorldQuantService.get_simulation_options()
            universes = options.get('universes', {})
            
            # 遍历嵌套结构: instrumentType -> EQUITY -> region -> USA -> [choices]
            if (universes and 
                'instrumentType' in universes and
                instrument_type in universes['instrumentType'] and
                'region' in universes['instrumentType'][instrument_type] and
                region in universes['instrumentType'][instrument_type]['region']):
                return universes['instrumentType'][instrument_type]['region'][region]
            
            return []
        except Exception as e:
            logger.error(f"获取股票池列表失败: {e}")
            return []
    
    @staticmethod
    def get_delays_for_region(instrument_type: str, region: str) -> List[Dict[str, Any]]:
        """根据工具类型和地区获取可用的延迟选项"""
        try:
            options = WorldQuantService.get_simulation_options()
            delays = options.get('delays', {})
            
            # 遍历嵌套结构: instrumentType -> EQUITY -> region -> USA -> [choices]
            if (delays and 
                'instrumentType' in delays and
                instrument_type in delays['instrumentType'] and
                'region' in delays['instrumentType'][instrument_type] and
                region in delays['instrumentType'][instrument_type]['region']):
                return delays['instrumentType'][instrument_type]['region'][region]
            
            return []
        except Exception as e:
            logger.error(f"获取延迟选项失败: {e}")
            return []
    
    @staticmethod
    def get_neutralizations_for_region(instrument_type: str, region: str) -> List[Dict[str, Any]]:
        """根据工具类型和地区获取可用的中性化选项"""
        try:
            options = WorldQuantService.get_simulation_options()
            neutralizations = options.get('neutralizations', {})
            
            # 遍历嵌套结构: instrumentType -> EQUITY -> region -> USA -> [choices]
            if (neutralizations and 
                'instrumentType' in neutralizations and
                instrument_type in neutralizations['instrumentType'] and
                'region' in neutralizations['instrumentType'][instrument_type] and
                region in neutralizations['instrumentType'][instrument_type]['region']):
                return neutralizations['instrumentType'][instrument_type]['region'][region]
            
            return []
        except Exception as e:
            logger.error(f"获取中性化选项失败: {e}")
            return []
