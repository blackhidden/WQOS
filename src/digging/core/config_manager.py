"""
配置管理器 (Config Manager)
作者：e.e.
日期：2025.09.05

负责管理因子挖掘的配置参数，包括：
- 配置文件加载
- 参数验证
- 默认值设置
- Tag配置管理
"""

import os
import sys
from typing import Optional

# 导入现有模块
try:
    from lib.config_utils import load_digging_config
    from utils.tag_generator import TagConfig
    from config import RECORDS_PATH
    from database.db_manager import FactorDatabaseManager
    from database.partitioned_db_manager import PartitionedFactorManager
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from lib.config_utils import load_digging_config
    from utils.tag_generator import TagConfig
    from config import RECORDS_PATH
    from database.db_manager import FactorDatabaseManager
    from database.partitioned_db_manager import PartitionedFactorManager


class ConfigManager:
    """配置管理器 - 统一管理因子挖掘的所有配置"""
    
    def __init__(self, config_file: Optional[str] = None):
        """初始化配置管理器
        
        Args:
            config_file: 可选的配置文件路径
        """
        # 加载配置（支持自定义配置文件路径）
        self.config = load_digging_config(config_file)
        
        # 基础参数
        self.region = self.config.get('region', 'USA')
        self.universe = self.config.get('universe', 'TOP3000')
        self.delay = self.config.get('delay', 1)
        self.decay = self.config.get('decay', 6)
        self.neutralization = self.config.get('neutralization', 'SUBINDUSTRY')
        
        # 新增配置参数
        self.instrument_type = self.config.get('instrument_type', 'EQUITY')
        self.max_trade = self.config.get('max_trade', 'OFF')
        self.recommended_name = self.config.get('recommended_name', 'custom_fields')
        self.recommended_fields = self.config.get('recommended_fields', '')
        
        # 推荐字段配置
        self.use_recommended_fields = self.config.get('use_recommended_fields', True)
        
        # 多模拟配置选项（默认关闭，通过参数启用）
        self.enable_multi_simulation = False
        self.multi_children_limit = 10  # 固定为10，吃满API上限
        self.multi_batch_limit = 8      # 默认值，将被n_jobs覆盖
        
        # 验证推荐字段配置
        if self.use_recommended_fields:
            if not self.recommended_name:
                raise ValueError("启用推荐字段模式时，recommended_name不能为空，请检查配置文件")
            if not self.recommended_fields:
                raise ValueError("启用推荐字段模式时，recommended_fields不能为空，请检查配置文件")
        
        # 确定当前使用的数据集
        if self.use_recommended_fields:
            self.current_dataset = self.recommended_name
        else:
            # 优先使用 dataset_id，如果没有则使用 priority_dataset，最后使用默认值
            self.current_dataset = (self.config.get('dataset_id') or 
                                  self.config.get('priority_dataset') or 
                                  'fundamental6')
        
        # 初始化tag配置
        self.tag_config = TagConfig(
            region=self.region,
            delay=self.delay,
            instrument_type=self.instrument_type,
            universe=self.universe,
            use_recommended_fields=self.use_recommended_fields,
            recommended_name=self.recommended_name if self.use_recommended_fields else None,
            dataset_id=None  # 将在运行时设置
        )
        
        # 数据库配置
        self.db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        self.use_partitioned_db = self.config.get('use_partitioned_db', True)
        
        # 通知相关配置
        self.notification_thresholds = [95.0]  # 完成度阈值，只在>95%时触发一次通知
        
        # 运行时参数
        self._n_jobs = None  # 运行时设置的并发数
    
    def get_database_manager(self):
        """获取数据库管理器实例"""
        if self.use_partitioned_db:
            return PartitionedFactorManager(self.db_path)
        else:
            return FactorDatabaseManager(self.db_path)
    
    def get_stage_config(self, stage: Optional[int] = None) -> int:
        """获取执行阶段配置"""
        return stage if stage is not None else self.config.get('stage', 1)
    
    def get_n_jobs_config(self, n_jobs: Optional[int] = None) -> int:
        """获取并发数配置"""
        if n_jobs is not None:
            # 如果传入了n_jobs，保存它
            self._n_jobs = n_jobs
            return n_jobs
        elif self._n_jobs is not None:
            # 如果之前保存过n_jobs，使用保存的值
            return self._n_jobs
        else:
            # 否则使用配置文件中的值
            return self.config.get('n_jobs', 5)
    
    def set_n_jobs(self, n_jobs: int):
        """设置并发数"""
        self._n_jobs = n_jobs
    
    def generate_tag(self, dataset_id: str, step: int) -> str:
        """生成新格式的tag名称"""
        from utils.tag_generator import TagGenerator
        
        # 更新tag配置中的dataset_id
        tag_config = TagConfig(
            region=self.tag_config.region,
            delay=self.tag_config.delay,
            instrument_type=self.tag_config.instrument_type,
            universe=self.tag_config.universe,
            use_recommended_fields=self.tag_config.use_recommended_fields,
            recommended_name=self.tag_config.recommended_name,
            dataset_id=dataset_id if not self.use_recommended_fields else None
        )
        
        return TagGenerator.generate_tag(tag_config, step)
    
    def get_recommended_fields(self):
        """获取解析后的推荐字段列表"""
        if not self.use_recommended_fields:
            return None
            
        try:
            if isinstance(self.recommended_fields, str):
                # 如果是JSON字符串，解析它
                import json
                pc_fields = json.loads(self.recommended_fields)
            elif isinstance(self.recommended_fields, list):
                # 如果已经是列表，直接使用
                pc_fields = self.recommended_fields
            else:
                # 如果配置为空或无效，抛出异常
                raise ValueError(f"推荐字段配置无效: {self.recommended_fields}，请检查配置文件中的recommended_fields设置")
            
            if not pc_fields:
                raise ValueError("推荐字段列表为空，请检查配置文件中的recommended_fields设置")
            
            return pc_fields
            
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"推荐字段解析失败: {e}，请检查配置文件中的recommended_fields格式是否正确")
    
    def set_multi_simulation_config(self, enable_multi_simulation: bool = False,
                                   multi_children_limit: int = 10,
                                   multi_batch_limit: int = 8):
        """动态设置多模拟配置
        
        Args:
            enable_multi_simulation: 是否启用多模拟模式
            multi_children_limit: 每个多模拟包含的子模拟数量 (2-10)
            multi_batch_limit: 同时执行的多模拟数量 (1-20)
        """
        # 验证参数
        if enable_multi_simulation:
            if not (2 <= multi_children_limit <= 10):
                raise ValueError("多模拟模式下，每个多模拟包含的子模拟数量必须在2-10之间")
            if not (1 <= multi_batch_limit <= 20):
                raise ValueError("多模拟模式下，同时执行的多模拟数量必须在1-20之间")
        
        # 设置配置
        self.enable_multi_simulation = enable_multi_simulation
        self.multi_children_limit = multi_children_limit
        self.multi_batch_limit = multi_batch_limit
    
    def log_config_summary(self, logger):
        """记录配置摘要到日志"""
        logger.info(f"🚀 因子挖掘配置摘要:")
        logger.info(f"  🌍 地区: {self.region}")
        logger.info(f"  🎛️ universe: {self.universe}")
        logger.info(f"  ⏱️ 延迟: {self.delay}")
        logger.info(f"  📉 衰减: {self.decay}")
        logger.info(f"  🏷️ 使用推荐字段: {self.use_recommended_fields}")
        if self.use_recommended_fields:
            logger.info(f"  📝 推荐名称: {self.recommended_name}")
        logger.info(f"  🔧 工具类型: {self.instrument_type}")
        logger.info(f"  📊 最大交易: {self.max_trade}")
        logger.info(f"  🗄️ 使用分库: {self.use_partitioned_db}")
        logger.info(f"  📊 当前数据集: {self.current_dataset}")
        logger.info(f"  📱 通知阈值: {self.notification_thresholds}%")
        
        # 多模拟配置摘要
        if self.enable_multi_simulation:
            logger.info(f"  🔥 多模拟模式: 启用")
            logger.info(f"  📊 子模拟数量: {self.multi_children_limit}")
            logger.info(f"  🏊‍♂️ 并发多模拟: {self.multi_batch_limit}")
            logger.info(f"  ⚡ 理论并发度: {self.multi_children_limit * self.multi_batch_limit} (vs 单模拟)")
        else:
            logger.info(f"  🚀 模拟模式: 单模拟")
