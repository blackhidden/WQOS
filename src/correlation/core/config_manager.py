"""
相关性检查配置管理器
"""

import os
from pathlib import Path
from lib.config_utils import load_digging_config
from config import RECORDS_PATH


class CorrelationConfigManager:
    """相关性检查配置管理器"""
    
    def __init__(self):
        """初始化配置管理器"""
        # 加载配置
        self.config = load_digging_config()
        
        # 相关性检查配置
        self.correlation_threshold = float(self.config.get('correlation_threshold', 0.7))
        self.ppac_threshold = float(self.config.get('ppac_threshold', 0.5))
        self.time_window_years = int(self.config.get('correlation_time_window', 4))
        self.batch_size = int(self.config.get('correlation_batch_size', 20))
        
        # API配置
        self.api_delay = float(self.config.get('api_retry_delay', 1))
        self.max_retries = int(self.config.get('api_max_retries', 3))
        
        # 数据存储路径
        self.data_path = Path(RECORDS_PATH) / 'correlation_data'
        self.data_path.mkdir(exist_ok=True)
        
        # 缓存文件路径
        self.pnl_cache_file = self.data_path / 'pnl_cache.pickle'
        
        # 数据库路径
        self.db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        
        # 相关性阈值别名 (用于兼容性)
        self.corr_threshold = self.correlation_threshold
    
    def log_config_summary(self, logger):
        """记录配置摘要"""
        logger.info(f"✅ 独立相关性检查器配置:")
        logger.info(f"  📊 SelfCorr阈值: {self.correlation_threshold}")
        logger.info(f"  🔵 PPAC阈值: {self.ppac_threshold}")
        logger.info(f"  ⏰ 时间窗口: {self.time_window_years}年")
        logger.info(f"  📦 批次大小: {self.batch_size}")
        logger.info(f"  📁 数据路径: {self.data_path}")
