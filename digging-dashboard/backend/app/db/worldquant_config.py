"""
WorldQuant配置选项数据库模型
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.sql import func
import json
from datetime import datetime

from app.db.database import Base


class WorldQuantConfig(Base):
    """WorldQuant配置选项表"""
    __tablename__ = "worldquant_config"
    
    id = Column(Integer, primary_key=True, index=True)
    config_type = Column(String(50), nullable=False, index=True)  # 配置类型：instrument_types, regions, universes, etc.
    config_key = Column(String(100), nullable=True)  # 配置键（如EQUITY, USA等）
    config_data = Column(Text, nullable=False)  # JSON格式的配置数据
    version = Column(String(20), default="1.0")  # 版本号
    is_active = Column(Boolean, default=True)  # 是否激活
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    synced_at = Column(DateTime, nullable=True)  # 最后同步时间
    
    def __repr__(self):
        return f"<WorldQuantConfig(type='{self.config_type}', key='{self.config_key}')>"
    
    def get_data(self):
        """获取解析后的配置数据"""
        try:
            return json.loads(self.config_data)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    def set_data(self, data):
        """设置配置数据"""
        self.config_data = json.dumps(data, ensure_ascii=False, indent=2)
        self.updated_at = datetime.utcnow()


class WorldQuantSyncLog(Base):
    """WorldQuant配置同步日志表"""
    __tablename__ = "worldquant_sync_log"
    
    id = Column(Integer, primary_key=True, index=True)
    sync_time = Column(DateTime, default=datetime.utcnow, nullable=False)
    success = Column(Boolean, nullable=False)
    message = Column(Text, nullable=True)  # 同步消息或错误信息
    total_configs = Column(Integer, default=0)  # 同步的配置项数量
    duration_seconds = Column(Integer, default=0)  # 同步耗时（秒）
    
    def __repr__(self):
        return f"<WorldQuantSyncLog(time='{self.sync_time}', success={self.success})>"
