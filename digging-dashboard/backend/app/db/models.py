"""
SQLAlchemy数据库模型
"""

import sqlalchemy as sa
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, func
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import json

from app.db.database import Base


class DashboardUser(Base):
    """用户表（唯一用户）"""
    __tablename__ = "dashboard_user"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    email = Column(String(100))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_login = Column(DateTime(timezone=True))
    is_active = Column(Boolean, default=True)
    
    # 关联关系
    config_templates = relationship("DiggingConfigTemplate", back_populates="created_by_user")
    audit_logs = relationship("AuditLog", back_populates="user")
    processes = relationship("DiggingProcess", back_populates="started_by_user")


class DiggingConfigTemplate(Base):
    """配置模板表"""
    __tablename__ = "digging_config_templates"
    
    id = Column(Integer, primary_key=True, index=True)
    template_name = Column(String(100), nullable=False, index=True)
    description = Column(Text)
    
    # 模式选择
    use_recommended_fields = Column(Boolean, nullable=False)
    
    # 基础配置
    region = Column(String(20), nullable=False)
    universe = Column(String(50), nullable=False)
    delay = Column(Integer, nullable=False)
    decay = Column(Integer, default=6)
    neutralization = Column(String(50), default="SUBINDUSTRY")
    instrument_type = Column(String(50), default="EQUITY")
    max_trade = Column(String(20), default="OFF")

    
    # 数据集模式配置
    dataset_id = Column(String(100))
    
    # 推荐字段模式配置
    recommended_name = Column(String(100))
    recommended_fields = Column(Text)  # JSON格式的字段列表
    
    # 元数据
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by = Column(Integer, ForeignKey("dashboard_user.id"))
    
    # 关联关系
    created_by_user = relationship("DashboardUser", back_populates="config_templates")
    processes = relationship("DiggingProcess", back_populates="config_template")
    usage_history = relationship("ConfigUsageHistory", back_populates="config_template")
    
    @property
    def tag_name(self):
        """生成tag名称"""
        parts = [
            self.region.upper(),
            str(self.delay),
            self.instrument_type.upper(),
            self.universe.upper()
        ]
        
        if self.use_recommended_fields:
            parts.append(self.recommended_name or "unknown")
        else:
            parts.append(self.dataset_id or "unknown")
        
        parts.append("step")
        return "_".join(parts)
    
    @property
    def recommended_fields_list(self):
        """获取推荐字段列表"""
        if self.recommended_fields:
            try:
                return json.loads(self.recommended_fields)
            except json.JSONDecodeError:
                return []
        return []


class DiggingProcess(Base):
    """挖掘进程表"""
    __tablename__ = "digging_processes"
    
    id = Column(Integer, primary_key=True, index=True)
    config_template_id = Column(Integer, ForeignKey("digging_config_templates.id"), nullable=True)  # 对于独立脚本可为空
    
    # 进程信息
    process_id = Column(Integer)
    status = Column(String(20), nullable=False, default="stopped", index=True)
    script_type = Column(String(50), nullable=False, default="unified_digging")  # 脚本类型
    tag_name = Column(String(200), nullable=False)
    
    # 时间信息
    started_at = Column(DateTime(timezone=True))
    stopped_at = Column(DateTime(timezone=True))
    
    # 日志和输出
    log_file_path = Column(String(500))
    error_message = Column(Text)
    
    # 进程统计
    total_expressions = Column(Integer, default=0)
    completed_expressions = Column(Integer, default=0)
    
    # 元数据
    started_by = Column(Integer, ForeignKey("dashboard_user.id"))
    notes = Column(Text)
    
    # 关联关系
    config_template = relationship("DiggingConfigTemplate", back_populates="processes")
    started_by_user = relationship("DashboardUser", back_populates="processes")
    
    @property
    def completion_rate(self):
        """计算完成率"""
        if self.total_expressions and self.total_expressions > 0:
            return self.completed_expressions / self.total_expressions
        return 0.0


class ConfigUsageHistory(Base):
    """配置使用历史表"""
    __tablename__ = "config_usage_history"
    
    id = Column(Integer, primary_key=True, index=True)
    config_template_id = Column(Integer, ForeignKey("digging_config_templates.id"))
    process_id = Column(Integer, ForeignKey("digging_processes.id"))
    used_at = Column(DateTime(timezone=True), server_default=func.now())
    used_by = Column(Integer, ForeignKey("dashboard_user.id"))
    
    # 关联关系
    config_template = relationship("DiggingConfigTemplate", back_populates="usage_history")


class DashboardLog(Base):
    """系统日志表"""
    __tablename__ = "dashboard_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    level = Column(String(10), nullable=False, index=True)
    message = Column(Text, nullable=False)
    context = Column(Text)  # JSON格式的上下文信息
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    user_id = Column(Integer, ForeignKey("dashboard_user.id"))
    
    @property
    def context_dict(self):
        """获取上下文字典"""
        if self.context:
            try:
                return json.loads(self.context)
            except json.JSONDecodeError:
                return {}
        return {}


class AuditLog(Base):
    """审计日志模型"""
    __tablename__ = "audit_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("dashboard_user.id"), nullable=False)
    action = Column(String(100), nullable=False)  # 操作类型
    resource_type = Column(String(50), nullable=False)  # 资源类型
    resource_id = Column(String(100), nullable=True)  # 资源ID
    details = Column(JSON, nullable=True)  # 详细信息
    ip_address = Column(String(45), nullable=True)  # IP地址
    user_agent = Column(Text, nullable=True)  # 用户代理
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # 关联关系
    user = relationship("DashboardUser", back_populates="audit_logs")
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, action='{self.action}', user_id={self.user_id})>"
