"""
配置管理服务
"""

import json
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.db.models import DiggingConfigTemplate, AuditLog
from app.core.exceptions import ValidationError, NotFoundError
from app.models.config import DiggingConfig, DiggingConfigCreate, DiggingConfigUpdate
from app.utils.tag_generator import TagGenerator
from app.utils.path_utils import detect_project_root, get_config_path


class ConfigService:
    """配置管理服务"""
    
    def __init__(self):
        # 自动检测项目根目录
        self.project_root = detect_project_root()
        self.config_path = get_config_path("digging_config.txt")
        
    def get_templates(self, db: Session, skip: int = 0, limit: int = 100) -> List[DiggingConfigTemplate]:
        """获取配置模板列表"""
        return db.query(DiggingConfigTemplate).order_by(desc(DiggingConfigTemplate.updated_at)).offset(skip).limit(limit).all()
    
    def get_template_by_id(self, db: Session, template_id: int) -> Optional[DiggingConfigTemplate]:
        """根据ID获取配置模板"""
        return db.query(DiggingConfigTemplate).filter(
            DiggingConfigTemplate.id == template_id
        ).first()
    
    def create_template(self, db: Session, config: DiggingConfigCreate, user_id: int) -> DiggingConfigTemplate:
        """创建配置模板"""
        try:
            # 验证配置
            self._validate_config(config)
            
            # 生成tag用于预览
            tag = TagGenerator.generate_tag(
                region=config.region,
                delay=config.delay,
                instrument_type=config.instrument_type,
                universe=config.universe,
                dataset_id=config.dataset_id if not config.use_recommended_fields else None,
                recommended_name=config.recommended_name if config.use_recommended_fields else None,
                step="step1"
            )
            
            # 创建数据库记录
            db_template = DiggingConfigTemplate(
                template_name=config.name,
                description=config.description,
                use_recommended_fields=config.use_recommended_fields,
                region=config.region,
                universe=config.universe,
                delay=config.delay,
                decay=config.decay,
                neutralization=config.neutralization,
                instrument_type=config.instrument_type,
                max_trade=config.max_trade,

                dataset_id=config.dataset_id,
                recommended_name=config.recommended_name,
                recommended_fields=json.dumps(config.recommended_fields) if config.recommended_fields else None,
                created_by=user_id
            )
            
            db.add(db_template)
            db.flush()  # 获取ID但不提交
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="CREATE_CONFIG_TEMPLATE",
                resource_type="CONFIG_TEMPLATE",
                resource_id=str(db_template.id),
                details={
                    "template_name": config.name,
                    "tag_preview": tag
                }
            )
            db.add(audit_log)
            db.commit()
            
            return db_template
            
        except Exception as e:
            db.rollback()
            if isinstance(e, ValidationError):
                raise
            raise ValidationError(f"创建配置模板失败: {str(e)}")
    
    def update_template(self, db: Session, template_id: int, config: DiggingConfigUpdate, user_id: int) -> DiggingConfigTemplate:
        """更新配置模板"""
        try:
            # 获取现有模板
            db_template = self.get_template_by_id(db, template_id)
            if not db_template:
                raise NotFoundError("配置模板不存在")
            
            # 更新字段
            update_data = config.dict(exclude_unset=True)
            
            if "name" in update_data:
                db_template.template_name = update_data["name"]
            if "description" in update_data:
                db_template.description = update_data["description"]
            
            # 更新配置字段
            field_mapping = {
                "region": "region",
                "universe": "universe", 
                "delay": "delay",
                "decay": "decay",
                "neutralization": "neutralization",
                "instrument_type": "instrument_type",
                "max_trade": "max_trade",

                "use_recommended_fields": "use_recommended_fields",
                "dataset_id": "dataset_id",
                "recommended_name": "recommended_name",
                "recommended_fields": "recommended_fields"
            }
            
            for key, value in update_data.items():
                if key in field_mapping:
                    # 特殊处理recommended_fields，需要转换为JSON字符串
                    if key == "recommended_fields" and isinstance(value, list):
                        setattr(db_template, field_mapping[key], json.dumps(value))
                    else:
                        setattr(db_template, field_mapping[key], value)
            
            # 构建完整配置以验证
            config_data = {
                "template_id": db_template.id,
                "region": db_template.region,
                "universe": db_template.universe,
                "delay": db_template.delay,
                "decay": db_template.decay,
                "neutralization": db_template.neutralization,
                "instrument_type": db_template.instrument_type,
                "max_trade": db_template.max_trade,

                "use_recommended_fields": db_template.use_recommended_fields,
                "dataset_id": db_template.dataset_id,
                "recommended_name": db_template.recommended_name,
                "recommended_fields": db_template.recommended_fields_list
            }
            
            # 验证更新后的配置
            full_config = DiggingConfig(**config_data)
            self._validate_config(full_config)
            
            # 设置更新时间
            db_template.updated_at = datetime.utcnow()
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="UPDATE_CONFIG_TEMPLATE",
                resource_type="CONFIG_TEMPLATE",
                resource_id=str(template_id),
                details={
                    "template_name": db_template.template_name,
                    "updated_fields": list(update_data.keys())
                }
            )
            db.add(audit_log)
            db.commit()
            
            return db_template
            
        except Exception as e:
            db.rollback()
            if isinstance(e, (ValidationError, NotFoundError)):
                raise
            raise ValidationError(f"更新配置模板失败: {str(e)}")
    
    def delete_template(self, db: Session, template_id: int, user_id: int) -> bool:
        """删除配置模板（软删除）"""
        try:
            db_template = self.get_template_by_id(db, template_id)
            if not db_template:
                raise NotFoundError("配置模板不存在")
            
            # 直接删除（因为没有is_active字段）
            template_name = db_template.template_name
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="DELETE_CONFIG_TEMPLATE",
                resource_type="CONFIG_TEMPLATE",
                resource_id=str(template_id),
                details={
                    "template_name": template_name
                }
            )
            db.add(audit_log)
            
            # 删除模板
            db.delete(db_template)
            db.commit()
            
            return True
            
        except Exception as e:
            db.rollback()
            if isinstance(e, NotFoundError):
                raise
            raise ValidationError(f"删除配置模板失败: {str(e)}")
    
    def get_current_config(self) -> Dict[str, Any]:
        """获取当前使用的配置文件内容"""
        try:
            config_data = {}
            
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('#') and ':' in line:
                        key, value = line.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # 类型转换
                        if value.lower() in ['true', 'false']:
                            config_data[key] = value.lower() == 'true'
                        elif value.isdigit():
                            config_data[key] = int(value)
                        elif value.replace('.', '').isdigit():
                            config_data[key] = float(value)
                        elif value.startswith('[') and value.endswith(']'):
                            try:
                                config_data[key] = json.loads(value)
                            except json.JSONDecodeError:
                                config_data[key] = value
                        else:
                            config_data[key] = value
            
            return config_data
            
        except Exception as e:
            raise ValidationError(f"读取当前配置失败: {str(e)}")
    
    def get_template_config(self, db: Session, template_id: int) -> DiggingConfig:
        """获取模板的配置对象"""
        template = self.get_template_by_id(db, template_id)
        if not template:
            raise NotFoundError("配置模板不存在")
        
        # 从数据库字段构建配置对象
        config_data = {
            "template_id": template.id,
            "region": template.region,
            "universe": template.universe,
            "delay": template.delay,
            "decay": template.decay,
            "neutralization": template.neutralization,
            "instrument_type": template.instrument_type,
            "max_trade": template.max_trade,

            "use_recommended_fields": template.use_recommended_fields,
            "dataset_id": template.dataset_id,
            "recommended_name": template.recommended_name,
            "recommended_fields": template.recommended_fields_list
        }
        
        return DiggingConfig(**config_data)
    
    def preview_tag(self, config: DiggingConfig) -> str:
        """预览配置生成的tag"""
        return TagGenerator.generate_tag(
            region=config.region,
            delay=config.delay,
            instrument_type=config.instrument_type,
            universe=config.universe,
            dataset_id=config.dataset_id if not config.use_recommended_fields else None,
            recommended_name=config.recommended_name if config.use_recommended_fields else None,
            step="step1"
        )
    
    def get_field_options(self) -> Dict[str, List[str]]:
        """获取字段选项"""
        return {
            "regions": ["USA", "CHN", "JPN", "EUR", "GBR", "AUS"],
            "universes": ["TOP3000", "TOP2000", "TOP1000", "TOP500", "TOP200"],
            "instrument_types": ["EQUITY", "FUTURES", "FOREX"],
            "max_trades": ["OFF", "ON", "FULL"],
            "neutralizations": ["MARKET", "INDUSTRY", "SUBINDUSTRY", "SECTOR"],
            "modes": ["USER", "CONSULTANT", "CONSULTANT_PPAC"]
        }
    
    def _validate_config(self, config: DiggingConfig):
        """验证配置 - 简化版本，只验证必要的业务逻辑"""
        
        # 只验证核心业务逻辑：推荐字段模式vs数据集模式
        if config.use_recommended_fields:
            if not config.recommended_name:
                raise ValidationError("使用推荐字段时必须提供recommended_name")
        else:
            if not config.dataset_id:
                raise ValidationError("不使用推荐字段时必须提供dataset_id")


# 全局实例
config_service = ConfigService()
