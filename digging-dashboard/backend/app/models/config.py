"""
配置管理相关的Pydantic模型
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, validator


class ConfigTemplateBase(BaseModel):
    """配置模板基础模型 - 简化版本，只保留必要验证"""
    template_name: str
    description: Optional[str] = None
    use_recommended_fields: bool
    region: str
    universe: str
    delay: int
    instrument_type: str = "EQUITY"
    max_trade: str = "OFF"
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    recommended_fields: Optional[List[str]] = None
    
    @validator('template_name')
    def template_name_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('模板名称不能为空')
        return v.strip()
    
    @validator('dataset_id')
    def dataset_id_required_for_dataset_mode(cls, v, values):
        if not values.get('use_recommended_fields', False) and not v:
            raise ValueError('数据集模式下dataset_id不能为空')
        return v
    
    @validator('recommended_name')
    def recommended_name_required_for_recommended_mode(cls, v, values):
        if values.get('use_recommended_fields', False) and not v:
            raise ValueError('推荐字段模式下recommended_name不能为空')
        return v


class ConfigTemplateCreate(ConfigTemplateBase):
    """创建配置模板"""
    pass


class ConfigTemplateUpdate(ConfigTemplateBase):
    """更新配置模板（所有字段都是可选的）"""
    template_name: Optional[str] = None
    use_recommended_fields: Optional[bool] = None
    region: Optional[str] = None
    universe: Optional[str] = None
    delay: Optional[int] = None
    instrument_type: Optional[str] = None
    max_trade: Optional[str] = None



class ConfigTemplate(ConfigTemplateBase):
    """配置模板响应"""
    id: int
    tag_name: str
    created_at: datetime
    updated_at: datetime
    created_by: int
    
    class Config:
        from_attributes = True


class ConfigValidationResult(BaseModel):
    """配置验证结果"""
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    tag_name: Optional[str] = None
    estimated_factors: Optional[Dict[str, int]] = None


class ConfigPreview(BaseModel):
    """配置预览"""
    tag_name: str
    config_summary: Dict[str, Any]
    estimated_performance: Optional[Dict[str, Any]] = None


class TagGenerationRequest(BaseModel):
    """Tag生成请求"""
    region: str
    delay: int
    instrument_type: str
    universe: str
    use_recommended_fields: bool
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    step: int = 1


class TagGenerationResponse(BaseModel):
    """Tag生成响应"""
    tag_name: str
    is_valid: bool
    conflicts: List[str] = []


class DiggingConfig(BaseModel):
    """挖掘配置模型 - 简化版本，移除硬编码验证"""
    template_id: Optional[int] = None
    region: str
    universe: str
    delay: int = 1
    decay: int = 6
    neutralization: str = "SUBINDUSTRY"
    instrument_type: str = "EQUITY"
    max_trade: str = "OFF"

    use_recommended_fields: bool = False
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    recommended_fields: Optional[List[str]] = None


class DiggingConfigCreate(BaseModel):
    """创建挖掘配置"""
    name: str
    description: Optional[str] = None
    region: str
    universe: str
    delay: int = 1
    decay: int = 6
    neutralization: str = "SUBINDUSTRY"
    instrument_type: str = "EQUITY"
    max_trade: str = "OFF"

    use_recommended_fields: bool = False
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    recommended_fields: Optional[List[str]] = None


class DiggingConfigUpdate(BaseModel):
    """更新挖掘配置（所有字段都是可选的）"""
    name: Optional[str] = None
    description: Optional[str] = None
    region: Optional[str] = None
    universe: Optional[str] = None
    delay: Optional[int] = None
    decay: Optional[int] = None
    neutralization: Optional[str] = None
    instrument_type: Optional[str] = None
    max_trade: Optional[str] = None

    mode: Optional[str] = None
    use_recommended_fields: Optional[bool] = None
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    recommended_fields: Optional[List[str]] = None
