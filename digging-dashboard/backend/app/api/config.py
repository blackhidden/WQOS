"""
配置管理API路由
"""

from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.core.auth import get_current_active_user
from app.db.models import DashboardUser
from app.models.config import (
    DiggingConfig, DiggingConfigCreate, DiggingConfigUpdate,
    TagGenerationRequest
)
from app.services.config_service import config_service
from app.services.worldquant_service import WorldQuantService
from app.services.worldquant_config_service import WorldQuantConfigService
from app.core.exceptions import ValidationError, NotFoundError

router = APIRouter()


@router.get("/templates")
async def get_config_templates(
    skip: int = 0,
    limit: int = 100,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """获取配置模板列表"""
    try:
        templates = config_service.get_templates(db, skip, limit)
        return [
            {
                "id": template.id,
                "name": template.template_name,
                "description": template.description,
                "config_data": {
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
                },
                "tag_preview": template.tag_name,
                "created_at": template.created_at.isoformat(),
                "updated_at": template.updated_at.isoformat(),
                "created_by": template.created_by
            }
            for template in templates
        ]
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/templates")
async def create_config_template(
    template: DiggingConfigCreate,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """创建新的配置模板"""
    try:
        db_template = config_service.create_template(db, template, current_user.id)
        return {
            "id": db_template.id,
            "name": db_template.template_name,
            "description": db_template.description,
            "config_data": {
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
            },
            "tag_preview": db_template.tag_name,
            "created_at": db_template.created_at.isoformat(),
            "message": "配置模板创建成功"
        }
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/templates/{template_id}")
async def get_config_template(
    template_id: int,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取指定配置模板"""
    try:
        template = config_service.get_template_by_id(db, template_id)
        if not template:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="配置模板不存在")
        
        return {
            "id": template.id,
            "name": template.template_name,
            "description": template.description,
            "config_data": {
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
            },
            "created_at": template.created_at.isoformat(),
            "updated_at": template.updated_at.isoformat(),
            "created_by": template.created_by
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put("/templates/{template_id}")
async def update_config_template(
    template_id: int,
    template: DiggingConfigUpdate,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """更新配置模板"""
    try:
        db_template = config_service.update_template(db, template_id, template, current_user.id)
        return {
            "id": db_template.id,
            "name": db_template.template_name,
            "description": db_template.description,
            "config_data": {
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
            },
            "updated_at": db_template.updated_at.isoformat(),
            "message": "配置模板更新成功"
        }
    except (ValidationError, NotFoundError) as e:
        status_code = status.HTTP_404_NOT_FOUND if isinstance(e, NotFoundError) else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete("/templates/{template_id}")
async def delete_config_template(
    template_id: int,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """删除配置模板"""
    try:
        config_service.delete_template(db, template_id, current_user.id)
        return {"message": "配置模板删除成功"}
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/validate")
async def validate_config(
    config: DiggingConfig,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """验证配置"""
    try:
        config_service._validate_config(config)
        tag_preview = config_service.preview_tag(config)
        return {
            "valid": True,
            "message": "配置验证通过",
            "tag_preview": tag_preview
        }
    except ValidationError as e:
        return {
            "valid": False,
            "message": str(e),
            "tag_preview": None
        }


@router.post("/generate-tag")
async def generate_tag(
    request: TagGenerationRequest,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, str]:
    """生成tag名称"""
    try:
        # 从请求构建配置对象
        config = DiggingConfig(**request.dict())
        tag = config_service.preview_tag(config)
        return {"tag": tag}
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/current")
async def get_current_config(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取当前使用的配置文件内容"""
    try:
        return config_service.get_current_config()
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/field-options")
async def get_field_options(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, List[str]]:
    """获取字段选项"""
    return config_service.get_field_options()


@router.get("/worldquant-options")
async def get_worldquant_options(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取WorldQuant Brain API的配置选项"""
    try:
        return WorldQuantService.get_simulation_options()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取WorldQuant配置选项失败: {str(e)}"
        )


@router.get("/regions/{instrument_type}")
async def get_regions_for_instrument(
    instrument_type: str,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """根据工具类型获取可用的地区列表"""
    try:
        return WorldQuantService.get_regions_for_instrument(instrument_type)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取地区列表失败: {str(e)}"
        )


@router.get("/universes/{instrument_type}/{region}")
async def get_universes_for_region(
    instrument_type: str,
    region: str,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """根据工具类型和地区获取可用的股票池列表"""
    try:
        return WorldQuantService.get_universes_for_region(instrument_type, region)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取股票池列表失败: {str(e)}"
        )


@router.get("/delays/{instrument_type}/{region}")
async def get_delays_for_region(
    instrument_type: str,
    region: str,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """根据工具类型和地区获取可用的延迟选项"""
    try:
        return WorldQuantService.get_delays_for_region(instrument_type, region)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取延迟选项失败: {str(e)}"
        )


@router.get("/neutralizations/{instrument_type}/{region}")
async def get_neutralizations_for_region(
    instrument_type: str,
    region: str,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """根据工具类型和地区获取可用的中性化选项"""
    try:
        return WorldQuantService.get_neutralizations_for_region(instrument_type, region)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取中性化选项失败: {str(e)}"
        )


@router.post("/sync-worldquant-config")
async def sync_worldquant_config(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """同步WorldQuant配置到数据库"""
    try:
        result = WorldQuantConfigService.sync_from_api()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"同步配置失败: {str(e)}"
        )


@router.get("/worldquant-sync-history")
async def get_worldquant_sync_history(
    limit: int = 10,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """获取WorldQuant配置同步历史"""
    try:
        return WorldQuantConfigService.get_sync_history(limit)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步历史失败: {str(e)}"
        )


@router.get("/worldquant-config-status")
async def get_worldquant_config_status(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取WorldQuant配置状态"""
    try:
        last_sync_time = WorldQuantConfigService.get_last_sync_time()
        is_available = WorldQuantConfigService.is_config_available()
        
        return {
            "is_available": is_available,
            "last_sync_time": last_sync_time.isoformat() if last_sync_time else None,
            "status": "已同步" if is_available else "未同步"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取配置状态失败: {str(e)}"
        )