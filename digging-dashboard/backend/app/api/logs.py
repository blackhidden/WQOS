"""
日志管理API路由
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.core.auth import get_current_active_user
from app.db.models import DashboardUser
from app.services.log_service import log_service
from app.core.exceptions import ValidationError

router = APIRouter()


@router.get("/sources")
async def get_log_sources(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> List[str]:
    """获取可用的日志源"""
    return log_service.get_available_log_sources()


@router.get("/stats")
async def get_log_stats(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取日志统计信息"""
    return log_service.get_log_stats()


@router.get("/")
async def get_logs(
    source: str = Query("unified_digging", description="日志源"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    level: Optional[str] = Query(None, description="日志级别过滤"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    search_text: Optional[str] = Query(None, description="搜索文本"),
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取日志"""
    try:
        # 解析时间参数
        start_dt = None
        end_dt = None
        
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无效的开始时间格式")
        
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无效的结束时间格式")
        
        return await log_service.get_logs(
            source=source,
            limit=limit,
            offset=offset,
            level=level,
            start_time=start_dt,
            end_time=end_dt,
            search_text=search_text
        )
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/search")
async def search_logs(
    query: str = Query(..., description="搜索查询"),
    source: str = Query("unified_digging", description="日志源"),
    level: Optional[str] = Query(None, description="日志级别过滤"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """搜索日志"""
    try:
        # 解析时间参数
        start_dt = None
        end_dt = None
        
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无效的开始时间格式")
        
        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无效的结束时间格式")
        
        return await log_service.get_logs(
            source=source,
            limit=limit,
            offset=offset,
            level=level,
            start_time=start_dt,
            end_time=end_dt,
            search_text=query
        )
    except ValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/levels")
async def get_log_levels(
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, List[str]]:
    """获取可用的日志级别"""
    return {
        "levels": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    }