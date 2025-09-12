"""
进程控制API路由
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.core.auth import get_current_active_user
from app.db.models import DashboardUser
from app.models.config import DiggingConfig
from app.services.process_service import process_service
from app.services.config_service import config_service
from app.core.exceptions import ProcessError, NotFoundError

router = APIRouter()


@router.get("/status")
async def get_process_status(
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取挖掘进程状态"""
    try:
        return process_service.get_current_process_status(db)
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/status/all")
async def get_all_processes_status(
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取所有进程状态统计信息"""
    try:
        return process_service.get_all_processes_status(db)
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/start")
async def start_process(
    config: DiggingConfig,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """启动挖掘进程"""
    try:
        return process_service.start_process(config, current_user.id, db)
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"启动进程失败: {str(e)}")


@router.post("/start-template/{template_id}")
async def start_process_from_template(
    template_id: int,
    stage: int = 1,
    n_jobs: int = 5,
    enable_multi_simulation: bool = False,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """从配置模板启动挖掘进程（支持多模拟选项）"""
    try:
        config = config_service.get_template_config(db, template_id)
        
        return process_service.start_process(config, current_user.id, db, stage, n_jobs, enable_multi_simulation)
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"从模板启动进程失败: {str(e)}")


@router.post("/stop")
async def stop_process(
    force: bool = False,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """停止挖掘进程"""
    try:
        return process_service.stop_process(current_user.id, db, force)
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"停止进程失败: {str(e)}")


@router.post("/restart")
async def restart_process(
    config: DiggingConfig,
    current_user: DashboardUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """重启挖掘进程"""
    try:
        # 先停止
        current_status = process_service.get_current_process_status(db)
        if current_status["status"] == "running":
            process_service.stop_process(current_user.id, db, force=False)
            
            # 等待一段时间确保进程已停止
            import time
            time.sleep(3)
        
        # 再启动
        return process_service.start_process(config, current_user.id, db)
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"重启进程失败: {str(e)}")


@router.get("/logs")
async def get_process_logs(
    limit: int = 100,
    offset: int = 0,
    current_user: DashboardUser = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """获取进程日志"""
    try:
        logs = process_service.get_process_logs(limit, offset)
        return {
            "logs": logs,
            "total": len(logs),
            "limit": limit,
            "offset": offset
        }
    except ProcessError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))