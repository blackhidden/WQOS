"""
独立脚本管理API
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Dict, Any, Optional
from pydantic import BaseModel

from app.core.auth import get_current_user
from app.db.database import get_db
from app.db.models import DashboardUser, DiggingProcess
from app.services.process_service import ProcessService
from app.core.exceptions import ProcessError, ValidationError

router = APIRouter()
process_service = ProcessService()


class StartScriptRequest(BaseModel):
    """启动脚本请求模型"""
    mode: Optional[str] = None  # CONSULTANT, USER, PPAC
    sharpe_threshold: Optional[float] = None
    fitness_threshold: Optional[float] = None
    start_date: Optional[str] = None  # 起始检查日期 (YYYY-MM-DD)


@router.get("/status")
async def get_all_scripts_status(
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """获取所有脚本的状态"""
    try:
        return process_service.get_all_scripts_status(db)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取脚本状态失败: {str(e)}"
        )


@router.post("/start/{script_type}")
async def start_script(
    script_type: str,
    request: StartScriptRequest,
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """启动独立脚本"""
    try:
        # 所有独立脚本（包括SessionKeeper）都使用通用启动方法
        # 将请求参数传递给进程服务
        script_params = {
            "mode": request.mode,
            "sharpe_threshold": request.sharpe_threshold,
            "fitness_threshold": request.fitness_threshold,
            "start_date": request.start_date
        }
        # 过滤掉None值
        script_params = {k: v for k, v in script_params.items() if v is not None}
        
        return process_service.start_independent_script(script_type, current_user.id, db, script_params)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ProcessError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"启动脚本失败: {str(e)}"
        )


@router.post("/stop/{script_type}")
async def stop_script(
    script_type: str,
    force: bool = False,
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """停止独立脚本"""
    try:
        # 所有独立脚本（包括SessionKeeper）都使用通用停止方法
        return process_service.stop_independent_script(script_type, current_user.id, db, force)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ProcessError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止脚本失败: {str(e)}"
        )


@router.get("/types")
async def get_script_types(
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, str]:
    """获取支持的脚本类型"""
    return process_service.script_names


@router.post("/stop/task/{task_id}")
async def stop_task(
    task_id: int,
    force: bool = False,
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """停止特定的任务"""
    try:
        return process_service.stop_task_by_id(task_id, current_user.id, db, force)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ProcessError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"停止任务失败: {str(e)}"
        )


@router.delete("/task/{task_id}")
async def delete_task(
    task_id: int,
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """删除已停止的任务及其日志"""
    try:
        return process_service.delete_task(task_id, current_user.id, db)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ProcessError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除任务失败: {str(e)}"
        )


@router.get("/logs/task/{task_id}")
async def get_task_logs(
    task_id: int,
    offset: int = 0,
    limit: int = 1000,
    include_rotated: bool = False,  # 新增：是否包含轮转文件
    db: Session = Depends(get_db),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """获取指定任务的日志文件内容
    
    Args:
        task_id: 任务ID
        offset: 开始行数偏移量（0表示从头开始）
        limit: 最大读取行数
        include_rotated: 是否包含轮转的备份文件（默认False，实时查看时只读主文件）
    """
    import os
    
    # 获取任务记录
    task = db.query(DiggingProcess).filter(DiggingProcess.id == task_id).first()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"任务不存在: {task_id}"
        )
    
    # 获取日志文件路径
    log_file = task.log_file_path
    if not log_file:
        return {
            "content": f"任务 {task_id} 没有日志文件",
            "total_lines": 0,
            "current_offset": 0,
            "returned_lines": 0
        }
    
    try:
        if include_rotated:
            # 完整模式：收集所有相关的日志文件（包括轮转的备份文件）
            log_files = []
            base_log_file = log_file
            
            # 检查主日志文件
            if os.path.exists(base_log_file):
                log_files.append((base_log_file, 0))  # (文件路径, 优先级)
            
            # 检查轮转的备份文件（按时间顺序：.1是最新的备份，.3是最旧的）
            for i in range(1, 4):  # .1, .2, .3
                backup_file = f"{base_log_file}.{i}"
                if os.path.exists(backup_file):
                    log_files.append((backup_file, i))
            
            if not log_files:
                return {
                    "content": f"日志文件不存在: {log_file}",
                    "total_lines": 0,
                    "current_offset": 0,
                    "returned_lines": 0,
                    "log_files": []
                }
            
            # 按优先级排序（主文件 -> .1 -> .2 -> .3，这样是按时间从新到旧）
            log_files.sort(key=lambda x: x[1])
            
            # 合并读取所有日志文件内容
            all_lines = []
            file_info = []
            
            for file_path, priority in log_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_lines = f.readlines()
                        all_lines.extend(file_lines)
                        file_info.append({
                            "file": os.path.basename(file_path),
                            "lines": len(file_lines),
                            "size": os.path.getsize(file_path)
                        })
                except Exception as e:
                    file_info.append({
                        "file": os.path.basename(file_path),
                        "lines": 0,
                        "size": 0,
                        "error": str(e)
                    })
            
            total_lines = len(all_lines)
            
        else:
            # 实时模式：只读取主日志文件（性能优化）
            base_log_file = log_file
            
            if not os.path.exists(base_log_file):
                return {
                    "content": f"日志文件不存在: {log_file}",
                    "total_lines": 0,
                    "current_offset": 0,
                    "returned_lines": 0,
                    "log_files": [],
                    "mode": "realtime"
                }
            
            # 只读取主文件
            with open(base_log_file, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
            
            total_lines = len(all_lines)
            file_info = [{
                "file": os.path.basename(base_log_file),
                "lines": total_lines,
                "size": os.path.getsize(base_log_file),
                "mode": "realtime_only"
            }]
            
        # 如果offset为负数，表示从末尾开始读取
        if offset < 0:
            # 负数offset：读取最后N行
            start_idx = max(0, total_lines + offset)
            end_idx = total_lines
            selected_lines = all_lines[start_idx:end_idx]
            actual_offset = start_idx
        else:
            # 正数offset：从指定位置开始读取
            if offset >= total_lines:
                # 偏移量超出文件行数，返回空内容
                selected_lines = []
                actual_offset = total_lines
            else:
                end_idx = min(offset + limit, total_lines)
                selected_lines = all_lines[offset:end_idx]
                actual_offset = offset
        
        content = ''.join(selected_lines)
        returned_lines = len(selected_lines)
        
        return {
            "content": content,
            "total_lines": total_lines,
            "current_offset": actual_offset,
            "returned_lines": returned_lines,
            "has_more": actual_offset + returned_lines < total_lines,
            "log_files": file_info,  # 显示相关日志文件的信息
            "mode": "full" if include_rotated else "realtime"  # 标识读取模式
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"读取日志文件失败: {str(e)}"
        )
