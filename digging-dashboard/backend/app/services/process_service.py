"""
挖掘进程管理服务
"""

import os
import sys
import signal
import subprocess
import psutil
import json
import time
import platform
from typing import Optional, Dict, List, Any
from datetime import datetime
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.database import get_db
from app.db.models import DiggingProcess, AuditLog
from app.core.exceptions import ProcessError, ValidationError
from app.models.config import DiggingConfig
from app.utils.tag_generator import TagGenerator
from app.utils.path_utils import detect_project_root, get_script_path, get_config_path

settings = get_settings()


class ProcessService:
    """挖掘进程管理服务"""
    
    def __init__(self):
        # 平台检测
        self.is_windows = platform.system().lower() == 'windows'
        
        # 自动检测项目根目录
        self.project_root = detect_project_root()
        self.script_path = get_script_path("unified_digging_scheduler")
        
        # 脚本路径映射
        self.script_paths = {
            "unified_digging": get_script_path("unified_digging_scheduler"),
            "check_optimized": get_script_path("check_optimized"),
            "correlation_checker": get_script_path("correlation_checker_independent"),
            "session_keeper": get_script_path("session_keeper")
        }
        
        # 脚本显示名称
        self.script_names = {
            "unified_digging": "因子挖掘",
            "check_optimized": "Alpha检查器", 
            "correlation_checker": "相关性检查器",
            "session_keeper": "会话保持器"
        }
        
        # 需要配置模板的脚本
        self.scripts_need_config = {"unified_digging"}
        
        # 独立脚本（不需要配置模板）
        self.independent_scripts = {"check_optimized", "correlation_checker", "session_keeper"}
        self.config_path = get_config_path("digging_config.txt")
        self.process_info: Optional[Dict[str, Any]] = None
    
    def _get_process_kwargs(self) -> Dict[str, Any]:
        """获取跨平台的进程创建参数"""
        kwargs = {}
        
        if not self.is_windows:
            # Unix/Linux 系统使用 setsid 创建新进程组
            kwargs['preexec_fn'] = os.setsid
        else:
            # Windows 系统使用 CREATE_NEW_PROCESS_GROUP
            kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
            
        return kwargs
    
    def _terminate_process_group(self, pid: int, force: bool = False) -> str:
        """跨平台终止进程组"""
        try:
            if not self.is_windows:
                # Unix/Linux 系统使用 killpg
                if force:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                    return "SIGKILL"
                else:
                    os.killpg(os.getpgid(pid), signal.SIGTERM)
                    return "SIGTERM"
            else:
                # Windows 系统使用 psutil
                try:
                    process = psutil.Process(pid)
                    # 终止进程及其所有子进程
                    for child in process.children(recursive=True):
                        try:
                            if force:
                                child.kill()
                            else:
                                child.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    
                    # 终止主进程
                    if force:
                        process.kill()
                        return "KILL"
                    else:
                        process.terminate()
                        return "TERMINATE"
                        
                except psutil.NoSuchProcess:
                    return "NOT_FOUND"
                    
        except Exception as e:
            raise ProcessError(f"终止进程失败: {e}")
        
    def get_current_process_status(self, db: Session) -> Dict[str, Any]:
        """获取当前进程状态"""
        try:
            # 从数据库获取最新的进程记录
            latest_process = db.query(DiggingProcess).order_by(
                DiggingProcess.started_at.desc()
            ).first()
            
            if not latest_process:
                return {
                    "status": "stopped",
                    "pid": None,
                    "config_id": None,
                    "start_time": None,
                    "uptime": None,
                    "memory_usage": None,
                    "cpu_usage": None
                }
            
            # 检查进程是否仍在运行
            is_running = False
            process_info = None
            
            if latest_process.process_id:
                try:
                    process = psutil.Process(latest_process.process_id)
                    if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                        is_running = True
                        # 使用进程的实际创建时间
                        try:
                            create_time = process.create_time()
                            uptime = int(time.time() - create_time)
                        except (psutil.AccessDenied, OSError):
                            # 如果无法获取创建时间，使用数据库时间
                            start_time = latest_process.started_at
                            uptime = int((datetime.now() - start_time).total_seconds()) if start_time else 0
                        
                        process_info = {
                            "memory_usage": process.memory_info().rss / 1024 / 1024,  # MB
                            "cpu_usage": process.cpu_percent(),
                            "uptime": max(0, uptime)
                        }
                except psutil.NoSuchProcess:
                    is_running = False
            
            # 更新数据库状态
            if not is_running and latest_process.status == "running":
                latest_process.status = "stopped"
                latest_process.stopped_at = datetime.now()
                db.commit()
            
            status = "running" if is_running else latest_process.status
            start_time = latest_process.started_at
            
            return {
                "status": status,
                "pid": latest_process.process_id if is_running else None,
                "config_id": latest_process.config_template_id,
                "start_time": start_time.isoformat() if start_time else None,
                "uptime": process_info["uptime"] if process_info and "uptime" in process_info else None,
                "memory_usage": process_info["memory_usage"] if process_info else None,
                "cpu_usage": process_info["cpu_usage"] if process_info else None,
                "tag": latest_process.tag_name,
                "error_message": latest_process.error_message
            }
            
        except Exception as e:
            raise ProcessError(f"获取进程状态失败: {str(e)}")

    def get_all_processes_status(self, db: Session) -> Dict[str, Any]:
        """获取所有进程状态统计信息"""
        try:
            # 从数据库获取所有运行中的进程
            running_processes = db.query(DiggingProcess).filter(
                DiggingProcess.status == "running"
            ).all()
            
            active_processes = []
            total_memory = 0
            max_uptime = 0
            
            for db_process in running_processes:
                if db_process.process_id:
                    try:
                        process = psutil.Process(db_process.process_id)
                        if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                            memory_mb = process.memory_info().rss / 1024 / 1024
                            
                            # 使用进程的实际创建时间计算uptime
                            try:
                                process_create_time = process.create_time()
                                uptime = int(time.time() - process_create_time)
                                actual_start_time = datetime.fromtimestamp(process_create_time)
                            except (psutil.AccessDenied, OSError):
                                # 如果无法获取进程创建时间，使用数据库时间
                                start_time = db_process.started_at
                                uptime = int((datetime.now() - start_time).total_seconds()) if start_time else 0
                                actual_start_time = start_time
                            
                            active_processes.append({
                                "pid": db_process.process_id,
                                "tag": db_process.tag_name,
                                "script_type": getattr(db_process, 'script_type', 'unknown'),
                                "start_time": actual_start_time.isoformat() if actual_start_time else None,
                                "uptime": max(0, uptime),  # 确保uptime不为负数
                                "memory_usage": memory_mb,
                                "cpu_usage": process.cpu_percent()
                            })
                            
                            total_memory += memory_mb
                            max_uptime = max(max_uptime, max(0, uptime))
                        else:
                            # 进程已停止，更新数据库状态
                            db_process.status = "stopped"
                            db_process.stopped_at = datetime.now()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # 进程不存在，更新数据库状态
                        db_process.status = "stopped"
                        db_process.stopped_at = datetime.now()
            
            # 提交数据库更改
            db.commit()
            
            return {
                "total_processes": len(active_processes),
                "status": "running" if active_processes else "stopped",
                "total_memory_usage": total_memory,
                "max_uptime": max_uptime,
                "processes": active_processes
            }
            
        except Exception as e:
            print(f"获取所有进程状态时出错: {e}")
            return {
                "total_processes": 0,
                "status": "error",
                "total_memory_usage": 0,
                "max_uptime": 0,
                "processes": [],
                "error_message": str(e)
            }
    
    def start_process(self, config: DiggingConfig, user_id: int, db: Session, stage: int = 1, n_jobs: int = 5, enable_multi_simulation: bool = False) -> Dict[str, Any]:
        """启动挖掘进程"""
        try:
            # 移除单实例限制，允许多个unified_digging实例运行
            # （每个实例可以处理不同的阶段或配置）
            
            # 生成配置文件
            config_file_path = self._create_config_file(config)
            
            # 生成tag (基于阶段)
            tag = TagGenerator.generate_tag(
                region=config.region,
                delay=config.delay,
                instrument_type=config.instrument_type,
                universe=config.universe,
                dataset_id=config.dataset_id if not config.use_recommended_fields else None,
                recommended_name=config.recommended_name if config.use_recommended_fields else None,
                step=f"step{stage}"
            )
            
            # 构建启动命令，包含新的参数
            cmd = [
                sys.executable,
                self.script_path,
                "--config", config_file_path,
                "--stage", str(stage),
                "--n_jobs", str(n_jobs)
            ]
            
            # 如果启用多模拟，添加参数
            if enable_multi_simulation:
                cmd.extend(["--enable_multi_simulation", "true"])
            
            # 确保日志目录存在
            log_dir = os.path.join(self.project_root, "logs")
            os.makedirs(log_dir, exist_ok=True)
            
            # 为每个任务创建唯一的日志文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_name = f"unified_digging_{timestamp}_{os.getpid()}.log"
            log_file = os.path.join(log_dir, log_file_name)
            
            # 检查并处理日志轮转（父进程级别的轮转管理）
            self._ensure_log_rotation(log_file)
            
            # 启动进程，重定向输出到独立的日志文件
            with open(log_file, 'w', encoding='utf-8') as f:
                # 写入启动时间
                f.write(f"\n\n=== 进程启动 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                f.write(f"命令: {' '.join(cmd)}\n")
                f.write(f"配置: {tag}\n")
                f.write("=" * 50 + "\n\n")
                f.flush()
                
                # 获取跨平台进程创建参数
                process_kwargs = self._get_process_kwargs()
                
                process = subprocess.Popen(
                    cmd,
                    cwd=self.project_root,
                    stdout=f,
                    stderr=subprocess.STDOUT,  # 将stderr重定向到stdout
                    **process_kwargs  # 跨平台进程组创建
                )
            
            # 记录到数据库
            db_process = DiggingProcess(
                config_template_id=config.template_id,
                tag_name=tag,
                process_id=process.pid,
                status="running",
                script_type="unified_digging",
                started_at=datetime.now(),
                log_file_path=log_file
            )
            db.add(db_process)
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="START_PROCESS",
                resource_type="DIGGING_PROCESS",
                resource_id=str(process.pid),
                details={
                    "config_id": config.template_id,
                    "tag": tag,
                    "command": " ".join(cmd)
                }
            )
            db.add(audit_log)
            db.commit()
            
            return {
                "status": "started",
                "pid": process.pid,
                "tag": tag,
                "config_id": config.template_id,
                "start_time": db_process.started_at.isoformat()
            }
            
        except Exception as e:
            db.rollback()
            raise ProcessError(f"启动进程失败: {str(e)}")
    

    def start_independent_script(self, script_type: str, user_id: int, db: Session, script_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """启动独立脚本（无需配置文件）"""
        try:
            # 验证脚本类型
            if script_type not in self.script_paths:
                raise ValidationError(f"不支持的脚本类型: {script_type}")
            
            # 检查是否为需要配置模板的脚本
            if script_type in self.scripts_need_config:
                raise ValidationError(f"{self.script_names[script_type]}需要配置模板，请使用进程控制页面启动")
            
            # 检查是否已有相同类型的脚本在运行
            existing_process = db.query(DiggingProcess).filter(
                DiggingProcess.script_type == script_type,
                DiggingProcess.status == "running"
            ).first()
            
            if existing_process:
                raise ProcessError(f"{self.script_names[script_type]}已在运行中（PID: {existing_process.process_id}），请先停止后再启动新实例")
            
            script_path = self.script_paths[script_type]
            script_name = self.script_names[script_type]
            
            # 生成简单的tag
            tag = f"{script_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 构建启动命令
            cmd = [sys.executable, script_path]
            
            # 为check_optimized脚本添加参数支持
            if script_type == "check_optimized" and script_params:
                if script_params.get("mode"):
                    cmd.extend(["--mode", script_params["mode"]])
                if script_params.get("sharpe_threshold") is not None:
                    cmd.extend(["--sharpe-threshold", str(script_params["sharpe_threshold"])])
                if script_params.get("fitness_threshold") is not None:
                    cmd.extend(["--fitness-threshold", str(script_params["fitness_threshold"])])
                if script_params.get("start_date"):
                    cmd.extend(["--start-date", script_params["start_date"]])
            
            # 为session_keeper脚本添加启动参数
            elif script_type == "session_keeper":
                cmd.extend(["--action", "start"])
            
            # 确保日志目录存在
            log_dir = os.path.join(self.project_root, "logs")
            os.makedirs(log_dir, exist_ok=True)
            
            # 为每个任务创建唯一的日志文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_name = f"{script_type}_{timestamp}_{os.getpid()}.log"
            log_file_path = os.path.join(log_dir, log_file_name)
            
            # 检查并处理日志轮转（父进程级别的轮转管理）
            self._ensure_log_rotation(log_file_path)
            
            # 获取跨平台进程创建参数
            process_kwargs = self._get_process_kwargs()
            process_kwargs.update({
                'text': True,
                'bufsize': 1  # 行缓冲
            })
            
            # 启动进程，重定向输出到独立的日志文件
            with open(log_file_path, 'w', encoding='utf-8') as log_file:
                process = subprocess.Popen(
                    cmd,
                    cwd=self.project_root,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    **process_kwargs  # 跨平台进程组创建
                )
            
            # 记录到数据库
            db_process = DiggingProcess(
                process_id=process.pid,
                status="running",
                script_type=script_type,
                tag_name=tag,
                started_at=datetime.now(),
                log_file_path=log_file_path
            )
            db.add(db_process)
            db.commit()
            
            # 记录审计日志
            audit_details = {
                "script_type": script_type,
                "script_name": script_name,
                "tag": tag,
                "command": " ".join(cmd)
            }
            # 添加脚本参数到审计日志
            if script_params:
                audit_details["script_params"] = script_params
                
            audit_log = AuditLog(
                user_id=user_id,
                action="start_script",
                resource_type="INDEPENDENT_SCRIPT",
                resource_id=str(process.pid),
                details=audit_details
            )
            db.add(audit_log)
            db.commit()
            
            return {
                "status": "started",
                "pid": process.pid,
                "script_type": script_type,
                "script_name": script_name,
                "tag": tag,
                "log_file": log_file_path,
                "start_time": db_process.started_at.isoformat()
            }
            
        except Exception as e:
            db.rollback()
            raise ProcessError(f"启动{self.script_names.get(script_type, script_type)}失败: {str(e)}")
    
    def stop_independent_script(self, script_type: str, user_id: int, db: Session, force: bool = False) -> Dict[str, Any]:
        """停止独立脚本"""
        try:
            # 验证脚本类型
            if script_type not in self.script_paths:
                raise ValidationError(f"不支持的脚本类型: {script_type}")
            
            # 获取当前运行的脚本进程
            db_process = db.query(DiggingProcess).filter(
                DiggingProcess.script_type == script_type,
                DiggingProcess.status == "running"
            ).first()
            
            if not db_process:
                raise ProcessError(f"{self.script_names[script_type]}未在运行")
            
            pid = db_process.process_id
            script_name = self.script_names[script_type]
            
            # 终止进程
            try:
                # 使用跨平台方法终止进程
                terminate_method = self._terminate_process_group(pid, force)
                
                if not force:
                    # 等待一段时间，如果还没终止则强制终止
                    time.sleep(5)
                    try:
                        process = psutil.Process(pid)
                        if process.is_running():
                            terminate_method = self._terminate_process_group(pid, force=True)
                            terminate_method += " (after timeout)"
                    except psutil.NoSuchProcess:
                        pass  # 进程已经终止
                        
            except ProcessLookupError:
                # 进程已经不存在
                pass
            except Exception as e:
                if not force:
                    raise ProcessError(f"终止进程失败: {str(e)}")
                # 如果是强制终止，忽略错误
            
            # 更新数据库状态
            db_process.status = "stopped"
            db_process.stopped_at = datetime.now()
            db.commit()
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="stop_script",
                resource_type="INDEPENDENT_SCRIPT",
                resource_id=str(pid),
                details={
                    "script_type": script_type,
                    "script_name": script_name,
                    "tag": db_process.tag_name,
                    "terminate_method": terminate_method
                }
            )
            db.add(audit_log)
            db.commit()
            
            return {
                "status": "stopped",
                "pid": pid,
                "script_type": script_type,
                "script_name": script_name,
                "tag": db_process.tag_name,
                "terminate_method": terminate_method,
                "stop_time": db_process.stopped_at.isoformat()
            }
            
        except Exception as e:
            db.rollback()
            raise ProcessError(f"停止{self.script_names.get(script_type, script_type)}失败: {str(e)}")
    
    def stop_task_by_id(self, task_id: int, user_id: int, db: Session, force: bool = False) -> Dict[str, Any]:
        """停止特定任务ID的脚本"""
        try:
            # 获取任务记录
            task = db.query(DiggingProcess).filter(DiggingProcess.id == task_id).first()
            if not task:
                raise ProcessError(f"任务不存在: {task_id}")
            
            # 只能停止正在运行的任务
            if task.status != "running":
                raise ProcessError(f"任务未在运行，当前状态: {task.status}")
            
            # 检查是否有进程ID
            if not task.process_id:
                raise ProcessError(f"任务没有关联的进程ID")
            
            # 停止进程
            try:
                import psutil
                process = psutil.Process(task.process_id)
                
                if force:
                    # 强制终止
                    process.kill()
                else:
                    # 优雅停止
                    process.terminate()
                
                # 更新任务状态
                task.status = "stopped"
                task.stopped_at = datetime.now()
                db.commit()
                
                script_name = self.script_names.get(task.script_type, task.script_type)
                display_info = task.tag_name if task.tag_name else f"{script_name} (ID: {task.id})"
                
                return {
                    "message": f"任务停止成功: {display_info}",
                    "task_id": task.id,
                    "script_type": task.script_type,
                    "force": force
                }
                
            except psutil.NoSuchProcess:
                # 进程已经不存在，更新状态
                task.status = "stopped"
                task.stopped_at = datetime.now()
                db.commit()
                
                return {
                    "message": f"进程已停止 (PID {task.process_id} 不存在)",
                    "task_id": task.id,
                    "script_type": task.script_type,
                    "force": force
                }
            except psutil.AccessDenied:
                raise ProcessError(f"没有权限停止进程 {task.process_id}")
            
        except Exception as e:
            db.rollback()
            if isinstance(e, ProcessError):
                raise
            raise ProcessError(f"停止任务失败: {str(e)}")
    
    def delete_task(self, task_id: int, user_id: int, db: Session) -> Dict[str, Any]:
        """删除已停止的任务及其日志"""
        try:
            # 获取任务记录
            task = db.query(DiggingProcess).filter(DiggingProcess.id == task_id).first()
            if not task:
                raise ProcessError(f"任务不存在")
            
            # 只能删除已停止的任务
            if task.status == "running":
                raise ProcessError(f"无法删除正在运行的任务")
            
            # 删除日志文件（如果存在）
            log_deleted = False
            log_deletion_details = []
            
            print(f"\n=== 开始删除任务 {task.id} 的日志文件 ===")
            print(f"任务信息: ID={task.id}, 脚本类型={task.script_type}, 状态={task.status}")
            
            if task.log_file_path:
                print(f"任务日志文件路径: {task.log_file_path}")
                
                # 检查日志目录中的所有文件（调试用）
                log_dir = os.path.dirname(task.log_file_path)
                if os.path.exists(log_dir):
                    all_log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
                    print(f"日志目录 {log_dir} 中的所有日志文件 ({len(all_log_files)}个):")
                    for i, f in enumerate(all_log_files):
                        print(f"  {i+1}. {f}")
                else:
                    print(f"日志目录不存在: {log_dir}")
                
                if os.path.exists(task.log_file_path):
                    try:
                        # 检查文件权限
                        if os.access(task.log_file_path, os.W_OK):
                            os.remove(task.log_file_path)
                            log_deleted = True
                            log_deletion_details.append(f"已删除日志文件: {task.log_file_path}")
                            print(f"成功删除日志文件: {task.log_file_path}")
                        else:
                            log_deletion_details.append(f"权限不足，无法删除: {task.log_file_path}")
                            print(f"警告: 权限不足，无法删除日志文件: {task.log_file_path}")
                    except Exception as e:
                        # 日志删除失败不影响任务删除
                        error_msg = f"删除日志文件失败: {e}"
                        log_deletion_details.append(error_msg)
                        print(f"警告: {error_msg}")
                else:
                    # 日志文件不存在（可能是早期任务使用统一日志文件）
                    info_msg = f"日志文件不存在: {task.log_file_path}"
                    log_deletion_details.append(info_msg)
                    print(f"信息: {info_msg}")
            else:
                # 早期任务可能没有记录日志文件路径
                info_msg = f"任务 {task.id} 没有关联的日志文件路径"
                log_deletion_details.append(info_msg)
                print(f"信息: {info_msg}")
            
            # 尝试删除相关的轮转日志文件和同PID的关联日志
            # 添加安全检查：只有当任务确实已停止时才清理相关日志
            if task.log_file_path and task.status != "running":
                # 进一步检查：验证没有其他正在运行的任务使用相同的日志模式
                if self._is_safe_to_cleanup_logs(task, db):
                    self._cleanup_related_log_files(task, log_deletion_details, db)
                else:
                    warning_msg = f"检测到相关的正在运行任务，跳过相关日志清理以确保安全"
                    log_deletion_details.append(warning_msg)
                    print(f"警告: {warning_msg}")
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="delete_task",
                resource_type="task",
                resource_id=str(task.id),
                details=f"删除任务: {task.script_type} (ID: {task.id})"
                # created_at 会自动设置为当前时间
            )
            db.add(audit_log)
            
            # 删除任务记录
            db.delete(task)
            db.commit()
            
            # 输出日志清理汇总
            print(f"\n=== 任务 {task.id} 删除完成 ===")
            print(f"日志清理详情 ({len(log_deletion_details)}条):")
            for detail in log_deletion_details:
                print(f"  - {detail}")
            
            # 再次检查日志目录（确认清理结果）
            if task.log_file_path:
                log_dir = os.path.dirname(task.log_file_path)
                if os.path.exists(log_dir):
                    remaining_log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
                    print(f"清理后剩余日志文件 ({len(remaining_log_files)}个):")
                    for i, f in enumerate(remaining_log_files):
                        print(f"  {i+1}. {f}")
            
            # 构建返回消息
            message = f"任务已删除"
            if log_deleted:
                message += "，日志文件已清理"
            elif task.log_file_path:
                message += "，日志文件不存在（可能是早期任务）"
            else:
                message += "，无关联日志文件"

            return {
                "status": "success",
                "message": message,
                "task_id": task_id,
                "script_type": task.script_type,
                "log_deleted": log_deleted,
                "log_deletion_details": log_deletion_details  # 添加详细的删除信息
            }
            
        except Exception as e:
            db.rollback()
            raise ProcessError(f"删除任务失败: {str(e)}")
    
    def get_all_scripts_status(self, db: Session) -> Dict[str, Any]:
        """获取所有脚本的状态 - 显示所有历史任务，活跃任务在顶端"""
        try:
            # 返回格式：{'scripts': [脚本实例列表], 'script_types': {类型映射}}
            result = {
                'scripts': [],
                'script_types': self.script_names
            }
            
            # 获取所有进程记录，按状态和时间排序（运行中的在前，然后按时间倒序）
            # 使用CASE语句确保running状态排在stopped状态前面
            from sqlalchemy import case
            all_processes = db.query(DiggingProcess).order_by(
                case(
                    (DiggingProcess.status == "running", 0),
                    (DiggingProcess.status == "stopped", 1),
                    else_=2
                ).asc(),  # running(0) < stopped(1)
                DiggingProcess.started_at.desc()  # 最新的在前
            ).all()
            
            # 处理所有进程记录
            for process_record in all_processes:
                script_info = {
                    "id": process_record.id,
                    "script_type": process_record.script_type,
                    "status": process_record.status,
                    "script_name": self.script_names.get(process_record.script_type, process_record.script_type),
                    "tag": process_record.tag_name,
                    "started_at": process_record.started_at.isoformat(),
                    "log_file": process_record.log_file_path
                }
                
                if process_record.status == "running":
                    # 验证进程是否真的在运行
                    try:
                        process = psutil.Process(process_record.process_id)
                        if process.is_running():
                            script_info["pid"] = process_record.process_id
                        else:
                            # 进程已死，更新数据库状态
                            process_record.status = "stopped"
                            process_record.stopped_at = datetime.now()
                            db.commit()
                            script_info["status"] = "stopped"
                    except psutil.NoSuchProcess:
                        # 进程不存在，更新数据库状态
                        process_record.status = "stopped"
                        process_record.stopped_at = datetime.now()
                        db.commit()
                        script_info["status"] = "stopped"
                else:
                    # 已停止的任务，添加停止时间
                    if process_record.stopped_at:
                        script_info["stopped_at"] = process_record.stopped_at.isoformat()
                
                result['scripts'].append(script_info)
            
            
            return result
            
        except Exception as e:
            raise ProcessError(f"获取脚本状态失败: {str(e)}")
    
    def stop_process(self, user_id: int, db: Session, force: bool = False) -> Dict[str, Any]:
        """停止挖掘进程"""
        try:
            # 获取当前运行的进程
            current_status = self.get_current_process_status(db)
            
            if current_status["status"] != "running":
                raise ProcessError("没有正在运行的挖掘进程")
            
            pid = current_status["pid"]
            if not pid:
                raise ProcessError("无法获取进程PID")
            
            # 获取数据库中的进程记录
            db_process = db.query(DiggingProcess).filter(
                DiggingProcess.process_id == pid,
                DiggingProcess.status == "running"
            ).first()
            
            if not db_process:
                raise ProcessError("数据库中未找到对应的进程记录")
            
            # 终止进程
            try:
                # 使用跨平台方法终止进程
                terminate_method = self._terminate_process_group(pid, force)
                
                if not force:
                    # 等待一段时间，如果还没终止则强制终止
                    time.sleep(5)
                    try:
                        process = psutil.Process(pid)
                        if process.is_running():
                            terminate_method = self._terminate_process_group(pid, force=True)
                            terminate_method += " (after timeout)"
                    except psutil.NoSuchProcess:
                        pass  # 进程已经终止
                        
            except ProcessLookupError:
                # 进程已经不存在
                pass
            
            # 更新数据库状态
            db_process.status = "stopped"
            db_process.stopped_at = datetime.now()
            
            # 记录审计日志
            audit_log = AuditLog(
                user_id=user_id,
                action="STOP_PROCESS",
                resource_type="DIGGING_PROCESS", 
                resource_id=str(pid),
                details={
                    "terminate_method": terminate_method,
                    "force": force
                }
            )
            db.add(audit_log)
            db.commit()
            
            return {
                "status": "stopped",
                "pid": pid,
                "terminate_method": terminate_method,
                "stop_time": db_process.stopped_at.isoformat()
            }
            
        except Exception as e:
            db.rollback()
            raise ProcessError(f"停止进程失败: {str(e)}")
    
    def get_process_logs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """获取进程日志"""
        try:
            log_file = os.path.join(self.project_root, "logs", "unified_digging.log")
            
            if not os.path.exists(log_file):
                return []
            
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 倒序获取最新的日志
            lines = lines[::-1]
            
            # 分页
            start = offset
            end = offset + limit
            selected_lines = lines[start:end]
            
            logs = []
            for i, line in enumerate(selected_lines):
                try:
                    # 尝试解析JSON格式的日志
                    log_data = json.loads(line.strip())
                    logs.append({
                        "id": offset + i,
                        "timestamp": log_data.get("timestamp"),
                        "level": log_data.get("level", "INFO"),
                        "message": log_data.get("message", line.strip()),
                        "logger": log_data.get("logger"),
                        "details": log_data
                    })
                except json.JSONDecodeError:
                    # 普通文本格式的日志
                    logs.append({
                        "id": offset + i,
                        "timestamp": None,
                        "level": "INFO",
                        "message": line.strip(),
                        "logger": None,
                        "details": {}
                    })
            
            return logs
            
        except Exception as e:
            raise ProcessError(f"获取进程日志失败: {str(e)}")
    
    def _create_config_file(self, config: DiggingConfig) -> str:
        """创建配置文件"""
        try:
            # 生成临时配置文件名
            timestamp = int(time.time())
            temp_config_path = os.path.join(
                self.project_root, 
                "config", 
                f"digging_config_temp_{timestamp}.txt"
            )
            
            # 读取原始配置文件
            with open(self.config_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 更新配置值
            config_dict = config.dict()
            
            # 字段名映射：数据库字段名 -> 配置文件字段名
            field_name_mapping = {
                'dataset_id': 'priority_dataset'  # 数据库中的dataset_id对应配置文件中的priority_dataset
            }
            
            updated_lines = []
            
            for line in lines:
                if ':' in line and not line.strip().startswith('#'):
                    key = line.split(':')[0].strip()
                    
                    # 查找匹配的配置值（考虑字段名映射）
                    config_value = None
                    if key in config_dict:
                        config_value = config_dict[key]
                    else:
                        # 检查是否有反向映射
                        for db_field, config_field in field_name_mapping.items():
                            if config_field == key and db_field in config_dict:
                                config_value = config_dict[db_field]
                                break
                    
                    if config_value is not None:
                        # 特殊处理recommended_fields（需要转为JSON字符串）
                        if key == 'recommended_fields':
                            value = json.dumps(config_value)
                        else:
                            value = str(config_value).lower() if isinstance(config_value, bool) else str(config_value)
                        updated_lines.append(f"{key}: {value}\n")
                    else:
                        updated_lines.append(line)
                else:
                    updated_lines.append(line)
            
            # 写入临时配置文件
            with open(temp_config_path, 'w', encoding='utf-8') as f:
                f.writelines(updated_lines)
            
            return temp_config_path
            
        except Exception as e:
            raise ProcessError(f"创建配置文件失败: {str(e)}")
    
    def _is_safe_to_cleanup_logs(self, task, db) -> bool:
        """检查是否安全清理日志文件（确保不会影响其他正在运行的任务）"""
        try:
            if not task.log_file_path:
                return True
            
            # 解析当前任务的日志文件名
            log_filename = os.path.basename(task.log_file_path)
            
            # 提取PID
            import re
            match = re.match(r'^(.+)_(\d{8})_(\d{6})_(\d+)\.log$', log_filename)
            if not match:
                # 无法解析文件名格式，保守起见不清理
                return False
            
            script_type = match.group(1)
            date_part = match.group(2)
            time_part = match.group(3)
            pid = match.group(4)
            
            # 检查数据库中是否有其他正在运行的任务使用相同的脚本类型
            from sqlalchemy.orm import Session
            from ..db.models import DiggingProcess
            
            running_tasks = db.query(DiggingProcess).filter(
                DiggingProcess.status == "running",
                DiggingProcess.script_type == script_type,
                DiggingProcess.id != task.id
            ).all()
            
            if running_tasks:
                print(f"安全检查: 发现 {len(running_tasks)} 个正在运行的 {script_type} 任务，跳过日志清理")
                return False
            
            return True
            
        except Exception as e:
            print(f"安全检查异常: {e}，保守起见跳过日志清理")
            return False

    def _is_pid_in_use_by_running_tasks(self, pid: str, script_type: str, exclude_task_id: int, db) -> bool:
        """检查指定PID是否被其他正在运行的任务使用"""
        try:
            from ..db.models import DiggingProcess
            
            # 查找使用相同PID且正在运行的任务
            running_tasks_with_pid = db.query(DiggingProcess).filter(
                DiggingProcess.status == "running",
                DiggingProcess.script_type == script_type,
                DiggingProcess.id != exclude_task_id,
                DiggingProcess.process_id == int(pid)
            ).all()
            
            return len(running_tasks_with_pid) > 0
            
        except (ValueError, Exception) as e:
            print(f"PID使用检查异常: {e}，保守起见认为正在使用")
            return True  # 保守策略：有疑虑就不删除

    def _cleanup_related_log_files(self, task, log_deletion_details, db):
        """清理与任务相关的日志文件"""
        import re
        
        try:
            log_dir = os.path.dirname(task.log_file_path)
            if not os.path.exists(log_dir):
                return
            
            # 解析当前任务的日志文件名
            log_filename = os.path.basename(task.log_file_path)
            
            # 提取文件名组件: script_type_YYYYMMDD_HHMMSS_pid.log
            match = re.match(r'^(.+)_(\d{8})_(\d{6})_(\d+)\.log$', log_filename)
            if not match:
                # 如果无法解析，使用原来的轮转日志清理逻辑
                self._cleanup_rotated_logs(task.log_file_path, log_deletion_details)
                return
            
            script_type = match.group(1)
            date_part = match.group(2)
            time_part = match.group(3)
            pid = match.group(4)
            
            print(f"清理任务日志: 脚本={script_type}, 日期={date_part}, 时间={time_part}, PID={pid}")
            
            # 查找相关日志文件（严格匹配，避免误删）
            related_files = []
            
            for file in os.listdir(log_dir):
                if not file.endswith('.log'):
                    continue
                
                file_path = os.path.join(log_dir, file)
                
                # 1. 轮转日志文件 (确切匹配: 原文件名.log.数字)
                if file.startswith(log_filename + ".") and file != log_filename:
                    # 检查是否为纯数字后缀的轮转文件
                    suffix = file[len(log_filename)+1:]
                    if suffix.isdigit():
                        related_files.append((file_path, "轮转日志"))
                        print(f"找到轮转日志: {file}")
                
                # 2. 【已移除】同PID关联日志清理逻辑
                # 
                # 原因：不同的任务实例可能使用相同的PID，删除一个任务时不应该
                # 仅仅因为PID相同就删除其他任务的日志文件。
                # 例如：
                # - unified_digging_20250904_213749_1.log (任务A, PID=1)  
                # - unified_digging_20250904_213838_1.log (任务B, PID=1)
                # 删除任务B时，不应该删除任务A的日志。
                # 
                # 注释原始逻辑：
                # elif file != log_filename and file.endswith(f"_{pid}.log"):
                #     if file.startswith(f"{script_type}_"):
                #         related_files.append((file_path, "同脚本同PID关联日志"))
                
                # 【保留注释以便调试】跳过同PID但不同任务的日志文件
                elif file != log_filename and file.endswith(f"_{pid}.log"):
                    if re.match(rf'^.+_\d{{8}}_\d{{6}}_{re.escape(pid)}\.log$', file):
                        if file.startswith(f"{script_type}_"):
                            print(f"🚫 跳过同脚本同PID日志: {file} (可能是不同任务实例)")
                        else:
                            print(f"🚫 跳过不同脚本类型的同PID日志: {file} (不同脚本类型)")
                
                # 3. 查找同一时间戳但不同PID的日志文件（处理双重日志问题）
                elif file != log_filename and file.startswith(f"{script_type}_{date_part}_{time_part}_"):
                    # 验证文件名格式：script_type_YYYYMMDD_HHMMSS_differentPID.log
                    import re
                    match_pattern = rf'^{re.escape(script_type)}_{re.escape(date_part)}_{re.escape(time_part)}_(\d+)\.log$'
                    if re.match(match_pattern, file):
                        # 进一步安全检查：确保这个PID不属于其他正在运行的任务
                        other_pid = re.match(match_pattern, file).group(1)
                        if not self._is_pid_in_use_by_running_tasks(other_pid, script_type, task.id, db):
                            related_files.append((file_path, "同时间戳关联日志"))
                            print(f"找到同时间戳日志: {file} (PID: {other_pid})")
                        else:
                            print(f"跳过同时间戳日志: {file} (PID {other_pid} 正在被其他任务使用)")
                
                # 4. 不删除其他任何文件，避免误删正在运行的任务日志
            
            # 删除找到的相关文件
            for file_path, file_type in related_files:
                try:
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
                        os.remove(file_path)
                        log_deletion_details.append(f"已删除{file_type}: {os.path.basename(file_path)} ({file_size:.2f}MB)")
                        print(f"已删除{file_type}: {os.path.basename(file_path)} ({file_size:.2f}MB)")
                except Exception as e:
                    log_deletion_details.append(f"删除{file_type}失败: {os.path.basename(file_path)} - {e}")
                    print(f"警告: 删除{file_type}失败: {os.path.basename(file_path)} - {e}")
            
            if related_files:
                print(f"清理完成: 删除了 {len(related_files)} 个相关日志文件")
            else:
                print(f"没有找到需要清理的相关日志文件")
                
        except Exception as e:
            log_deletion_details.append(f"清理相关日志文件时出错: {e}")
            print(f"警告: 清理相关日志文件时出错: {e}")
    
    def _cleanup_rotated_logs(self, log_file_path, log_deletion_details):
        """清理轮转日志文件（兼容旧逻辑）- 修复过度删除问题"""
        try:
            log_dir = os.path.dirname(log_file_path)
            log_filename = os.path.basename(log_file_path)
            
            if not os.path.exists(log_dir):
                return
            
            # 查找相关的轮转日志文件 (只查找确切的轮转文件: original.log.1, original.log.2, 等)
            rotated_files = []
            for file in os.listdir(log_dir):
                file_path = os.path.join(log_dir, file)
                
                # 只匹配确切的轮转文件格式: 原文件名.log.数字
                # 例如: correlation_checker_20250904_192615_123.log.1
                if file.startswith(log_filename + ".") and file != log_filename:
                    # 进一步检查是否为数字后缀（轮转文件的特征）
                    suffix = file[len(log_filename)+1:]  # 去掉 "原文件名." 部分
                    if suffix.isdigit():  # 只有纯数字后缀才是轮转文件
                        rotated_files.append(file_path)
                        print(f"找到轮转日志文件: {file}")
            
            # 删除轮转日志文件
            for rotated_file in rotated_files:
                try:
                    if os.path.exists(rotated_file):
                        file_size = os.path.getsize(rotated_file) / 1024 / 1024  # MB
                        os.remove(rotated_file)
                        log_deletion_details.append(f"已删除轮转日志: {os.path.basename(rotated_file)} ({file_size:.2f}MB)")
                        print(f"已删除轮转日志: {os.path.basename(rotated_file)} ({file_size:.2f}MB)")
                except Exception as e:
                    log_deletion_details.append(f"删除轮转日志失败: {os.path.basename(rotated_file)} - {e}")
                    print(f"警告: 删除轮转日志失败: {os.path.basename(rotated_file)} - {e}")
            
            if rotated_files:
                print(f"轮转日志清理完成: 删除了 {len(rotated_files)} 个轮转文件")
            else:
                print(f"没有找到需要清理的轮转日志文件")
                    
        except Exception as e:
            log_deletion_details.append(f"查找轮转日志时出错: {e}")
            print(f"警告: 查找轮转日志时出错: {e}")

    def cleanup_temp_configs(self):
        """清理临时配置文件"""
        try:
            config_dir = os.path.join(self.project_root, "config")
            current_time = time.time()
            
            for filename in os.listdir(config_dir):
                if filename.startswith("digging_config_temp_"):
                    file_path = os.path.join(config_dir, filename)
                    file_time = os.path.getctime(file_path)
                    
                    # 删除超过1小时的临时配置文件
                    if current_time - file_time > 3600:
                        os.remove(file_path)
                        
        except Exception as e:
            # 清理失败不抛出异常，只记录日志
            print(f"清理临时配置文件失败: {str(e)}")

    def _ensure_log_rotation(self, log_file_path: str, max_size_mb: int = 10, max_backups: int = 3):
        """
        父进程级别的日志轮转管理
        
        Args:
            log_file_path: 日志文件路径
            max_size_mb: 最大文件大小(MB)，默认10MB
            max_backups: 最大备份文件数量，默认3个
        """
        try:
            # 如果日志文件不存在，无需轮转
            if not os.path.exists(log_file_path):
                return
            
            # 检查文件大小
            file_size_mb = os.path.getsize(log_file_path) / 1024 / 1024
            
            if file_size_mb <= max_size_mb:
                return  # 文件未超过大小限制
            
            print(f"📏 日志文件超过大小限制: {file_size_mb:.2f}MB > {max_size_mb}MB，开始轮转...")
            
            # 执行轮转：向后移动现有备份文件
            for i in range(max_backups, 0, -1):
                old_backup = f"{log_file_path}.{i}"
                new_backup = f"{log_file_path}.{i+1}"
                
                if i == max_backups:
                    # 删除最老的备份
                    if os.path.exists(old_backup):
                        os.remove(old_backup)
                        print(f"🗑️ 删除最老备份: {os.path.basename(old_backup)}")
                else:
                    # 移动备份文件
                    if os.path.exists(old_backup):
                        os.rename(old_backup, new_backup)
                        print(f"📦 移动备份: {os.path.basename(old_backup)} → {os.path.basename(new_backup)}")
            
            # 将当前文件移动为第一个备份
            first_backup = f"{log_file_path}.1"
            os.rename(log_file_path, first_backup)
            print(f"🔄 轮转完成: {os.path.basename(log_file_path)} → {os.path.basename(first_backup)}")
            
            # 创建新的日志文件头部信息
            with open(log_file_path, 'w', encoding='utf-8') as f:
                f.write(f"🔄 日志文件轮转完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"📏 轮转原因: 文件大小 {file_size_mb:.2f}MB 超过限制 {max_size_mb}MB\n")
                f.write(f"📦 备份文件: {os.path.basename(first_backup)}\n")
                f.write("=" * 60 + "\n\n")
            
            print(f"✅ 日志轮转完成，创建新文件: {os.path.basename(log_file_path)}")
            
        except Exception as e:
            print(f"❌ 日志轮转失败: {e}")
            # 轮转失败不影响进程启动，继续执行


# 全局实例
process_service = ProcessService()
