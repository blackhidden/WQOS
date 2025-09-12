"""
WebSocket API路由
"""

from typing import List, Dict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
import json
import asyncio

router = APIRouter()


class ConnectionManager:
    """WebSocket连接管理器"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_info: Dict[WebSocket, Dict] = {}
    
    async def connect(self, websocket: WebSocket, user_id: int, connection_type: str = "logs"):
        """建立连接"""
        await websocket.accept()
        self.active_connections.append(websocket)
        self.connection_info[websocket] = {
            "user_id": user_id,
            "type": connection_type,
            "connected_at": asyncio.get_event_loop().time()
        }
    
    def disconnect(self, websocket: WebSocket):
        """断开连接"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.connection_info:
            del self.connection_info[websocket]
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        """发送个人消息"""
        try:
            await websocket.send_text(message)
        except:
            self.disconnect(websocket)
    
    async def broadcast(self, message: str, connection_type: str = None):
        """广播消息"""
        disconnected = []
        
        for connection in self.active_connections:
            try:
                # 如果指定了连接类型，只发送给对应类型的连接
                if connection_type:
                    info = self.connection_info.get(connection, {})
                    if info.get("type") != connection_type:
                        continue
                
                await connection.send_text(message)
            except:
                disconnected.append(connection)
        
        # 清理断开的连接
        for connection in disconnected:
            self.disconnect(connection)


# 全局连接管理器
manager = ConnectionManager()


@router.websocket("/logs")
async def websocket_logs_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
    source: str = Query("unified_digging")
):
    """实时日志WebSocket连接"""
    # TODO: 验证token
    
    await manager.connect(websocket, 0, "logs")  # 暂时使用user_id=0
    
    try:
        # 发送欢迎消息
        welcome_message = {
            "type": "connection",
            "message": f"日志流连接已建立 - 源: {source}",
            "timestamp": asyncio.get_event_loop().time(),
            "source": source
        }
        await manager.send_personal_message(json.dumps(welcome_message), websocket)
        
        # 导入日志服务
        from app.services.log_service import log_service
        
        # 创建日志流任务
        async def stream_logs():
            try:
                async for log_entry in log_service.stream_logs(source=source, follow=True):
                    log_message = {
                        "type": "log",
                        "data": log_entry,
                        "source": source,
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    await manager.send_personal_message(json.dumps(log_message), websocket)
            except Exception as e:
                error_message = {
                    "type": "error",
                    "message": f"日志流错误: {str(e)}",
                    "timestamp": asyncio.get_event_loop().time()
                }
                await manager.send_personal_message(json.dumps(error_message), websocket)
        
        # 启动日志流任务
        log_task = asyncio.create_task(stream_logs())
        
        # 保持连接并处理客户端消息
        while True:
            try:
                # 接收客户端消息（心跳等）
                data = await websocket.receive_text()
                
                # 处理客户端消息
                try:
                    message = json.loads(data)
                    if message.get("type") == "ping":
                        pong_message = {
                            "type": "pong",
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(pong_message), websocket)
                    elif message.get("type") == "change_source":
                        new_source = message.get("source", "unified_digging")
                        # 取消当前日志流任务
                        log_task.cancel()
                        # 启动新的日志流任务
                        log_task = asyncio.create_task(stream_logs())
                        source = new_source
                except json.JSONDecodeError:
                    pass
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                print(f"WebSocket错误: {e}")
                break
    
    finally:
        # 清理资源
        if 'log_task' in locals():
            log_task.cancel()
        manager.disconnect(websocket)


@router.websocket("/process-status")
async def websocket_process_status_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
    user_id: int = Query(...)
):
    """实时进程状态WebSocket连接"""
    # TODO: 验证token和user_id
    
    await manager.connect(websocket, user_id, "process")
    
    try:
        # 发送欢迎消息
        welcome_message = {
            "type": "connection",
            "message": "进程状态监控连接已建立",
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.send_personal_message(json.dumps(welcome_message), websocket)
        
        # 保持连接并定期发送状态更新
        while True:
            try:
                # TODO: 获取实际的进程状态
                status_message = {
                    "type": "process_status",
                    "status": "stopped",
                    "timestamp": asyncio.get_event_loop().time()
                }
                await manager.send_personal_message(json.dumps(status_message), websocket)
                
                # 等待5秒后发送下一次更新
                await asyncio.sleep(5)
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                print(f"进程状态WebSocket错误: {e}")
                break
    
    finally:
        manager.disconnect(websocket)


# 提供给其他模块使用的广播函数
async def broadcast_log_message(message: dict):
    """广播日志消息"""
    await manager.broadcast(json.dumps(message), "logs")


async def broadcast_process_status(status: dict):
    """广播进程状态"""
    await manager.broadcast(json.dumps(status), "process")


@router.websocket("/dataset-fields-progress")
async def websocket_dataset_fields_progress_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
    task_id: str = Query(...)
):
    """数据集字段获取进度WebSocket连接"""
    # TODO: 验证token
    
    await manager.connect(websocket, 0, f"dataset-progress-{task_id}")
    
    try:
        # 发送欢迎消息
        welcome_message = {
            "type": "connection",
            "message": f"数据集字段获取进度监控已连接 - 任务ID: {task_id}",
            "timestamp": asyncio.get_event_loop().time(),
            "task_id": task_id
        }
        await manager.send_personal_message(json.dumps(welcome_message), websocket)
        
        # 导入progress store
        from app.api.dataset import progress_store
        
        # 定期发送进度更新
        while True:
            try:
                # 获取当前进度
                progress_info = progress_store.get(task_id)
                if progress_info:
                    # 转换data为字典（如果存在）
                    data = progress_info.get('data')
                    serializable_data = None
                    if data is not None:
                        try:
                            # 如果是Pydantic模型，转换为字典
                            if hasattr(data, 'dict'):
                                serializable_data = data.dict()
                            else:
                                serializable_data = data
                        except Exception:
                            serializable_data = None
                    
                    progress_message = {
                        "type": "progress_update",
                        "task_id": task_id,
                        "status": progress_info['status'],
                        "progress": progress_info['progress'],
                        "message": progress_info['message'],
                        "details": progress_info.get('details'),
                        "data": serializable_data,
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    await manager.send_personal_message(json.dumps(progress_message), websocket)
                    
                    # 如果任务完成或失败，发送最终消息并断开连接
                    if progress_info['status'] in ['completed', 'failed']:
                        final_message = {
                            "type": "task_finished",
                            "task_id": task_id,
                            "status": progress_info['status'],
                            "data": serializable_data,  # 包含序列化后的数据
                            "timestamp": asyncio.get_event_loop().time()
                        }
                        await manager.send_personal_message(json.dumps(final_message), websocket)
                        break
                else:
                    # 任务不存在，可能已经被清理
                    error_message = {
                        "type": "task_not_found",
                        "task_id": task_id,
                        "message": "任务不存在或已被清理",
                        "timestamp": asyncio.get_event_loop().time()
                    }
                    await manager.send_personal_message(json.dumps(error_message), websocket)
                    break
                
                # 等待1秒后发送下一次更新（比轮询更频繁）
                await asyncio.sleep(1)
                
            except WebSocketDisconnect:
                break
            except Exception:
                break
    
    finally:
        manager.disconnect(websocket)


async def broadcast_dataset_progress(task_id: str, progress_data: dict):
    """广播数据集字段获取进度更新"""
    message = {
        "type": "progress_broadcast",
        "task_id": task_id,
        **progress_data,
        "timestamp": asyncio.get_event_loop().time()
    }
    await manager.broadcast(json.dumps(message), f"dataset-progress-{task_id}")
