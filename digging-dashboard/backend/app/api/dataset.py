"""
数据集字段查询API
"""

import time
import asyncio
import uuid
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel

# 导入WorldQuant相关模块
import sys
import os
from pathlib import Path

# 添加src目录到Python路径
current_dir = Path(__file__).parent
src_dir = current_dir.parent.parent.parent.parent / 'src'
sys.path.insert(0, str(src_dir))

try:
    from lib.data_client import get_datafields
    from sessions.session_client import get_session
except ImportError as e:
    print(f"Warning: Could not import required modules: {e}")
    get_datafields = None
    get_session = None

router = APIRouter(prefix="/dataset", tags=["dataset"])

# 进度跟踪存储
progress_store = {}

# 移除全局session变量，直接使用session_client

async def get_datafields_with_progress(
    session,
    dataset_id: str,
    region: str = 'USA',
    universe: str = 'TOP3000',
    delay: int = 1,
    instrument_type: str = 'EQUITY',
    task_id: str = None
):
    """
    获取数据集字段信息，并实时更新进度
    这是对machine_lib_ee.get_datafields的包装，添加了分页进度跟踪
    """
    import requests
    import pandas as pd
    
    base_url = "https://api.worldquantbrain.com/data-fields"
    limit = 50
    offset = 0
    aggregated_results = []
    
    # 进度跟踪变量
    page_count = 0
    start_time = time.time()
    estimated_total = None

    # 构建固定查询参数
    def make_params(current_offset: int):
        params = {
            'instrumentType': instrument_type,
            'region': region,
            'delay': str(delay),
            'universe': universe,
            'limit': str(limit),
            'offset': str(current_offset),
        }
        if dataset_id:
            params['dataset.id'] = dataset_id
        return params

    # 进度更新函数
    def update_progress(progress_percent, message, details):
        if task_id and task_id in progress_store:
            progress_store[task_id].update({
                'progress': progress_percent,
                'message': message,
                'details': details
            })
            print(f"📊 进度更新: {progress_percent}% - {message}")

    try:
        while True:
            params = make_params(offset)
            
            # 发送请求
            resp = session.get(base_url, params=params, timeout=15)
            
            if resp.status_code != 200:
                print(f"get_datafields_with_progress: HTTP {resp.status_code}: {resp.text[:200]}")
                break
            
            try:
                data = resp.json()
            except Exception as e:
                print(f"get_datafields_with_progress: JSON解析失败: {e}")
                break
            
            # 处理结果
            results = data.get('results', [])
            if not results:
                break
                
            aggregated_results.extend(results)
            page_count += 1
            
            # 首次获取，尝试估算总记录数
            if estimated_total is None and 'count' in data:
                estimated_total = data['count']
            elif estimated_total is None and len(results) == limit:
                # 粗略估算为至少2倍当前已获取数
                estimated_total = len(aggregated_results) * 2
            
            # 计算并更新进度
            current_count = len(aggregated_results)
            elapsed_time = time.time() - start_time
            
            # 计算进度百分比
            if estimated_total and estimated_total > 0:
                progress_percent = min(95, int((current_count / estimated_total) * 100))
            else:
                # 没有总数时，基于页数给出渐进进度
                progress_percent = min(80, 30 + page_count * 5)
            
            # 估算剩余时间
            if page_count > 1 and current_count > 0:
                avg_time_per_record = elapsed_time / current_count
                if estimated_total:
                    remaining_records = estimated_total - current_count
                    estimated_remaining_time = remaining_records * avg_time_per_record
                else:
                    estimated_remaining_time = None
            else:
                estimated_remaining_time = None
            
            # 构造进度消息
            if estimated_total:
                message = f"正在获取数据集字段... 第{page_count}页，已获取 {current_count}/{estimated_total} 个字段"
            else:
                message = f"正在获取数据集字段... 第{page_count}页，已获取 {current_count} 个字段"
            
            if estimated_remaining_time and estimated_remaining_time > 0:
                message += f"，预计剩余 {estimated_remaining_time:.1f}秒"
            
            # 更新进度
            update_progress(progress_percent, message, {
                'page_count': page_count,
                'current_count': current_count,
                'estimated_total': estimated_total,
                'elapsed_time': elapsed_time,
                'estimated_remaining_time': estimated_remaining_time
            })
            
            # 结束条件：返回数量小于limit
            if len(results) < limit:
                # 最后一次进度更新，设为100%
                final_count = len(aggregated_results)
                final_elapsed = time.time() - start_time
                update_progress(100, f"完成！共获取 {final_count} 个字段，耗时 {final_elapsed:.2f}秒", {
                    'page_count': page_count,
                    'current_count': final_count,
                    'estimated_total': final_count,
                    'elapsed_time': final_elapsed,
                    'estimated_remaining_time': 0
                })
                break
            
            # 准备下一页
            offset += limit
            
            # 短暂延迟，避免请求过快
            await asyncio.sleep(0.1)
        
        return pd.DataFrame(aggregated_results)
        
    except Exception as e:
        print(f"get_datafields_with_progress: 发生异常: {e}")
        # 如果自定义方法失败，回退到原始方法
        return get_datafields(
            session,
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
            instrument_type=instrument_type
        )

class DatasetField(BaseModel):
    """数据集字段模型"""
    id: str
    description: str = ""
    type: str = ""

class DatasetFieldsResponse(BaseModel):
    """数据集字段响应模型"""
    dataset_id: str
    region: str
    universe: str
    delay: int
    total_fields: int
    raw_fields: list[DatasetField]
    fetch_time: Optional[float] = None
    error: Optional[str] = None

class DatasetFieldsProgressDetails(BaseModel):
    """数据集字段获取进度详细信息"""
    page_count: int = 0
    current_count: int = 0
    estimated_total: Optional[int] = None
    elapsed_time: float = 0.0
    estimated_remaining_time: Optional[float] = None

class DatasetFieldsProgressResponse(BaseModel):
    """数据集字段进度响应模型"""
    task_id: str
    status: str  # 'pending', 'running', 'completed', 'failed'
    progress: int  # 0-100
    message: str
    data: Optional[DatasetFieldsResponse] = None
    details: Optional[DatasetFieldsProgressDetails] = None

async def fetch_dataset_fields_async(
    task_id: str,
    dataset_id: str,
    region: str,
    universe: str,
    delay: int,
    instrument_type: str
):
    """异步获取数据集字段，支持详细进度跟踪"""
    try:
        # 更新进度：开始获取
        progress_store[task_id] = {
            'status': 'running',
            'progress': 5,
            'message': f'正在连接到WorldQuant平台...',
            'data': None,
            'details': {
                'page_count': 0,
                'current_count': 0,
                'estimated_total': None,
                'elapsed_time': 0,
                'estimated_remaining_time': None
            }
        }
        
        # 使用session_client获取轻量级session
        print(f"📡 使用SessionClient获取数据集 {dataset_id} 的字段信息")
        session = get_session()
        if session is None:
            raise Exception("无法获取有效的session")
        
        # 更新进度：已连接
        progress_store[task_id]['progress'] = 10
        progress_store[task_id]['message'] = f'已连接，开始请求数据集 {dataset_id} 的字段信息...'
        
        # 获取字段信息（使用自定义分页进度跟踪）
        start_time = time.time()
        df = await get_datafields_with_progress(
            session,
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
            instrument_type=instrument_type,
            task_id=task_id
        )
        fetch_time = time.time() - start_time
        
        print(f"✅ 成功获取数据集 {dataset_id} 的 {len(df)} 个字段 (耗时: {fetch_time:.2f}s)")
        
        # 检查是否有数据
        if df.empty:
            result = DatasetFieldsResponse(
                dataset_id=dataset_id,
                region=region,
                universe=universe,
                delay=delay,
                total_fields=0,
                raw_fields=[],
                fetch_time=fetch_time,
                error="未获取到任何字段信息"
            )
        else:
            # 转换为字段列表
            raw_fields = []
            for _, row in df.iterrows():
                field = DatasetField(
                    id=str(row.get('id', '')),
                    description=str(row.get('description', '')),
                    type=str(row.get('type', ''))
                )
                raw_fields.append(field)
            
            result = DatasetFieldsResponse(
                dataset_id=dataset_id,
                region=region,
                universe=universe,
                delay=delay,
                total_fields=len(raw_fields),
                raw_fields=raw_fields,
                fetch_time=fetch_time
            )
        
        # 更新进度：完成
        if task_id in progress_store:
            progress_store[task_id].update({
                'status': 'completed',
                'progress': 100,
                'message': f'成功获取 {result.total_fields} 个字段',
                'data': result
            })
        else:
            progress_store[task_id] = {
                'status': 'completed',
                'progress': 100,
                'message': f'成功获取 {result.total_fields} 个字段',
                'data': result,
                'details': {
                    'page_count': 0,
                    'current_count': result.total_fields,
                    'estimated_total': result.total_fields,
                    'elapsed_time': fetch_time,
                    'estimated_remaining_time': 0
                }
            }
        
    except Exception as e:
        # 更新进度：失败
        error_msg = str(e)
        print(f"获取数据集字段失败: {error_msg}")
        
        if task_id in progress_store:
            progress_store[task_id].update({
                'status': 'failed',
                'progress': 0,
                'message': f'获取失败: {error_msg}',
                'data': DatasetFieldsResponse(
                    dataset_id=dataset_id,
                    region=region,
                    universe=universe,
                    delay=delay,
                    total_fields=0,
                    raw_fields=[],
                    error=error_msg
                )
            })
        else:
            progress_store[task_id] = {
                'status': 'failed',
                'progress': 0,
                'message': f'获取失败: {error_msg}',
                'data': DatasetFieldsResponse(
                    dataset_id=dataset_id,
                    region=region,
                    universe=universe,
                    delay=delay,
                    total_fields=0,
                    raw_fields=[],
                    error=error_msg
                ),
                'details': {
                    'page_count': 0,
                    'current_count': 0,
                    'estimated_total': 0,
                    'elapsed_time': 0,
                    'estimated_remaining_time': 0
                }
            }

@router.post("/fields/async", response_model=DatasetFieldsProgressResponse)
async def start_dataset_fields_fetch(
    background_tasks: BackgroundTasks,
    dataset_id: str = Query(..., description="数据集ID"),
    region: str = Query("USA", description="地区"),
    universe: str = Query("TOP3000", description="universe"),
    delay: int = Query(1, description="延迟", ge=0, le=30),
    instrument_type: str = Query("EQUITY", description="工具类型")
):
    """
    启动异步获取数据集字段任务
    """
    if get_datafields is None or get_session is None:
        raise HTTPException(
            status_code=500, 
            detail="必需模块未正确加载，无法获取数据集字段"
        )
    
    # 生成任务ID
    task_id = str(uuid.uuid4())
    
    # 初始化进度
    progress_store[task_id] = {
        'status': 'pending',
        'progress': 0,
        'message': '任务已创建，等待开始...',
        'data': None
    }
    
    # 启动后台任务
    background_tasks.add_task(
        fetch_dataset_fields_async,
        task_id,
        dataset_id,
        region,
        universe,
        delay,
        instrument_type
    )
    
    return DatasetFieldsProgressResponse(
        task_id=task_id,
        status='pending',
        progress=0,
        message='任务已创建，正在启动...',
        data=None
    )

@router.get("/fields/progress/{task_id}", response_model=DatasetFieldsProgressResponse)
async def get_dataset_fields_progress(task_id: str):
    """
    获取数据集字段获取任务的进度
    """
    if task_id not in progress_store:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    progress_info = progress_store[task_id]
    
    # 构造详细信息
    details = None
    if 'details' in progress_info:
        details = DatasetFieldsProgressDetails(**progress_info['details'])
    
    return DatasetFieldsProgressResponse(
        task_id=task_id,
        status=progress_info['status'],
        progress=progress_info['progress'],
        message=progress_info['message'],
        data=progress_info['data'],
        details=details
    )

@router.get("/fields", response_model=DatasetFieldsResponse)
async def get_dataset_fields(
    dataset_id: str = Query(..., description="数据集ID"),
    region: str = Query("USA", description="地区"),
    universe: str = Query("TOP3000", description="universe"),
    delay: int = Query(1, description="延迟", ge=0, le=30),
    instrument_type: str = Query("EQUITY", description="工具类型")
):
    """
    获取指定数据集的字段信息
    """
    if get_datafields is None or get_session is None:
        raise HTTPException(
            status_code=500, 
            detail="必需模块未正确加载，无法获取数据集字段"
        )
    
    try:
        # 使用session_client获取轻量级session
        print(f"📡 使用SessionClient获取数据集 {dataset_id} 的字段信息")
        session = get_session()
        if session is None:
            raise HTTPException(status_code=500, detail="无法获取有效的session")
        
        # 获取字段信息
        start_time = time.time()
        df = get_datafields(
            session,
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
            instrument_type=instrument_type
        )
        fetch_time = time.time() - start_time
        
        print(f"✅ 成功获取数据集 {dataset_id} 的 {len(df)} 个字段 (耗时: {fetch_time:.2f}s)")
        
        # 检查是否有数据
        if df.empty:
            return DatasetFieldsResponse(
                dataset_id=dataset_id,
                region=region,
                universe=universe,
                delay=delay,
                total_fields=0,
                raw_fields=[],
                fetch_time=fetch_time,
                error="未获取到任何字段信息"
            )
        
        # 转换为字段列表
        raw_fields = []
        for _, row in df.iterrows():
            field = DatasetField(
                id=str(row.get('id', '')),
                description=str(row.get('description', '')),
                type=str(row.get('type', ''))
            )
            raw_fields.append(field)
        
        return DatasetFieldsResponse(
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
            total_fields=len(raw_fields),
            raw_fields=raw_fields,
            fetch_time=fetch_time
        )
        
    except Exception as e:
        # 记录错误信息
        error_msg = str(e)
        print(f"获取数据集字段失败: {error_msg}")
        
        return DatasetFieldsResponse(
            dataset_id=dataset_id,
            region=region,
            universe=universe,
            delay=delay,
            total_fields=0,
            raw_fields=[],
            error=error_msg
        )
