"""
Alpha状态查看API
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel
from app.core.auth import get_current_user
from app.db.models import DashboardUser
import os
import sys

# 添加项目根目录到路径，以便导入数据库管理器
project_root = os.environ.get('PROJECT_ROOT', '/app')
sys.path.insert(0, project_root)

# 尝试导入数据库管理器
try:
    from database.db_manager import FactorDatabaseManager
except ImportError as e:
    # 如果导入失败，创建一个模拟的数据库管理器
    import sqlite3
    from contextlib import contextmanager
    
    class FactorDatabaseManager:
        """简化的数据库管理器，用于API服务"""
        
        def __init__(self, db_path):
            self.db_path = db_path
        
        @contextmanager
        def get_connection(self):
            """获取数据库连接的上下文管理器"""
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 使用行工厂
            try:
                yield conn
            finally:
                conn.close()

router = APIRouter()

# 请求模型
class ManualRemoveRequest(BaseModel):
    """手动移除请求模型"""
    alpha_ids: List[str]

# 配置数据库路径
def get_db_manager():
    """获取数据库管理器实例"""
    from app.utils.path_utils import detect_project_root
    project_root = detect_project_root()
    db_path = os.path.join(project_root, 'database', 'factors.db')
    return FactorDatabaseManager(db_path)


@router.get("/submitable")
async def get_submitable_alphas(
    tab: str = Query(..., description="页签类型: ppac, normal, pending"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(50, ge=1, le=200, description="每页数量"),
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """获取可提交Alpha列表
    
    Args:
        tab: 页签类型
            - ppac: PPAC因子 (BLUE色)
            - normal: 普通因子 (GREEN色)  
            - pending: 待检测因子 (YELLOW色)
        page: 页码，从1开始
        page_size: 每页数量，1-200
    
    Returns:
        {
            "data": [Alpha列表],
            "total": 总数量,
            "page": 当前页码,
            "page_size": 每页数量,
            "tab": 页签类型
        }
    """
    try:
        # 验证页签类型
        if tab not in ['ppac', 'normal', 'pending']:
            raise HTTPException(status_code=400, detail=f"无效的页签类型: {tab}")
        
        # 页签到颜色的映射
        tab_color_map = {
            'ppac': 'BLUE',
            'normal': 'GREEN', 
            'pending': 'YELLOW'
        }
        
        color = tab_color_map[tab]
        
        # 获取数据库管理器
        db = get_db_manager()
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询数据
        with db.get_connection() as conn:
            # 查询总数
            count_cursor = conn.execute("""
                SELECT COUNT(*) 
                FROM submitable_alphas 
                WHERE color = ?
            """, (color,))
            total = count_cursor.fetchone()[0]
            
            # 查询数据列表
            if tab == 'ppac':
                # PPAC因子显示prod_corr
                query = """
                    SELECT alpha_id, tags, fitness, sharpe, prod_corr as correlation_value, aggressive_mode
                    FROM submitable_alphas 
                    WHERE color = ?
                    ORDER BY date_created DESC
                    LIMIT ? OFFSET ?
                """
            elif tab == 'normal':
                # 普通因子显示self_corr
                query = """
                    SELECT alpha_id, tags, fitness, sharpe, self_corr as correlation_value, aggressive_mode
                    FROM submitable_alphas 
                    WHERE color = ?
                    ORDER BY date_created DESC
                    LIMIT ? OFFSET ?
                """
            else:  # pending
                # 待检测因子不显示相关性字段
                query = """
                    SELECT alpha_id, tags, fitness, sharpe, NULL as correlation_value, aggressive_mode
                    FROM submitable_alphas 
                    WHERE color = ?
                    ORDER BY date_created DESC
                    LIMIT ? OFFSET ?
                """
            
            cursor = conn.execute(query, (color, page_size, offset))
            rows = cursor.fetchall()
            
            # 转换为字典列表
            alphas = []
            for row in rows:
                alpha = {
                    "alpha_id": row[0],
                    "tags": row[1],
                    "fitness": row[2],
                    "sharpe": row[3],
                    "correlation_value": row[4],
                    "aggressive_mode": bool(row[5]) if row[5] is not None else False
                }
                alphas.append(alpha)
        
        return {
            "data": alphas,
            "total": total,
            "page": page,
            "page_size": page_size,
            "tab": tab
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询Alpha列表失败: {str(e)}")


@router.get("/statistics")
async def get_alpha_statistics(
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """获取Alpha统计信息
    
    Returns:
        {
            "ppac_count": PPAC因子数量,
            "normal_count": 普通因子数量,
            "pending_count": 待检测因子数量,
            "total_count": 总数量
        }
    """
    try:
        # 获取数据库管理器
        db = get_db_manager()
        
        with db.get_connection() as conn:
            # 查询各种颜色的Alpha数量
            cursor = conn.execute("""
                SELECT 
                    color,
                    COUNT(*) as count
                FROM submitable_alphas 
                GROUP BY color
            """)
            
            color_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # 获取总数
            total_cursor = conn.execute("SELECT COUNT(*) FROM submitable_alphas")
            total_count = total_cursor.fetchone()[0]
        
        return {
            "ppac_count": color_counts.get('BLUE', 0),
            "normal_count": color_counts.get('GREEN', 0), 
            "pending_count": color_counts.get('YELLOW', 0),
            "red_count": color_counts.get('RED', 0),  # 可能存在的RED状态
            "total_count": total_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询Alpha统计失败: {str(e)}")


@router.post("/remove")
async def remove_alphas_from_database(
    request: ManualRemoveRequest,
    current_user: DashboardUser = Depends(get_current_user)
) -> Dict[str, Any]:
    """手动从数据库移除Alpha
    
    Args:
        request: 包含alpha_ids列表的请求
    
    Returns:
        {
            "success": True/False,
            "message": "操作结果消息",
            "removed_count": 移除的Alpha数量,
            "failed_alphas": 操作失败的Alpha列表
        }
    """
    try:
        if not request.alpha_ids:
            raise HTTPException(status_code=400, detail="Alpha ID列表不能为空")
        
        # 获取数据库管理器
        db = get_db_manager()
        
        removed_count = 0
        failed_alphas = []
        
        with db.get_connection() as conn:
            for alpha_id in request.alpha_ids:
                try:
                    # 首先检查Alpha是否存在
                    check_cursor = conn.execute("""
                        SELECT alpha_id FROM submitable_alphas 
                        WHERE alpha_id = ?
                    """, (alpha_id,))
                    
                    alpha_row = check_cursor.fetchone()
                    if not alpha_row:
                        failed_alphas.append(f"{alpha_id}: Alpha不存在")
                        continue
                    
                    # 直接从数据库中删除该记录
                    delete_cursor = conn.execute("""
                        DELETE FROM submitable_alphas 
                        WHERE alpha_id = ?
                    """, (alpha_id,))
                    
                    if delete_cursor.rowcount > 0:
                        removed_count += 1
                    else:
                        failed_alphas.append(f"{alpha_id}: 删除失败")
                        
                except Exception as e:
                    failed_alphas.append(f"{alpha_id}: {str(e)}")
            
            # 提交事务
            conn.commit()
        
        # 构建响应消息
        if removed_count == len(request.alpha_ids):
            message = f"成功移除 {removed_count} 个Alpha"
            success = True
        elif removed_count > 0:
            message = f"成功移除 {removed_count} 个Alpha，{len(failed_alphas)} 个失败"
            success = True
        else:
            message = "所有Alpha移除失败"
            success = False
        
        return {
            "success": success,
            "message": message,
            "removed_count": removed_count,
            "failed_alphas": failed_alphas,
            "total_requested": len(request.alpha_ids)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"移除Alpha失败: {str(e)}")
