"""
数据集分库管理器
作者：e.e.
日期：2025.09.05

功能：
- 为每个数据集创建独立的SQLite数据库
- 只存储factor_expressions表
- 其他表仍使用主数据库
- 提供统一的查询接口
"""

import os
import sqlite3
import threading
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from database.db_manager import FactorDatabaseManager


class PartitionedFactorManager:
    """数据集分库因子管理器"""
    
    def __init__(self, main_db_path: str, partitions_dir: str = None):
        """
        初始化分库管理器
        
        Args:
            main_db_path: 主数据库路径
            partitions_dir: 分库存储目录，默认为主数据库同目录下的partitions文件夹
        """
        self.main_db_path = main_db_path
        
        # 设置分库目录
        if partitions_dir is None:
            db_dir = os.path.dirname(main_db_path)
            self.partitions_dir = os.path.join(db_dir, 'partitions')
        else:
            self.partitions_dir = partitions_dir
            
        # 确保分库目录存在
        os.makedirs(self.partitions_dir, exist_ok=True)
        
        # 主数据库管理器（用于其他表）
        self.main_db = FactorDatabaseManager(main_db_path)
        
        # 分库连接池
        self._partition_connections = {}
        self._connection_locks = {}
        self._global_lock = threading.Lock()
        
    def _get_partition_db_path(self, dataset_id: str) -> str:
        """获取数据集分库路径"""
        return os.path.join(self.partitions_dir, f'dataset_{dataset_id}.db')
    
    def _ensure_partition_db(self, dataset_id: str) -> str:
        """确保数据集分库存在并初始化"""
        db_path = self._get_partition_db_path(dataset_id)
        
        if not os.path.exists(db_path):
            # 创建新的分库
            conn = sqlite3.connect(db_path)
            conn.execute('''
                CREATE TABLE factor_expressions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    expression TEXT NOT NULL UNIQUE,
                    dataset_id VARCHAR(50) NOT NULL,
                    region VARCHAR(10) NOT NULL,
                    step INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(expression, dataset_id, region, step)
                )
            ''')
            
            # 创建优化索引
            conn.execute('''
                CREATE INDEX idx_expressions_region_step 
                ON factor_expressions(region, step)
            ''')
            conn.execute('''
                CREATE INDEX idx_expressions_created 
                ON factor_expressions(created_at)
            ''')
            conn.execute('''
                CREATE INDEX idx_expressions_covering 
                ON factor_expressions(region, step, expression)
            ''')
            
            conn.commit()
            conn.close()
            print(f"✅ 创建数据集分库: {db_path}")
        
        return db_path
    
    @contextmanager
    def _get_partition_connection(self, dataset_id: str):
        """获取数据集分库连接（线程安全）"""
        with self._global_lock:
            if dataset_id not in self._connection_locks:
                self._connection_locks[dataset_id] = threading.Lock()
        
        with self._connection_locks[dataset_id]:
            db_path = self._ensure_partition_db(dataset_id)
            
            if dataset_id not in self._partition_connections:
                self._partition_connections[dataset_id] = sqlite3.connect(
                    db_path, check_same_thread=False
                )
                self._partition_connections[dataset_id].row_factory = sqlite3.Row
            
            yield self._partition_connections[dataset_id]
    
    def add_factor_expression(self, expression: str, dataset_id: str, 
                            region: str, step: int) -> bool:
        """添加单个因子表达式到对应的数据集分库"""
        try:
            with self._get_partition_connection(dataset_id) as conn:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO factor_expressions 
                    (expression, dataset_id, region, step) 
                    VALUES (?, ?, ?, ?)
                """, (expression, dataset_id, region, step))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"❌ 添加因子表达式失败 [{dataset_id}]: {e}")
            return False
    
    def add_factor_expressions_batch(self, expressions: List[str], dataset_id: str, 
                                   region: str, step: int) -> int:
        """批量添加因子表达式到对应的数据集分库"""
        try:
            with self._get_partition_connection(dataset_id) as conn:
                data = [(expr, dataset_id, region, step) for expr in expressions]
                cursor = conn.executemany("""
                    INSERT OR IGNORE INTO factor_expressions 
                    (expression, dataset_id, region, step) 
                    VALUES (?, ?, ?, ?)
                """, data)
                conn.commit()
                return cursor.rowcount
        except Exception as e:
            print(f"❌ 批量添加因子表达式失败 [{dataset_id}]: {e}")
            return 0
    
    def get_factor_expressions(self, dataset_id: str, region: str, step: int) -> List[str]:
        """从对应的数据集分库获取因子表达式列表"""
        try:
            with self._get_partition_connection(dataset_id) as conn:
                cursor = conn.execute("""
                    SELECT expression FROM factor_expressions 
                    WHERE dataset_id = ? AND region = ? AND step = ?
                    ORDER BY created_at
                """, (dataset_id, region, step))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"❌ 获取因子表达式失败 [{dataset_id}]: {e}")
            return []
    
    def is_expression_exists(self, expression: str, dataset_id: str, 
                           region: str, step: int) -> bool:
        """检查表达式是否已存在于对应的数据集分库"""
        try:
            with self._get_partition_connection(dataset_id) as conn:
                cursor = conn.execute("""
                    SELECT 1 FROM factor_expressions 
                    WHERE expression = ? AND dataset_id = ? AND region = ? AND step = ?
                    LIMIT 1
                """, (expression, dataset_id, region, step))
                return cursor.fetchone() is not None
        except Exception as e:
            print(f"❌ 检查表达式存在性失败 [{dataset_id}]: {e}")
            return False
    
    def get_expression_count(self, dataset_id: str, region: str = None, step: int = None) -> int:
        """获取指定数据集的表达式数量"""
        try:
            with self._get_partition_connection(dataset_id) as conn:
                if region and step:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM factor_expressions 
                        WHERE dataset_id = ? AND region = ? AND step = ?
                    """, (dataset_id, region, step))
                else:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM factor_expressions 
                        WHERE dataset_id = ?
                    """, (dataset_id,))
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"❌ 获取表达式数量失败 [{dataset_id}]: {e}")
            return 0
    
    def get_all_datasets(self) -> List[str]:
        """获取所有已创建的数据集分库列表"""
        datasets = []
        if os.path.exists(self.partitions_dir):
            for filename in os.listdir(self.partitions_dir):
                if filename.startswith('dataset_') and filename.endswith('.db'):
                    dataset_id = filename[8:-3]  # 移除 'dataset_' 前缀和 '.db' 后缀
                    datasets.append(dataset_id)
        return sorted(datasets)
    
    def get_partition_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有分库的统计信息"""
        stats = {}
        for dataset_id in self.get_all_datasets():
            try:
                with self._get_partition_connection(dataset_id) as conn:
                    # 总记录数
                    cursor = conn.execute("SELECT COUNT(*) FROM factor_expressions")
                    total_count = cursor.fetchone()[0]
                    
                    # 按地区统计
                    cursor = conn.execute("""
                        SELECT region, COUNT(*) as count 
                        FROM factor_expressions 
                        GROUP BY region
                    """)
                    by_region = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # 按步骤统计
                    cursor = conn.execute("""
                        SELECT step, COUNT(*) as count 
                        FROM factor_expressions 
                        GROUP BY step
                    """)
                    by_step = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # 数据库文件大小
                    db_path = self._get_partition_db_path(dataset_id)
                    file_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
                    
                    stats[dataset_id] = {
                        'total_expressions': total_count,
                        'by_region': by_region,
                        'by_step': by_step,
                        'db_size_mb': round(file_size / 1024 / 1024, 2),
                        'db_path': db_path
                    }
            except Exception as e:
                print(f"❌ 获取分库统计失败 [{dataset_id}]: {e}")
                stats[dataset_id] = {'error': str(e)}
        
        return stats
    
    def migrate_from_main_db(self, dataset_ids: List[str] = None) -> Dict[str, int]:
        """从主数据库迁移数据到分库"""
        migration_stats = {}
        
        try:
            # 从主数据库获取所有factor_expressions数据
            with self.main_db.get_connection() as conn:
                if dataset_ids:
                    placeholders = ','.join(['?' for _ in dataset_ids])
                    cursor = conn.execute(f"""
                        SELECT dataset_id, region, step, expression 
                        FROM factor_expressions 
                        WHERE dataset_id IN ({placeholders})
                        ORDER BY dataset_id, created_at
                    """, dataset_ids)
                else:
                    cursor = conn.execute("""
                        SELECT dataset_id, region, step, expression 
                        FROM factor_expressions 
                        ORDER BY dataset_id, created_at
                    """)
                
                # 按数据集分组
                dataset_expressions = {}
                for row in cursor.fetchall():
                    dataset_id, region, step, expression = row
                    if dataset_id not in dataset_expressions:
                        dataset_expressions[dataset_id] = []
                    dataset_expressions[dataset_id].append((expression, region, step))
                
                # 迁移到对应分库
                for dataset_id, expressions in dataset_expressions.items():
                    print(f"🔄 迁移数据集 {dataset_id}: {len(expressions)} 条记录")
                    
                    with self._get_partition_connection(dataset_id) as partition_conn:
                        data = [(expr, dataset_id, region, step) for expr, region, step in expressions]
                        cursor = partition_conn.executemany("""
                            INSERT OR IGNORE INTO factor_expressions 
                            (expression, dataset_id, region, step) 
                            VALUES (?, ?, ?, ?)
                        """, data)
                        partition_conn.commit()
                        migration_stats[dataset_id] = cursor.rowcount
                        print(f"✅ 数据集 {dataset_id} 迁移完成: {cursor.rowcount} 条记录")
        
        except Exception as e:
            print(f"❌ 数据迁移失败: {e}")
        
        return migration_stats
    
    def cleanup_main_db_expressions(self, dataset_ids: List[str] = None) -> int:
        """清理主数据库中已迁移的factor_expressions数据"""
        try:
            with self.main_db.get_connection() as conn:
                if dataset_ids:
                    placeholders = ','.join(['?' for _ in dataset_ids])
                    cursor = conn.execute(f"""
                        DELETE FROM factor_expressions 
                        WHERE dataset_id IN ({placeholders})
                    """, dataset_ids)
                else:
                    cursor = conn.execute("DELETE FROM factor_expressions")
                
                conn.commit()
                deleted_count = cursor.rowcount
                print(f"✅ 主数据库清理完成: 删除 {deleted_count} 条记录")
                return deleted_count
        except Exception as e:
            print(f"❌ 主数据库清理失败: {e}")
            return 0
    
    def close_all_connections(self):
        """关闭所有分库连接"""
        with self._global_lock:
            for conn in self._partition_connections.values():
                try:
                    conn.close()
                except:
                    pass
            self._partition_connections.clear()
            self._connection_locks.clear()
    
    # 代理主数据库的其他方法
    def __getattr__(self, name):
        """代理到主数据库管理器的其他方法"""
        return getattr(self.main_db, name)


# 便捷函数
def create_partitioned_manager(db_path: str = None) -> PartitionedFactorManager:
    """创建分库管理器实例"""
    if db_path is None:
        # 使用默认路径
        current_dir = os.path.dirname(__file__)
        db_path = os.path.join(current_dir, 'factors.db')
    
    return PartitionedFactorManager(db_path)


if __name__ == "__main__":
    # 测试代码
    manager = create_partitioned_manager()
    
    # 显示分库统计
    stats = manager.get_partition_stats()
    print("📊 分库统计信息:")
    for dataset_id, info in stats.items():
        if 'error' not in info:
            print(f"  {dataset_id}: {info['total_expressions']} 条记录, {info['db_size_mb']} MB")
        else:
            print(f"  {dataset_id}: 错误 - {info['error']}")
