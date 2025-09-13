"""
结果收集器 (Result Collector)
作者：e.e.
日期：2025.09.08

提供统一的模拟结果收集和处理功能
"""

from typing import List, Dict, Any, Optional


class ResultCollector:
    """模拟结果收集器 - 统一处理单模拟和多模拟的结果"""
    
    def __init__(self):
        self.logger = None
        self.collected_alpha_ids = []
        self.failed_tasks = []
        
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
        
    def add_alpha_ids(self, alpha_ids: List[str], task_info: Optional[Dict] = None):
        """添加成功的Alpha ID
        
        Args:
            alpha_ids: Alpha ID列表
            task_info: 任务信息（可选）
        """
        if alpha_ids:
            self.collected_alpha_ids.extend(alpha_ids)
            if self.logger:
                self.logger.info(f"✅ 收集到 {len(alpha_ids)} 个Alpha ID")
    
    def add_failed_task(self, task_info: Dict, error: str):
        """添加失败的任务
        
        Args:
            task_info: 任务信息
            error: 错误信息
        """
        self.failed_tasks.append({
            'task_info': task_info,
            'error': error
        })
        if self.logger:
            self.logger.warning(f"❌ 记录失败任务: {error}")
    
    def get_summary(self) -> Dict[str, Any]:
        """获取结果摘要
        
        Returns:
            Dict[str, Any]: 结果摘要
        """
        return {
            'total_alpha_ids': len(self.collected_alpha_ids),
            'alpha_ids': self.collected_alpha_ids,
            'total_failed_tasks': len(self.failed_tasks),
            'failed_tasks': self.failed_tasks,
            'success_rate': len(self.collected_alpha_ids) / (len(self.collected_alpha_ids) + len(self.failed_tasks)) * 100 if (len(self.collected_alpha_ids) + len(self.failed_tasks)) > 0 else 0
        }
    
    def clear(self):
        """清空收集的结果"""
        self.collected_alpha_ids.clear()
        self.failed_tasks.clear()
