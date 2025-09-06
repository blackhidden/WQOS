"""
进度跟踪器 (Progress Tracker)
作者：e.e.
日期：2025.09.05

负责跟踪因子挖掘的进度，包括：
- 完成度计算
- 进度统计
- 通知触发判断
- 数据集完成状态检查
"""

import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Tuple, Set, Optional

try:
    from machine_lib_ee import (
        first_order_factory, get_alphas, transform, 
        get_group_second_order_factory
    )
    from digging.utils.common_utils import get_filtered_operators
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from machine_lib_ee import (
        first_order_factory, get_alphas, transform, 
        get_group_second_order_factory
    )
    from digging.utils.common_utils import get_filtered_operators


class ProgressTracker:
    """进度跟踪器 - 负责跟踪和计算因子挖掘的各种进度指标"""
    
    def __init__(self, config_manager, notification_service=None):
        """初始化进度跟踪器
        
        Args:
            config_manager: 配置管理器实例
            notification_service: 通知服务实例（可选）
        """
        self.config_manager = config_manager
        self.notification_service = notification_service
        self.logger = None  # 将在设置时注入
        
        # 通知状态跟踪
        self.notified_thresholds = set()  # 已通知的阈值，避免重复通知
    
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
    
    def get_completed_expressions(self, dataset_id: str, step: int) -> Set[str]:
        """获取已完成的因子表达式
        
        Args:
            dataset_id: 数据集ID
            step: 挖掘步骤
            
        Returns:
            Set[str]: 已完成的因子表达式集合
        """
        try:
            if self.logger:
                self.logger.debug(f"🔍 查询已完成表达式: dataset_id={dataset_id}, region={self.config_manager.region}, step={step}")
            
            db = self.config_manager.get_database_manager()
            expressions = db.get_factor_expressions(
                dataset_id=dataset_id, 
                region=self.config_manager.region, 
                step=step
            )
            
            if self.logger:
                self.logger.info(f"  📊 数据库查询结果: 找到{len(expressions)}个已完成表达式")
            
            if len(expressions) > 0:
                # 显示前几个表达式作为验证
                sample_expressions = expressions[:3]
                for i, expr in enumerate(sample_expressions):
                    if self.logger:
                        self.logger.debug(f"    示例{i+1}: {expr[:50]}...")
            
            return set(expressions)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"  ❌ 从数据库读取已完成表达式失败: {e}")
                import traceback
                traceback.print_exc()
            return set()
    
    def calculate_first_order_progress(self, dataset_id: str) -> Tuple[int, int, float]:
        """计算一阶挖掘进度
        
        Args:
            dataset_id: 数据集ID
            
        Returns:
            Tuple[int, int, float]: (已完成数量, 总数量, 完成率)
        """
        try:
            # 获取已完成的因子表达式
            completed_expressions = self.get_completed_expressions(dataset_id, step=1)
            
            # 生成所有可能的因子表达式来计算总数
            if self.config_manager.use_recommended_fields:
                # 使用配置中的推荐字段
                pc_fields = self.config_manager.get_recommended_fields()
            else:
                # 简化：使用默认字段数量估算
                pc_fields = [f'field_{i}' for i in range(100)]  # 使用固定的估算值
            
            # 获取过滤后的操作符
            ts_ops, basic_ops, group_ops = get_filtered_operators()
            first_order = first_order_factory(pc_fields, ts_ops + basic_ops)
            total_factors = len(first_order)
            
            # 计算完成率
            completed_count = len(completed_expressions)
            completion_rate = completed_count / total_factors * 100 if total_factors > 0 else 0
            
            return completed_count, total_factors, completion_rate
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 计算一阶挖掘进度失败: {e}")
            return 0, 0, 0.0
    
    def calculate_second_order_progress(self, dataset_id: str) -> Tuple[int, int, float]:
        """计算二阶挖掘进度
        
        Args:
            dataset_id: 数据集ID
            
        Returns:
            Tuple[int, int, float]: (已完成数量, 总数量, 完成率)
        """
        try:
            # 获取已完成的二阶因子表达式
            completed_expressions = self.get_completed_expressions(dataset_id, step=2)
            
            # 基于一阶符合条件的因子数量计算二阶总数
            step1_tag = self.config_manager.generate_tag(dataset_id, 1)
            
            fo_tracker = get_alphas("2024-10-07", "2025-12-31",
                                   0.75, 0.5, 100, 100,
                                   self.config_manager.region, 
                                   self.config_manager.universe, 
                                   self.config_manager.delay, 
                                   "EQUITY",
                                   500, "track", tag=step1_tag)
            
            if not fo_tracker['next'] and not fo_tracker['decay']:
                return len(completed_expressions), 0, 100.0  # 没有符合条件的一阶因子
            
            fo_layer = transform(fo_tracker['next'] + fo_tracker['decay'])
            ts_ops, basic_ops, group_ops = get_filtered_operators()
            so_alpha_dict = defaultdict(list)
            for expr, decay in fo_layer:
                for alpha in get_group_second_order_factory([expr], group_ops, self.config_manager.region):
                    so_alpha_dict[self.config_manager.region].append((alpha, decay))
            
            total_factors = len(so_alpha_dict[self.config_manager.region])
            completed_count = len(completed_expressions)
            completion_rate = completed_count / total_factors * 100 if total_factors > 0 else 0
            
            return completed_count, total_factors, completion_rate
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 计算二阶挖掘进度失败: {e}")
            return 0, 0, 0.0
    
    def is_dataset_step_completed(self, dataset_id: str, step: int) -> bool:
        """检查指定数据集的指定步骤是否已完成
        
        Args:
            dataset_id: 数据集ID
            step: 挖掘步骤
            
        Returns:
            bool: 是否已完成
        """
        try:
            if step == 1:
                completed_count, total_factors, completion_rate = self.calculate_first_order_progress(dataset_id)
            elif step == 2:
                completed_count, total_factors, completion_rate = self.calculate_second_order_progress(dataset_id)
            else:
                if self.logger:
                    self.logger.warning(f"⚠️ 不支持的挖掘步骤: {step}")
                return False
            
            if self.logger:
                self.logger.info(f"  📊 数据集 {dataset_id} 第{step}阶完成情况:")
                self.logger.info(f"    已完成: {completed_count:,}")
                self.logger.info(f"    总计: {total_factors:,}")
                self.logger.info(f"    完成率: {completion_rate:.1%}")
            
            # 95%以上算完成
            return completion_rate >= 95.0
            
        except Exception as e:
            if self.logger:
                self.logger.info(f"  ❌ 检查数据集完成状态失败: {e}")
            return False
    
    def get_qualified_factors_count(self, dataset_id: str) -> int:
        """获取符合条件的一阶因子数量（用于触发二阶挖掘）
        
        Args:
            dataset_id: 数据集ID
            
        Returns:
            int: 符合条件的因子数量
        """
        try:
            step1_tag = self.config_manager.generate_tag(dataset_id, 1)
            
            # 查询符合条件的一阶因子
            fo_tracker = get_alphas("2024-10-07", "2025-12-31",
                                   0.75, 0.5, 100, 100,
                                   self.config_manager.region, 
                                   self.config_manager.universe, 
                                   self.config_manager.delay, 
                                   "EQUITY",
                                   500, "track", tag=step1_tag)
            
            total_qualified = len(fo_tracker.get('next', [])) + len(fo_tracker.get('decay', []))
            return total_qualified
            
        except Exception as e:
            if self.logger:
                self.logger.info(f"  ❌ 查询符合条件因子数量失败: {e}")
            return 0
    
    def check_and_send_completion_notification(self, dataset_id: str, completion_rate: float,
                                             completed_count: int, total_count: int, 
                                             remaining_count: int, start_time: datetime,
                                             stage: int = 1):
        """检查是否需要发送完成度通知（避免重复通知）
        
        Args:
            dataset_id: 数据集ID
            completion_rate: 完成率
            completed_count: 已完成数量
            total_count: 总数量
            remaining_count: 剩余数量
            start_time: 开始时间
            stage: 挖掘阶段（默认为1，只有一阶才发送完成度通知）
        """
        try:
            # 只有一阶挖掘才发送完成度通知
            if stage != 1 or not self.notification_service:
                return
            
            # 检查是否达到任何通知阈值
            for threshold in self.config_manager.notification_thresholds:
                if completion_rate >= threshold and threshold not in self.notified_thresholds:
                    if self.logger:
                        self.logger.info(f"🔔 触发完成度通知: {completion_rate:.2f}% >= {threshold}%")
                    
                    # 发送通知
                    success = self.notification_service.send_completion_notification(
                        dataset_id, completion_rate, completed_count, 
                        total_count, remaining_count, start_time
                    )
                    
                    if success:
                        # 标记该阈值已通知
                        self.notified_thresholds.add(threshold)
                        if self.logger:
                            self.logger.info(f"✅ 完成度通知已发送并标记: {threshold}%")
                    else:
                        if self.logger:
                            self.logger.warning(f"❌ 完成度通知发送失败: {threshold}%")
                    
                    # 只发送一次通知（发送最高达到的阈值）
                    break
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 检查完成度通知时出错: {e}")
    
    def reset_notification_state(self):
        """重置通知状态（用于新的数据集或新的挖掘周期）"""
        self.notified_thresholds.clear()
        if self.logger:
            self.logger.info("🔄 通知状态已重置")
    
    def log_progress_summary(self, dataset_id: str, step: int, 
                           completed_count: int, total_count: int, completion_rate: float):
        """记录进度摘要到日志
        
        Args:
            dataset_id: 数据集ID
            step: 挖掘步骤
            completed_count: 已完成数量
            total_count: 总数量
            completion_rate: 完成率
        """
        if self.logger:
            self.logger.info(f"📊 因子统计: 总计{total_count:,}个 | 已完成{completed_count:,}个({completion_rate:.1f}%) | 待处理{total_count - completed_count:,}个")
