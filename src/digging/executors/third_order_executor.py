"""
三阶挖掘执行器 (Third Order Executor)
作者：e.e.
日期：2025.09.05

负责执行三阶因子挖掘，包括：
- 基于二阶符合条件因子生成三阶因子
- 持续监控二阶挖掘产出
- 进度跟踪
- 模拟执行
"""

import os
import sys
from collections import defaultdict
from typing import List, Dict, Tuple

from .base_executor import BaseExecutor

try:
    from lib.data_client import get_alphas
    from lib.factor_generator import transform, trade_when_factory
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from lib.data_client import get_alphas
    from lib.factor_generator import transform, trade_when_factory


class ThirdOrderExecutor(BaseExecutor):
    """三阶挖掘执行器 - 负责执行三阶因子挖掘的完整流程"""
    
    def get_stage_number(self) -> int:
        """获取执行器对应的阶段号"""
        return 3
    
    def get_qualified_second_order_factors(self) -> Tuple[List[str], List[str], int]:
        """获取符合条件的二阶因子
        
        Returns:
            Tuple[List[str], List[str], int]: (next因子列表, decay因子列表, 总数量)
        """
        step2_tag = self.config_manager.generate_tag(self.current_dataset, 2)
        
        # 计算最近一年的日期范围（end_date使用明天避免时差问题）
        from datetime import datetime, timedelta
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 获取符合条件的二阶因子
        so_tracker = get_alphas(
            start_date, end_date,
            1.00, 0.75,
            100, 100,
            self.config_manager.region, 
            self.config_manager.universe, 
            self.config_manager.delay, 
            self.config_manager.instrument_type,
            500, "track", tag=step2_tag,
        )
        
        next_factors = so_tracker.get('next', [])
        decay_factors = so_tracker.get('decay', [])
        total_qualified = len(next_factors) + len(decay_factors)
        
        return next_factors, decay_factors, total_qualified
    
    def generate_third_order_factors(self, next_factors: List[str], decay_factors: List[str]) -> List[Tuple[str, int]]:
        """生成三阶因子列表
        
        Args:
            next_factors: next类型的二阶因子
            decay_factors: decay类型的二阶因子
            
        Returns:
            List[Tuple[str, int]]: 三阶因子表达式和衰减值的元组列表
        """
        # 转换二阶因子格式
        so_layer = transform(next_factors + decay_factors)
        
        # 生成三阶因子
        third_order_factors = []
        self.logger.info(f"请构建三阶因子表达式")
        
        if self.logger:
            self.logger.info(f"📊 生成三阶因子: {len(third_order_factors):,} 个")
        
        return third_order_factors
    
    def filter_completed_third_order_factors(self, all_factors: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        """过滤已完成的三阶因子
        
        Args:
            all_factors: 所有三阶因子列表
            
        Returns:
            List[Tuple[str, int]]: 待处理的三阶因子列表
        """
        completed_expressions = self.progress_tracker.get_completed_expressions(
            self.current_dataset, step=3
        )
        
        valid_factors = [alpha_decay for alpha_decay in all_factors 
                        if alpha_decay[0] not in completed_expressions]
        
        completion_rate = len(completed_expressions) / len(all_factors) * 100 if all_factors else 0
        
        if self.logger:
            self.progress_tracker.log_progress_summary(
                self.current_dataset, 3, len(completed_expressions), 
                len(all_factors), completion_rate
            )
        
        return valid_factors
    
    async def execute_third_order_batch(self, valid_factors: List[Tuple[str, int]]) -> List[Dict]:
        """执行三阶因子模拟批次
        
        Args:
            valid_factors: 待处理的三阶因子列表
            
        Returns:
            List[Dict]: 执行结果列表
        """
        if not valid_factors:
            return []
        
        # 分离Alpha表达式和衰减值
        alpha_list = [alpha_decay[0] for alpha_decay in valid_factors]
        decay_list = [alpha_decay[1] for alpha_decay in valid_factors]
        
        # 准备其他参数
        region_list = [(self.config_manager.region, self.config_manager.universe)] * len(alpha_list)
        delay_list = [self.config_manager.delay] * len(alpha_list)
        stone_bag = []
        step3_tag = self.config_manager.generate_tag(self.current_dataset, 3)
        
        # 执行模拟 - 传递规范tag
        await self.simulation_executor.execute_batch(
            alpha_list, self.current_dataset, stage=3, tags=[step3_tag]
        )
        
        return [{'alpha': alpha, 'tag': step3_tag} for alpha in alpha_list]
    
    async def run_continuous_monitoring(self, retry_count: int = 0) -> List[Dict]:
        """运行持续监控模式（递归监控二阶挖掘产出）
        
        Args:
            retry_count: 重试计数
            
        Returns:
            List[Dict]: 执行结果列表
        """
        all_results = []
        
        while True:
            try:
                # 1. 获取符合条件的二阶因子
                next_factors, decay_factors, total_qualified = self.get_qualified_second_order_factors()
                
                if self.logger:
                    self.logger.info(f"📊 符合条件二阶因子: next{len(next_factors):,}个 + decay{len(decay_factors):,}个 = 总计{total_qualified:,}个")
                
                if total_qualified == 0:
                    total_wait_hours = retry_count + 1
                    if self.logger:
                        self.logger.warning(f"⚠️  暂无符合条件的二阶因子 (第{retry_count + 1}次检查，已等待{retry_count}小时)")
                        self.logger.info(f"🔄 三阶挖掘持续等待二阶挖掘产生符合条件的因子...")
                        self.logger.info(f"💡 这是正常现象：三阶挖掘依赖二阶挖掘的输出，需要耐心等待")
                    
                    await self.simulation_executor.sleep_with_countdown(3600, "等待二阶挖掘产生更多因子")
                    retry_count += 1
                    continue
                
                # 2. 生成三阶因子
                third_order_factors = self.generate_third_order_factors(next_factors, decay_factors)
                
                # 3. 过滤已完成的三阶因子
                valid_factors = self.filter_completed_third_order_factors(third_order_factors)
                
                if not valid_factors:
                    if self.logger:
                        self.logger.info(f"✅ 数据集 {self.current_dataset} 三阶挖掘当前批次已完成")
                        self.logger.info(f"🔄 继续监控二阶挖掘，等待新的符合条件因子...")
                    
                    await self.simulation_executor.sleep_with_countdown(1800, "等待二阶挖掘产生新的符合条件因子")  # 30分钟
                    retry_count += 1
                    continue
                
                # 4. 执行三阶挖掘
                batch_results = await self.execute_third_order_batch(valid_factors)
                all_results.extend(batch_results)
                
                if self.logger:
                    self.logger.info(f"✅ 三阶挖掘批次完成: {len(batch_results):,}个因子")
                
                # 完成当前批次后，继续监控新的符合条件因子
                if self.logger:
                    self.logger.info(f"🔄 当前批次完成，继续监控二阶挖掘产生新的符合条件因子...")
                
                await self.simulation_executor.sleep_with_countdown(1800, "等待二阶挖掘产生新的符合条件因子")  # 30分钟
                retry_count += 1
                
            except KeyboardInterrupt:
                if self.logger:
                    self.logger.info(f"⚠️  用户中断，三阶挖掘停止")
                break
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 三阶挖掘监控循环异常: {e}")
                    import traceback
                    traceback.print_exc()
                # 等待一段时间后重试
                await self.simulation_executor.sleep_with_countdown(300, "异常恢复等待")
                retry_count += 1
        
        return all_results
    
    async def execute(self) -> List[Dict]:
        """执行三阶挖掘的完整流程
        
        Returns:
            List[Dict]: 执行结果列表
        """
        stage = self.get_stage_number()
        self.log_execution_start(stage)
        
        try:
            # 1. 初始化会话和操作符
            if not self.ensure_session_and_operators():
                raise Exception("会话和操作符初始化失败")
            
            if self.logger:
                self.logger.info(f"\n⚡ 三阶挖掘 | 数据集: {self.current_dataset}")
            
            # 2. 运行持续监控模式
            results = await self.run_continuous_monitoring()
            
            self.log_execution_end(stage, results, success=True)
            return results
            
        except Exception as e:
            self.handle_execution_error(stage, e)
            self.log_execution_end(stage, [], success=False)
            return []
