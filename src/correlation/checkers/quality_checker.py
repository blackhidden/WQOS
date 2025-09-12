"""
质量检查器 - 负责Alpha质量标准检查
作者：e.e.
微信：Enkidu_lin
日期：2025.09.11
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta


class QualityChecker:
    """Alpha质量检查器"""
    
    def __init__(self, config, session_service, pnl_manager, logger):
        """初始化质量检查器"""
        self.config = config
        self.session_service = session_service
        self.pnl_manager = pnl_manager
        self.logger = logger
        
        # 质量检查标准阈值
        self.zero_coverage_threshold = 5  # 连续零覆盖天数阈值
        
        # 初始化数据加载器（用于获取PnL数据时的配置信息）
        from ..data.data_loader import DataLoader
        self.data_loader = DataLoader(config, session_service, pnl_manager, logger)
    
    def check_zero_coverage(self, alpha_id: str, daily_pnl_data: Optional[pd.Series] = None) -> Dict:
        """检查Zero Coverage - 连续零覆盖天数
        
        Args:
            alpha_id: Alpha ID
            daily_pnl_data: 每日PnL数据，如果为None则从API获取
            
        Returns:
            检查结果字典
        """
        try:
            # 如果没有提供数据，从API获取
            if daily_pnl_data is None:
                daily_pnl_data = self._get_daily_pnl_data(alpha_id)
            
            if daily_pnl_data.empty:
                self.logger.error(f"❌ Alpha {alpha_id} Zero Coverage检查失败: 无法获取daily-pnl数据")
                return {
                    'check_type': 'zero_coverage',
                    'alpha_id': alpha_id,
                    'pass': False,
                    'max_consecutive_zero_days': 0,
                    'threshold': self.zero_coverage_threshold,
                    'message': '无法获取daily-pnl数据',
                    'error': 'No daily-pnl data available'
                }
            
            # 计算连续零覆盖天数
            consecutive_stats = self._calculate_consecutive_zeros(daily_pnl_data)
            
            # 判断是否通过检查
            is_pass = consecutive_stats['max_consecutive'] <= self.zero_coverage_threshold
            
            result = {
                'check_type': 'zero_coverage',
                'alpha_id': alpha_id,
                'pass': is_pass,
                'max_consecutive_zero_days': consecutive_stats['max_consecutive'],
                'total_zero_days': consecutive_stats['total_zeros'],
                'data_points': len(daily_pnl_data),
                'threshold': self.zero_coverage_threshold,
                'zero_periods': consecutive_stats['zero_periods'],
                'message': self._format_zero_coverage_message(consecutive_stats, is_pass),
                'check_date': datetime.now().isoformat()
            }
            
            if is_pass:
                self.logger.info(f"    ✅ Alpha {alpha_id} Zero Coverage检查通过: 最长连续零覆盖{consecutive_stats['max_consecutive']}天")
            else:
                self.logger.warning(f"    ❌ Alpha {alpha_id} Zero Coverage检查失败: 最长连续零覆盖{consecutive_stats['max_consecutive']}天 > 阈值{self.zero_coverage_threshold}天")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Alpha {alpha_id} Zero Coverage检查异常: {e}")
            return {
                'check_type': 'zero_coverage',
                'alpha_id': alpha_id,
                'pass': False,
                'max_consecutive_zero_days': 0,
                'threshold': self.zero_coverage_threshold,
                'message': f'检查异常: {str(e)}',
                'error': str(e),
                'check_date': datetime.now().isoformat()
            }
    
    def _get_daily_pnl_data(self, alpha_id: str) -> pd.Series:
        """获取指定alpha的每日PnL数据"""
        return self.pnl_manager.get_alpha_daily_pnl(alpha_id)
    
    def _calculate_consecutive_zeros(self, pnl_data: pd.Series) -> Dict:
        """计算连续零值统计信息（排除Alpha启动前的初始零期间）"""
        if pnl_data.empty:
            return {
                'max_consecutive': 0,
                'total_zeros': 0,
                'zero_periods': [],
                'analysis': '无PnL数据'
            }
        
        # 找到Alpha真正开始运行的日期（第一个非零PnL）
        first_nonzero_idx = None
        for i, (date, pnl) in enumerate(pnl_data.items()):
            if not (pd.isna(pnl) or pnl == 0):
                first_nonzero_idx = i
                break
        
        if first_nonzero_idx is None:
            # 如果所有PnL都是0，说明Alpha从未运行
            return {
                'max_consecutive': 0,
                'total_zeros': 0,
                'zero_periods': [],
                'analysis': 'Alpha从未产生非零PnL，可能从未启动'
            }
        
        # 只分析Alpha启动后的数据
        active_period_pnl = pnl_data.iloc[first_nonzero_idx:]
        first_active_date = pnl_data.index[first_nonzero_idx]
        
        # 确保日期是datetime格式
        if isinstance(first_active_date, str):
            first_active_date = pd.to_datetime(first_active_date)
        
        # 将NaN视为零
        is_zero = (active_period_pnl == 0) | active_period_pnl.isna()
        
        max_consecutive = 0
        current_consecutive = 0
        total_zeros = is_zero.sum()
        zero_periods = []  # 记录所有连续零期间
        period_start = None
        
        for date, is_zero_val in is_zero.items():
            if is_zero_val:
                if current_consecutive == 0:
                    period_start = date
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                if current_consecutive > 0:
                    # 确保日期是datetime格式
                    if isinstance(period_start, str):
                        period_start = pd.to_datetime(period_start)
                    end_date = date - timedelta(days=1)
                    if isinstance(end_date, str):
                        end_date = pd.to_datetime(end_date)
                    
                    # 记录这个零期间
                    zero_periods.append({
                        'start_date': period_start.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d'),
                        'days': current_consecutive
                    })
                current_consecutive = 0
        
        # 处理以零结尾的情况
        if current_consecutive > 0 and period_start is not None:
            # 确保日期是datetime格式
            if isinstance(period_start, str):
                period_start = pd.to_datetime(period_start)
            last_date = active_period_pnl.index[-1]
            if isinstance(last_date, str):
                last_date = pd.to_datetime(last_date)
                
            zero_periods.append({
                'start_date': period_start.strftime('%Y-%m-%d'),
                'end_date': last_date.strftime('%Y-%m-%d'),
                'days': current_consecutive
            })
        
        return {
            'max_consecutive': max_consecutive,
            'total_zeros': int(total_zeros),
            'zero_periods': zero_periods,
            'analysis': f"Alpha从 {first_active_date.strftime('%Y-%m-%d')} 开始活跃，发现 {len(zero_periods)} 个零覆盖期间",
            'first_active_date': first_active_date.strftime('%Y-%m-%d')
        }
    
    def _format_zero_coverage_message(self, stats: Dict, is_pass: bool) -> str:
        """格式化Zero Coverage检查消息"""
        max_consecutive = stats['max_consecutive']
        total_zeros = stats['total_zeros']
        zero_periods = stats['zero_periods']
        
        if is_pass:
            if max_consecutive == 0:
                return "完美：无零覆盖期间"
            else:
                return f"通过：最长连续零覆盖{max_consecutive}天（≤{self.zero_coverage_threshold}天阈值）"
        else:
            message = f"失败：最长连续零覆盖{max_consecutive}天（>{self.zero_coverage_threshold}天阈值）"
            
            # 添加主要零期间信息
            if zero_periods:
                longest_period = max(zero_periods, key=lambda x: x['days'])
                message += f"，最长期间：{longest_period['start_date']}至{longest_period['end_date']}（{longest_period['days']}天）"
            
            return message
    
    def check_factory_pattern(self, alpha_id: str, alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> Dict:
        """检查厂字型Alpha - 收益率标准差为0或NaN
        
        Args:
            alpha_id: Alpha ID  
            alpha_result: Alpha详细信息
            alpha_pnls: PnL数据
            
        Returns:
            检查结果字典
        """
        try:
            # 准备Alpha数据（从BaseChecker借用逻辑）
            alpha_rets = self._prepare_alpha_returns(alpha_id, alpha_result, alpha_pnls)
            
            if alpha_rets.empty:
                return {
                    'check_type': 'factory_pattern',
                    'alpha_id': alpha_id,
                    'pass': False,
                    'message': '无法获取Alpha收益率数据',
                    'check_date': datetime.now().isoformat()
                }
            
            # 检查Alpha收益率的标准差（厂字型检测）
            alpha_std = alpha_rets.std()
            is_factory = alpha_std == 0 or np.isnan(alpha_std)
            
            if is_factory:
                message = f"    厂字型Alpha：收益率标准差为0或NaN (std={alpha_std})"
                self.logger.warning(f"    🏭 Alpha {alpha_id}: {message}")
            else:
                message = f"    正常Alpha：收益率标准差 = {alpha_std:.6f}"
                self.logger.info(f"    ✅ Alpha {alpha_id}: {message}")
            
            return {
                'check_type': 'factory_pattern',
                'alpha_id': alpha_id,
                'pass': not is_factory,
                'std_value': float(alpha_std) if not np.isnan(alpha_std) else None,
                'message': message,
                'check_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"    ❌ Alpha {alpha_id} 厂字型检查异常: {e}")
            return {
                'check_type': 'factory_pattern',
                'alpha_id': alpha_id,
                'pass': False,
                'message': f'检查异常: {str(e)}',
                'error': str(e),
                'check_date': datetime.now().isoformat()
            }
    
    def _prepare_alpha_returns(self, alpha_id: str, alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> pd.Series:
        """准备Alpha收益率数据（借用BaseChecker的逻辑）"""
        try:
            # 获取region
            region = 'USA'  # 默认值，实际从alpha_result获取
            if alpha_result and 'settings' in alpha_result:
                region = alpha_result['settings'].get('region', 'USA')
            
            # 获取alpha的PnL数据
            if alpha_pnls is None:
                try:
                    _, alpha_pnls_data = self.pnl_manager.get_alpha_pnls([alpha_result])
                    alpha_pnls = alpha_pnls_data[alpha_id]
                except Exception as e:
                    self.logger.error(f"❌ 获取Alpha {alpha_id} PnL数据失败: {e}")
                    return pd.Series()
            
            # 计算收益率
            alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
            
            # 限制时间窗口（使用配置的时间窗口）
            cutoff_date = pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=self.config.time_window_years)
            alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index) > cutoff_date]
            
            # 清理数据，移除无效数据
            valid_alpha_mask = ~(alpha_rets.isna() | np.isinf(alpha_rets))
            if not valid_alpha_mask.any():
                self.logger.warning(f"⚠️ Alpha {alpha_id} 收益率数据全部无效")
                return pd.Series()
            
            return alpha_rets[valid_alpha_mask]
            
        except Exception as e:
            self.logger.error(f"❌ 准备Alpha {alpha_id} 收益率数据失败: {e}")
            return pd.Series()
    
    def run_quality_checks(self, alpha_id: str, alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> Dict:
        """运行所有质量检查
        
        Args:
            alpha_id: Alpha ID
            alpha_result: Alpha详细信息，用于获取PnL数据
            alpha_pnls: 可选的PnL数据，用于避免重复API调用
            
        Returns:
            综合质量检查结果
        """
        results = {
            'alpha_id': alpha_id,
            'check_date': datetime.now().isoformat(),
            'checks': {},
            'overall_pass': True,
            'failed_checks': []
        }
        
        try:
            # 1. 厂字型Alpha检查（优先检查，如果是厂字型直接返回）
            factory_result = self.check_factory_pattern(alpha_id, alpha_result, alpha_pnls)
            results['checks']['factory_pattern'] = factory_result
            
            if not factory_result['pass']:
                results['overall_pass'] = False
                results['failed_checks'].append('factory_pattern')
                # 厂字型Alpha直接返回，不进行后续检查
                results['summary'] = f"🏭 Alpha {alpha_id} 质量检查失败: 厂字型Alpha (收益率标准差为0)"
                self.logger.warning(results['summary'])
                return results
            
            # 2. Zero Coverage检查
            # 注意：Zero Coverage需要使用daily-pnl数据，不是相关性检查的PnL数据
            # 这里传入None，让check_zero_coverage方法自己调用daily-pnl API
            zero_coverage_result = self.check_zero_coverage(alpha_id, None)
            results['checks']['zero_coverage'] = zero_coverage_result
            
            if not zero_coverage_result['pass']:
                results['overall_pass'] = False
                results['failed_checks'].append('zero_coverage')
            
            # 未来可以在这里添加其他质量检查
            # weight_concentration_result = self.check_weight_concentration(alpha_id)
            # results['checks']['weight_concentration'] = weight_concentration_result
            
            # 生成总结消息
            if results['overall_pass']:
                results['summary'] = f"    ✅ Alpha {alpha_id} 通过所有质量检查"
                self.logger.info(results['summary'])
            else:
                failed_list = ', '.join(results['failed_checks'])
                results['summary'] = f"    ❌ Alpha {alpha_id} 质量检查失败: {failed_list}"
                self.logger.warning(results['summary'])
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Alpha {alpha_id} 质量检查异常: {e}")
            results['overall_pass'] = False
            results['error'] = str(e)
            results['summary'] = f"❌ Alpha {alpha_id} 质量检查异常: {str(e)}"
            return results
