"""
激进模式检测器 - 检测"早期为0，近期强势上涨"的Alpha
"""

import pandas as pd
import numpy as np
from .base_checker import BaseChecker


class AggressiveChecker(BaseChecker):
    """激进模式检测器"""
    
    def detect_aggressive_pattern(self, alpha_rets: pd.Series, alpha_id: str) -> bool:
        """检测"早期为0，近期强势上涨"的激进模式Alpha
        
        检测逻辑：
        1. 早期大部分时间PnL为0或接近0（如前70%的时间）
        2. 近期有显著的上涨趋势（如后30%的时间）
        3. 整体收益率标准差不为0（与厂字型Alpha区分）
        """
        try:
            if len(alpha_rets) < 100:  # 数据点太少，无法判断
                return False
            
            # 将数据按时间分为早期和近期两部分
            # 对于更长的时间序列，使用更合理的分割比例
            total_length = len(alpha_rets)
            
            # 如果数据超过4年（约1000个交易日），使用80%/20%分割
            # 如果数据少于4年，使用70%/30%分割
            split_ratio = 0.8 if total_length > 1000 else 0.7
            early_split = int(total_length * split_ratio)
            
            self.logger.debug(f"      📊 时间分割: 总长度{total_length}天，使用{split_ratio*100:.0f}%/{(1-split_ratio)*100:.0f}%分割")
            
            early_rets = alpha_rets.iloc[:early_split]
            recent_rets = alpha_rets.iloc[early_split:]
            
            # 检查早期是否大部分时间为0或接近0（放宽条件）
            early_zero_ratio = (abs(early_rets) < 1e-6).sum() / len(early_rets)
            early_low_activity = early_zero_ratio > 0.6  # 早期60%以上时间为0（放宽从80%）
            
            # 检查近期是否有显著活动
            recent_std = recent_rets.std()
            recent_has_activity = recent_std > 0 and not np.isnan(recent_std)
            
            # 检查近期是否有上涨趋势（累积收益为正）
            recent_cumulative = recent_rets.cumsum()
            recent_upward_trend = recent_cumulative.iloc[-1] > recent_cumulative.iloc[0]
            
            # 检查近期收益的绝对值是否显著大于早期（放宽条件）
            early_abs_mean = abs(early_rets).mean()
            recent_abs_mean = abs(recent_rets).mean()
            recent_more_active = recent_abs_mean > early_abs_mean * 1.5  # 近期活动度至少是早期的1.5倍（放宽从2倍）
            
            # 额外检查：近期收益率的标准差应该明显大于早期
            early_std = early_rets.std()
            recent_std_increase = recent_std > early_std * 1.5 if early_std > 0 else recent_std > 1e-6
            
            # 综合判断（放宽条件）
            is_aggressive_pattern = (
                early_low_activity and          # 早期大部分时间为0（60%以上）
                recent_has_activity and         # 近期有活动
                recent_upward_trend and         # 近期有上涨趋势
                (recent_more_active or recent_std_increase)  # 近期活动度增加或波动性增加
            )
            
            # 为特定Alpha添加详细调试信息
            if alpha_id == "glz3VdQ" or is_aggressive_pattern:
                # 计算实际时间跨度
                time_span_days = (pd.to_datetime(alpha_rets.index).max() - pd.to_datetime(alpha_rets.index).min()).days
                time_span_years = time_span_days / 365.25
                
                self.logger.debug(f"    🔍 Alpha {alpha_id} 激进模式检测详情:")
                self.logger.debug(f"      📊 数据长度: {len(alpha_rets)}天 (约{time_span_years:.1f}年)")
                self.logger.debug(f"      📊 时间分割: {split_ratio*100:.0f}%早期({len(early_rets)}天) / {(1-split_ratio)*100:.0f}%近期({len(recent_rets)}天)")
                self.logger.debug(f"      📊 早期零值比例: {early_zero_ratio:.1%} (需要>60%)")
                self.logger.debug(f"      📈 近期标准差: {recent_std:.6f} (需要>0)")
                self.logger.debug(f"      📈 早期标准差: {early_std:.6f}")
                self.logger.debug(f"      📈 近期累积收益: {recent_cumulative.iloc[-1]:.2f} (需要>开始值{recent_cumulative.iloc[0]:.2f})")
                self.logger.debug(f"      📊 活动度比较: 近期{recent_abs_mean:.6f} vs 早期{early_abs_mean:.6f} (需要>1.5倍)")
                self.logger.debug(f"      📊 标准差比较: 近期{recent_std:.6f} vs 早期{early_std:.6f} (需要>1.5倍)")
                self.logger.debug(f"      🎯 检测条件:")
                self.logger.debug(f"        - 早期低活动: {early_low_activity} ({early_zero_ratio:.1%} > 60%)")
                self.logger.debug(f"        - 近期有活动: {recent_has_activity}")
                self.logger.debug(f"        - 上涨趋势: {recent_upward_trend}")
                self.logger.debug(f"        - 活动度增加: {recent_more_active}")
                self.logger.debug(f"        - 波动性增加: {recent_std_increase}")
                self.logger.debug(f"      🏁 最终结果: {'✅ 激进模式' if is_aggressive_pattern else '❌ 非激进模式'}")
            
            return is_aggressive_pattern
            
        except Exception as e:
            self.logger.warning(f"    ⚠️ Alpha {alpha_id} 激进模式检测异常: {e}")
            return False
    
    def check_correlation(self, alpha_id: str, region: str, 
                         alpha_result=None, alpha_pnls=None, 
                         use_extended_window=False):
        """检查激进模式（使用扩展时间窗口）
        
        Args:
            use_extended_window: 是否使用扩展的时间窗口进行激进模式检测
        """
        try:
            # 准备Alpha数据
            alpha_rets, region = self._prepare_alpha_data(alpha_id, alpha_result, alpha_pnls)
            
            # 清理数据
            clean_alpha_rets = self._clean_alpha_data(alpha_rets, alpha_id)
            if clean_alpha_rets.empty:
                return False
            
            # 检查基本数据质量
            quality_check = self._check_data_quality(clean_alpha_rets, alpha_id)
            if quality_check is not None:
                return False  # 厂字型Alpha不是激进模式
            
            # 为激进模式检测使用更长的时间窗口（如果启用）
            if use_extended_window:
                aggressive_time_window_years = max(6, self.config.time_window_years + 2)
                aggressive_cutoff_date = pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=aggressive_time_window_years)
                aggressive_alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index) > aggressive_cutoff_date]
                
                # 数据清理（针对激进模式检测的更长时间序列）
                aggressive_valid_mask = ~(aggressive_alpha_rets.isna() | np.isinf(aggressive_alpha_rets))
                aggressive_clean_rets = aggressive_alpha_rets[aggressive_valid_mask] if aggressive_valid_mask.any() else clean_alpha_rets
                
                return self.detect_aggressive_pattern(aggressive_clean_rets, alpha_id)
            else:
                return self.detect_aggressive_pattern(clean_alpha_rets, alpha_id)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Alpha {alpha_id} 激进模式检查异常: {e}")
            return False
