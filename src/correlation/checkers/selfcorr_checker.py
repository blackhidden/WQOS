"""
普通相关性检查器 - 检查Alpha与已提交Alpha的自相关性
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict, Optional
from .base_checker import BaseChecker
from .aggressive_checker import AggressiveChecker


class SelfCorrChecker(BaseChecker):
    """普通相关性检查器"""
    
    def __init__(self, config_manager, session_service, data_loader, logger):
        """初始化普通相关性检查器"""
        super().__init__(config_manager, session_service, data_loader, logger)
        # 初始化激进模式检测器
        self.aggressive_checker = AggressiveChecker(config_manager, session_service, data_loader, logger)
    
    def check_correlation(self, alpha_id: str, region: str, 
                         alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> Tuple[bool, float]:
        """检查普通相关性"""
        try:
            # 准备Alpha数据
            alpha_rets, region = self._prepare_alpha_data(alpha_id, alpha_result, alpha_pnls)
            
            # 清理数据
            clean_alpha_rets = self._clean_alpha_data(alpha_rets, alpha_id)
            if clean_alpha_rets.empty:
                return False, 0.0
            
            # 检查基本数据质量
            quality_check = self._check_data_quality(clean_alpha_rets, alpha_id)
            if quality_check is not None:
                return False, quality_check  # 返回特殊标记值
            
            # 检测激进模式Alpha
            is_aggressive = self.aggressive_checker.check_correlation(
                alpha_id, region, alpha_result, alpha_pnls, use_extended_window=True
            )
            if is_aggressive:
                self.logger.info(f"🚀 检测到激进模式Alpha {alpha_id}：早期为0，近期强势上涨（使用扩展时间窗口）")
                return False, -888.0  # 特殊返回值标识激进模式Alpha
            
            # 计算与区域的相关性
            max_correlation = self._calculate_region_correlation(clean_alpha_rets, region, alpha_id)
            
            # 判断是否通过检查
            passed = max_correlation < self.config.correlation_threshold
            
            return passed, max_correlation
            
        except Exception as e:
            self.logger.error(f"❌ Alpha {alpha_id} 普通相关性检查异常: {e}")
            return False, 0.0
    
    def check_correlation_with_data(self, alpha_id: str, region: str, selfcorr_data: Dict,
                                   alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> Tuple[bool, float]:
        """使用预加载数据检查普通相关性"""
        try:
            if not selfcorr_data:
                self.logger.warning(f"  ⚠️ Alpha {alpha_id} SelfCorr数据不可用，默认不通过")
                return False, 0.0
            
            # 临时使用预加载的数据
            old_os_alpha_ids = self.data_loader.os_alpha_ids
            old_os_alpha_rets = self.data_loader.os_alpha_rets
            
            self.data_loader.os_alpha_ids = selfcorr_data['os_alpha_ids']
            self.data_loader.os_alpha_rets = selfcorr_data['os_alpha_rets']
            
            try:
                # 计算普通相关性
                passed, max_correlation = self.check_correlation(alpha_id, region, alpha_result, alpha_pnls)
                
                # 恢复原有数据
                self.data_loader.os_alpha_ids = old_os_alpha_ids
                self.data_loader.os_alpha_rets = old_os_alpha_rets
                
                self.logger.debug(f"    SelfCorr检查: {max_correlation:.4f} < {selfcorr_data['threshold']} = {passed}")
                return passed, max_correlation
                
            except Exception as e:
                # 恢复原有数据
                self.data_loader.os_alpha_ids = old_os_alpha_ids
                self.data_loader.os_alpha_rets = old_os_alpha_rets
                raise e
            
        except Exception as e:
            self.logger.warning(f"  ⚠️ Alpha {alpha_id} SelfCorr检查异常: {e}，默认不通过")
            return False, 0.0
    
    def recalc_correlation_for_aggressive(self, alpha_id: str, region: str, selfcorr_data: Dict, 
                                         alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> float:
        """为激进模式Alpha重新计算实际相关性（跳过激进模式检测）"""
        try:
            if not selfcorr_data:
                return 0.0
            
            # 临时使用预加载的数据
            old_os_alpha_ids = self.data_loader.os_alpha_ids
            old_os_alpha_rets = self.data_loader.os_alpha_rets
            
            self.data_loader.os_alpha_ids = selfcorr_data['os_alpha_ids']
            self.data_loader.os_alpha_rets = selfcorr_data['os_alpha_rets']
            
            try:
                # 准备Alpha数据（不进行激进模式检测）
                alpha_rets, region = self._prepare_alpha_data(alpha_id, alpha_result, alpha_pnls)
                
                # 清理数据
                clean_alpha_rets = self._clean_alpha_data(alpha_rets, alpha_id)
                if clean_alpha_rets.empty:
                    return 0.0
                
                # 直接计算相关性，跳过质量检查和激进模式检测
                max_correlation = self._calculate_region_correlation(clean_alpha_rets, region, alpha_id)
                
                # 恢复原有数据
                self.data_loader.os_alpha_ids = old_os_alpha_ids
                self.data_loader.os_alpha_rets = old_os_alpha_rets
                
                return max_correlation if not np.isnan(max_correlation) else 0.0
                
            except Exception as e:
                # 恢复原有数据
                self.data_loader.os_alpha_ids = old_os_alpha_ids
                self.data_loader.os_alpha_rets = old_os_alpha_rets
                raise e
            
        except Exception as e:
            self.logger.warning(f"      ⚠️ 重新计算Alpha {alpha_id}普通相关性异常: {e}")
            return 0.0
