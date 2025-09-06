"""
基础检查器 - 所有相关性检查器的基类
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from abc import ABC, abstractmethod


class BaseChecker(ABC):
    """基础检查器抽象类"""
    
    def __init__(self, config_manager, session_service, data_loader, logger):
        """初始化基础检查器"""
        self.config = config_manager
        self.session_service = session_service
        self.data_loader = data_loader
        self.logger = logger
    
    @abstractmethod
    def check_correlation(self, alpha_id: str, region: str, 
                         alpha_result: Dict = None, alpha_pnls: pd.DataFrame = None) -> Tuple[bool, float]:
        """检查相关性 - 子类必须实现"""
        pass
    
    def _get_alpha_region(self, alpha_id: str, alpha_result: Dict = None) -> str:
        """获取Alpha的区域信息（优先从数据库获取）"""
        try:
            # 如果已有alpha_result，直接返回region
            if alpha_result and 'settings' in alpha_result:
                return alpha_result['settings']['region']
            
            # 尝试从数据库获取region信息，避免API调用
            from database.db_manager import FactorDatabaseManager
            db = FactorDatabaseManager(self.config.db_path)
            with db.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT region FROM submitable_alphas 
                    WHERE alpha_id = ?
                """, (alpha_id,))
                result = cursor.fetchone()
                
            if result:
                region = result[0]
                self.logger.debug(f"🗃️ 从数据库获取Alpha {alpha_id} region: {region}")
                return region
            else:
                # 数据库中没有，回退到API调用
                self.logger.warning(f"⚠️ 数据库中未找到Alpha {alpha_id}，使用API获取详细信息")
                response = self.session_service.wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                alpha_result = response.json()
                return alpha_result['settings']['region']
                
        except Exception as e:
            self.logger.error(f"❌ 获取Alpha {alpha_id}详细信息失败: {e}")
            return 'USA'  # 默认值
    
    def _prepare_alpha_data(self, alpha_id: str, alpha_result: Dict = None, 
                           alpha_pnls: pd.DataFrame = None) -> Tuple[pd.Series, str]:
        """准备Alpha数据用于相关性计算"""
        # 获取region
        region = self._get_alpha_region(alpha_id, alpha_result)
        
        # 获取alpha的PnL数据
        if alpha_pnls is None:
            try:
                _, alpha_pnls_data = self.data_loader.pnl_manager.get_alpha_pnls([alpha_result])
                alpha_pnls = alpha_pnls_data[alpha_id]
            except Exception as e:
                self.logger.error(f"❌ 获取Alpha {alpha_id} PnL数据失败: {e}")
                raise
        
        # 计算收益率
        alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
        
        # 限制时间窗口
        cutoff_date = pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=self.config.time_window_years)
        alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index) > cutoff_date]
        
        return alpha_rets, region
    
    def _clean_alpha_data(self, alpha_rets: pd.Series, alpha_id: str) -> pd.Series:
        """清理Alpha数据，移除无效数据"""
        # 移除包含NaN或inf的Alpha
        valid_alpha_mask = ~(alpha_rets.isna() | np.isinf(alpha_rets))
        if not valid_alpha_mask.any():
            self.logger.warning(f"⚠️ Alpha {alpha_id} 收益率数据全部无效")
            return pd.Series()
        
        return alpha_rets[valid_alpha_mask]
    
    def _check_data_quality(self, alpha_rets: pd.Series, alpha_id: str) -> Optional[float]:
        """检查数据质量，返回特殊标记值或None"""
        # 检查Alpha收益率的标准差（检测厂字型Alpha）
        alpha_std = alpha_rets.std()
        if alpha_std == 0 or np.isnan(alpha_std):
            self.logger.warning(f"🏭 检测到厂字型Alpha {alpha_id}：收益率标准差为0或NaN")
            return -999.0  # 特殊返回值标识厂字型Alpha
        
        return None  # 数据质量正常
    
    def _calculate_region_correlation(self, alpha_rets: pd.Series, region: str, alpha_id: str) -> float:
        """计算与区域Alpha的相关性"""
        # 检查区域是否存在
        if region not in self.data_loader.os_alpha_ids or not self.data_loader.os_alpha_ids[region]:
            self.logger.warning(f"⚠️ {region} 区域没有参考数据")
            return 0.0
        
        # 计算与同区域其他alpha的相关性
        region_alphas = self.data_loader.os_alpha_ids[region]
        region_rets = self.data_loader.os_alpha_rets[region_alphas]
        
        # 对区域数据进行相同的清理
        clean_region_rets = region_rets.loc[alpha_rets.index]
        
        # 移除标准差为0或包含NaN的区域Alpha
        region_stds = clean_region_rets.std()
        valid_region_alphas = region_stds[(region_stds > 0) & (~region_stds.isna())].index
        
        if len(valid_region_alphas) == 0:
            self.logger.warning(f"⚠️ {region} 区域没有有效的参考Alpha")
            return 0.0
        
        clean_region_rets = clean_region_rets[valid_region_alphas]
        
        # 使用numpy警告抑制来计算相关性
        with np.errstate(divide='ignore', invalid='ignore'):
            correlations = clean_region_rets.corrwith(alpha_rets)
        
        # 保存相关性结果到文件
        corr_file = self.config.data_path / 'os_alpha_corr.csv'
        correlations.sort_values(ascending=False).round(4).to_csv(corr_file)
        
        # 获取最大相关性，过滤掉NaN值
        valid_correlations = correlations.dropna()
        if len(valid_correlations) == 0:
            self.logger.warning(f"⚠️ Alpha {alpha_id} 与所有参考Alpha的相关性都无法计算")
            return 0.0
        else:
            max_corr = valid_correlations.max()
            return max_corr if not np.isnan(max_corr) else 0.0
