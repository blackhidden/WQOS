"""
数据加载器 - 负责OS Alpha数据的下载和加载
"""

import pickle
import pandas as pd
from typing import List, Dict, Tuple, Optional
from collections import defaultdict


class DataLoader:
    """数据加载器"""
    
    def __init__(self, config_manager, session_service, pnl_manager, logger):
        """初始化数据加载器"""
        self.config = config_manager
        self.session_service = session_service
        self.pnl_manager = pnl_manager
        self.logger = logger
        
        # 本地数据缓存
        self.os_alpha_ids = None
        self.os_alpha_rets = None
        self.ppac_alpha_ids = []
        self.data_loaded = False
        self.current_check_type = None
    
    def save_obj(self, obj: object, name: str) -> None:
        """保存对象到文件中，以pickle格式序列化"""
        file_path = self.config.data_path / f"{name}.pickle"
        with open(file_path, 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
    
    def load_obj(self, name: str) -> object:
        """从pickle文件中加载对象"""
        file_path = self.config.data_path / f"{name}.pickle"
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    
    def get_os_alphas(self, limit: int = 100, get_first: bool = False) -> List[Dict]:
        """获取OS阶段的alpha列表"""
        fetched_alphas = []
        offset = 0
        total_alphas = 100
        
        while len(fetched_alphas) < total_alphas:
            self.logger.info(f"📥 获取alpha列表: offset={offset}, limit={limit}")
            url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"
            
            try:
                response = self.session_service.wait_get(url, message=f"获取已提交alpha列表")
                res = response.json()
                
                if offset == 0:
                    total_alphas = res['count']
                    self.logger.info(f"📊 总共找到 {total_alphas} 个OS阶段的alpha")
                
                alphas = res["results"]
                fetched_alphas.extend(alphas)
                
                if len(alphas) < limit:
                    break
                
                offset += limit
                
                if get_first:
                    break
                    
            except Exception as e:
                self.logger.error(f"❌ 获取alpha列表失败: {e}")
                break
        
        return fetched_alphas[:total_alphas]
    
    def download_data(self, flag_increment=True):
        """下载并保存相关性检查所需的数据"""
        self.logger.info(f"📥 开始下载数据...")
        
        # 记录是否有新的已提交Alpha
        has_new_submitted_alphas = False
        
        if flag_increment:
            try:
                os_alpha_ids = self.load_obj('os_alpha_ids')
                os_alpha_pnls = self.load_obj('os_alpha_pnls')
                ppac_alpha_ids = self.load_obj('ppac_alpha_ids')
                exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
                self.logger.info(f"📂 加载现有数据: {len(exist_alpha)} 个alpha")
            except Exception as e:
                self.logger.warning(f"⚠️ 无法加载现有数据: {e}，重新开始下载")
                os_alpha_ids = None
                os_alpha_pnls = None
                exist_alpha = []
                ppac_alpha_ids = []
        else:
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []
        
        # 获取alpha列表
        if os_alpha_ids is None:
            alphas = self.get_os_alphas(limit=100, get_first=False)
        else:
            alphas = self.get_os_alphas(limit=30, get_first=True)
        
        # 过滤新alpha
        alphas = [item for item in alphas if item['id'] not in exist_alpha]
        self.logger.info(f"📊 找到 {len(alphas)} 个新alpha")
        
        if not alphas:
            self.logger.info(f"✅ 没有新的alpha需要下载")
            return False  # 返回False表示没有新Alpha
        
        # 有新Alpha，设置标志
        has_new_submitted_alphas = True
        
        # 识别PPAC alpha
        new_ppac_ids = []
        for alpha in alphas:
            classifications = alpha.get('classifications', [])
            for classification in classifications:
                if isinstance(classification, dict) and classification.get('name') == 'Power Pool Alpha':
                    new_ppac_ids.append(alpha['id'])
                    break
        
        ppac_alpha_ids.extend(new_ppac_ids)
        self.logger.info(f"🔵 找到 {len(new_ppac_ids)} 个新PPAC alpha")
        
        # 获取PnL数据
        os_alpha_ids, os_alpha_pnls = self.pnl_manager.get_alpha_pnls(alphas, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
        
        # 从数据库中移除新提交的alpha（避免和自己产生1.0相关性）
        if has_new_submitted_alphas:
            new_alpha_ids = [alpha['id'] for alpha in alphas]
            self.logger.info(f"🗑️ 从数据库中移除 {len(new_alpha_ids)} 个新提交的Alpha（避免自相关）...")
            self._remove_submitted_alphas_from_database(new_alpha_ids)
        
        # 保存数据
        self.save_obj(os_alpha_ids, 'os_alpha_ids')
        self.save_obj(os_alpha_pnls, 'os_alpha_pnls')
        self.save_obj(ppac_alpha_ids, 'ppac_alpha_ids')
        
        self.logger.info(f'✅ 数据下载完成: 新增 {len(alphas)} 个alpha, 总计 {os_alpha_pnls.shape[1]} 个alpha')
        
        return has_new_submitted_alphas  # 返回是否有新Alpha
    
    def load_data(self, tag=None):
        """加载数据并根据标签过滤"""
        try:
            os_alpha_ids = self.load_obj('os_alpha_ids')
            os_alpha_pnls = self.load_obj('os_alpha_pnls')
            ppac_alpha_ids = self.load_obj('ppac_alpha_ids')
        except FileNotFoundError as e:
            self.logger.error(f"❌ 数据文件不存在: {e}")
            self.logger.info(f"💡 请先运行数据下载: download_data()")
            return None, None
        
        # 根据标签过滤数据
        if tag == 'PPAC':
            self.logger.info(f"🔵 加载PPAC类型数据")
            for region in os_alpha_ids:
                os_alpha_ids[region] = [alpha for alpha in os_alpha_ids[region] if alpha in ppac_alpha_ids]
        elif tag == 'SelfCorr':
            self.logger.info(f"🟢 加载普通相关性数据")
            for region in os_alpha_ids:
                os_alpha_ids[region] = [alpha for alpha in os_alpha_ids[region] if alpha not in ppac_alpha_ids]
        else:
            self.logger.info(f"📊 加载所有数据")
        
        # 获取现有alpha列表
        exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        os_alpha_pnls = os_alpha_pnls[exist_alpha]
        
        # 计算收益率
        os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
        
        # 限制时间窗口
        cutoff_date = pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=self.config.time_window_years)
        os_alpha_rets = os_alpha_rets[pd.to_datetime(os_alpha_rets.index) > cutoff_date]
        
        self.logger.info(f"📊 数据加载完成: {len(os_alpha_ids)} 个区域, {os_alpha_rets.shape[1]} 个alpha")
        self.logger.info(f"📅 时间范围: {os_alpha_rets.index.min()} 到 {os_alpha_rets.index.max()}")
        
        return os_alpha_ids, os_alpha_rets
    
    def ensure_data_loaded(self, check_type=None, force_reload=False, force_check_new=False):
        """确保数据已加载"""
        # 在持续监控模式下，即使数据已加载也要检查新Alpha
        if self.data_loaded and not force_reload and self.current_check_type == check_type and not force_check_new:
            self.logger.info(f"📊 使用已加载的数据")
            return True, False
        
        self.logger.info(f"📂 加载数据 (类型: {check_type if check_type else '全部'})...")
        
        try:
            # 先尝试下载最新数据（检查新Alpha）
            has_new_alphas = self.download_data(flag_increment=True)
            
            # 如果只是检查新Alpha而数据已加载，不需要重新加载全部数据
            if self.data_loaded and not force_reload and self.current_check_type == check_type:
                self.logger.info(f"📊 数据已加载，仅检查新Alpha")
                return True, has_new_alphas
            
            # 加载数据
            self.os_alpha_ids, self.os_alpha_rets = self.load_data(tag=check_type)
            
            if self.os_alpha_ids is None or self.os_alpha_rets is None:
                return False, False
            
            self.current_check_type = check_type
            self.data_loaded = True
            
            return True, has_new_alphas
            
        except Exception as e:
            self.logger.error(f"❌ 数据加载失败: {e}")
            import traceback
            traceback.print_exc()
            return False, False
    
    def load_local_data_only(self, check_type=None, force_reload=False):
        """仅加载本地数据，不进行新Alpha检查和下载
        
        Args:
            check_type: 数据类型 ("PPAC" 或 "SelfCorr")
            force_reload: 是否强制重新加载
            
        Returns:
            tuple: (success, has_cached_data)
        """
        # 如果数据已加载且类型匹配，直接返回
        if self.data_loaded and not force_reload and self.current_check_type == check_type:
            self.logger.debug(f"📊 使用已加载的{check_type if check_type else '全部'}数据")
            return True, True
        
        self.logger.info(f"📂 仅加载本地{check_type if check_type else '全部'}数据（跳过新Alpha检查）...")
        
        try:
            # 直接加载本地数据，不进行新Alpha检查
            self.os_alpha_ids, self.os_alpha_rets = self.load_data(tag=check_type)
            
            if self.os_alpha_ids is None or self.os_alpha_rets is None:
                return False, False
            
            self.current_check_type = check_type
            self.data_loaded = True
            
            return True, True
            
        except Exception as e:
            self.logger.error(f"❌ 本地数据加载失败: {e}")
            import traceback
            traceback.print_exc()
            return False, False
    
    def _remove_submitted_alphas_from_database(self, alpha_ids: List[str]):
        """从数据库中移除已提交的Alpha（避免和自己产生1.0相关性）"""
        if not alpha_ids:
            return
        
        try:
            # 初始化数据库管理器
            from database.db_manager import FactorDatabaseManager
            db = FactorDatabaseManager(self.config.db_path)
            
            # 批量移除已提交的Alpha
            removed_count = db.remove_submitable_alphas_batch(alpha_ids)
            
            if removed_count > 0:
                self.logger.info(f"    ✅ 成功从数据库移除 {removed_count} 个已提交Alpha")
            else:
                self.logger.info(f"    ℹ️ 这些Alpha不在数据库中，无需移除")
                
        except Exception as e:
            self.logger.error(f"    ❌ 批量移除已提交Alpha失败: {e}")
            # 回退到单个移除
            self.logger.info(f"    🔄 回退到单个移除模式...")
            try:
                from database.db_manager import FactorDatabaseManager
                db = FactorDatabaseManager(self.config.db_path)
                
                success_count = 0
                for alpha_id in alpha_ids:
                    try:
                        if db.remove_submitable_alpha(alpha_id):
                            success_count += 1
                    except Exception as e:
                        self.logger.debug(f"      移除Alpha {alpha_id}失败: {e}")
                
                if success_count > 0:
                    self.logger.info(f"    ✅ 单个移除完成，成功移除 {success_count}/{len(alpha_ids)} 个Alpha")
                else:
                    self.logger.info(f"    ℹ️ 这些Alpha不在数据库中，无需移除")
                    
            except Exception as e2:
                self.logger.error(f"    ❌ 单个移除也失败: {e2}")
