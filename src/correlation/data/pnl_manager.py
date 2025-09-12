"""
PnL数据管理器 - 负责PnL数据的获取和缓存
"""

import time
import pickle
import pandas as pd
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


class PnLManager:
    """PnL数据管理器"""
    
    def __init__(self, config_manager, session_service, logger):
        """初始化PnL管理器"""
        self.config = config_manager
        self.session_service = session_service
        self.logger = logger
        
        # PnL数据缓存机制
        self.pnl_cache = {}  # {alpha_id: pd.Series} - 缓存已下载的PnL数据
        self.cache_loaded = False
    
    def load_pnl_cache(self):
        """加载PnL数据缓存"""
        if self.cache_loaded:
            return
            
        try:
            if self.config.pnl_cache_file.exists():
                with open(self.config.pnl_cache_file, 'rb') as f:
                    self.pnl_cache = pickle.load(f)
                self.logger.info(f"📂 加载PnL缓存: {len(self.pnl_cache)} 个Alpha")
            else:
                self.pnl_cache = {}
                self.logger.info(f"📂 初始化空PnL缓存")
        except Exception as e:
            self.logger.warning(f"⚠️ 加载PnL缓存失败: {e}，使用空缓存")
            self.pnl_cache = {}
        
        self.cache_loaded = True
    
    def save_pnl_cache(self):
        """保存PnL数据缓存"""
        try:
            with open(self.config.pnl_cache_file, 'wb') as f:
                pickle.dump(self.pnl_cache, f, pickle.HIGHEST_PROTOCOL)
            self.logger.debug(f"💾 保存PnL缓存: {len(self.pnl_cache)} 个Alpha")
        except Exception as e:
            self.logger.error(f"❌ 保存PnL缓存失败: {e}")
    
    def cleanup_pnl_cache(self, keep_alpha_ids: List[str]):
        """清理PnL缓存，只保留通过检测的Alpha数据"""
        if not self.pnl_cache:
            return
            
        original_count = len(self.pnl_cache)
        # 只保留在keep_alpha_ids中的Alpha数据
        self.pnl_cache = {aid: pnl for aid, pnl in self.pnl_cache.items() if aid in keep_alpha_ids}
        cleaned_count = original_count - len(self.pnl_cache)
        
        if cleaned_count > 0:
            self.logger.info(f"🧹 清理PnL缓存: 移除 {cleaned_count} 个未通过检测的Alpha，保留 {len(self.pnl_cache)} 个")
            self.save_pnl_cache()
        else:
            self.logger.debug(f"🧹 PnL缓存无需清理")
    
    def _get_alpha_pnl(self, alpha_id: str) -> pd.DataFrame:
        """获取指定alpha的PnL数据"""
        try:
            url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/pnl"
            response = self.session_service.wait_get(url, message=f"获取Alpha {alpha_id} pnl数据")
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
            pnl_data = response.json()
            
            # 检查响应数据结构
            if 'records' not in pnl_data or 'schema' not in pnl_data:
                raise Exception(f"响应数据格式错误: {pnl_data}")
            
            if not pnl_data['records']:
                # 如果没有记录，返回空DataFrame
                return pd.DataFrame(columns=['Date', alpha_id])
            
            # 构建DataFrame
            columns = [item['name'] for item in pnl_data['schema']['properties']]
            df = pd.DataFrame(pnl_data['records'], columns=columns)
            
            # 重命名列
            df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
            
            # 确保需要的列存在
            if 'Date' not in df.columns:
                raise Exception(f"响应数据中缺少date字段: {columns}")
            if alpha_id not in df.columns:
                raise Exception(f"响应数据中缺少pnl字段: {columns}")
            
            df = df[['Date', alpha_id]]
            return df
            
        except Exception as e:
            raise Exception(f"获取Alpha {alpha_id} PnL数据失败: {e}")
    
    def get_alpha_daily_pnl(self, alpha_id: str) -> pd.Series:
        """获取指定alpha的每日PnL数据（用于质量检查）"""
        try:
            url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/daily-pnl"
            response = self.session_service.wait_get(url, message=f"获取Alpha {alpha_id} daily-pnl数据")
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
            data = response.json()
            
            # 检查响应数据结构
            if 'records' not in data or 'schema' not in data:
                raise Exception(f"响应数据格式错误: {data}")
            
            if not data['records']:
                self.logger.warning(f"⚠️ Alpha {alpha_id} 没有daily-pnl数据")
                return pd.Series()
            
            # 构建DataFrame
            columns = [item['name'] for item in data['schema']['properties']]
            df = pd.DataFrame(data['records'], columns=columns)
            
            # 设置索引和返回Series
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date').sort_index()
            
            return df['pnl']
            
        except Exception as e:
            self.logger.error(f"❌ 获取Alpha {alpha_id} daily-pnl数据失败: {e}")
            return pd.Series()
    
    def get_alpha_pnls(self, alphas: List[Dict], 
                      alpha_pnls: Optional[pd.DataFrame] = None,
                      alpha_ids: Optional[Dict[str, List]] = None) -> Tuple[Dict[str, List], pd.DataFrame]:
        """获取alpha的PnL数据，并按区域分类alpha的ID（支持缓存机制）"""
        if alpha_ids is None:
            alpha_ids = defaultdict(list)
        if alpha_pnls is None or alpha_pnls.empty:
            alpha_pnls = pd.DataFrame()
        
        # 确保缓存已加载
        self.load_pnl_cache()
        
        # 过滤出新的alpha（既不在现有DataFrame中，也不在缓存中）
        existing_columns = alpha_pnls.columns.tolist() if not alpha_pnls.empty else []
        new_alphas = [item for item in alphas if item['id'] not in existing_columns]
        
        # 进一步分类：从缓存中可以获取的和需要重新下载的
        alphas_from_cache = []
        alphas_to_download = []
        
        for alpha in new_alphas:
            alpha_id = alpha['id']
            if alpha_id in self.pnl_cache:
                alphas_from_cache.append(alpha)
            else:
                alphas_to_download.append(alpha)
        
        if not new_alphas:
            return alpha_ids, alpha_pnls
        
        self.logger.info(f"📥 处理 {len(new_alphas)} 个新Alpha的PnL数据:")
        self.logger.info(f"  💾 从缓存获取: {len(alphas_from_cache)} 个")
        self.logger.info(f"  🌐 需要下载: {len(alphas_to_download)} 个")
        
        # 按区域分类所有alpha
        for alpha in new_alphas:
            region = alpha['settings']['region']
            alpha_ids[region].append(alpha['id'])
        
        # 首先从缓存中获取PnL数据
        cached_results = []
        if alphas_from_cache:
            self.logger.info(f"📂 从缓存加载 {len(alphas_from_cache)} 个Alpha的PnL数据...")
            for alpha in alphas_from_cache:
                alpha_id = alpha['id']
                try:
                    cached_pnl = self.pnl_cache[alpha_id].copy()
                    cached_pnl.name = alpha_id
                    cached_df = cached_pnl.to_frame().T
                    cached_df.index = [alpha_id]
                    cached_df = cached_df.T
                    cached_results.append(cached_df)
                except Exception as e:
                    self.logger.warning(f"  ⚠️ 从缓存获取Alpha {alpha_id}失败: {e}，将重新下载")
                    alphas_to_download.append(alpha)
        
        if not alphas_to_download:
            # 所有数据都从缓存获取
            if cached_results:
                if alpha_pnls.empty:
                    alpha_pnls = pd.concat(cached_results, axis=1)
                else:
                    alpha_pnls = pd.concat([alpha_pnls] + cached_results, axis=1)
                alpha_pnls.sort_index(inplace=True)
            return alpha_ids, alpha_pnls
        
        # 并行获取需要下载的PnL数据
        self.logger.info(f"🌐 开始下载 {len(alphas_to_download)} 个Alpha的PnL数据...")
        
        def fetch_pnl_func(alpha_data):
            alpha_id = alpha_data['id']
            try:
                result = self._get_alpha_pnl(alpha_id).set_index('Date')
                return alpha_id, result, None
            except Exception as e:
                self.logger.error(f"❌ 获取Alpha {alpha_id} PnL失败: {e}")
                return alpha_id, pd.DataFrame(), str(e)
        
        # 使用较少的并发数以避免API限制
        max_workers = min(3, len(alphas_to_download))  # 降低并发数
        results = []
        
        # 分批处理以避免API限制
        batch_size = 10
        for i in range(0, len(alphas_to_download), batch_size):
            batch = alphas_to_download[i:i + batch_size]
            self.logger.info(f"📥 处理批次 {i//batch_size + 1}/{(len(alphas_to_download)-1)//batch_size + 1}: {len(batch)} 个Alpha")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(fetch_pnl_func, batch))
                results.extend(batch_results)
            
            # 批次间延迟
            if i + batch_size < len(alphas_to_download):
                self.logger.info(f"⏸️ 批次间休息2秒...")
                time.sleep(2)
        
        # 处理下载结果并更新缓存
        downloaded_results = []
        failed_alphas = []
        newly_cached_count = 0
        
        for alpha_id, result, error in results:
            if not result.empty and error is None:
                downloaded_results.append(result)
                # 将成功下载的PnL数据添加到缓存
                self.pnl_cache[alpha_id] = result.iloc[:, 0]  # 保存为Series
                newly_cached_count += 1
            else:
                failed_alphas.append(alpha_id)
        
        # 保存更新的缓存
        if newly_cached_count > 0:
            self.save_pnl_cache()
            self.logger.info(f"💾 新增缓存: {newly_cached_count} 个Alpha的PnL数据")
        
        if failed_alphas:
            self.logger.warning(f"⚠️ {len(failed_alphas)} 个Alpha的PnL数据获取失败，跳过这些Alpha")
            self.logger.info(f"失败的Alpha ID: {failed_alphas[:10]}{'...' if len(failed_alphas) > 10 else ''}")
        
        # 合并所有结果（缓存的 + 新下载的）
        all_results = cached_results + downloaded_results
        
        if all_results:
            if alpha_pnls.empty:
                alpha_pnls = pd.concat(all_results, axis=1)
            else:
                alpha_pnls = pd.concat([alpha_pnls] + all_results, axis=1)
            alpha_pnls.sort_index(inplace=True)
            
            # 更清楚地显示PnL数据信息
            total_alphas = len(alphas_from_cache) + len(downloaded_results)
            self.logger.info(f"📊 PnL数据汇总: {total_alphas} 个Alpha ({len(alphas_from_cache)}缓存+{len(downloaded_results)}新下载), {len(alpha_pnls)} 个交易日, {alpha_pnls.shape[1]} 列")
        
        return alpha_ids, alpha_pnls
