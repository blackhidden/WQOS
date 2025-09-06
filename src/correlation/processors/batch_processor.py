"""
批量处理器 - 负责批量相关性检查的核心逻辑
"""

import time
from typing import List, Dict, Tuple
from collections import defaultdict
from ..data.alpha_data_manager import AlphaDataManager


class BatchProcessor:
    """批量处理器"""
    
    def __init__(self, config_manager, session_service, data_loader, 
                 selfcorr_checker, ppac_checker, alpha_marker, database_updater, logger):
        """初始化批量处理器"""
        self.config = config_manager
        self.session_service = session_service
        self.data_loader = data_loader
        self.selfcorr_checker = selfcorr_checker
        self.ppac_checker = ppac_checker
        self.alpha_marker = alpha_marker
        self.database_updater = database_updater
        self.logger = logger
        
        # 初始化Alpha数据管理器
        self.alpha_data_manager = AlphaDataManager(config_manager, data_loader, logger)
    
    def _preload_correlation_data(self, check_type: str) -> Dict:
        """预加载相关性数据，避免重复加载"""
        try:
            self.logger.info(f"  📂 预加载{check_type}数据...")
            
            # 临时保存当前状态
            old_check_type = self.data_loader.current_check_type
            old_data_loaded = self.data_loader.data_loaded
            old_os_alpha_ids = self.data_loader.os_alpha_ids
            old_os_alpha_rets = self.data_loader.os_alpha_rets
            
            # 重置状态并加载特定数据
            self.data_loader.current_check_type = None
            self.data_loader.data_loaded = False
            success, _ = self.data_loader.ensure_data_loaded(check_type=check_type)
            
            if success:
                # 保存加载的数据
                data = {
                    'os_alpha_ids': self.data_loader.os_alpha_ids.copy() if self.data_loader.os_alpha_ids else {},
                    'os_alpha_rets': self.data_loader.os_alpha_rets.copy() if self.data_loader.os_alpha_rets is not None else None,
                    'threshold': self.config.ppac_threshold if check_type == "PPAC" else self.config.correlation_threshold
                }
                self.logger.info(f"  ✅ {check_type}数据预加载完成")
            else:
                data = None
                self.logger.warning(f"  ❌ {check_type}数据预加载失败")
            
            # 恢复原有状态
            self.data_loader.current_check_type = old_check_type
            self.data_loader.data_loaded = old_data_loaded
            self.data_loader.os_alpha_ids = old_os_alpha_ids
            self.data_loader.os_alpha_rets = old_os_alpha_rets
            
            return data
            
        except Exception as e:
            self.logger.error(f"❌ 预加载{check_type}数据异常: {e}")
            return None
    
    def batch_check_correlations(self, yellow_alphas: List[Dict]) -> Tuple[List[str], List[str], List[str], List[str], List[str], Dict]:
        """批量检查相关性
        
        优化逻辑：
        1. PPAC检测：所有Alpha都需要检测
        2. 普通检测：只检查 sharpe>1.58 且 fitness>1 的Alpha
        3. 预加载数据，避免重复加载
        """
        if not yellow_alphas:
            return [], [], [], [], [], {}
        
        self.logger.info(f"\n🔍 开始批量相关性检查: {len(yellow_alphas)} 个Yellow Alpha")
        
        # 预加载PPAC和SelfCorr数据
        self.logger.info(f"📂 预加载相关性检查数据...")
        ppac_data = self._preload_correlation_data("PPAC")
        selfcorr_data = self._preload_correlation_data("SelfCorr")
        
        # 按区域分组并应用新的过滤逻辑
        region_groups = defaultdict(list)
        filtered_count = 0
        
        for alpha in yellow_alphas:
            region = alpha.get('region', 'USA')
            sharpe = alpha.get('sharpe', 0.0) or 0.0
            fitness = alpha.get('fitness', 0.0) or 0.0
            operator_count = alpha.get('operator_count', 0) or 0
            
            # 检查是否满足普通检测条件
            meets_selfcorr_criteria = sharpe > 1.58 and fitness > 1.0
            
            # 检查是否需要PPAC检查：operator_count <= 8 才检查PPAC
            needs_ppac_check = operator_count <= 8
            
            # 为Alpha添加检查标记
            alpha['needs_ppac_check'] = needs_ppac_check
            alpha['needs_selfcorr_check'] = meets_selfcorr_criteria
            alpha['operator_count'] = operator_count
            
            if not meets_selfcorr_criteria:
                filtered_count += 1
                self.logger.debug(f"  ⚠️ Alpha {alpha['id']}: sharpe={sharpe:.3f}, fitness={fitness:.3f} - 不满足普通检测条件")
            
            if not needs_ppac_check:
                self.logger.debug(f"  📊 Alpha {alpha['id']}: operator_count={operator_count} > 8 - 跳过PPAC检查")
            
            region_groups[region].append(alpha)
        
        # 统计各种过滤条件
        ppac_skipped_count = sum(1 for alpha in yellow_alphas if not alpha['needs_ppac_check'])
        
        if filtered_count > 0 or ppac_skipped_count > 0:
            self.logger.info(f"📊 检测条件统计: {len(yellow_alphas)} 个Alpha中")
            if filtered_count > 0:
                self.logger.info(f"  ⚠️ {filtered_count} 个不满足普通检测条件 (sharpe≤1.58 或 fitness≤1)")
            if ppac_skipped_count > 0:
                self.logger.info(f"  📊 {ppac_skipped_count} 个跳过PPAC检查 (operator_count > 8)")
        
        green_alphas = []   # 通过普通相关性检查的Alpha
        blue_alphas = []    # 通过PPAC检查但未通过普通检查的Alpha  
        red_alphas = []     # 未通过任何检查的Alpha
        purple_alphas = []  # 厂字型Alpha（标准差为0等数据质量问题）
        aggressive_alphas = []  # 激进模式Alpha（早期为0，近期强势上涨）
        
        # 保存相关性数值用于数据库更新
        correlation_results = {}  # {alpha_id: {'self_corr': float, 'prod_corr': float}}
        
        for region, alphas in region_groups.items():
            self.logger.info(f"\n🌍 处理 {region} 区域: {len(alphas)} 个Alpha")
            
            # 检查区域是否在数据中
            if region not in self.data_loader.os_alpha_ids or not self.data_loader.os_alpha_ids[region]:
                self.logger.warning(f"⚠️ {region} 区域无参考数据，根据条件分配颜色")
                for alpha in alphas:
                    if alpha['needs_selfcorr_check']:
                        green_alphas.append(alpha['id'])  # 满足条件但无参考数据，默认GREEN
                    else:
                        red_alphas.append(alpha['id'])    # 不满足条件，标记RED
                continue
            
            self.logger.info(f"📊 {region} 区域参考alpha数量: {len(self.data_loader.os_alpha_ids[region])}")
            
            # 分批检查
            for i in range(0, len(alphas), self.config.batch_size):
                batch = alphas[i:i + self.config.batch_size]
                self.logger.info(f"  📦 批次 {i//self.config.batch_size + 1}: {len(batch)} 个Alpha")
                
                # 批量获取当前批次Alpha的详细信息和PnL数据
                self.logger.info(f"    📂 获取批次Alpha详细信息和PnL数据...")
                alpha_details_and_pnls = self.alpha_data_manager.batch_get_alpha_details_and_pnls(batch)
                
                # 处理每个Alpha
                batch_results = self._process_alpha_batch(
                    batch, alpha_details_and_pnls, ppac_data, selfcorr_data, region
                )
                
                # 合并批次结果
                for key, values in batch_results.items():
                    if key == 'green_alphas':
                        green_alphas.extend(values)
                    elif key == 'blue_alphas':
                        blue_alphas.extend(values)
                    elif key == 'red_alphas':
                        red_alphas.extend(values)
                    elif key == 'purple_alphas':
                        purple_alphas.extend(values)
                    elif key == 'aggressive_alphas':
                        aggressive_alphas.extend(values)
                    elif key == 'correlation_results':
                        correlation_results.update(values)
                
                # 处理批次结果
                self._handle_batch_results(batch, batch_results)
                
                # 批次间延迟
                if i + self.config.batch_size < len(alphas):
                    self.logger.info(f"  ⏸️  批次间休息 3 秒...")
                    time.sleep(3)
        
        # 统计结果并显示详细信息
        total_checked = len(green_alphas) + len(blue_alphas) + len(red_alphas) + len(purple_alphas)
        self.logger.info(f"\n📊 相关性检查统计:")
        self.logger.info(f"  📈 总检查: {total_checked} 个Alpha")
        self.logger.info(f"  🟢 GREEN: {len(green_alphas)} 个 ({len(green_alphas)/total_checked*100:.1f}%) - 通过普通检查")
        self.logger.info(f"  🔵 BLUE: {len(blue_alphas)} 个 ({len(blue_alphas)/total_checked*100:.1f}%) - 仅通过PPAC检查")
        self.logger.info(f"  🔴 RED: {len(red_alphas)} 个 ({len(red_alphas)/total_checked*100:.1f}%) - 未通过检查")
        self.logger.info(f"  🟣 PURPLE: {len(purple_alphas)} 个 ({len(purple_alphas)/total_checked*100:.1f}%) - 厂字型Alpha")
        if aggressive_alphas:
            self.logger.info(f"  🚀 AGGRESSIVE: {len(aggressive_alphas)} 个 - 激进模式Alpha (早期为0，近期强势上涨)")
        self.logger.info(f"  ✅ 保留率: {(len(green_alphas)+len(blue_alphas))/total_checked*100:.1f}%")
        self.logger.info(f"  🗑️ 移除率: {(len(red_alphas)+len(purple_alphas))/total_checked*100:.1f}%")
        
        # 清理PnL缓存，只保留通过检测的Alpha数据
        passed_alphas = green_alphas + blue_alphas
        if passed_alphas:
            self.data_loader.pnl_manager.cleanup_pnl_cache(passed_alphas)
        
        return green_alphas, blue_alphas, red_alphas, purple_alphas, aggressive_alphas, correlation_results
    
    def _process_alpha_batch(self, batch: List[Dict], alpha_details_and_pnls: Dict, 
                           ppac_data: Dict, selfcorr_data: Dict, region: str) -> Dict:
        """处理单个批次的Alpha检查"""
        results = {
            'green_alphas': [],
            'blue_alphas': [],
            'red_alphas': [],
            'purple_alphas': [],
            'aggressive_alphas': [],
            'correlation_results': {}
        }
        
        for alpha in batch:
            alpha_id = alpha['id']
            sharpe = alpha.get('sharpe', 0.0) or 0.0
            fitness = alpha.get('fitness', 0.0) or 0.0
            operator_count = alpha.get('operator_count', 0) or 0
            
            self.logger.info(f"      🔍 检查Alpha {alpha_id}")
            self.logger.info(f"         📈 Alpha指标: Sharpe={sharpe:.3f}, Fitness={fitness:.3f}, Operators={operator_count}")
            self.logger.info(f"         📋 检测策略: 需要PPAC检查={alpha['needs_ppac_check']}, 需要普通检查={alpha['needs_selfcorr_check']}")
            
            # 获取预加载的数据
            alpha_result = alpha_details_and_pnls['alpha_results'].get(alpha_id)
            alpha_pnls = alpha_details_and_pnls['alpha_pnls'].get(alpha_id)
            
            if alpha_result is None:
                self.logger.warning(f"      ⚠️ Alpha {alpha_id} 详细信息不可用，跳过检查")
                continue
            
            # 处理单个Alpha
            alpha_results = self._process_single_alpha(
                alpha, alpha_result, alpha_pnls, ppac_data, selfcorr_data, region
            )
            
            # 合并结果
            for key, value in alpha_results.items():
                if key in ['green_alphas', 'blue_alphas', 'red_alphas', 'purple_alphas']:
                    if value:
                        results[key].append(alpha_id)
                elif key == 'aggressive_alphas':
                    if value and alpha_id not in results['aggressive_alphas']:  # 避免重复添加
                        results['aggressive_alphas'].append(alpha_id)
                elif key == 'correlation_result':
                    if value:
                        results['correlation_results'][alpha_id] = value
        
        return results
    
    def _process_single_alpha(self, alpha: Dict, alpha_result: Dict, alpha_pnls, 
                             ppac_data: Dict, selfcorr_data: Dict, region: str) -> Dict:
        """处理单个Alpha的检查逻辑"""
        alpha_id = alpha['id']
        is_aggressive_from_ppac = False
        
        # 检查PPAC相关性（根据operator_count决定是否检查）
        if alpha['needs_ppac_check']:
            ppac_passed, ppac_corr = self.ppac_checker.check_correlation_with_data(
                alpha_id, region, ppac_data, alpha_result, alpha_pnls)
            
            # 检查是否为厂字型Alpha
            if ppac_corr == -999.0:
                self.logger.info(f"      🟣 Alpha {alpha_id}: 检测到厂字型Alpha → PURPLE")
                self.logger.info(f"         🏭 数据质量问题: 收益率标准差为0或数据无效")
                return {'purple_alphas': True}
            
            # 检查是否为激进模式Alpha（但继续进行后续检查）
            if ppac_corr == -888.0:
                is_aggressive_from_ppac = True
                self.logger.info(f"      🚀 Alpha {alpha_id}: 检测到激进模式Alpha → 设置aggressive_mode=True")
                self.logger.info(f"         📈 模式特征: 早期为0，近期强势上涨")
                # 激进模式Alpha继续进行正常的相关性检查，不跳过
        else:
            # operator_count > 8，默认不通过PPAC
            ppac_passed, ppac_corr = False, 999.0
            self.logger.info(f"         📊 算子数量 {alpha['operator_count']} > 8，跳过PPAC检查，默认不通过")
        
        # 检查普通相关性（只检查满足条件的Alpha）
        if alpha['needs_selfcorr_check']:
            result = self._process_selfcorr_check(
                alpha, alpha_result, alpha_pnls, selfcorr_data, ppac_data, 
                region, ppac_passed, ppac_corr
            )
            # 如果PPAC检查检测到激进模式，需要合并结果
            if is_aggressive_from_ppac:
                result['aggressive_alphas'] = True
            return result
        else:
            result = self._process_non_selfcorr_alpha(
                alpha, alpha_result, alpha_pnls, ppac_data, 
                region, ppac_passed, ppac_corr
            )
            # 如果PPAC检查检测到激进模式，需要合并结果
            if is_aggressive_from_ppac:
                result['aggressive_alphas'] = True
            return result
    
    def _process_selfcorr_check(self, alpha: Dict, alpha_result: Dict, alpha_pnls,
                               selfcorr_data: Dict, ppac_data: Dict, region: str,
                               ppac_passed: bool, ppac_corr: float) -> Dict:
        """处理满足普通检测条件的Alpha"""
        alpha_id = alpha['id']
        
        selfcorr_passed, selfcorr_corr = self.selfcorr_checker.check_correlation_with_data(
            alpha_id, region, selfcorr_data, alpha_result, alpha_pnls)
        
        # 检查是否为厂字型Alpha（在普通检查中也可能检测到）
        if selfcorr_corr == -999.0:
            self.logger.info(f"      🟣 Alpha {alpha_id}: 检测到厂字型Alpha → PURPLE")
            self.logger.info(f"         🏭 数据质量问题: 收益率标准差为0或数据无效")
            return {'purple_alphas': True}
        
        # 检查是否为激进模式Alpha（在普通检查中也可能检测到）
        is_aggressive_from_selfcorr = False
        if selfcorr_corr == -888.0:
            is_aggressive_from_selfcorr = True
            self.logger.info(f"      🚀 Alpha {alpha_id}: 检测到激进模式Alpha → 设置aggressive_mode=True")
            self.logger.info(f"         📈 模式特征: 早期为0，近期强势上涨")
            # 激进模式Alpha继续进行正常的相关性检查，不跳过
        
        # 保存相关性数值（激进模式alpha需要重新计算实际相关性）
        actual_selfcorr = selfcorr_corr if selfcorr_corr != -888.0 else self.selfcorr_checker.recalc_correlation_for_aggressive(alpha_id, region, selfcorr_data, alpha_result, alpha_pnls)
        actual_ppac_corr = ppac_corr if ppac_corr != -888.0 else self.ppac_checker.recalc_correlation_for_aggressive(alpha_id, region, ppac_data, alpha_result, alpha_pnls)
        
        correlation_result = {
            'self_corr': actual_selfcorr,
            'prod_corr': actual_ppac_corr
        }
        
        # 使用实际相关性值进行判断
        actual_selfcorr_passed = actual_selfcorr < selfcorr_data['threshold']
        actual_ppac_passed = actual_ppac_corr < ppac_data['threshold']
        
        result = {'correlation_result': correlation_result}
        if is_aggressive_from_selfcorr:
            result['aggressive_alphas'] = True
        
        if actual_selfcorr_passed:
            result['green_alphas'] = True
            self.logger.info(f"      ✅ Alpha {alpha_id}: 通过普通检查 → GREEN")
            self.logger.info(f"         📊 SelfCorr: {actual_selfcorr:.4f} < {selfcorr_data['threshold']} | PPAC: {actual_ppac_corr:.4f} < {ppac_data['threshold']}")
        elif actual_ppac_passed:
            result['blue_alphas'] = True
            self.logger.info(f"      🔵 Alpha {alpha_id}: 仅通过PPAC检查 → BLUE")
            self.logger.info(f"         📊 SelfCorr: {actual_selfcorr:.4f} ≥ {selfcorr_data['threshold']} | PPAC: {actual_ppac_corr:.4f} < {ppac_data['threshold']}")
        else:
            result['red_alphas'] = True
            self.logger.info(f"      ❌ Alpha {alpha_id}: 未通过任何检查 → RED")
            self.logger.info(f"         📊 SelfCorr: {actual_selfcorr:.4f} ≥ {selfcorr_data['threshold']} | PPAC: {actual_ppac_corr:.4f} ≥ {ppac_data['threshold']}")
        
        return result
    
    def _process_non_selfcorr_alpha(self, alpha: Dict, alpha_result: Dict, alpha_pnls,
                                   ppac_data: Dict, region: str, 
                                   ppac_passed: bool, ppac_corr: float) -> Dict:
        """处理不满足普通检测条件的Alpha"""
        alpha_id = alpha['id']
        sharpe = alpha.get('sharpe', 0.0) or 0.0
        fitness = alpha.get('fitness', 0.0) or 0.0
        operator_count = alpha.get('operator_count', 0) or 0
        
        # 保存相关性数值（激进模式alpha需要重新计算实际相关性）
        actual_ppac_corr = ppac_corr if ppac_corr != -888.0 else self.ppac_checker.recalc_correlation_for_aggressive(alpha_id, region, ppac_data, alpha_result, alpha_pnls)
        
        correlation_result = {
            'self_corr': 999.0,  # 不满足条件，未检查普通相关性
            'prod_corr': actual_ppac_corr
        }
        
        # 使用实际相关性值进行判断
        actual_ppac_passed = actual_ppac_corr < ppac_data['threshold']
        
        # 生成详细的条件说明
        sharpe_status = f"Sharpe: {sharpe:.3f} {'✗' if sharpe <= 1.58 else '✓'} (需要 > 1.58)"
        fitness_status = f"Fitness: {fitness:.3f} {'✗' if fitness <= 1.0 else '✓'} (需要 > 1.0)"
        
        result = {'correlation_result': correlation_result}
        
        if actual_ppac_passed:
            result['blue_alphas'] = True
            self.logger.info(f"      🔵 Alpha {alpha_id}: 不满足普通检测条件，仅通过PPAC → BLUE")
            ppac_status = f"PPAC: {actual_ppac_corr:.4f} < {ppac_data['threshold']}" if alpha['needs_ppac_check'] else f"PPAC: 跳过 (算子数 {operator_count} > 8)"
            self.logger.info(f"         📊 {sharpe_status} | {fitness_status} | {ppac_status}")
        else:
            result['red_alphas'] = True
            self.logger.info(f"      ❌ Alpha {alpha_id}: 不满足普通检测条件且未通过PPAC → RED")
            if alpha['needs_ppac_check']:
                ppac_status = f"PPAC: {actual_ppac_corr:.4f} ≥ {ppac_data['threshold']}"
            else:
                ppac_status = f"PPAC: 不通过 (算子数 {operator_count} > 8)"
            self.logger.info(f"         📊 {sharpe_status} | {fitness_status} | {ppac_status}")
        
        return result
    
    def _handle_batch_results(self, batch: List[Dict], batch_results: Dict):
        """处理批次结果 - 更新数据库和标记颜色"""
        # 获取批次中各种类型的Alpha
        batch_green = batch_results.get('green_alphas', [])
        batch_blue = batch_results.get('blue_alphas', [])
        batch_red = batch_results.get('red_alphas', [])
        batch_purple = batch_results.get('purple_alphas', [])
        batch_aggressive = batch_results.get('aggressive_alphas', [])
        batch_correlation_updates = []
        
        # 准备相关性更新数据
        for alpha in batch:
            alpha_id = alpha['id']
            if alpha_id in batch_results.get('correlation_results', {}):
                corr_result = batch_results['correlation_results'][alpha_id]
                batch_correlation_updates.append({
                    'alpha_id': alpha_id,
                    'self_corr': corr_result['self_corr'],
                    'prod_corr': corr_result['prod_corr']
                })
        
        if batch_green or batch_blue or batch_red or batch_purple:
            self.logger.info(f"    🎨 批次结果处理...")
            self.logger.info(f"      🟢 GREEN: {len(batch_green)} | 🔵 BLUE: {len(batch_blue)} | 🔴 RED: {len(batch_red)} | 🟣 PURPLE: {len(batch_purple)}")
            
            # 更新数据库中的相关性数值
            if batch_correlation_updates:
                self.logger.info(f"      📊 更新 {len(batch_correlation_updates)} 个Alpha的相关性数值...")
                self.database_updater.batch_update_correlations(batch_correlation_updates)
            
            # 更新激进模式Alpha的aggressive_mode字段
            if batch_aggressive:
                self.logger.info(f"      🚀 更新 {len(batch_aggressive)} 个Alpha的aggressive_mode为True...")
                self.database_updater.batch_update_aggressive_mode(batch_aggressive)
            
            # 标记Alpha颜色并更新数据库
            if batch_green:
                self.logger.info(f"      🟢 标记 {len(batch_green)} 个Alpha为GREEN...")
                self.alpha_marker.batch_set_color(batch_green, "GREEN")
                self.database_updater.update_database_colors(batch_green, "GREEN")
            
            if batch_blue:
                self.logger.info(f"      🔵 标记 {len(batch_blue)} 个Alpha为BLUE...")
                self.alpha_marker.batch_set_color(batch_blue, "BLUE")
                self.database_updater.update_database_colors(batch_blue, "BLUE")
            
            if batch_red:
                self.logger.info(f"      🔴 标记 {len(batch_red)} 个Alpha为RED...")
                self.alpha_marker.batch_set_color(batch_red, "RED")
                self.database_updater.update_database_colors(batch_red, "RED")
                
                # 从数据库中移除RED Alpha
                self.logger.info(f"      🗑️ 从数据库中移除 {len(batch_red)} 个RED Alpha...")
                self.database_updater.remove_alphas_batch(batch_red)
            
            if batch_purple:
                self.logger.info(f"      🟣 标记 {len(batch_purple)} 个厂字型Alpha为PURPLE...")
                self.alpha_marker.batch_set_color(batch_purple, "PURPLE")
                self.database_updater.update_database_colors(batch_purple, "PURPLE")
                
                # 从数据库中移除PURPLE Alpha（厂字型Alpha）
                self.logger.info(f"      🗑️ 从数据库中移除 {len(batch_purple)} 个厂字型Alpha...")
                self.database_updater.remove_alphas_batch(batch_purple)
