"""
批量处理器 - 负责批量相关性检查的核心逻辑
"""

import time
import pandas as pd
from typing import List, Dict, Tuple
from collections import defaultdict
from ..data.alpha_data_manager import AlphaDataManager


class BatchProcessor:
    """批量处理器"""
    
    def __init__(self, config_manager, session_service, data_loader, 
                 selfcorr_checker, ppac_checker, alpha_marker, database_updater, 
                 quality_checker, logger):
        """初始化批量处理器"""
        self.config = config_manager
        self.session_service = session_service
        self.data_loader = data_loader
        self.selfcorr_checker = selfcorr_checker
        self.ppac_checker = ppac_checker
        self.alpha_marker = alpha_marker
        self.database_updater = database_updater
        self.quality_checker = quality_checker
        self.logger = logger
        
        # 初始化Alpha数据管理器
        self.alpha_data_manager = AlphaDataManager(config_manager, data_loader, logger)
        
        # 初始化激进模式检查器（避免重复检查）
        from ..checkers.aggressive_checker import AggressiveChecker
        self.aggressive_checker = AggressiveChecker(config_manager, session_service, data_loader, logger)
    
    def _preload_correlation_data(self, check_type: str) -> Dict:
        """预加载相关性数据，仅加载本地数据不检查新Alpha"""
        try:
            self.logger.info(f"  📂 预加载{check_type}数据...")
            
            # 临时保存当前状态
            old_check_type = self.data_loader.current_check_type
            old_data_loaded = self.data_loader.data_loaded
            old_os_alpha_ids = self.data_loader.os_alpha_ids
            old_os_alpha_rets = self.data_loader.os_alpha_rets
            
            # 重置状态并仅加载本地数据（不检查新Alpha）
            self.data_loader.current_check_type = None
            self.data_loader.data_loaded = False
            success, has_cached_data = self.data_loader.load_local_data_only(check_type=check_type)
            
            if success:
                # 保存加载的数据
                data = {
                    'os_alpha_ids': self.data_loader.os_alpha_ids.copy() if self.data_loader.os_alpha_ids else {},
                    'os_alpha_rets': self.data_loader.os_alpha_rets.copy() if self.data_loader.os_alpha_rets is not None else None,
                    'threshold': self.config.ppac_threshold if check_type == "PPAC" else self.config.correlation_threshold,
                    'has_cached_data': has_cached_data  # 传递缓存数据标记
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
    
    def batch_check_correlations(self, yellow_alphas: List[Dict], recheck_mode: bool = False) -> Tuple[List[str], List[str], List[str], List[str], List[str], Dict]:
        """批量检查相关性
        
        集成检查流程（每个Alpha）：
        1. 质量检查：Zero Coverage等质量检查（复查模式下跳过）
        2. 激进模式检查：检测早期为0，近期强势上涨的Alpha（复查模式下跳过）
        3. 相关性检查：普通相关性检查和PPAC检查（根据条件判断）
        
        Args:
            yellow_alphas: 待检查的Alpha列表
            recheck_mode: 是否为复查模式，复查模式下跳过质量检查和激进模式检查
        """
        if not yellow_alphas:
            return [], [], [], [], [], {}
        
        mode_desc = "复查模式" if recheck_mode else "完整检查模式"
        self.logger.info(f"\n🔍 开始批量集成检查 ({mode_desc}): {len(yellow_alphas)} 个Alpha")
        
        if recheck_mode:
            self.logger.info(f"📋 复查模式说明:")
            self.logger.info(f"  ✅ 跳过质量检查（Zero Coverage、厂字型等）")
            self.logger.info(f"  ✅ 跳过激进模式检查")
            self.logger.info(f"  🔍 仅进行相关性检查（SelfCorr/PPAC）")
            self.logger.info(f"  💡 通过的Alpha不会重复标记颜色")
        
        # 预加载PPAC和SelfCorr数据
        self.logger.info(f"📂 预加载相关性检查数据...")
        ppac_data = self._preload_correlation_data("PPAC")
        selfcorr_data = self._preload_correlation_data("SelfCorr")
        
        # 按区域分组进行批量处理
        region_groups = defaultdict(list)
        for alpha in yellow_alphas:
            region = alpha.get('region', 'USA')
            region_groups[region].append(alpha)
        
        # 初始化结果列表
        green_alphas = []   # 通过普通相关性检查的Alpha
        blue_alphas = []    # 通过PPAC检查但未通过普通检查的Alpha
        red_alphas = []     # 未通过任何检查的Alpha
        purple_alphas = []  # 质量检查失败的Alpha
        aggressive_alphas = []  # 激进模式Alpha（仅用于数据库标记，不影响颜色）
        
        # 保存相关性数值用于数据库更新
        correlation_results = {}  # {alpha_id: {'self_corr': float, 'prod_corr': float}}
        
        for region, alphas in region_groups.items():
            self.logger.info(f"\n🌍 处理 {region} 区域: {len(alphas)} 个Alpha")
            
            # 获取区域Alpha的详细信息和PnL数据
            alpha_data = self.alpha_data_manager.batch_get_alpha_details_and_pnls(alphas)
            alpha_details_and_pnls = alpha_data['alpha_results']  # 提取alpha_results部分
            alpha_pnls_data = alpha_data['alpha_pnls']  # 提取alpha_pnls部分
            
            # 分批处理以避免内存溢出
            for i in range(0, len(alphas), self.config.batch_size):
                batch = alphas[i:i + self.config.batch_size]
                batch_end = min(i + self.config.batch_size, len(alphas))
                
                self.logger.info(f"  📦 处理批次 {i//self.config.batch_size + 1}: Alpha {i+1}-{batch_end} / {len(alphas)}")
                
                # 使用集成检查处理批次
                batch_results = self._process_alpha_batch_integrated(batch, alpha_details_and_pnls, alpha_pnls_data, ppac_data, selfcorr_data, region, recheck_mode)
                
                # 合并结果
                green_alphas.extend(batch_results['green_alphas'])
                blue_alphas.extend(batch_results['blue_alphas'])
                red_alphas.extend(batch_results['red_alphas'])
                purple_alphas.extend(batch_results['purple_alphas'])
                aggressive_alphas.extend(batch_results['aggressive_alphas'])
                correlation_results.update(batch_results['correlation_results'])
                
                # 处理批次结果（标记和数据库操作）
                self._handle_batch_results_integrated(batch, batch_results, recheck_mode)
                
                # 批次间延迟
                if i + self.config.batch_size < len(alphas):
                    self.logger.info(f"  ⏸️  批次间休息 3 秒...")
                    time.sleep(3)
        
        # 统计结果并显示详细信息
        total_checked = len(green_alphas) + len(blue_alphas) + len(red_alphas) + len(purple_alphas)
        self.logger.info(f"\n📊 相关性检查统计:")
        self.logger.info(f"  📈 总检查: {total_checked} 个Alpha")
        
        if total_checked > 0:
            self.logger.info(f"  🟢 GREEN: {len(green_alphas)} 个 ({len(green_alphas)/total_checked*100:.1f}%) - 通过普通检查")
            self.logger.info(f"  🔵 BLUE: {len(blue_alphas)} 个 ({len(blue_alphas)/total_checked*100:.1f}%) - 仅通过PPAC检查")
            self.logger.info(f"  🔴 RED: {len(red_alphas)} 个 ({len(red_alphas)/total_checked*100:.1f}%) - 未通过检查")
            self.logger.info(f"  🟣 PURPLE: {len(purple_alphas)} 个 ({len(purple_alphas)/total_checked*100:.1f}%) - 厂字型Alpha")
            if aggressive_alphas:
                self.logger.info(f"  🚀 AGGRESSIVE: {len(aggressive_alphas)} 个 - 激进模式Alpha (数据库标记，不影响颜色分类)")
            self.logger.info(f"  ✅ 保留率: {(len(green_alphas)+len(blue_alphas))/total_checked*100:.1f}%")
            self.logger.info(f"  🗑️ 移除率: {(len(red_alphas)+len(purple_alphas))/total_checked*100:.1f}%")
        else:
            self.logger.warning(f"  ⚠️ 没有Alpha被成功检查 - 所有Alpha详细信息都不可用")
            self.logger.info(f"  🟢 GREEN: {len(green_alphas)} 个")
            self.logger.info(f"  🔵 BLUE: {len(blue_alphas)} 个") 
            self.logger.info(f"  🔴 RED: {len(red_alphas)} 个")
            self.logger.info(f"  🟣 PURPLE: {len(purple_alphas)} 个")
            if aggressive_alphas:
                self.logger.info(f"  🚀 AGGRESSIVE: {len(aggressive_alphas)} 个")
        
        # 注意：在重新检测过程中，不在批次完成后立即清理PnL缓存
        # 因为此时数据库中的alpha状态都是YELLOW，会导致错误清理其他批次的alpha缓存
        # PnL缓存清理将在整个检测流程完成后统一进行
        self.logger.info(f"💾 PnL缓存暂不清理，等待整个检测流程完成后统一处理")
        self.logger.debug(f"🔄 原因: 重新检测期间所有alpha都处于YELLOW状态，无法准确识别历史通过的alpha")
        
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
    
    def _quality_check_filter(self, yellow_alphas: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """质量检查过滤器
        
        Args:
            yellow_alphas: 待检查的Alpha列表
            
        Returns:
            (通过质量检查的Alpha列表, 未通过质量检查的Alpha列表)
        """
        passed_alphas = []
        failed_alphas = []
        
        for alpha in yellow_alphas:
            alpha_id = alpha['id']
            
            try:
                # 执行质量检查
                quality_result = self.quality_checker.run_quality_checks(alpha_id)
                
                if quality_result['overall_pass']:
                    passed_alphas.append(alpha)
                    self.logger.debug(f"  ✅ Alpha {alpha_id} 通过质量检查")
                else:
                    failed_alphas.append(alpha)
                    # 质量检查失败，收集到失败列表，稍后统一处理
                    self.logger.warning(f"  🟣 Alpha {alpha_id} 质量检查失败: {quality_result.get('summary', 'Unknown error')}")
                    
            except Exception as e:
                self.logger.error(f"  ❌ Alpha {alpha_id} 质量检查异常: {e}")
                failed_alphas.append(alpha)
                # 异常情况也收集到失败列表，稍后统一处理
        
        return passed_alphas, failed_alphas
    
    def _process_single_alpha_integrated(self, alpha: Dict, alpha_result: Dict, alpha_pnls: pd.DataFrame,
                                        ppac_data: Dict, selfcorr_data: Dict, region: str, recheck_mode: bool = False) -> Dict:
        """集成检查单个Alpha的完整流程
        
        流程：
        - 完整模式：1.质量检查 → 2.激进模式检查 → 3.相关性检查（普通+PPAC）
        - 复查模式：3.相关性检查（普通+PPAC）- 跳过质量检查和激进模式检查
        """
        alpha_id = alpha['id']
        result = {
            'status': None,  # 'green', 'blue', 'red', 'purple'
            'is_aggressive': False,
            'self_corr': None,
            'prod_corr': None,
            'message': ''
        }
        
        try:
            if not recheck_mode:
                # 第一步：质量检查（复查模式下跳过）
                self.logger.info(f"🔍 Alpha {alpha_id}: 开始质量检查")
                quality_result = self.quality_checker.run_quality_checks(alpha_id, alpha_result, alpha_pnls)
                
                if not quality_result['overall_pass']:
                    result['status'] = 'purple'
                    result['message'] = f"质量检查失败: {quality_result.get('summary', 'Unknown error')}"
                    self.logger.info(f"    🟣 Alpha {alpha_id}: 质量检查失败")
                    return result
                
                
                # 第二步：激进模式检查（仅标记，不影响后续流程）
                aggressive_result = self.aggressive_checker.check_correlation(
                    alpha_id, region, alpha_result, alpha_pnls, use_extended_window=True
                )
                
                if aggressive_result:
                    result['is_aggressive'] = True
                    self.logger.info(f"    🚀 Alpha {alpha_id}: 检测到激进模式（仅标记数据库）")
                    # 激进模式Alpha继续进行相关性检查，不直接返回
                else:
                    self.logger.info(f"    ✅ Alpha {alpha_id}: 激进模式检查完成（非激进模式）")
            else:
                self.logger.info(f"🔄 Alpha {alpha_id}: 复查模式 - 跳过质量检查和激进模式检查")
            
            # 第三步：相关性检查
            # 获取Alpha基本信息
            sharpe = alpha.get('sharpe', 0.0) or 0.0
            fitness = alpha.get('fitness', 0.0) or 0.0
            operator_count = alpha.get('operator_count', 0) or 0
            
            # 判断检查条件
            needs_selfcorr_check = sharpe > 1.58 and fitness > 1.0
            needs_ppac_check = operator_count <= 8
            
            # 初始化相关性结果
            selfcorr_passed = False
            ppac_passed = False
            self_corr_value = 0.0
            prod_corr_value = 0.0
            
            # 普通相关性检查
            if needs_selfcorr_check:
                selfcorr_passed, self_corr_value = self.selfcorr_checker.check_correlation_with_data(
                    alpha_id, region, selfcorr_data, alpha_result, alpha_pnls
                )
                result['self_corr'] = self_corr_value
                corr_str = f"{self_corr_value:.4f}" if self_corr_value is not None else "None"
                self.logger.info(f"    📈 Alpha {alpha_id}: 普通检查结果: {selfcorr_passed}, 相关性: {corr_str}")
            else:
                self.logger.info(f"    ⚠️ Alpha {alpha_id}: 跳过普通检查 (sharpe={sharpe:.3f}, fitness={fitness:.3f})")
            
            # PPAC检查
            if needs_ppac_check:
                ppac_passed, prod_corr_value = self.ppac_checker.check_correlation_with_data(
                    alpha_id, region, ppac_data, alpha_result, alpha_pnls
                )
                result['prod_corr'] = prod_corr_value
                corr_str = f"{prod_corr_value:.4f}" if prod_corr_value is not None else "None"
                self.logger.info(f"    📊 Alpha {alpha_id}: PPAC检查结果: {ppac_passed}, 相关性: {corr_str}")
            else:
                self.logger.info(f"    📊 Alpha {alpha_id}: 跳过PPAC检查 (operator_count={operator_count})")
            
            # 决定最终状态
            if selfcorr_passed:
                result['status'] = 'green'
                result['message'] = '通过普通相关性检查'
                self.logger.info(f"    🟢 Alpha {alpha_id}: 最终结果 - GREEN (通过普通相关性检查)")
            elif ppac_passed:
                result['status'] = 'blue'
                result['message'] = '通过PPAC检查但未通过普通检查'
                self.logger.info(f"    🔵 Alpha {alpha_id}: 最终结果 - BLUE (仅通过PPAC检查)")
            else:
                result['status'] = 'red'
                result['message'] = '未通过任何相关性检查'
                self.logger.info(f"    🔴 Alpha {alpha_id}: 最终结果 - RED (未通过任何检查)")
            
            return result
            
        except Exception as e:
            self.logger.error(f"    ❌ Alpha {alpha_id} 集成检查异常: {e}")
            result['status'] = 'purple'
            result['message'] = f'检查异常: {str(e)}'
            return result
    
    def _process_alpha_batch_integrated(self, batch: List[Dict], alpha_details_and_pnls: Dict, alpha_pnls_data: Dict,
                                       ppac_data: Dict, selfcorr_data: Dict, region: str, recheck_mode: bool = False) -> Dict:
        """使用集成检查处理单个批次的Alpha"""
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
            
            # 获取Alpha详细信息
            alpha_result = alpha_details_and_pnls.get(alpha_id)
            if alpha_result is None:
                self.logger.warning(f"      ⚠️ Alpha {alpha_id} 详细信息不可用，跳过检查")
                continue
            
            # 获取Alpha的PnL数据
            alpha_pnls = alpha_pnls_data.get(alpha_id)
            
            # 执行集成检查
            check_result = self._process_single_alpha_integrated(
                alpha, alpha_result, alpha_pnls, ppac_data, selfcorr_data, region, recheck_mode
            )
            
            # 根据检查结果分类
            status = check_result['status']
            if status == 'green':
                results['green_alphas'].append(alpha_id)
            elif status == 'blue':
                results['blue_alphas'].append(alpha_id)
            elif status == 'red':
                results['red_alphas'].append(alpha_id)
            elif status == 'purple':
                results['purple_alphas'].append(alpha_id)
            
            # 记录激进模式Alpha
            if check_result['is_aggressive']:
                results['aggressive_alphas'].append(alpha_id)
            
            # 保存相关性数值
            if check_result['self_corr'] is not None or check_result['prod_corr'] is not None:
                results['correlation_results'][alpha_id] = {
                    'self_corr': check_result['self_corr'],
                    'prod_corr': check_result['prod_corr']
                }
        
        return results
    
    def _handle_batch_results_integrated(self, batch: List[Dict], batch_results: Dict, recheck_mode: bool = False):
        """处理集成检查的批次结果（标记和数据库操作）"""
        batch_green = batch_results['green_alphas']
        batch_blue = batch_results['blue_alphas']
        batch_red = batch_results['red_alphas']
        batch_purple = batch_results['purple_alphas']
        batch_aggressive = batch_results['aggressive_alphas']
        batch_correlation_updates = []
        
        # 准备相关性数值更新
        for alpha_id, corr_data in batch_results['correlation_results'].items():
            if corr_data['self_corr'] is not None or corr_data['prod_corr'] is not None:
                batch_correlation_updates.append({
                    'alpha_id': alpha_id,
                    'self_corr': corr_data['self_corr'],
                    'prod_corr': corr_data['prod_corr']
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
                if recheck_mode:
                    self.logger.info(f"      🟢 复查模式: 跳过 {len(batch_green)} 个GREEN Alpha的颜色标记和数据库更新")
                else:
                    self.logger.info(f"      🟢 标记 {len(batch_green)} 个Alpha为GREEN...")
                    self.alpha_marker.batch_set_color(batch_green, "GREEN")
                    self.database_updater.update_database_colors(batch_green, "GREEN")
            
            if batch_blue:
                if recheck_mode:
                    self.logger.info(f"      🔵 复查模式: 跳过 {len(batch_blue)} 个BLUE Alpha的颜色标记和数据库更新")
                else:
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
                self.logger.info(f"      🟣 标记 {len(batch_purple)} 个Alpha为PURPLE...")
                self.alpha_marker.batch_set_color(batch_purple, "PURPLE")
                self.database_updater.update_database_colors(batch_purple, "PURPLE")
                
                # 从数据库中移除PURPLE Alpha（质量检查失败）
                self.logger.info(f"      🗑️ 从数据库中移除 {len(batch_purple)} 个PURPLE Alpha...")
                self.database_updater.remove_alphas_batch(batch_purple)
        
        # 如果是复查模式，清除本批次已检查Alpha的复查标记
        if recheck_mode:
            batch_alpha_ids = [alpha['id'] for alpha in batch]
            if batch_alpha_ids:
                self.logger.info(f"      🔄 清除本批次 {len(batch_alpha_ids)} 个Alpha的复查标记...")
                # 只清除这个批次已检查的Alpha的复查标记
                from database.db_manager import FactorDatabaseManager
                db = FactorDatabaseManager(self.config.db_path)
                cleared_count = db.clear_recheck_flags(batch_alpha_ids)
                self.logger.debug(f"      ✅ 成功清除 {cleared_count} 个Alpha的复查标记")
    
