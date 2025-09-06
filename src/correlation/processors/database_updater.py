"""
数据库更新器 - 负责数据库中Alpha信息的更新
"""

from typing import List, Dict
from database.db_manager import FactorDatabaseManager


class DatabaseUpdater:
    """数据库更新器"""
    
    def __init__(self, config_manager, logger):
        """初始化数据库更新器"""
        self.config = config_manager
        self.logger = logger
        self.db = FactorDatabaseManager(self.config.db_path)
    
    def update_database_colors(self, alpha_ids: List[str], color: str):
        """更新数据库中Alpha的颜色字段"""
        try:
            updated_count = 0
            with self.db.get_connection() as conn:
                for alpha_id in alpha_ids:
                    conn.execute("""
                        UPDATE submitable_alphas 
                        SET color = ?
                        WHERE alpha_id = ?
                    """, (color, alpha_id))
                    updated_count += 1
                
                conn.commit()
            
            self.logger.info(f"        💾 数据库更新完成: {updated_count} 个Alpha设为{color}")
            
        except Exception as e:
            self.logger.error(f"        ❌ 数据库颜色更新失败: {e}")
            import traceback
            traceback.print_exc()
    
    def batch_update_correlations(self, correlation_updates: List[Dict]):
        """批量更新数据库中的相关性数值"""
        if not correlation_updates:
            return
        
        try:
            updated_count = 0
            with self.db.get_connection() as conn:
                for update in correlation_updates:
                    alpha_id = update['alpha_id']
                    self_corr = update['self_corr']
                    prod_corr = update['prod_corr']
                    
                    # 更新数据库中的相关性字段
                    conn.execute("""
                        UPDATE submitable_alphas 
                        SET self_corr = ?, prod_corr = ?
                        WHERE alpha_id = ?
                    """, (self_corr, prod_corr, alpha_id))
                    
                    updated_count += 1
                    self.logger.debug(f"        更新Alpha {alpha_id}: self_corr={self_corr:.4f}, prod_corr={prod_corr:.4f}")
                
                conn.commit()
            
            self.logger.info(f"        ✅ 成功更新 {updated_count} 个Alpha的相关性数值")
            
        except Exception as e:
            self.logger.error(f"        ❌ 批量更新相关性数值失败: {e}")
            import traceback
            traceback.print_exc()
    
    def batch_update_aggressive_mode(self, alpha_ids: List[str]):
        """批量更新数据库中Alpha的aggressive_mode字段"""
        if not alpha_ids:
            return
        
        try:
            updated_count = 0
            with self.db.get_connection() as conn:
                for alpha_id in alpha_ids:
                    conn.execute("""
                        UPDATE submitable_alphas 
                        SET aggressive_mode = 1
                        WHERE alpha_id = ?
                    """, (alpha_id,))
                    updated_count += 1
                    self.logger.debug(f"        更新Alpha {alpha_id}: aggressive_mode=True")
                
                conn.commit()
            
            self.logger.info(f"        ✅ 成功更新 {updated_count} 个Alpha的aggressive_mode为True")
            
        except Exception as e:
            self.logger.error(f"        ❌ 批量更新aggressive_mode失败: {e}")
            import traceback
            traceback.print_exc()
    
    def remove_alphas_batch(self, alpha_ids: List[str]) -> int:
        """批量移除Alpha"""
        try:
            removed_count = self.db.remove_submitable_alphas_batch(alpha_ids)
            self.logger.info(f"      ✅ 成功移除 {removed_count} 个Alpha")
            return removed_count
        except Exception as e:
            self.logger.error(f"      ❌ 批量移除失败: {e}")
            # 回退到单个移除
            self.logger.info(f"      🔄 回退到单个移除模式...")
            success_count = 0
            for alpha_id in alpha_ids:
                try:
                    if self.db.remove_submitable_alpha(alpha_id):
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"      ❌ 移除Alpha {alpha_id}失败: {e}")
            self.logger.info(f"      ✅ 单个移除完成，成功移除 {success_count}/{len(alpha_ids)} 个Alpha")
            return success_count
    
    def get_alphas_by_color(self, color: str) -> List[Dict]:
        """获取指定颜色的Alpha"""
        return self.db.get_alphas_by_color(color)
    
    def reset_alphas_to_yellow(self, affected_regions: List[str]):
        """将指定区域的Alpha重置为YELLOW状态（仅更新数据库，不调用API）"""
        self.logger.info(f"\n🔄 检测到新提交的Alpha，重置相关区域的Alpha为YELLOW...")
        
        reset_count = 0
        for region in affected_regions:
            try:
                # 获取该区域所有非YELLOW状态的Alpha
                with self.db.get_connection() as conn:
                    cursor = conn.execute("""
                        SELECT alpha_id FROM submitable_alphas 
                        WHERE region = ? AND color != 'YELLOW'
                    """, (region,))
                    region_alphas = [row[0] for row in cursor.fetchall()]
                
                if region_alphas:
                    self.logger.info(f"  🌍 {region} 区域: 重置 {len(region_alphas)} 个Alpha为YELLOW")
                    
                    # 只更新数据库中的color字段，不调用API设置平台属性
                    with self.db.get_connection() as conn:
                        for alpha_id in region_alphas:
                            conn.execute("""
                                UPDATE submitable_alphas 
                                SET color = 'YELLOW' 
                                WHERE alpha_id = ?
                            """, (alpha_id,))
                    
                    reset_count += len(region_alphas)
                    self.logger.info(f"    ✅ 数据库更新完成: {len(region_alphas)} 个Alpha")
                else:
                    self.logger.info(f"  🌍 {region} 区域: 没有需要重置的Alpha")
                
            except Exception as e:
                self.logger.error(f"  ❌ 重置 {region} 区域Alpha失败: {e}")
        
        self.logger.info(f"🔄 重置完成: 总计 {reset_count} 个Alpha在数据库中被重置为YELLOW")
        self.logger.info(f"💡 注意: 仅更新数据库，平台属性将在后续检测完成后统一更新")
