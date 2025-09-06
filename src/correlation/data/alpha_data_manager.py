"""
Alpha数据管理器 - 负责Alpha详细信息的批量获取和管理
"""

from typing import List, Dict


class AlphaDataManager:
    """Alpha数据管理器"""
    
    def __init__(self, config_manager, data_loader, logger):
        """初始化Alpha数据管理器"""
        self.config = config_manager
        self.data_loader = data_loader
        self.logger = logger
    
    def batch_get_alpha_details_and_pnls(self, yellow_alphas: List[Dict]) -> Dict:
        """批量获取Alpha详细信息和PnL数据（使用数据库信息优化API调用）"""
        alpha_results = {}
        alpha_pnls_dict = {}
        
        try:
            # 1. 使用数据库信息构建Alpha详细信息，避免API调用
            self.logger.info(f"      📋 从数据库构建 {len(yellow_alphas)} 个Alpha的详细信息...")
            alpha_details = []
            
            for alpha in yellow_alphas:
                alpha_id = alpha['id']  # 数据库查询中alpha_id被重命名为id
                
                # 使用数据库中的信息构建alpha_result，避免API调用
                alpha_result = {
                    'id': alpha_id,
                    'settings': {
                        'region': alpha.get('region', 'USA'),  # 从数据库获取region
                        'universe': alpha.get('universe', ''),
                        'instrumentType': alpha.get('instrument_type', ''),
                        'delay': alpha.get('delay', 0)
                    },
                    'type': alpha.get('type', ''),
                    'author': alpha.get('author', ''),
                    'name': alpha.get('name', ''),
                    'tags': alpha.get('tags', []) if isinstance(alpha.get('tags'), list) else [],
                    'classifications': alpha.get('classifications', []) if isinstance(alpha.get('classifications'), list) else []
                }
                
                alpha_results[alpha_id] = alpha_result
                alpha_details.append(alpha_result)
                self.logger.debug(f"      📝 Alpha {alpha_id}: region={alpha_result['settings']['region']}")
            
            # 2. 批量获取PnL数据
            if alpha_details:
                self.logger.info(f"      📊 批量获取 {len(alpha_details)} 个Alpha的PnL数据...")
                _, alpha_pnls_dict = self.data_loader.pnl_manager.get_alpha_pnls(alpha_details)
                self.logger.info(f"      ✅ 成功获取 {len(alpha_pnls_dict)} 个Alpha的PnL数据")
            
            self.logger.info(f"      🚀 性能优化: 避免了 {len(yellow_alphas)} 次API调用，使用数据库信息")
            
            return {
                'alpha_results': alpha_results,
                'alpha_pnls': alpha_pnls_dict
            }
            
        except Exception as e:
            self.logger.error(f"❌ 批量获取Alpha数据异常: {e}")
            return {'alpha_results': {}, 'alpha_pnls': {}}
