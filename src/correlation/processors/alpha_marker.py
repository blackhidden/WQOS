"""
Alpha标记器 - 负责Alpha颜色标记和API更新
"""

import time
from typing import List
from machine_lib_ee import set_alpha_properties, batch_set_alpha_properties


class AlphaMarker:
    """Alpha标记器"""
    
    def __init__(self, config_manager, session_service, logger):
        """初始化Alpha标记器"""
        self.config = config_manager
        self.session_service = session_service
        self.logger = logger
    
    def batch_set_color(self, alpha_ids: List[str], color: str):
        """批量设置Alpha颜色 - 使用新的批量API并同步更新数据库"""
        if not alpha_ids:
            return
        
        self.logger.info(f"        🎨 开始批量设置 {len(alpha_ids)} 个Alpha为{color}...")
        
        # 准备批量API数据格式
        alpha_data = [{"id": alpha_id, "color": color} for alpha_id in alpha_ids]
        
        try:
            # 使用批量API，分批大小设为30避免请求过大
            result = batch_set_alpha_properties(self.session_service.session, alpha_data, max_batch_size=30)
            
            success_count = result["success"]
            failed_count = result["failed"]
            
            self.logger.info(f"        📊 {color}标记完成: 成功 {success_count}, 失败 {failed_count}")
            
            # 显示详细信息
            for detail in result["details"]:
                self.logger.info(f"        📋 {detail}")
            
            return success_count, failed_count
            
        except Exception as e:
            self.logger.error(f"        ❌ 批量API异常: {e}")
            self.logger.info(f"        🔄 回退到单个设置模式...")
            
            # 回退到单个设置
            return self._fallback_individual_color_set(alpha_ids, color)
    
    def _fallback_individual_color_set(self, alpha_ids: List[str], color: str):
        """回退方案：使用单个API设置颜色"""
        success_count = 0
        failed_count = 0
        
        for i, alpha_id in enumerate(alpha_ids):
            retry_count = 0
            success = False
            
            while retry_count < self.config.max_retries and not success:
                try:
                    result = set_alpha_properties(self.session_service.session, alpha_id, color=color)
                    if result == True:
                        success = True
                        success_count += 1
                        break
                    else:
                        retry_count += 1
                        if retry_count < self.config.max_retries:
                            time.sleep(self.config.api_delay * (2 ** retry_count))
                except Exception as e:
                    retry_count += 1
                    if retry_count < self.config.max_retries:
                        time.sleep(self.config.api_delay * (2 ** retry_count))
                    else:
                        self.logger.error(f"        ❌ Alpha {alpha_id} 设置颜色失败: {e}")
            
            if not success:
                failed_count += 1
            
            # 进度显示
            if (i + 1) % 10 == 0 or i == len(alpha_ids) - 1:
                self.logger.info(f"        📊 单个设置进度: {i+1}/{len(alpha_ids)} (成功: {success_count}, 失败: {failed_count})")
            
            # 请求间延迟
            if i < len(alpha_ids) - 1:
                time.sleep(self.config.api_delay)
        
        return success_count, failed_count
