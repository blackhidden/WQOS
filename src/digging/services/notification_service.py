"""
通知服务 (Notification Service)
作者：e.e.
日期：2025.09.05

负责发送各种通知，包括：
- 一阶挖掘完成度通知
- 重要进度节点通知
- 错误和异常通知
"""

import requests
from datetime import datetime
from typing import Optional

try:
    from machine_lib_ee import load_user_config
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from machine_lib_ee import load_user_config


class NotificationService:
    """通知服务 - 负责发送各种挖掘进度和状态通知"""
    
    def __init__(self, config_manager):
        """初始化通知服务
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager
        self.logger = None  # 将在设置时注入
        
        # 从用户配置加载通知相关设置
        try:
            self.user_config = load_user_config()
            self.server_secret = self.user_config.get('server_secret', '')
        except Exception:
            self.user_config = {}
            self.server_secret = ''
    
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
    
    def send_completion_notification(self, dataset_id: str, completion_rate: float, 
                                   completed_count: int, total_count: int, 
                                   remaining_count: int, start_time: datetime) -> bool:
        """发送一阶挖掘完成度通知
        
        Args:
            dataset_id: 数据集ID
            completion_rate: 完成率 (0-100)
            completed_count: 已完成因子数量
            total_count: 总因子数量
            remaining_count: 剩余因子数量
            start_time: 开始时间
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if not self.server_secret:
                if self.logger:
                    self.logger.info("📱 未配置server_secret，跳过完成度通知")
                return False
            
            # 计算总耗时
            total_time = datetime.now() - start_time
            hours, remainder = divmod(total_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # 构建通知内容
            if completion_rate >= 99.5:
                title = f"🎉 一阶挖掘即将完成 - {dataset_id} ({completion_rate:.1f}%)"
                urgency = "🔥 紧急"
                action_needed = "**请立即准备切换到下一个数据集！**"
            elif completion_rate >= 99.0:
                title = f"⚠️ 一阶挖掘接近完成 - {dataset_id} ({completion_rate:.1f}%)"
                urgency = "🟡 重要"
                action_needed = "**建议开始准备下一个数据集**"
            elif completion_rate >= 98.0:
                title = f"📊 一阶挖掘进度更新 - {dataset_id} ({completion_rate:.1f}%)"
                urgency = "🟢 提醒"
                action_needed = "可以开始考虑下一个数据集的选择"
            else:
                title = f"📈 一阶挖掘进度报告 - {dataset_id} ({completion_rate:.1f}%)"
                urgency = "ℹ️ 信息"
                action_needed = "继续监控挖掘进度"
            
            content_lines = [f"**一阶因子挖掘进度报告:**"]
            content_lines.append(f"- {urgency} 级别通知")
            content_lines.append(f"- 数据集: {dataset_id}")
            content_lines.append(f"- 地区: {self.config_manager.region}")
            content_lines.append(f"- 宇宙: {self.config_manager.universe}")
            content_lines.append("")
            
            # 进度统计
            content_lines.append(f"**挖掘进度统计:**")
            content_lines.append(f"- 📊 总体进度: {completion_rate:.2f}%")
            content_lines.append(f"- ✅ 已完成: {completed_count:,} 个因子")
            content_lines.append(f"- 📝 总计: {total_count:,} 个因子")
            content_lines.append(f"- ⏳ 剩余: {remaining_count:,} 个因子")
            content_lines.append("")
            
            # 耗时统计
            content_lines.append(f"**耗时统计:**")
            if hours > 0:
                content_lines.append(f"- 已运行: {hours}小时{minutes}分{seconds}秒")
            else:
                content_lines.append(f"- 已运行: {minutes}分{seconds}秒")
            
            if completed_count > 0:
                avg_time = total_time.seconds / completed_count
                content_lines.append(f"- 平均每个: {avg_time:.1f}秒")
            content_lines.append("")
            
            # 行动建议
            content_lines.append(f"**行动建议:**")
            content_lines.append(f"- {action_needed}")
            
            if completion_rate >= 99.0:
                content_lines.append("- 💡 建议提前准备好下一个数据集配置")
                content_lines.append("- 🔄 挖掘完成后系统会自动停止")
            elif completion_rate >= 95.0:
                content_lines.append("- 📋 可以开始规划下一阶段的挖掘策略")
                content_lines.append("- 🔍 监控剩余因子的处理速度")
            
            content_lines.append("")
            content_lines.append(f"- 报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            content = "\n".join(content_lines)
            
            # 发送通知
            url = f"https://sctapi.ftqq.com/{self.server_secret}.send"
            data = {
                "text": title,
                "desp": content
            }
            
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                if self.logger:
                    self.logger.info(f"📱 完成度通知发送成功 ({completion_rate:.1f}%)")
                return True
            else:
                if self.logger:
                    self.logger.warning(f"📱 完成度通知发送失败: {response.status_code}")
                return False
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"📱 发送完成度通知时出错: {e}")
            return False
    
    def send_error_notification(self, error_type: str, error_message: str, 
                              dataset_id: Optional[str] = None, stage: Optional[int] = None) -> bool:
        """发送错误通知
        
        Args:
            error_type: 错误类型
            error_message: 错误消息
            dataset_id: 数据集ID（可选）
            stage: 挖掘阶段（可选）
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if not self.server_secret:
                return False
            
            title = f"❌ 因子挖掘错误 - {error_type}"
            
            content_lines = [f"**因子挖掘错误报告:**"]
            content_lines.append(f"- 错误类型: {error_type}")
            content_lines.append(f"- 错误消息: {error_message}")
            
            if dataset_id:
                content_lines.append(f"- 数据集: {dataset_id}")
            if stage:
                content_lines.append(f"- 挖掘阶段: 第{stage}阶")
                
            content_lines.append(f"- 地区: {self.config_manager.region}")
            content_lines.append(f"- 宇宙: {self.config_manager.universe}")
            content_lines.append("")
            content_lines.append(f"- 错误时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            content = "\n".join(content_lines)
            
            # 发送通知
            url = f"https://sctapi.ftqq.com/{self.server_secret}.send"
            data = {
                "text": title,
                "desp": content
            }
            
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                if self.logger:
                    self.logger.info(f"📱 错误通知发送成功")
                return True
            else:
                if self.logger:
                    self.logger.warning(f"📱 错误通知发送失败: {response.status_code}")
                return False
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"📱 发送错误通知时出错: {e}")
            return False
    
    def send_stage_completion_notification(self, stage: int, dataset_id: str, 
                                         total_factors: int, execution_time: float) -> bool:
        """发送阶段完成通知
        
        Args:
            stage: 完成的挖掘阶段
            dataset_id: 数据集ID
            total_factors: 处理的因子总数
            execution_time: 执行时间（秒）
            
        Returns:
            bool: 发送是否成功
        """
        try:
            if not self.server_secret:
                return False
            
            title = f"✅ 第{stage}阶挖掘完成 - {dataset_id}"
            
            hours, remainder = divmod(int(execution_time), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            content_lines = [f"**第{stage}阶因子挖掘完成报告:**"]
            content_lines.append(f"- 数据集: {dataset_id}")
            content_lines.append(f"- 挖掘阶段: 第{stage}阶")
            content_lines.append(f"- 地区: {self.config_manager.region}")
            content_lines.append(f"- 宇宙: {self.config_manager.universe}")
            content_lines.append("")
            
            content_lines.append(f"**执行统计:**")
            content_lines.append(f"- 处理因子: {total_factors:,} 个")
            
            if hours > 0:
                content_lines.append(f"- 执行时间: {hours}小时{minutes}分{seconds}秒")
            else:
                content_lines.append(f"- 执行时间: {minutes}分{seconds}秒")
                
            if total_factors > 0:
                avg_time = execution_time / total_factors
                content_lines.append(f"- 平均每个: {avg_time:.1f}秒")
            
            content_lines.append("")
            content_lines.append(f"- 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            content = "\n".join(content_lines)
            
            # 发送通知
            url = f"https://sctapi.ftqq.com/{self.server_secret}.send"
            data = {
                "text": title,
                "desp": content
            }
            
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                if self.logger:
                    self.logger.info(f"📱 第{stage}阶完成通知发送成功")
                return True
            else:
                if self.logger:
                    self.logger.warning(f"📱 第{stage}阶完成通知发送失败: {response.status_code}")
                return False
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"📱 发送第{stage}阶完成通知时出错: {e}")
            return False
