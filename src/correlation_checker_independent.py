"""
重构后的相关性检查器 - 主入口文件
作者：e.e.
微信：Enkidu_lin
日期：2025.09.05
"""

import time
import argparse
from pathlib import Path

# 导入重构后的模块
from correlation.core.config_manager import CorrelationConfigManager
from correlation.services.session_service import SessionService
from correlation.data.data_loader import DataLoader
from correlation.data.pnl_manager import PnLManager
from correlation.checkers.selfcorr_checker import SelfCorrChecker
from correlation.checkers.ppac_checker import PPACChecker
from correlation.processors.alpha_marker import AlphaMarker
from correlation.processors.database_updater import DatabaseUpdater
from correlation.processors.batch_processor import BatchProcessor
from correlation.utils.logging_utils import setup_correlation_logger


class RefactoredCorrelationChecker:
    """重构后的相关性检查器主类"""
    
    def __init__(self):
        """初始化相关性检查器"""
        # 设置日志系统
        self.logger = setup_correlation_logger('correlation_checker_refactored')
        
        # 初始化配置管理器
        self.config = CorrelationConfigManager()
        self.config.log_config_summary(self.logger)
        
        # 初始化核心服务
        self.session_service = SessionService(self.config, self.logger)
        
        # 初始化数据管理组件
        self.pnl_manager = PnLManager(self.config, self.session_service, self.logger)
        self.data_loader = DataLoader(self.config, self.session_service, self.pnl_manager, self.logger)
        
        # 初始化检查器
        self.selfcorr_checker = SelfCorrChecker(self.config, self.session_service, self.data_loader, self.logger)
        self.ppac_checker = PPACChecker(self.config, self.session_service, self.data_loader, self.logger)
        
        # 初始化处理器
        self.alpha_marker = AlphaMarker(self.config, self.session_service, self.logger)
        self.database_updater = DatabaseUpdater(self.config, self.logger)
        
        # 初始化批量处理器
        self.batch_processor = BatchProcessor(
            self.config, self.session_service, self.data_loader,
            self.selfcorr_checker, self.ppac_checker, 
            self.alpha_marker, self.database_updater, self.logger
        )
    
    def initialize_session(self):
        """初始化会话"""
        return self.session_service.initialize_session()
    
    def run_single_check_cycle(self):
        """执行单次检查周期"""
        try:
            # 加载数据并检测是否有新Alpha（在持续监控模式下总是检查新Alpha）
            success, has_new_alphas = self.data_loader.ensure_data_loaded(force_check_new=True)
            if not success:
                self.logger.error(f"❌ 无法加载数据，跳过本次检查")
                return False
            
            # 如果有新提交的Alpha，重置相关区域的Alpha为YELLOW
            if has_new_alphas:
                # 获取所有受影响的区域
                affected_regions = list(self.data_loader.os_alpha_ids.keys())
                self.database_updater.reset_alphas_to_yellow(affected_regions)
            
            # 获取所有Yellow状态的Alpha
            yellow_alphas = self.database_updater.get_alphas_by_color('YELLOW')
            
            if not yellow_alphas:
                if has_new_alphas:
                    self.logger.info(f"📝 检测到 {len([alpha for ids in self.data_loader.os_alpha_ids.values() for alpha in ids])} 个新Alpha但数据库中暂无Yellow状态Alpha，可能数据同步中...")
                else:
                    self.logger.info(f"📝 没有找到Yellow状态的Alpha，跳过本次检查")
                return has_new_alphas  # 如果有新Alpha则返回True，表示有工作完成
            
            self.logger.info(f"📊 找到 {len(yellow_alphas)} 个Yellow状态的Alpha")
            
            # 批量检查相关性
            green_alphas, blue_alphas, red_alphas, purple_alphas, aggressive_alphas, correlation_results = self.batch_processor.batch_check_correlations(yellow_alphas)
            
            # 结果已在批次处理中标记和统计，这里只显示简要完成信息
            total_checked = len(green_alphas) + len(blue_alphas) + len(red_alphas) + len(purple_alphas)
            self.logger.info(f"\n✅ 本轮检查完成: {total_checked}个Alpha处理完毕")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 本轮检查异常: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_correlation_check(self, continuous_mode=True, check_interval=300):
        """运行相关性检查 (支持持续监控模式)
        
        Args:
            continuous_mode: 是否启用持续监控模式
            check_interval: 检查间隔（秒），默认5分钟
        """
        try:
            mode_desc = "持续监控模式" if continuous_mode else "单次检查模式"
            self.logger.info(f"🚀 启动批量相关性检查 ({mode_desc})...")
            
            # 初始化会话
            if not self.initialize_session():
                self.logger.error(f"❌ 会话初始化失败，检查终止")
                return
            
            if not continuous_mode:
                # 单次检查模式
                self.run_single_check_cycle()
                return
            
            # 持续监控模式
            self.logger.info(f"🔄 启动持续监控，检查间隔: {check_interval}秒 ({check_interval//60}分钟)")
            cycle_count = 0
            
            while True:
                cycle_count += 1
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"🔍 开始第 {cycle_count} 轮检查 - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.logger.info(f"{'='*60}")
                
                # 记录开始时间
                start_time = time.time()
                
                # 执行检查周期
                has_work = self.run_single_check_cycle()
                
                # 计算耗时
                end_time = time.time()
                elapsed_time = end_time - start_time
                
                if has_work:
                    self.logger.info(f"✅ 第 {cycle_count} 轮检查完成，有Alpha被处理")
                else:
                    self.logger.info(f"📭 第 {cycle_count} 轮检查完成，暂无需要处理的Alpha")
                
                # 智能等待逻辑：如果检查耗时超过间隔时间，立即开始下轮检查
                if elapsed_time > check_interval:
                    self.logger.info(f"🕒 检查周期完成，耗时: {elapsed_time:.2f}s，超过 {check_interval}秒 ({check_interval//60}分钟)，跳过等待立即开始下轮检查")
                    continue
                
                # 计算剩余等待时间
                remaining_wait = check_interval - elapsed_time
                self.logger.info(f"⏰ 检查周期完成，耗时: {elapsed_time:.2f}s，{remaining_wait:.2f}秒后开始下轮检查...")
                self.logger.info(f"💡 提示: 按 Ctrl+C 可停止监控")
                
                try:
                    time.sleep(remaining_wait)
                except KeyboardInterrupt:
                    self.logger.info(f"\n👋 收到停止信号，退出持续监控模式")
                    break
                    
        except Exception as e:
            self.logger.error(f"❌ 相关性检查异常: {e}")
            import traceback
            traceback.print_exc()


def main():
    """主函数 - 支持单次和持续监控模式"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='重构后的相关性检查器 - 支持单次和持续监控模式')
    parser.add_argument('--mode', choices=['single', 'continuous'], default='continuous',
                       help='运行模式: single=单次检查, continuous=持续监控 (默认: continuous)')
    parser.add_argument('--interval', type=int, default=300,
                       help='持续监控模式下的检查间隔(秒), 默认300秒(5分钟)')
    
    args = parser.parse_args()
    continuous_mode = (args.mode == 'continuous')
    check_interval = args.interval
    
    # 创建检查器实例
    checker = RefactoredCorrelationChecker()
    
    mode_desc = "持续监控模式" if continuous_mode else "单次检查模式"
    checker.logger.info(f"🚀 启动重构后的相关性检查器 ({mode_desc})...")
    
    if continuous_mode:
        checker.logger.info(f"⏰ 检查间隔: {check_interval}秒 ({check_interval//60}分钟)")
        checker.logger.info(f"💡 提示: 使用 '--mode single' 可切换到单次检查模式")
    
    try:
        # 运行相关性检查
        checker.run_correlation_check(
            continuous_mode=continuous_mode,
            check_interval=check_interval
        )
        
        if not continuous_mode:
            checker.logger.info("\n✅ 单次相关性检查完成")
    except KeyboardInterrupt:
        checker.logger.info("\n👋 收到中断信号，正在退出...")
    except Exception as e:
        checker.logger.error(f"❌ 主循环异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        checker.logger.info("\n👋 程序结束")


if __name__ == '__main__':
    main()
