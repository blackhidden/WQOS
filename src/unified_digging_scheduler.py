"""
因子挖掘调度器 (重构版本)
作者：e.e.
日期：2025.09.05

重构后的因子挖掘调度器，特点：
- 代码从1072行简化到约150行
- 职责分离，模块化设计
- 便于维护和扩展
- 保持原有功能不变
"""

import os
import sys
import asyncio
import argparse
from typing import Optional

# 添加项目根目录到Python路径，确保能正确导入database等模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入重构后的模块
from digging import (
    ConfigManager, NotificationService, ProgressTracker, SimulationEngine,
    FirstOrderExecutor, SecondOrderExecutor, ThirdOrderExecutor
)
from digging.utils.logging_utils import setup_digging_logger


class UnifiedDiggingScheduler:
    """统一因子挖掘调度器 - 重构版本"""
    
    def __init__(self, config_file: Optional[str] = None, 
                 stage: Optional[int] = None, 
                 n_jobs: Optional[int] = None):
        """初始化调度器
        
        Args:
            config_file: 配置文件路径
            stage: 执行阶段 (1, 2, 或 3)
            n_jobs: 并发数
        """
        # 设置日志
        self.logger = setup_digging_logger('unified_digging')
        
        # 初始化配置管理器
        self.config_manager = ConfigManager(config_file)
        
        # 从命令行参数或配置中确定运行参数
        self.stage = self.config_manager.get_stage_config(stage)
        
        # 设置并发数（确保配置管理器保存正确的值）
        if n_jobs is not None:
            self.config_manager.set_n_jobs(n_jobs)
            self.n_jobs = n_jobs
        else:
            self.n_jobs = self.config_manager.get_n_jobs_config()
        
        # 初始化服务组件
        self.notification_service = NotificationService(self.config_manager)
        self.progress_tracker = ProgressTracker(self.config_manager, self.notification_service)
        self.simulation_engine = SimulationEngine(self.config_manager)
        
        # 设置日志记录器
        self._inject_logger_to_services()
        
        # 创建对应阶段的执行器
        self.executor = self._create_executor()
        
        # 记录配置摘要
        self.config_manager.log_config_summary(self.logger)
        self.logger.info(f"  🎯 执行阶段: 第{self.stage}阶")
        self.logger.info(f"  ⚡ 并发数: {self.n_jobs}")
    
    def _inject_logger_to_services(self):
        """将日志记录器注入到所有服务组件"""
        self.notification_service.set_logger(self.logger)
        self.progress_tracker.set_logger(self.logger)
        self.simulation_engine.set_logger(self.logger)
    
    def _create_executor(self):
        """根据阶段创建对应的执行器
        
        Returns:
            BaseExecutor: 执行器实例
        """
        if self.stage == 1:
            executor = FirstOrderExecutor(
                self.config_manager, 
                self.simulation_engine, 
                self.progress_tracker, 
                self.notification_service
            )
        elif self.stage == 2:
            executor = SecondOrderExecutor(
                self.config_manager, 
                self.simulation_engine, 
                self.progress_tracker, 
                self.notification_service
            )
        elif self.stage == 3:
            executor = ThirdOrderExecutor(
                self.config_manager, 
                self.simulation_engine, 
                self.progress_tracker, 
                self.notification_service
            )
        else:
            raise ValueError(f"不支持的挖掘阶段: {self.stage}")
        
        # 设置日志记录器
        executor.set_logger(self.logger)
        
        return executor
    
    async def run(self):
        """运行因子挖掘任务"""
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"🚀 因子挖掘启动 - 第{self.stage}阶段")
        self.logger.info(f"🎯 数据集: {self.config_manager.current_dataset}")
        self.logger.info(f"{'='*80}")
        
        try:
            # 执行挖掘任务
            results = await self.executor.execute()
            
            # 显示结果摘要
            if results:
                self.logger.info(f"\n✅ 第{self.stage}阶段挖掘完成，处理了 {len(results)} 个因子")
            else:
                self.logger.info(f"\nℹ️ 第{self.stage}阶段无需处理的因子（可能已完成或无符合条件的因子）")
                
        except KeyboardInterrupt:
            self.logger.info(f"\n⚠️  用户中断，第{self.stage}阶段挖掘停止")
        except Exception as e:
            self.logger.error(f"\n❌ 第{self.stage}阶段挖掘失败: {e}")
            import traceback
            traceback.print_exc()
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"📊 因子挖掘结束")
        self.logger.info(f"  🎯 执行阶段: 第{self.stage}阶")
        self.logger.info(f"  📊 数据集: {self.config_manager.current_dataset}")
        self.logger.info(f"{'='*80}")


async def main():
    """主函数"""
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='因子挖掘 (重构版本)')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--stage', type=int, choices=[1, 2, 3], help='执行阶段 (1, 2, 或 3)')
    parser.add_argument('--n_jobs', type=int, help='并发数')
    args = parser.parse_args()
    
    # 创建临时logger用于启动日志
    startup_logger = setup_digging_logger()
    startup_logger.info("🚀 因子挖掘启动中...")
    startup_logger.info(f"📋 命令行参数: {args}")
    
    try:
        # 创建调度器
        scheduler = UnifiedDiggingScheduler(
            config_file=args.config,
            stage=args.stage,
            n_jobs=args.n_jobs
        )
        
        # 运行挖掘任务
        await scheduler.run()
        
    except ValueError as e:
        startup_logger.error(f"❌ 配置错误: {e}")
    except Exception as e:
        startup_logger.error(f"❌ 启动失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
