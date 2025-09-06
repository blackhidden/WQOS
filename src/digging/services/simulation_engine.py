"""
模拟执行引擎 (Simulation Engine)
作者：e.e.
日期：2025.09.05

负责执行因子模拟，包括：
- 异步模拟执行
- 进度跟踪
- 倒计时休眠
- 会话管理
"""

import asyncio
import time
import os
import sys
from datetime import datetime
from typing import List, Tuple

try:
    from machine_lib_ee import simulate_single
    from session_client import get_session as get_session_manager, get_session_cookies
    print("✅ 使用简化会话客户端")
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
    from machine_lib_ee import simulate_single
    from session_client import get_session as get_session_manager, get_session_cookies
    print("✅ 使用简化会话客户端")


class SimulationEngine:
    """模拟执行引擎 - 负责因子的异步模拟执行"""
    
    def __init__(self, config_manager):
        """初始化模拟执行引擎
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager
        self.logger = None  # 将在设置时注入
    
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
    
    async def sleep_with_countdown(self, seconds: int, message: str = "休眠中"):
        """带倒计时显示的休眠函数
        
        Args:
            seconds: 休眠秒数
            message: 休眠消息
        """
        total_minutes = seconds // 60
        if self.logger:
            self.logger.info(f"😴 {message}，共需等待 {total_minutes} 分钟...")
            self.logger.info(f"💤 休眠期间脚本继续运行，可通过日志监控状态...")
        
        # 每5分钟显示一次进度
        interval = 300  # 5分钟
        elapsed = 0
        
        while elapsed < seconds:
            remaining = seconds - elapsed
            remaining_minutes = remaining // 60
            
            if elapsed > 0:  # 不在开始时显示
                current_time = datetime.now().strftime("%H:%M:%S")
                if self.logger:
                    self.logger.info(f"⏰ [{current_time}] 倒计时: 还需等待 {remaining_minutes} 分钟...")
            
            # 休眠5分钟或剩余时间（取较小值）
            sleep_time = min(interval, remaining)
            await asyncio.sleep(sleep_time)
            elapsed += sleep_time
        
        current_time = datetime.now().strftime("%H:%M:%S")
        if self.logger:
            self.logger.info(f"✅ [{current_time}] 等待结束，重新检查符合条件的因子...")
    
    async def simulate_multiple_alphas(self, alpha_list: List[str], region_list: List[Tuple], 
                                     decay_list: List[int], delay_list: List[int], 
                                     name: str, neut: str, stone_bag: List = None, 
                                     n_jobs: int = 5) -> None:
        """执行多个Alpha的异步模拟（带详细进度日志）
        
        Args:
            alpha_list: Alpha表达式列表
            region_list: 地区列表
            decay_list: 衰减列表
            delay_list: 延迟列表
            name: Tag名称
            neut: 中性化方式
            stone_bag: 禁用的Alpha列表
            n_jobs: 并发数
        """
        if stone_bag is None:
            stone_bag = []
            
        n = n_jobs
        semaphore = asyncio.Semaphore(n)
        tasks = []
        tags = [name]
        
        total_alphas = len(alpha_list)
        if self.logger:
            self.logger.info(f"🚀 开始模拟: {total_alphas:,}个因子 | 并发: {n_jobs}")
        
        # 进度跟踪变量
        completed_count = 0
        progress_lock = asyncio.Lock()
        start_time = time.time()

        # 使用统一会话管理器（支持自动会话刷新）
        if self.logger:
            self.logger.info(f"🔄 获取统一会话管理器...")
        try:
            import aiohttp
            import yarl
            
            # 获取统一会话管理器和cookies
            unified_session_manager = get_session_manager()
            cookies = get_session_cookies()
            
            if not cookies:
                raise Exception("无法获取有效的会话cookies")
            
            # 创建异步会话
            cookie_jar = aiohttp.CookieJar()
            
            # 将 requests cookies 转换为 aiohttp cookies
            cookie_dict = {}
            for cookie_name, cookie_value in cookies.items():
                cookie_dict[cookie_name] = cookie_value
            
            # 更新 cookie jar
            if cookie_dict:
                cookie_jar.update_cookies(cookie_dict, response_url=yarl.URL("https://api.worldquantbrain.com"))
            
            session_manager = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=300),
                connector=aiohttp.TCPConnector(limit=50),
                cookie_jar=cookie_jar
            )
            
            if self.logger:
                self.logger.info(f"✅ 异步会话创建成功")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 统一会话管理器获取失败: {e}")
            raise

        async def simulate_with_progress(session_mgr, alpha, region, name, neut, decay, delay, stone_bag, tags, semaphore, alpha_index):
            """带进度跟踪的单个模拟函数"""
            nonlocal completed_count
            
            try:
                # 使用统一会话管理器执行模拟，自动处理会话过期和刷新
                result = await simulate_single(session_mgr, alpha, region, name, neut, decay, delay, stone_bag, tags, semaphore, max_trade=self.config_manager.max_trade)
                
                # 更新进度
                async with progress_lock:
                    completed_count += 1
                    progress_percent = (completed_count / total_alphas) * 100
                    elapsed_time = time.time() - start_time
                    
                    # 估算剩余时间
                    if completed_count > 0:
                        avg_time_per_alpha = elapsed_time / completed_count
                        remaining_alphas = total_alphas - completed_count
                        estimated_remaining = avg_time_per_alpha * remaining_alphas
                        eta_minutes = estimated_remaining / 60
                    else:
                        eta_minutes = 0
                    
                    # 每20个或每10%打印一次进度
                    if (completed_count % 20 == 0 or 
                        completed_count % max(1, total_alphas // 10) == 0 or 
                        completed_count == total_alphas):
                        
                        if self.logger:
                            self.logger.info(f"   进度: {completed_count:>4}/{total_alphas} ({progress_percent:>5.1f}%) | 预计剩余: {eta_minutes:>4.1f}min")
                
                return result
                
            except Exception as e:
                async with progress_lock:
                    completed_count += 1
                    if self.logger:
                        self.logger.info(f"    ❌ 模拟失败 ({completed_count}/{total_alphas}): {str(e)[:50]}...")
                raise

        # 将任务划分成 n 份
        chunk_size = (len(alpha_list) + n - 1) // n  # 向上取整
        task_chunks = [alpha_list[i:i + chunk_size] for i in range(0, len(alpha_list), chunk_size)]
        region_chunks = [region_list[i:i + chunk_size] for i in range(0, len(region_list), chunk_size)]
        decay_chunks = [decay_list[i:i + chunk_size] for i in range(0, len(decay_list), chunk_size)]
        delay_chunks = [delay_list[i:i + chunk_size] for i in range(0, len(delay_list), chunk_size)]

        alpha_index = 0
        for i, (alpha_chunk, region_chunk, decay_chunk, delay_chunk) in (
                enumerate(zip(task_chunks, region_chunks, decay_chunks, delay_chunks))):
            for alpha, region, decay, delay in zip(alpha_chunk, region_chunk, decay_chunk, delay_chunk):
                # 将任务与统一会话管理器关联，并添加进度跟踪
                task = simulate_with_progress(session_manager, alpha, region, name, neut, 
                                            decay, delay, stone_bag, tags, semaphore, alpha_index)
                tasks.append(task)
                alpha_index += 1

        await asyncio.gather(*tasks)
        
        # 关闭异步会话
        await session_manager.close()
        
        total_time = time.time() - start_time
        if self.logger:
            self.logger.info(f"✅ 模拟完成: {total_time:.1f}s, 平均{total_time/total_alphas:.2f}s/因子")

        # 统一会话管理器会自动管理会话生命周期，无需手动关闭
        if self.logger:
            self.logger.info(f"🔄 模拟任务完成，统一会话管理器继续维护会话状态")
    
    async def execute_simulation_batch(self, alpha_list: List[str], dataset_id: str, step: int) -> List[dict]:
        """执行模拟批次
        
        Args:
            alpha_list: Alpha表达式列表
            dataset_id: 数据集ID
            step: 挖掘步骤
            
        Returns:
            List[dict]: 执行结果列表
        """
        if not alpha_list:
            return []
        
        # 生成tag
        tag_name = self.config_manager.generate_tag(dataset_id, step)
        
        # 准备参数
        region_tuple = (self.config_manager.region, self.config_manager.universe)
        region_list = [region_tuple] * len(alpha_list)
        decay_list = [self.config_manager.decay] * len(alpha_list)
        delay_list = [self.config_manager.delay] * len(alpha_list)
        stone_bag = []
        
        # 执行模拟
        await self.simulate_multiple_alphas(
            alpha_list, region_list, decay_list, delay_list,
            tag_name, self.config_manager.neutralization, stone_bag, 
            self.config_manager.get_n_jobs_config()
        )
        
        # 返回结果
        return [{'alpha': alpha, 'tag': tag_name} for alpha in alpha_list]
