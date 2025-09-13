"""
单模拟策略 (Single Simulation Strategy)
作者：e.e.
日期：2025.09.08

实现单模拟执行策略，每个alpha单独模拟
"""

import asyncio
from typing import List, Tuple, Any
from ..core.base_strategy import BaseSimulationStrategy


class SingleSimulationStrategy(BaseSimulationStrategy):
    """单模拟策略 - 每个alpha单独执行模拟"""
    
    def get_strategy_name(self) -> str:
        """获取策略名称"""
        return "单模拟"
    
    def should_use_strategy(self, alpha_count: int, config_manager) -> bool:
        """判断是否应该使用单模拟策略
        
        Args:
            alpha_count: Alpha数量
            config_manager: 配置管理器
            
        Returns:
            bool: 是否使用单模拟策略
        """
        # 如果明确禁用了多模拟，或者alpha数量较少，使用单模拟
        if hasattr(config_manager, 'enable_multi_simulation'):
            if not config_manager.enable_multi_simulation:
                return True
        
        # 少于10个alpha时，单模拟可能更高效
        return alpha_count < 10
    
    async def group_tasks(self, alpha_list: List[str], decay_list: List[int]) -> List[Tuple[str, int]]:
        """将alpha列表分组为单个alpha任务
        
        Args:
            alpha_list: Alpha表达式列表
            decay_list: 衰减值列表
            
        Returns:
            List[Tuple[str, int]]: 单个alpha任务列表
        """
        # 单模拟：每个alpha一个任务
        return [(alpha_list[i], decay_list[i]) for i in range(len(alpha_list))]
    
    async def execute_task_group(self, task_group: Tuple[str, int], session_manager,
                                region: str, universe: str, neut: str, 
                                delay: int, name: str, stage: int) -> List[str]:
        """执行单个alpha模拟任务
        
        Args:
            task_group: (alpha_expression, decay) 元组
            session_manager: 会话管理器
            region: 地区
            universe: universe
            neut: 中性化方式
            delay: 延迟
            name: 名称/标签
            stage: 执行阶段
            
        Returns:
            List[str]: 创建的Alpha ID列表（单个alpha）
        """
        alpha_expression, decay = task_group
        
        # 获取配置参数（与多模拟保持一致）
        max_trade = "OFF"
        instrument_type = "EQUITY"
        
        if hasattr(self.config_manager, 'max_trade'):
            max_trade = self.config_manager.max_trade
        if hasattr(self.config_manager, 'instrument_type'):
            instrument_type = self.config_manager.instrument_type
        
        # 构建模拟数据（与多模拟格式完全一致）
        simulation_data = {
            "type": "REGULAR",
            "settings": {
                "maxTrade": max_trade,
                "instrumentType": instrument_type,
                "region": region,
                "universe": universe,
                "delay": delay,
                "decay": decay,
                "neutralization": neut,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "truncation": 0.08,
                "nanHandling": "OFF",
                "language": "FASTEXPR",
                "testPeriod": "P1Y",
                "visualization": False
            },
            "regular": alpha_expression
        }
        
        # 🚀 阶段1：模拟提交（仿照原始simulation_engine.py的逻辑）
        simulation_progress_url = None
        while True:
            try:
                # 提交单模拟
                async with session_manager.post("https://api.worldquantbrain.com/simulations", 
                                              json=simulation_data) as response:
                    if response.status == 201:
                        # 获取进度URL（与原始逻辑一致）
                        simulation_progress_url = response.headers.get('Location')
                        if simulation_progress_url:
                            if self.logger:
                                self.logger.info(f"✅ 单模拟已提交，开始占用槽位: {simulation_progress_url}")
                            break
                        else:
                            if self.logger:
                                self.logger.warning(f"⚠️ 单模拟提交成功但缺少Location头")
                            return []
                    
                    elif response.status in (401, 403):
                        # 处理401/403会话过期（仿照原始simulation_engine.py的逻辑）
                        if self.logger:
                            self.logger.warning(f"⚠️ 模拟提交未授权 ({response.status}): 会话可能过期，尝试刷新会话...")
                        await self._refresh_session_cookies(session_manager)
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                    
                    elif response.status == 429:
                        # 429错误：仿照原来的处理方式
                        retry_after_hdr = response.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        if self.logger:
                            self.logger.info(f"🚨 平台模拟槽位已满 (HTTP 429)，等待{wait_s}s后重试")
                        await asyncio.sleep(wait_s)
                        continue
                    
                    else:
                        # 检查是否是CONCURRENT_SIMULATION_LIMIT_EXCEEDED错误
                        try:
                            error_text = await response.text()
                            
                            # 解析错误详情
                            try:
                                import json
                                error_data = json.loads(error_text)
                            except:
                                error_data = error_text
                            
                            def extract_detail(payload):
                                if isinstance(payload, dict):
                                    return payload.get('detail') or payload.get('message') or payload.get('error') or ''
                                return str(payload)
                            
                            detail = extract_detail(error_data)
                            detail_str = str(detail)
                            
                            # 并发上限 → 等待重试
                            if 'CONCURRENT_SIMULATION_LIMIT_EXCEEDED' in detail_str:
                                if self.logger:
                                    self.logger.info("⚠️ 平台并发限制已达上限，等待5s后重试")
                                await asyncio.sleep(5)
                                continue
                            
                            # 重复表达式 → 记录并跳过
                            if 'duplicate' in detail_str.lower():
                                if self.logger:
                                    self.logger.info("⚠️ Alpha表达式重复")
                                return []
                            
                            # 其他错误：退出重试循环
                            if self.logger:
                                self.logger.warning(f"⚠️ 单模拟失败: HTTP {response.status}, 详情: {detail_str}")
                            return []
                            
                        except Exception as parse_error:
                            if self.logger:
                                self.logger.warning(f"⚠️ 单模拟失败: HTTP {response.status}, 解析错误: {parse_error}")
                            return []
                        
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 单模拟请求异常: {e}")
                # 等待一段时间再重试
                await asyncio.sleep(2)
                continue
        
        # 🔄 阶段2：进度轮询（仿照原始simulation_engine.py的逻辑）
        if self.logger:
            self.logger.info(f"⏳ 轮询进度 (槽位占用中): {alpha_expression[:50]}...")
        
        json_data = None
        while True:
            try:
                async with session_manager.get(simulation_progress_url) as response:
                    # 处理401/403会话过期
                    if response.status in (401, 403):
                        if self.logger:
                            self.logger.warning("⚠️ 进度查询未授权，会话可能过期，尝试刷新会话...")
                        await self._refresh_session_cookies(session_manager)
                        await asyncio.sleep(2)
                        continue
                        
                    if response.status == 429:
                        retry_after_hdr = response.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        if self.logger:
                            self.logger.debug(f"🚨 进度查询速率限制，等待{wait_s}s后重试")
                        await asyncio.sleep(wait_s)
                        continue
                    
                    # 非JSON响应处理
                    content_type = (response.headers.get('Content-Type') or '').lower()
                    if 'application/json' not in content_type:
                        if self.logger:
                            self.logger.debug(f"⚠️ 非JSON响应: status={response.status}, content-type={content_type}")
                        if response.status in (500, 502, 503, 504, 408):
                            await asyncio.sleep(5)
                            continue
                        else:
                            await asyncio.sleep(2)
                            continue
                    
                    json_data = await response.json()
                    
                    # 检查Retry-After头
                    retry_after_hdr = response.headers.get('Retry-After')
                    try:
                        retry_after_val = float(retry_after_hdr) if retry_after_hdr is not None else 0.0
                    except Exception:
                        retry_after_val = 0.0
                    
                    if retry_after_val <= 0:
                        # 模拟完成
                        if self.logger:
                            self.logger.info(f"✅ 单模拟完成，开始获取结果: {alpha_expression[:50]}...")
                        break
                    
                    # 轮询等待时输出进度日志
                    if self.logger and retry_after_val > 0:
                        self.logger.debug(f"⏳ 模拟进行中，等待 {retry_after_val}s 后继续轮询: {alpha_expression[:50]}...")
                    await asyncio.sleep(retry_after_val)
                    
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"⚠️ 进度查询异常: {e}")
                await asyncio.sleep(10)
                continue
        
        # 🎯 阶段3：处理模拟结果（仿照原始simulation_engine.py的逻辑）
        if not json_data:
            if self.logger:
                self.logger.warning(f"⚠️ 未获取到模拟结果数据")
            return []
        
        try:
            # 检查模拟状态
            status = json_data.get("status")
            if status == "ERROR":
                message = json_data.get("message", "Unknown error")
                if self.logger:
                    self.logger.warning(f"⚠️ 模拟失败: {message}")
                return []
            
            # 获取alpha_id
            alpha_id = json_data.get("alpha")
            if not alpha_id:
                if self.logger:
                    self.logger.warning(f"⚠️ 模拟完成但未返回alpha_id")
                return []
            
            if self.logger:
                self.logger.info(f"✅ 单模拟完成，获得alpha_id: {alpha_id}")
            
            # 🏷️ 阶段4：设置Alpha属性和写入数据库
            await self._set_alpha_properties_async(session_manager, alpha_id, name, [name])
            await self._write_to_database_async(alpha_expression, alpha_id, name, stage)
            
            return [alpha_id]
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理模拟结果异常: {e}")
            return []
    
    async def _set_alpha_properties_async(self, session_manager, alpha_id: str, name: str, tags: list):
        """异步设置Alpha属性"""
        try:
            patch_data = {
                "category": None,
                "regular": {"description": None},
                "name": alpha_id  # 使用alpha_id作为name，与原始逻辑保持一致
            }
            if tags:
                patch_data["tags"] = tags
            
            patch_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
            
            # 支持会话刷新重试
            while True:
                try:
                    async with session_manager.patch(patch_url, json=patch_data) as response:
                        # 处理401/403会话过期
                        if response.status in (401, 403):
                            if self.logger:
                                self.logger.warning(f"⚠️ Alpha {alpha_id} 属性设置未授权，尝试刷新会话...")
                            await self._refresh_session_cookies(session_manager)
                            await asyncio.sleep(2)
                            continue
                        
                        elif response.status == 200:
                            if self.logger:
                                self.logger.debug(f"✅ Alpha {alpha_id} 属性设置成功")
                            return True
                        else:
                            if self.logger:
                                self.logger.warning(f"⚠️ Alpha {alpha_id} 属性设置失败: HTTP {response.status}")
                            return False
                            
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ Alpha {alpha_id} 属性设置请求异常: {e}")
                    return False
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Alpha {alpha_id} 属性设置异常: {e}")
            return False
    
    
    async def _write_to_database_async(self, alpha_expression: str, alpha_id: str, dataset_name: str, stage: int):
        """异步写入数据库"""
        try:
            # 导入数据库写入功能
            from lib.database_utils import _write_to_database
            
            # 直接调用异步函数（_write_to_database 本身就是async的）
            await _write_to_database(alpha_expression, dataset_name, alpha_id)
                
            if self.logger:
                self.logger.debug(f"✅ Alpha {alpha_id} 数据库写入成功")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Alpha {alpha_id} 数据库写入失败: {e}")
            # 数据库写入失败不影响模拟成功
    
    async def _refresh_session_cookies(self, session_manager):
        """
        刷新aiohttp会话的cookies（参考multi_simulation_engine.py的实现）
        """
        try:
            from sessions.session_client import get_session_cookies
            import yarl
            
            # 获取SessionClient维护的cookies
            current_cookies = get_session_cookies()
            if current_cookies:
                if self.logger:
                    self.logger.info("🔍 检查SessionClient是否已有新的cookies...")
                
                # 更新aiohttp的cookies
                session_manager.cookie_jar.clear()
                cookie_dict = {}
                for name_val, value in current_cookies.items():
                    cookie_dict[name_val] = value
                
                if cookie_dict:
                    session_manager.cookie_jar.update_cookies(
                        cookie_dict, 
                        response_url=yarl.URL("https://api.worldquantbrain.com")
                    )
                    if self.logger:
                        self.logger.info(f"✅ aiohttp cookies更新完成，包含{len(cookie_dict)}个cookie")
                else:
                    if self.logger:
                        self.logger.warning("⚠️ 当前cookies为空，尝试强制刷新...")
                    raise Exception("当前cookies为空")
            else:
                if self.logger:
                    self.logger.warning("⚠️ 无法获取当前cookies，尝试强制刷新...")
                raise Exception("无法获取当前cookies")
                
        except Exception as e:
            # 如果获取现有cookies失败，记录但不中断流程
            if self.logger:
                self.logger.warning(f"🔄 获取现有cookies失败({e})，继续尝试...")
    
    async def _execute_task_groups(self, task_groups: List[Tuple[str, int]], session_manager,
                                  region: str, universe: str, neut: str, 
                                  delay: int, name: str, stage: int,
                                  progress_tracker=None) -> List[str]:
        """并发执行所有单模拟任务
        
        Args:
            task_groups: 单模拟任务列表
            session_manager: 会话管理器
            region: 地区
            universe: universe
            neut: 中性化方式
            delay: 延迟
            name: 名称
            stage: 执行阶段
            progress_tracker: 进度追踪器
            
        Returns:
            List[str]: 所有创建的Alpha ID
        """
        # 获取并发数配置
        n_jobs = 5  # 默认值
        if hasattr(self.config_manager, 'get_n_jobs_config'):
            n_jobs = self.config_manager.get_n_jobs_config()
        elif hasattr(self.config_manager, 'n_jobs'):
            n_jobs = self.config_manager.n_jobs
        
        if self.logger:
            self.logger.info(f"🚀 单模拟并发执行: {len(task_groups)} 个任务，并发数 {n_jobs}")
        
        # 创建信号量控制并发数
        semaphore = asyncio.Semaphore(n_jobs)
        
        async def execute_with_semaphore(task_idx: int, task_group: Tuple[str, int]):
            """带信号量控制的任务执行"""
            async with semaphore:
                try:
                    alpha_ids = await self.execute_task_group(
                        task_group, session_manager, region, universe, 
                        neut, delay, name, stage
                    )
                    
                    # 更新进度
                    if progress_tracker:
                        progress_tracker.update_progress(task_idx + 1, len(task_groups))
                    
                    return alpha_ids
                    
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 任务 {task_idx + 1} 执行异常: {e}")
                    return []
        
        # 创建所有任务
        tasks = [
            execute_with_semaphore(i, task_group) 
            for i, task_group in enumerate(task_groups)
        ]
        
        # 并发执行所有任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 收集所有成功的Alpha ID
        all_alpha_ids = []
        for result in results:
            if isinstance(result, list):
                all_alpha_ids.extend(result)
            elif isinstance(result, Exception):
                if self.logger:
                    self.logger.error(f"❌ 任务执行异常: {result}")
        
        if self.logger:
            self.logger.info(f"📊 单模拟完成统计: "
                           f"处理 {len(task_groups)} 个任务，"
                           f"成功创建 {len(all_alpha_ids)} 个alpha")
        
        return all_alpha_ids
