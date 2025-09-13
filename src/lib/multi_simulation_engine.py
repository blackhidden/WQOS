#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多模拟引擎模块 - 参考单模拟的并发控制模式
分离自 machine_lib_ee.py，专门处理WorldQuant Brain多模拟API相关功能

作者: WorldQuant挖掘系统
版本: v2.0 - 参考单模拟并发模式重构
创建时间: 2025年1月
"""

import asyncio
import json
import time
import logging
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
import aiohttp
import yarl

# 配置日志
logger = logging.getLogger(__name__)

# 直接从database_utils导入数据库写入函数，避免循环导入
try:
    from lib.database_utils import _write_to_database
    DATABASE_WRITE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ 无法导入database_utils._write_to_database: {e}")
    _write_to_database = None
    DATABASE_WRITE_AVAILABLE = False

# 导入通知服务
try:
    from digging.services.notification_service import NotificationService
    NOTIFICATION_SERVICE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ 无法导入NotificationService: {e}")
    NOTIFICATION_SERVICE_AVAILABLE = False

# WorldQuant Brain API配置
brain_api_url = "https://api.worldquantbrain.com"


# 任务池函数已移除 - 智能调度器直接处理多模拟任务列表


class MultiSimulationProgressTracker:
    """多模拟进度追踪器 - 负责进度统计和微信通知"""
    
    def __init__(self, config_manager=None, stage=1):
        self.config_manager = config_manager
        self.notification_service = None
        self.notification_sent = False  # 是否已发送通知，避免重复通知
        self.notification_retry_count = 0  # 通知重试次数
        self.start_time = None
        self.total_tasks = 0
        self.completed_tasks = 0
        self.stage = stage  # 挖掘阶段，只有一阶才发送微信通知
        
        # 初始化通知服务
        if NOTIFICATION_SERVICE_AVAILABLE and config_manager:
            try:
                self.notification_service = NotificationService(config_manager)
                self.notification_service.set_logger(logger)
            except Exception as e:
                logger.warning(f"⚠️ 初始化通知服务失败: {e}")
    
    def start_tracking(self, total_tasks: int):
        """开始追踪多模拟进度"""
        self.start_time = datetime.now()
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.notification_sent = False  # 重置通知状态
        self.notification_retry_count = 0  # 重置重试计数
        logger.info(f"📊 开始追踪多模拟进度: 总计{total_tasks}个多模拟任务")
        
        if self.stage == 1:
            logger.info(f"📱 第{self.stage}阶挖掘：将在达到进度阈值(>95%)时发送一次微信通知")
        elif self.stage == 0:
            logger.info(f"📱 多模拟内部追踪：仅统计进度，不发送微信通知（避免与统一追踪器重复）")
        else:
            logger.info(f"📱 第{self.stage}阶挖掘：不发送微信通知（仅一阶发送，因为一阶因子数量有限且关键）")
    
    def update_progress(self, completed_count: int):
        """更新进度并检查是否需要发送通知"""
        if self.total_tasks == 0:
            return
            
        self.completed_tasks = completed_count
        completion_rate = (completed_count / self.total_tasks) * 100
        remaining_count = self.total_tasks - completed_count
        
        # 检查是否需要发送完成度通知（仅在达到阈值时）
        self._check_and_send_notification(completion_rate, remaining_count)
    
    def _check_and_send_notification(self, completion_rate: float, remaining_count: int):
        """检查是否需要发送完成度通知（避免重复通知）"""
        if not self.notification_service or not self.config_manager:
            return
        
        # 只有一阶挖掘才发送完成度通知，stage=0表示内部统计不发送通知
        if self.stage != 1:
            return
            
        # 避免重复发送通知
        if self.notification_sent:
            return
            
        # 限制重试次数，避免无限重试
        if self.notification_retry_count >= 3:
            logger.warning(f"⚠️ 多模拟通知已重试{self.notification_retry_count}次，停止重试")
            return
            
        # 只在进度超过95%且未完成时发送一次通知（不包括100%完成通知）
        if completion_rate > 95.0 and completion_rate < 100.0:
            try:
                self.notification_retry_count += 1
                logger.info(f"🔔 触发多模拟进度通知 (第{self.notification_retry_count}次尝试): {completion_rate:.2f}% > 95%")
                
                # 发送多模拟专用通知
                success = self._send_multi_simulation_notification(
                    completion_rate, remaining_count
                )
                
                if success:
                    # 只有发送成功才标记为已发送，避免重复
                    self.notification_sent = True
                    logger.info(f"✅ 多模拟进度通知已发送并标记，不会再次发送")
                else:
                    # 发送失败不标记，下次进度更新时会重试
                    logger.warning(f"❌ 多模拟进度通知发送失败 (第{self.notification_retry_count}/3次)，下次进度更新时将重试")
                    
            except Exception as e:
                logger.error(f"❌ 发送多模拟进度通知时出错 (第{self.notification_retry_count}/3次): {e}")
    
    def _send_multi_simulation_notification(self, completion_rate: float, 
                                          remaining_count: int) -> bool:
        """发送多模拟专用完成度通知"""
        try:
            if not self.notification_service.server_secret:
                logger.info("📱 未配置server_secret，跳过多模拟完成度通知")
                return False
            
            # 计算总耗时
            total_time = datetime.now() - self.start_time
            hours, remainder = divmod(total_time.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # 构建通知内容
            dataset_id = getattr(self.config_manager, 'current_dataset', '未知数据集')
            
            # 简化为单一通知类型
            title = f"📊 一阶多模拟进度报告 - {dataset_id} ({completion_rate:.1f}%)"
            urgency = "ℹ️ 信息"
            action_needed = "继续监控多模拟进度"
            
            content_lines = [f"**一阶多模拟挖掘进度报告:**"]
            content_lines.append(f"- {urgency} 级别通知")
            content_lines.append(f"- 数据集: {dataset_id}")
            content_lines.append(f"- 地区: {self.config_manager.region}")
            content_lines.append(f"- universe: {self.config_manager.universe}")
            content_lines.append(f"- 模式: **多模拟模式** (高并发)")
            content_lines.append("")
            
            # 进度统计
            content_lines.append(f"**挖掘进度统计:**")
            content_lines.append(f"- 📊 总体进度: {completion_rate:.2f}%")
            content_lines.append(f"- ✅ 已完成: {self.completed_tasks} 个多模拟任务")
            content_lines.append(f"- 📝 总计: {self.total_tasks} 个多模拟任务")
            content_lines.append(f"- ⏳ 剩余: {remaining_count} 个多模拟任务")
            content_lines.append("")
            
            # 耗时统计
            content_lines.append(f"**耗时统计:**")
            if hours > 0:
                content_lines.append(f"- 已运行: {hours}小时{minutes}分{seconds}秒")
            else:
                content_lines.append(f"- 已运行: {minutes}分{seconds}秒")
            
            if self.completed_tasks > 0:
                avg_time = total_time.seconds / self.completed_tasks
                content_lines.append(f"- 平均每个多模拟: {avg_time:.1f}秒")
            content_lines.append("")
            
            # 行动建议
            content_lines.append(f"**行动建议:**")
            content_lines.append(f"- {action_needed}")
            content_lines.append("- 📋 多模拟接近尾声，运行正常")
            content_lines.append("- 🔍 监控剩余多模拟的处理速度")
            
            content_lines.append("")
            content_lines.append(f"- 报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            content = "\n".join(content_lines)
            
            # 发送通知
            import requests
            url = f"https://sctapi.ftqq.com/{self.notification_service.server_secret}.send"
            data = {
                "text": title,
                "desp": content
            }
            
            response = requests.post(url, data=data, timeout=10)
            if response.status_code == 200:
                logger.info(f"📱 多模拟完成度通知发送成功 ({completion_rate:.1f}%)")
                return True
            else:
                logger.warning(f"📱 多模拟完成度通知发送失败: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"📱 发送多模拟完成度通知时出错: {e}")
            return False


async def refresh_session_cookies(session_manager):
    """
    刷新aiohttp会话的cookies（参考machine_lib_ee.py的实现）
    """
    try:
        from sessions.session_client import get_session_cookies
        import yarl
        
        # 获取SessionClient维护的cookies
        current_cookies = get_session_cookies()
        if current_cookies:
            logger.info("🔍 检查SessionClient是否已有新的cookies...")
            
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
                logger.info(f"✅ aiohttp cookies更新完成，包含{len(cookie_dict)}个cookie")
            else:
                logger.warning("⚠️ 当前cookies为空，尝试强制刷新...")
                raise Exception("当前cookies为空")
        else:
            logger.warning("⚠️ 无法获取当前cookies，尝试强制刷新...")
            raise Exception("无法获取当前cookies")
            
    except Exception as e:
        # 如果获取现有cookies失败，才进行强制刷新
        logger.info(f"🔄 获取现有cookies失败({e})，强制刷新会话...")


def generate_multi_sim_data(alpha_list, region, universe, neut, delay=1, max_trade="OFF", 
                           instrument_type="EQUITY", default_decay=6):
    """
    生成多模拟API所需的数据格式
    
    Args:
        alpha_list: Alpha表达式列表 (可以是字符串列表或(alpha, decay)元组列表)
        region: 地区
        universe: universe
        neut: 中性化方式
        delay: 延迟 (默认1)
        max_trade: 最大交易设置 (默认"OFF")
        instrument_type: 工具类型 (默认"EQUITY")
        default_decay: 默认衰减值，当alpha_list中没有指定decay时使用 (默认6)
    
    Returns:
        多模拟数据列表
    """
    
    multi_sim_data = []
    for item in alpha_list:
        # 处理不同的输入格式
        if isinstance(item, tuple):
            alpha_expr, decay = item
        else:
            alpha_expr = item
            decay = default_decay  # 使用传递的默认衰减值
            
        sim_data = {
            "type": "REGULAR",
            "settings": {
                "maxTrade": max_trade,  # 从上层传递
                "instrumentType": instrument_type,  # 从上层传递
                "region": region,
                "universe": universe,
                "delay": delay,
                "decay": decay,
                "neutralization": neut,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",  # 注意：API使用驼峰命名
                "truncation": 0.08,
                "nanHandling": "OFF",  # 修改为OFF，与平台一致
                "language": "FASTEXPR",
                "testPeriod": "P1Y",  # 修改为P1Y，与平台一致
                "visualization": False  # 必需字段
            },
            "regular": alpha_expr  # 注意：这里应该是字符串，不是对象
        }
        multi_sim_data.append(sim_data)
    return multi_sim_data


async def async_set_alpha_properties(session_manager, alpha_id, name, tags=None, 
                                     alpha_expression=None, tag_name=None):
    """
    异步设置Alpha属性 (name和tags)，并写入数据库
    根据官方API文档，使用统一的PATCH请求设置所有属性
    
    Args:
        session_manager: HTTP会话管理器
        alpha_id: Alpha ID
        name: Alpha名称
        tags: Alpha标签列表
        alpha_expression: 因子表达式（用于数据库写入）
        tag_name: 标签名称（用于数据库写入）
    
    Returns:
        bool: 设置成功返回True，失败返回False
    """
    if tags is None:
        tags = []
    
    try:
        # 根据官方API示例，使用统一的PATCH请求设置所有属性
        patch_data = {
            "color": None,
            "name": alpha_id,
            "tags": tags,
            "category": None,
            "regular": {
                "description": None
            }
        }
        
        patch_url = f"{brain_api_url}/alphas/{alpha_id}"
        
        # 支持会话刷新重试
        while True:
            try:
                async with session_manager.patch(patch_url, json=patch_data) as response:
                    # 处理401/403会话过期
                    if response.status in (401, 403):
                        logger.info(f"Alpha {alpha_id} 属性设置未授权，尝试刷新会话...")
                        await refresh_session_cookies(session_manager)
                        await asyncio.sleep(2)
                        continue
                        
                    elif response.status == 200:
                        logger.info(f"✅ Alpha {alpha_id} 属性设置成功 (名称: {name}, 标签: {tags})")
                        
                        # ✅ 属性设置成功后，写入数据库（调用machine_lib_ee中的函数）
                        if alpha_expression and tag_name and DATABASE_WRITE_AVAILABLE:
                            await _write_to_database(alpha_expression, tag_name, alpha_id)
                        elif alpha_expression and tag_name and not DATABASE_WRITE_AVAILABLE:
                            logger.warning(f"⚠️ 数据库写入功能不可用，跳过Alpha {alpha_id}的数据库写入")
                        
                        return True
                        
                    else:
                        response_text = await response.text()
                        logger.warning(f"设置Alpha {alpha_id} 属性失败: HTTP {response.status}, {response_text[:200]}")
                        return False
                        
            except Exception as e:
                logger.error(f"Alpha {alpha_id} 属性设置请求异常: {e}")
                return False
        
    except Exception as e:
        logger.error(f"设置Alpha {alpha_id} 属性时异常: {e}")
        return False




async def async_multi_simulate_with_concurrent_control(session_manager, multi_sim_tasks, region, universe, neut, 
                                                     delay, name, tags=None, n_jobs=8, progress_tracker=None, max_trade="OFF",
                                                     instrument_type="EQUITY", default_decay=6):
    """
    智能多模拟调度器：提交并监控模式
    关键特性：提交成功后立即监控，完成后释放槽位给新任务
    
    Args:
        session_manager: HTTP会话管理器
        multi_sim_tasks: 多模拟任务列表（每个任务包含10个alpha）
        region: 地区
        universe: universe
        neut: 中性化方式
        delay: 延迟
        name: Alpha名称
        tags: Alpha标签列表
        n_jobs: 并发数（对应槽位数）
    
    Returns:
        List[str]: 成功创建的Alpha ID列表
    """
    if tags is None:
        tags = [name]
    
    logger.info(f"🔥 多模拟引擎启动（智能调度模式）")
    logger.info(f"📊 任务统计: {len(multi_sim_tasks)} 个多模拟任务")
    logger.info(f"⚡ 智能调度: {n_jobs} 个槽位，提交后立即监控，完成后释放槽位")
    
    
    total_alpha_ids = []
    running_tasks = set()  # 正在运行的任务集合
    task_queue = list(enumerate(multi_sim_tasks))  # 待处理任务队列
    completed_count = 0
    background_property_tasks = []  # 跟踪后台属性设置任务
    
    logger.info(f"🚀 启动智能调度器，处理 {len(task_queue)} 个多模拟任务...")
    
    while task_queue or running_tasks:
        # 阶段1：填充空闲槽位
        while len(running_tasks) < n_jobs and task_queue:
            task_idx, alpha_task = task_queue.pop(0)
            
            # 创建提交并监控任务
            monitor_task = asyncio.create_task(
                submit_and_monitor_single_multi_simulation(
                    session_manager, alpha_task, region, universe, neut, 
                    delay, name, tags, task_idx, max_trade, instrument_type, default_decay
                )
            )
            running_tasks.add(monitor_task)
            
            logger.info(f"🎯 槽位分配: 启动多模拟 {task_idx + 1} ({len(running_tasks)}/{n_jobs} 槽位使用中)")
        
        # 阶段2：等待至少一个任务完成
        if running_tasks:
            logger.info(f"⏳ 等待 {len(running_tasks)} 个运行中的多模拟完成...")
            
            # 使用as_completed等待第一个完成的任务
            done, pending = await asyncio.wait(
                running_tasks, 
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # 处理已完成的任务
            for completed_task in done:
                try:
                    result = await completed_task
                    if isinstance(result, tuple) and len(result) == 2:
                        alpha_ids, background_task = result
                        total_alpha_ids.extend(alpha_ids)
                        completed_count += 1
                        logger.info(f"✅ 多模拟完成 ({completed_count}/{len(multi_sim_tasks)}): 获得 {len(alpha_ids)} 个alpha")
                        
                        # 收集后台属性设置任务
                        if background_task:
                            background_property_tasks.append(background_task)
                        
                        # 更新进度追踪器（仅在超过阈值时发送微信通知）
                        if progress_tracker:
                            progress_tracker.update_progress(completed_count)
                    elif isinstance(result, list):
                        # 兼容旧格式
                        total_alpha_ids.extend(result)
                        completed_count += 1
                        logger.info(f"✅ 多模拟完成 ({completed_count}/{len(multi_sim_tasks)}): 获得 {len(result)} 个alpha")
                        
                        # 更新进度追踪器（仅在超过阈值时发送微信通知）
                        if progress_tracker:
                            progress_tracker.update_progress(completed_count)
                            
                    else:
                        logger.warning(f"⚠️ 多模拟返回异常结果: {result}")
                        completed_count += 1
                        
                        # 也要更新进度追踪器
                        if progress_tracker:
                            progress_tracker.update_progress(completed_count)
                        
                except Exception as e:
                    logger.error(f"❌ 多模拟任务异常: {e}")
                    completed_count += 1
                    
                    # 异常情况也要更新进度
                    if progress_tracker:
                        progress_tracker.update_progress(completed_count)
                
                # 从运行集合中移除已完成的任务
                running_tasks.discard(completed_task)
            
            # 更新运行任务集合
            running_tasks = pending
            
            logger.info(f"🔄 槽位释放: {len(done)} 个任务完成，{len(running_tasks)} 个仍在运行，{len(task_queue)} 个等待中")
    
    logger.info(f"🎉 智能调度完成: 处理 {len(multi_sim_tasks)} 个多模拟，创建 {len(total_alpha_ids)} 个alpha")
    
    # 等待所有后台属性设置任务完成
    if background_property_tasks:
        logger.info(f"⏳ 等待 {len(background_property_tasks)} 个后台属性设置任务完成...")
        try:
            await asyncio.gather(*background_property_tasks, return_exceptions=True)
            logger.info(f"✅ 所有后台属性设置任务已完成")
        except Exception as e:
            logger.warning(f"⚠️ 等待后台属性设置任务时出现异常: {e}")
    
    return total_alpha_ids


async def submit_and_monitor_single_multi_simulation(session_manager, alpha_task, region, universe, 
                                                   neut, delay, name, tags, task_idx, max_trade="OFF",
                                                   instrument_type="EQUITY", default_decay=6):
    """
    提交单个多模拟并监控到完成
    这是一个完整的生命周期管理：提交 → 监控 → 获取结果
    
    Returns:
        Tuple[List[str], Optional[asyncio.Task]]: (alpha_ids, background_property_task)
    """
    collected_alpha_ids = []
    background_task = None
    
    try:
        # 生成多模拟数据
        multi_sim_data = generate_multi_sim_data(alpha_task, region, universe, neut, delay, max_trade, 
                                                instrument_type, default_decay)
        
        # 基础数据验证
        if not multi_sim_data:
            logger.error(f"❌ 多模拟 {task_idx + 1} 数据为空")
            return [], None
        
        # 验证关键数据格式
        for i, data in enumerate(multi_sim_data):
            if not isinstance(data, dict) or "type" not in data or "settings" not in data or "regular" not in data:
                logger.error(f"❌ 多模拟 {task_idx + 1} 数据格式错误[{i}]: 缺少必需字段")
                return [], None

        # 提交多模拟
        logger.info(f"📤 提交多模拟 {task_idx + 1}: {len(alpha_task)} 个alpha")
        
        # 记录负载信息（用于调试测试）
        import json
        payload_str = json.dumps(multi_sim_data, indent=2, ensure_ascii=False)
        logger.info(f"📋 多模拟 {task_idx + 1} 负载摘要: {len(multi_sim_data)} 个模拟, 总大小 {len(payload_str)} 字符")
        logger.debug(f"📋 多模拟 {task_idx + 1} 完整负载:\n{payload_str}")
        
        while True:
            try:
                async with session_manager.post(f"{brain_api_url}/simulations", 
                                              json=multi_sim_data) as response:
                    
                    # 处理401/403会话过期
                    if response.status in (401, 403):
                        logger.warning(f"  ⚠️ 多模拟提交未授权 ({response.status}): 会话可能过期，尝试刷新会话...")
                        await refresh_session_cookies(session_manager)
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                        
                    elif response.status == 201:
                        # 获取进度URL
                        location_header = response.headers.get('Location')
                        if location_header:
                            if location_header.startswith('/'):
                                progress_url = f"{brain_api_url}{location_header}"
                            else:
                                progress_url = location_header
                            
                            logger.info(f"  ✅ 多模拟已提交: {progress_url}")
                            
                            # 立即开始监控进度，传递alpha表达式信息
                            collected_alpha_ids, background_task = await monitor_multi_simulation_until_complete(
                                session_manager, progress_url, name, tags, task_idx, alpha_task
                            )
                            break
                            
                        else:
                            logger.error(f"  ❌ 多模拟提交成功但缺少Location头")
                            break
                            
                    elif response.status == 429:
                        # 对于429错误，等待并重试
                        logger.debug(f"⏳ 多模拟提交速率限制，等待 2 s")
                        await asyncio.sleep(2)
                        continue
                        
                    else:
                        # 提交失败
                        error_text = await response.text()
                        logger.error(f"❌ 多模拟 {task_idx + 1} 提交失败: HTTP {response.status}, {error_text[:200]}")
                        
                        break
                        
            except Exception as e:
                logger.error(f"多模拟提交请求异常: {e}")
                break
                
    except Exception as e:
        logger.error(f"多模拟任务 {task_idx + 1} 处理失败: {e}")
    
    return collected_alpha_ids, background_task


async def monitor_multi_simulation_until_complete(session_manager, progress_url, name, tags, task_idx, alpha_task):
    """
    监控多模拟直到完成，并获取所有alpha结果
    
    Args:
        session_manager: HTTP会话管理器
        progress_url: 进度查询URL
        name: Alpha名称
        tags: Alpha标签列表
        task_idx: 任务索引
        alpha_task: 原始alpha表达式列表（用于数据库写入）
        
    Returns:
        Tuple[List[str], Optional[asyncio.Task]]: (alpha_ids, background_property_task)
    """
    collected_alpha_ids = []
    
    while True:
        try:
            async with session_manager.get(progress_url) as response:
                # 处理401/403会话过期
                if response.status in (401, 403):
                    logger.info("多模拟进度查询未授权，尝试刷新会话...")
                    await refresh_session_cookies(session_manager)
                    await asyncio.sleep(2)
                    continue
                    
                elif response.status == 200:
                    response_json = await response.json()
                    
                    # 检查是否完成
                    if "status" in response_json:
                        status = response_json.get("status")
                        if status == "COMPLETE":
                            logger.info(f"  🎉 多模拟 {task_idx + 1} 完成！")
                            
                            # 处理子模拟：快速收集Alpha IDs和表达式信息
                            children = response_json.get("children", [])
                            alpha_info_list = []  # 存储Alpha信息，用于后台异步处理
                            
                            for idx, child_id in enumerate(children):
                                try:
                                    child_url = f"{brain_api_url}/simulations/{child_id}"
                                    
                                    # 支持会话刷新重试获取子模拟
                                    while True:
                                        try:
                                            async with session_manager.get(child_url) as child_response:
                                                # 处理401/403会话过期
                                                if child_response.status in (401, 403):
                                                    logger.info(f"获取子模拟 {child_id} 未授权，尝试刷新会话...")
                                                    await refresh_session_cookies(session_manager)
                                                    await asyncio.sleep(2)
                                                    continue
                                                    
                                                elif child_response.status == 200:
                                                    child_data = await child_response.json()
                                                    alpha_id = child_data.get("alpha")
                                                    if alpha_id:
                                                        collected_alpha_ids.append(alpha_id)
                                                        
                                                        # 获取对应的alpha表达式（用于数据库写入）
                                                        alpha_expression = None
                                                        if idx < len(alpha_task):
                                                            if isinstance(alpha_task[idx], tuple):
                                                                alpha_expression = alpha_task[idx][0]  # (expression, decay)
                                                            else:
                                                                alpha_expression = alpha_task[idx]  # 直接是表达式
                                                        
                                                        # 收集Alpha信息，不立即处理
                                                        alpha_info_list.append({
                                                            'alpha_id': alpha_id,
                                                            'alpha_expression': alpha_expression,
                                                            'name': name,
                                                            'tags': tags,
                                                            'tag_name': name
                                                        })
                                                    
                                                    break  # 成功获取，跳出重试循环
                                                    
                                                else:
                                                    logger.warning(f"获取子模拟 {child_id} 失败: HTTP {child_response.status}")
                                                    break  # 非401/403错误，跳出重试循环
                                                    
                                        except Exception as e:
                                            logger.error(f"获取子模拟 {child_id} 请求异常: {e}")
                                            break  # 异常，跳出重试循环
                                                
                                except Exception as e:
                                    logger.error(f"  ❌ 处理子模拟 {child_id} 失败: {e}")
                            
                            logger.info(f"  ✅ 多模拟 {task_idx + 1} 处理完成: 获得 {len(collected_alpha_ids)} 个alpha")
                            logger.info(f"  🔄 准备异步处理 {len(alpha_info_list)} 个Alpha属性设置...")
                            
                            # 在后台异步处理属性设置，不阻塞槽位释放
                            background_task = None
                            if alpha_info_list:
                                background_task = asyncio.create_task(
                                    handle_alpha_properties_async_improved(
                                        session_manager, alpha_info_list, task_idx
                                    )
                                )
                            
                            # 返回结果和后台任务
                            return collected_alpha_ids, background_task
                            
                        elif status in ["ERROR", "FAIL", "TIMEOUT"]:
                            # 尝试获取更详细的错误信息
                            error_message = response_json.get("message", "")
                            error_detail = response_json.get("error", "")
                            errors = response_json.get("errors", [])
                            details = response_json.get("details", "")
                            
                            error_info = f"状态: {status}"
                            if error_message:
                                error_info += f", 消息: {error_message}"
                            if error_detail:
                                error_info += f", 详情: {error_detail}"
                            if details:
                                error_info += f", 细节: {details}"
                            if errors:
                                error_info += f", 错误列表: {errors}"
                            
                            logger.error(f"  ❌ 多模拟 {task_idx + 1} 失败: {error_info}")
                            logger.info(f"  📋 完整响应: {response_json}")
                            return collected_alpha_ids, None
                            
                    # 如果还在进行中，等待后继续
                    progress = response_json.get("progress", 0)
                    if progress > 0:
                        logger.debug(f"⏳ 多模拟 {task_idx + 1} 进行中: {progress*100:.1f}%")
                    
                    # 根据Retry-After头决定等待时间
                    retry_after = response.headers.get('Retry-After', '5')
                    wait_time = float(retry_after)
                    await asyncio.sleep(wait_time)
                    
                else:
                    logger.warning(f"  ⚠️ 多模拟 {task_idx + 1} 查询失败: HTTP {response.status}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            logger.error(f"  ❌ 监控多模拟 {task_idx + 1} 异常: {e}")
            await asyncio.sleep(5)
    
    return collected_alpha_ids, None


async def handle_alpha_properties_async_improved(session_manager, alpha_info_list, task_idx):
    """
    改进的异步Alpha属性设置处理函数
    这个函数在后台运行，不阻塞槽位释放，真正实现异步处理
    """
    try:
        logger.info(f"  🔧 开始异步处理多模拟 {task_idx + 1} 的Alpha属性设置...")
        
        # 创建所有属性设置任务
        property_tasks = []
        for alpha_info in alpha_info_list:
            task = asyncio.create_task(
                async_set_alpha_properties(
                    session_manager,
                    alpha_info['alpha_id'],
                    alpha_info['name'],
                    alpha_info['tags'],
                    alpha_expression=alpha_info['alpha_expression'],
                    tag_name=alpha_info['tag_name']
                )
            )
            property_tasks.append(task)
        
        # 等待所有属性设置任务完成
        results = await asyncio.gather(*property_tasks, return_exceptions=True)
        
        success_count = 0
        for result in results:
            if result is True:
                success_count += 1
            elif isinstance(result, Exception):
                logger.warning(f"⚠️ Alpha属性设置异常: {result}")
        
        logger.info(f"  ✅ 多模拟 {task_idx + 1} 的Alpha属性设置完成: {success_count}/{len(alpha_info_list)} 成功")
        
    except Exception as e:
        logger.error(f"  ❌ 多模拟 {task_idx + 1} 异步属性处理失败: {e}")


async def simulate_multiple_alphas_with_multi_mode(
    alpha_list: List[str], region_list: List[Tuple], 
    decay_list: List[int], delay_list: List[int], 
    name: str, neut: str, stone_bag: List = None, 
    n_jobs: int = 5, enable_multi_simulation: bool = False,
    config_manager=None
) -> None:
    """
    支持单模拟和多模拟的统一模拟执行函数
    参考单模拟的任务拆分模式，使用并发控制
    """
    if stone_bag is None:
        stone_bag = []
        
    total_alphas = len(alpha_list)
    
    if enable_multi_simulation:
        # 多模拟模式：固定每个多模拟10个alpha，使用n_jobs作为并发数
        multi_children_limit = 10  # 固定为10，吃满API上限
        multi_batch_limit = n_jobs  # 使用n_jobs作为并发数
        
        logger.info(f"🔥 启用多模拟模式: {total_alphas:,}个因子")
        logger.info(f"📊 多模拟配置: {multi_children_limit}个alpha/多模拟, {multi_batch_limit}个并发多模拟")
        logger.info(f"⚡ 理论并发度: {multi_children_limit * multi_batch_limit} = {multi_children_limit * multi_batch_limit} (vs 单模拟的{n_jobs})")
        
        # 准备多模拟数据：直接按10个alpha一组分组
        alpha_decay_list = [(alpha_list[i], decay_list[i]) 
                           for i in range(len(alpha_list))]
        
        # 直接分组，不使用任务池概念
        multi_sim_tasks = [alpha_decay_list[i:i + multi_children_limit] 
                          for i in range(0, len(alpha_decay_list), multi_children_limit)]
        
        # 检查任务分组的合理性
        total_alphas_in_tasks = sum(len(task) for task in multi_sim_tasks)
        if total_alphas_in_tasks != len(alpha_decay_list):
            logger.warning(f"⚠️ 任务分组异常: 分组后Alpha总数({total_alphas_in_tasks}) != 原始Alpha数({len(alpha_decay_list)})")
        
        # 获取统一会话管理器
        try:
            import sys
            import os
            
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.append(current_dir)
                
            from sessions.session_client import get_session_cookies
            
            cookies = get_session_cookies()
            if not cookies:
                raise Exception("无法获取有效的会话cookies")
            
            cookie_jar = aiohttp.CookieJar()
            cookie_dict = {}
            for cookie_name, cookie_value in cookies.items():
                cookie_dict[cookie_name] = cookie_value
            
            if cookie_dict:
                cookie_jar.update_cookies(cookie_dict, response_url=yarl.URL("https://api.worldquantbrain.com"))
            
            session_manager = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=600, connect=30, sock_read=300),
                connector=aiohttp.TCPConnector(
                    limit=50, 
                    ttl_dns_cache=300,
                    use_dns_cache=True,
                    keepalive_timeout=60,
                    enable_cleanup_closed=True
                ),
                cookie_jar=cookie_jar
            )
            
            logger.info(f"✅ 多模拟会话创建成功")
            
        except Exception as e:
            logger.error(f"❌ 多模拟会话创建失败: {e}")
            raise
        
        try:
            # 执行多模拟（使用新的并发控制模式）
            region, universe = region_list[0]  # 所有alpha使用相同的region设置
            delay = delay_list[0]  # 所有alpha使用相同的delay设置
            
            # 初始化进度追踪器
            progress_tracker = None
            try:
                # 使用传入的config_manager或创建新的
                if config_manager is None:
                    from digging.core.config_manager import ConfigManager
                    config_manager = ConfigManager()
                
                # 获取当前执行阶段（默认为1，这里需要从系统参数或配置中获取）
                current_stage = getattr(config_manager, '_current_stage', 1)  # 默认为一阶
                
                progress_tracker = MultiSimulationProgressTracker(config_manager, stage=current_stage)
                progress_tracker.start_tracking(len(multi_sim_tasks))
            except Exception as e:
                logger.warning(f"⚠️ 初始化多模拟进度追踪器失败: {e}")
            
            # 从配置管理器获取配置参数
            max_trade = "OFF"  # 默认值
            instrument_type = "EQUITY"  # 默认值
            default_decay = 6  # 默认值
            
            if config_manager:
                if hasattr(config_manager, 'max_trade'):
                    max_trade = config_manager.max_trade
                if hasattr(config_manager, 'instrument_type'):
                    instrument_type = config_manager.instrument_type
                if hasattr(config_manager, 'decay'):
                    default_decay = config_manager.decay
            
            alpha_ids = await async_multi_simulate_with_concurrent_control(
                session_manager, multi_sim_tasks, region, universe, neut, 
                delay, name, [name], n_jobs=multi_batch_limit, 
                progress_tracker=progress_tracker, max_trade=max_trade,
                instrument_type=instrument_type, default_decay=default_decay
            )
            
            logger.info(f"🎉 多模拟完成: 共处理 {total_alphas} 个因子，创建 {len(alpha_ids)} 个alpha")
            
        finally:
            await session_manager.close()
            
    else:
        logger.info(f"🔄 多模拟未启用，回退到原有单模拟逻辑")
        raise NotImplementedError("单模拟模式请使用原有的 machine_lib_ee.py 中的函数")


if __name__ == "__main__":
    # 简单测试
    print("多模拟引擎模块加载成功 (v3.0 - 智能调度器)")
    print(f"支持的功能:")
    print(f"- generate_multi_sim_data: 多模拟数据生成")
    print(f"- async_multi_simulate_with_concurrent_control: 智能调度的多模拟执行")
    print(f"- simulate_multiple_alphas_with_multi_mode: 统一模拟接口")
    print(f"- 智能槽位管理: 动态调度，异步属性设置，数据库写入")