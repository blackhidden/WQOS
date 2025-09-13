"""
模拟引擎模块 (Simulation Engine)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的模拟相关功能：
- 单次模拟
- 异步Alpha属性设置
- 会话刷新处理
"""

import os
import asyncio
import aiohttp
import logging as logger

from config import RECORDS_PATH


async def simulate_single(session_manager, alpha_expression, region_info, name, neut,
                          decay, delay, stone_bag, tags=['None'],
                          semaphore=None, max_trade='OFF'):
    """
    单次模拟一个alpha表达式对应的某个地区的信息
    正确的并发控制：semaphore控制整个模拟生命周期，直到模拟完成才释放槽位
    """
    # 注意：会话管理已由统一会话管理器在后台处理，无需在此检查过期

    region, uni = region_info
    alpha = "%s" % (alpha_expression)

    logger.debug("Simulating for alpha: %s, region: %s, universe: %s, decay: %s" % (alpha, region, uni, decay))

    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': region,
            'universe': uni,
            'delay': delay,
            'decay': decay,
            'neutralization': neut,
            'maxTrade': max_trade,
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False,
        },
        'regular': alpha
    }

    # 🎯 关键修正：semaphore控制整个模拟生命周期，直到模拟完成
    async with semaphore:
        logger.info(f"🔒 [槽位占用] 获取模拟槽位: {alpha[:50]}...")
        
        # 🚀 阶段1：模拟提交
        simulation_progress_url = None
        while True:
            try:
                async with session_manager.post('https://api.worldquantbrain.com/simulations',
                                                json=simulation_data) as resp:
                    # 速率与鉴权处理
                    if resp.status in (401, 403):
                        logger.info("Unauthorized/Forbidden on POST, session issue detected")
                        # 获取最新会话（SessionKeeper会自动维护）
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
                    
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                    if resp.status == 429:
                        retry_after_hdr = resp.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        logger.info(f"🚨 平台模拟槽位已满 (HTTP 429)，等待{wait_s}s后重试")
                        await asyncio.sleep(wait_s)
                        continue
                    simulation_progress_url = resp.headers.get('Location', 0)
                    if simulation_progress_url == 0:
                        # 无 Location，解析错误主体，兼容多种结构
                        try:
                            json_data = await resp.json()
                        except Exception:
                            json_data = await resp.text()

                        def extract_detail(payload):
                            if isinstance(payload, dict):
                                return payload.get('detail') or payload.get('message') or payload.get('error') or ''
                            return str(payload)

                        detail = extract_detail(json_data)
                        detail_str = str(detail)

                        # 并发上限 → 等待重试
                        if 'CONCURRENT_SIMULATION_LIMIT_EXCEEDED' in detail_str:
                            logger.info("⚠️ 平台并发限制已达上限，可能存在其他进程的模拟任务占用槽位")
                            logger.info("💡 建议：检查平台是否有其他模拟任务正在运行")
                            await asyncio.sleep(5)
                            continue

                        # 重复表达式 → 记录并跳过
                        if 'duplicate' in detail_str.lower():
                            logger.info("Alpha expression is duplicated")
                            await asyncio.sleep(1)
                            return 0

                        # 其他错误 → 打印并跳过
                        logger.info(f"detail: {detail_str}")
                        logger.info(f"json_data: {json_data}")
                        await asyncio.sleep(1)
                        return 0
                    else:
                        logger.info(f'✅ 模拟已提交，开始占用槽位: {simulation_progress_url}')
                        break
            except KeyError:
                logger.info("Location key error during simulation request")
                await asyncio.sleep(60)
                return
            except Exception as e:
                logger.info(f"An error occurred: {str(e)}")
                await asyncio.sleep(60)
                return
        
        # 🔄 阶段2：进度轮询 - 仍在槽位占用中
        logger.info(f"⏳ 轮询进度 (槽位占用中): {alpha[:50]}...")
        
        while True:
            try:
                async with session_manager.get(simulation_progress_url) as resp:
                    # 速率与鉴权处理
                    if resp.status in (401, 403):
                        logger.info("Unauthorized/Forbidden on GET, session issue detected")
                        # 获取最新会话（SessionKeeper会自动维护）
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
                                for name, value in current_cookies.items():
                                    cookie_dict[name] = value
                                
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
                        
                        await asyncio.sleep(2)  # 给会话更新一点时间
                        continue
                    if resp.status == 429:
                        retry_after_hdr = resp.headers.get('Retry-After')
                        try:
                            wait_s = float(retry_after_hdr) if retry_after_hdr is not None else 5.0
                        except Exception:
                            wait_s = 5.0
                        logger.info(f"Rate limited on GET, sleep {wait_s}s then retry")
                        await asyncio.sleep(wait_s)
                        continue
                    
                    # 非 JSON 响应（如 504 HTML 网关页）健壮处理
                    content_type = (resp.headers.get('Content-Type') or '').lower()
                    if 'application/json' not in content_type:
                        try:
                            body_preview = (await resp.text())[:200]
                        except Exception:
                            body_preview = '<non-json>'
                        logger.info(f"Non-JSON progress response: status={resp.status}, content-type={content_type}, body[:200]={body_preview}")
                        if resp.status in (500, 502, 503, 504, 408):
                            await asyncio.sleep(5)
                            continue
                        else:
                            await asyncio.sleep(2)
                            continue

                    json_data = await resp.json()
                    # 获取响应头并处理 Retry-After
                    retry_after_hdr = resp.headers.get('Retry-After')
                    try:
                        retry_after_val = float(retry_after_hdr) if retry_after_hdr is not None else 0.0
                    except Exception:
                        retry_after_val = 0.0
                    if retry_after_val <= 0:
                        break
                    await asyncio.sleep(retry_after_val)
            except Exception as e:
                logger.info(f"Error while checking progress: {str(e)}")
                await asyncio.sleep(60)

    logger.info("%s done simulating, getting alpha details" % (simulation_progress_url))
    try:
        # 首先检查模拟状态
        status = json_data.get("status")
        if status == "ERROR":
            # 模拟失败，记录失败表达式
            message = json_data.get("message", "Unknown error")
            logger.info(f"Simulation failed: {message}")
            
            # 判断是否为真正的表达式错误（不是临时问题）
            message_str = str(message).lower() if message else ""
            is_expression_error = (
                "end of input" in message_str or
                "syntax" in message_str or
                "parse" in message_str or
                "invalid" in message_str or
                "undefined" in message_str or
                "unknown" in message_str or
                ("error" in message_str and "duplicate" not in message_str)
            )
            
            if is_expression_error:
                failure_reason = message if message else "Expression error"
                if "end of input" in message_str:
                    failure_reason = "Unexpected end of input"
                elif "syntax" in message_str:
                    failure_reason = "Syntax error"
                elif "parse" in message_str:
                    failure_reason = "Parse error"
                elif "invalid" in message_str:
                    failure_reason = "Invalid expression"
                elif "undefined" in message_str or "unknown" in message_str:
                    failure_reason = "Undefined function or field"
                
                from .database_utils import _record_failed_expression
                await _record_failed_expression(
                    alpha_expression=alpha_expression,
                    tag_name=name,
                    failure_reason=failure_reason,
                    error_details=str(json_data)
                )
            else:
                logger.info("Skipping record - temporary failure (duplication, rate limit, etc.)")
            
            return 0
        
        alpha_id = json_data.get("alpha")
        if not alpha_id:
            logger.info(f"No alpha_id returned for simulation: {simulation_progress_url}")
            return 0

        logger.info(f"🔓 [槽位释放] 模拟完成，立即释放槽位: {alpha[:50]}... -> Alpha ID: {alpha_id}")

    except KeyError:
        logger.info("Failed to retrieve alpha ID for: %s" % simulation_progress_url)
        logger.info(f"🔓 [槽位释放] 因错误释放槽位: {alpha[:50]}...")
        return 0
    except Exception as e:
        logger.info(f"An error occurred during simulation: {str(e)}")
        logger.info(f"🔓 [槽位释放] 因错误释放槽位: {alpha[:50]}...")
        return 0

    # 💾 阶段3：属性设置 - 不占用模拟槽位，可以异步并发进行
    logger.info(f"🏷️ 开始属性设置 (已释放槽位): {alpha[:50]}... -> Alpha ID: {alpha_id}")
    
    try:
        # 调用属性设置函数，并传递必要的数据库写入参数
        success = await async_set_alpha_properties(session_manager,
                                         alpha_id,
                                         name="%s" % name,
                                         color=None,
                                         tags=tags,
                                         # 传递数据库写入所需参数
                                         alpha_expression=alpha,
                                         tag_name=name)

        # stone_bag.append(alpha_id)
        
        logger.info(f"✅ 任务完全完成: {alpha[:50]}... -> 模拟ID: {alpha_id}, 属性设置: {'成功' if success else '失败'}")
        
    except Exception as e:
        logger.info(f"An error occurred while setting alpha properties: {str(e)}")
        logger.info(f"⚠️ 属性设置失败，但模拟已完成: {alpha[:50]}... -> Alpha ID: {alpha_id}")

    # return stone_bag
    return 0


async def async_set_alpha_properties(
        session,  # aiohttp 的 session
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = None,
        combo_desc: str = None,
        tags: list = None,
        # 新增参数用于数据库写入
        alpha_expression: str = None,
        tag_name: str = None,
):
    """
    异步函数，修改 alpha 的描述参数
    成功后写入数据库
    
    Returns:
        bool: True if successful, False otherwise
    """

    params = {
        "category": None,
        "regular": {"description": None},
        "name": alpha_id  # 使用alpha_id作为name，不使用tag名称
    }
    if color:
        params["color"] = color
    if tags:
        params["tags"] = tags
    if combo_desc:
        params["combo"] = {"description": combo_desc}
    if selection_desc:
        params["selection"] = {"description": selection_desc}

    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"

    try:
        async with session.patch(url, json=params) as response:
            # 检查状态码，确保请求成功
            if response.status == 200:
                logger.info(f"Alpha {alpha_id} properties updated successfully! Tag: {tags}")
                
                # ✅ 属性设置成功后，写入数据库
                if alpha_expression and tag_name:
                    from .database_utils import _write_to_database
                    await _write_to_database(alpha_expression, tag_name, alpha_id)
                
                return True
            else:
                logger.info(
                    f"Failed to update alpha {alpha_id}. Status code: {response.status}, Response: {await response.text()}")
                return False

    except aiohttp.ClientError as e:
        logger.info(f"Error during patch request for alpha {alpha_id}: {e}")
        return False
    except Exception as e:
        logger.info(f"An unexpected error occurred for alpha {alpha_id}: {e}")
        return False
