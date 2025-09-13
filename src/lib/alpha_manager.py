"""
Alpha管理模块 (Alpha Manager)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的Alpha相关功能：
- Alpha属性设置
- 批量Alpha处理
"""

import time
import logging as logger


def batch_set_alpha_properties(
        s,
        alpha_data: list,  # [{"id": "alpha_id", "color": "GREEN"}, ...]
        max_batch_size: int = 100
):
    """
    批量设置Alpha颜色（使用新的批量API - PATCH /alphas）
    注意：此API目前只支持批量设置颜色，不支持name、tags等其他属性
    
    Args:
        s: session对象
        alpha_data: Alpha数据列表，每个元素必须包含id和color字段
                   例：[{"id": "ZVO5aLY", "color": "GREEN"}, {"id": "QR09ZpG", "color": "BLUE"}]
        max_batch_size: 最大批次大小
    Returns:
        dict: {"success": int, "failed": int, "details": [...]}
    """
    if not alpha_data:
        return {"success": 0, "failed": 0, "details": []}
    
    success_count = 0
    failed_count = 0
    details = []
    
    # 分批处理，避免单次请求过大
    for i in range(0, len(alpha_data), max_batch_size):
        batch = alpha_data[i:i + max_batch_size]
        batch_num = i//max_batch_size + 1
        
        # 重试机制：处理速率限制
        retry_count = 0
        max_retries = 3
        batch_success = False
        
        while retry_count < max_retries and not batch_success:
            try:
                response = s.patch(
                    "https://api.worldquantbrain.com/alphas",
                    json=batch
                )
                
                if response.status_code == 200:
                    batch_success_count = len(batch)
                    success_count += batch_success_count
                    details.append(f"批次 {batch_num}: 成功设置 {batch_success_count} 个Alpha")
                    batch_success = True
                    
                elif response.status_code == 429:
                    # 速率限制，等待后重试
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = min(2 ** retry_count, 8)  # 指数退避，最大8秒
                        details.append(f"批次 {batch_num}: API速率限制，等待 {wait_time}秒后重试 ({retry_count}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        # 重试次数用完，标记为失败
                        batch_failed = len(batch)
                        failed_count += batch_failed
                        details.append(f"批次 {batch_num}: API速率限制重试失败，已达最大重试次数")
                        
                else:
                    # 其他HTTP错误，不重试
                    batch_failed = len(batch)
                    failed_count += batch_failed
                    error_msg = f"批次 {batch_num}: HTTP {response.status_code}"
                    try:
                        error_data = response.json()
                        if 'message' in error_data:
                            error_msg += f" - {error_data['message']}"
                    except:
                        error_msg += f" - {response.text[:100]}"
                    details.append(error_msg)
                    break
                    
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(2 ** retry_count, 8)
                    details.append(f"批次 {batch_num}: 网络异常，等待 {wait_time}秒后重试 ({retry_count}/{max_retries}) - {str(e)}")
                    time.sleep(wait_time)
                else:
                    batch_failed = len(batch)
                    failed_count += batch_failed
                    details.append(f"批次 {batch_num}: 网络异常重试失败 - {str(e)}")
                    
        # 批次间休息，避免API压力（成功的批次也要休息）
        if i + max_batch_size < len(alpha_data):
            time.sleep(1)
    
    return {
        "success": success_count,
        "failed": failed_count,
        "details": details
    }


def set_alpha_properties(
        s,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = None,
        combo_desc: str = None,
        tags: list = None,  # ['tag1', 'tag2']
):
    """
    Function changes alpha's description parameters
    Returns:
    - True: 成功
    - False: 一般失败
    - 'RATE_LIMITED': HTTP 429速率限制，建议重新登录
    """
    params = {
        "category": None,
        "regular": {"description": None},
        "name": alpha_id if alpha_id else name  # 默认使用alpha_id作为name
    }
    if color:
        params["color"] = color
    if tags:
        params["tags"] = tags
    if combo_desc:
        params["combo"] = {"description": combo_desc}
    if selection_desc:
        params["selection"] = {"description": selection_desc}

    try:
        response = s.patch(
            "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
        )
        
        # 检查响应状态
        if response.status_code == 200:
            return True
        else:
            logger.warning(f"❌ Alpha {alpha_id} 属性设置失败: HTTP {response.status_code}")
            
            # 特别处理429错误 - 返回特殊标识
            if response.status_code == 429:
                try:
                    error_text = response.text
                    if error_text:
                        logger.warning(f"错误详情: {error_text}")
                    try:
                        error_data = response.json()
                        if 'message' in error_data:
                            logger.info(f"API速率限制: {error_data['message']}")
                        if 'retry_after' in error_data:
                            logger.info(f"建议等待时间: {error_data['retry_after']}秒")
                    except:
                        logger.info("API速率限制超出，建议重新登录")
                except Exception as parse_error:
                    logger.info(f"解析429错误响应时异常: {parse_error}")
                
                # 返回特殊标识，表示需要重新登录
                return 'RATE_LIMITED'
            
            # 其他HTTP错误
            try:
                error_text = response.text
                if error_text:
                    logger.warning(f"错误详情: {error_text}")
            except Exception as parse_error:
                logger.info(f"解析错误响应时异常: {parse_error}")
            return False
            
    except Exception as e:
        logger.warning(f"❌ Alpha {alpha_id} 属性设置异常: {e}")
        return False
