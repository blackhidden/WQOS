"""
数据获取客户端 (Data Client)
作者：e.e.
日期：2025年9月

从machine_lib_ee.py迁移的数据获取相关功能：
- 数据集获取
- 数据字段获取
- Alpha获取
- 数据处理
"""

import time
import pandas as pd
import requests
import math
import logging as logger

from .config_utils import load_digging_config
from .operator_manager import get_vec_fields


def get_datasets(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000'
):
    """获取数据集列表"""
    url = "https://api.worldquantbrain.com/data-sets?" + \
          f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df


def get_datafields(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000',
        dataset_id: str = '',
        search: str = ''
):
    """健壮获取数据字段列表，支持分页、重试和会话刷新

    变更点：
    - 不再依赖 response.json()['count']，改为基于分页直到返回数量<limit
    - 对 429 速率限制尊重 Retry-After 头并退避重试
    - 对 401/403 或异常响应尝试重新登录
    - 任何异常情况下返回空 DataFrame，而不是抛出 KeyError
    """
    base_url = "https://api.worldquantbrain.com/data-fields"
    limit = 50
    offset = 0
    aggregated_results = []

    # 读取重试配置
    try:
        cfg = load_digging_config()
        max_retries = cfg.get('api_max_retries', 3)
        retry_delay = cfg.get('api_retry_delay', 5)
        call_interval = cfg.get('api_call_interval', 0.0)
        burst_delay = cfg.get('api_burst_delay', 0.0)
    except Exception:
        max_retries = 3
        retry_delay = 5
        call_interval = 0.0
        burst_delay = 0.0

    # 构建固定查询参数
    def make_params(current_offset: int):
        params = {
            'instrumentType': instrument_type,
            'region': region,
            'delay': str(delay),
            'universe': universe,
            'limit': str(limit),
            'offset': str(current_offset),
        }
        if dataset_id and not search:
            params['dataset.id'] = dataset_id
        if search:
            params['search'] = search
        return params

    while True:
        params = make_params(offset)

        # 单次请求的重试（处理429/401/临时错误）
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = s.get(base_url, params=params, timeout=15)

                # 速率限制处理
                if resp.status_code == 429:
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after is not None:
                        time.sleep(float(retry_after))
                    else:
                        time.sleep(retry_delay * attempt)
                    if attempt < max_retries:
                        continue
                    else:
                        logger.info("get_datafields: 连续速率限制，提前结束分页")
                        return pd.DataFrame([])

                # 未授权/禁止，尝试获取最新会话
                if resp.status_code in (401, 403):
                    logger.info(f"get_datafields: {resp.status_code}，尝试获取最新会话后重试")
                    try:
                        # 使用SessionClient获取最新会话（SessionKeeper会自动维护）
                        logger.info(f"get_datafields: 获取最新会话...")
                        from sessions.session_client import get_session
                        new_session = get_session()
                        logger.info(f"get_datafields: SessionClient获取成功")
                        
                        # 重要：更新传入的session对象的属性
                        s.cookies.update(new_session.cookies)
                        s.headers.update(new_session.headers)
                        if hasattr(new_session, 'auth'):
                            s.auth = new_session.auth
                        logger.info("get_datafields: session更新成功")
                    except Exception as e:
                        logger.error(f"get_datafields: 会话更新失败({e})，使用SessionClient")
                        from sessions.session_client import get_session
                        new_session = get_session()
                        s.cookies.update(new_session.cookies)
                        s.headers.update(new_session.headers)
                        if hasattr(new_session, 'auth'):
                            s.auth = new_session.auth
                        logger.info(f"get_datafields: SessionClient恢复成功")
                    if attempt < max_retries:
                        continue
                    else:
                        return pd.DataFrame([])

                if resp.status_code != 200:
                    logger.info(f"get_datafields: HTTP {resp.status_code}: {resp.text[:200]}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 尝试解析JSON
                try:
                    data = resp.json()
                except Exception as e:
                    logger.info(f"get_datafields: JSON解析失败: {e}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 如果返回的是错误消息格式，尝试退避
                if isinstance(data, dict) and 'message' in data and 'results' not in data:
                    logger.info(f"get_datafields: API错误: {data.get('message')}")
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return pd.DataFrame([])

                # 正常处理结果
                results = data.get('results', [])
                aggregated_results.extend(results)

                # 控制请求节奏
                if call_interval > 0:
                    time.sleep(call_interval)

                # 结束条件：返回数量小于limit
                if len(results) < limit:
                    return pd.DataFrame(aggregated_results)

                # 准备下一页
                offset += limit
                # 可选的批次间延迟
                if burst_delay > 0:
                    time.sleep(burst_delay)
                # 跳出重试循环，进行下一页
                break

            except requests.RequestException as e:
                logger.info(f"get_datafields: 请求异常: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    return pd.DataFrame([])
            except Exception as e:
                logger.info(f"get_datafields: 未知异常: {e}")
                return pd.DataFrame([])


def process_datafields(df, data_type):
    """处理数据字段，支持空DataFrame的健壮处理"""
    # 检查DataFrame是否为空或缺少必要列
    if df.empty or 'type' not in df.columns or 'id' not in df.columns:
        logger.info(f"process_datafields: DataFrame为空或缺少必要列(type/id)，返回空字段列表")
        return []
    
    try:
        if data_type == "matrix":
            datafields = df[df['type'] == "MATRIX"]["id"].tolist()
        elif data_type == "vector":
            datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
        else:
            logger.info(f"process_datafields: 未知数据类型: {data_type}")
            return []

        tb_fields = []
        for field in datafields:
            tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)" % field)
        return tb_fields
        
    except Exception as e:
        logger.info(f"process_datafields: 处理{data_type}字段时发生异常: {e}")
        return []


def get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th, region, universe, delay,
               instrumentType, alpha_num, usage, tag: str = '', color_exclude='', s=None, end_date_time=None):
    """获取Alpha列表
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD) 
        end_date_time: 可选，结束日期的具体时间 (HH:MM:SS)，用于精确控制查询范围
                      如果不提供，默认使用 00:00:00
                      注意：服务器时间为上海时区，但查询时区为UTC-4，函数会自动转换
        其他参数: 保持原有含义
    """
    # color None, RED, YELLOW, GREEN, BLUE, PURPLE
    if s is None:
        from sessions.session_client import get_session
        s = get_session()
    
    # 处理end_date_time参数和时区转换
    if end_date_time is not None:
        from datetime import datetime, timedelta, timezone
        
        try:
            # 解析输入的上海时区时间 (UTC+8)
            end_datetime_str = f"{end_date} {end_date_time}"
            end_datetime_naive = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')
            
            # 创建上海时区的datetime对象 (UTC+8)
            shanghai_tz = timezone(timedelta(hours=8))
            end_datetime_shanghai = end_datetime_naive.replace(tzinfo=shanghai_tz)
            
            # 转换为UTC-4时区
            utc_minus_4_tz = timezone(timedelta(hours=-4))
            end_datetime_utc_minus_4 = end_datetime_shanghai.astimezone(utc_minus_4_tz)
            
            # 格式化为API需要的格式
            end_date_formatted = end_datetime_utc_minus_4.strftime('%Y-%m-%d')
            end_time_formatted = end_datetime_utc_minus_4.strftime('%H:%M:%S')
            
            logger.info(f"时区转换: 上海时间 {end_date} {end_date_time} -> UTC-4时间 {end_date_formatted} {end_time_formatted}")
            
            # 更新end_date为转换后的日期和时间
            end_date_query = f"{end_date_formatted}T{end_time_formatted}-04:00"
            
        except Exception as e:
            logger.warning(f"时区转换失败: {e}, 使用默认格式")
            end_date_query = f"{end_date}T00:00:00-04:00"
    else:
        # 保持原有行为
        end_date_query = f"{end_date}T00:00:00-04:00"
    alpha_list = []
    next_alphas = []
    decay_alphas = []
    check_alphas = []
    # 3E large 3C less
    # 正的
    i = 0
    while True:
        # 构建基础URL
        url_e = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                 f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                 f"&settings.region={region}&is.sharpe%3E={sharpe_th}")
        
        # 只有在fitness_th不为None时才添加fitness过滤
        if fitness_th is not None:
            url_e += f"&is.fitness%3E={fitness_th}"
        
        url_e += (f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                 f"T00:00:00-04:00&dateCreated%3C{end_date_query}&type=REGULAR&color!={color_exclude}&"
                 f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

        response = s.get(url_e)
        # logger.info(response.json())
        try:
            logger.info(i)
            i += 100
            count = response.json()["count"]
            logger.info("count: %d" % count)
            alpha_list.extend(response.json()["results"])
            if i >= count or i == 9900:
                break
            time.sleep(0.01)
        except Exception as e:
            logger.info(f"Failed to get alphas: {e}")
            i -= 100
            logger.info("%d finished re-login" % i)
            from sessions.session_client import get_session
            s = get_session()

    # 负的
    if usage != "submit":
        i = 0
        while True:
            url_c = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                     f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                     f"&settings.region={region}&is.sharpe%3C=-{sharpe_th}&is.fitness%3C=-{fitness_th}"
                     f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                     f"T00:00:00-04:00&dateCreated%3C{end_date_query}&type=REGULAR&color!={color_exclude}&"
                     f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

            response = s.get(url_c)
            # logger.info(response.json())
            try:
                count = response.json()["count"]
                if i >= count or i == 9900:
                    break
                alpha_list.extend(response.json()["results"])
                i += 100
            except Exception as e:
                logger.info(f"Failed to get alphas: {e}")
                logger.info("%d finished re-login" % i)
                from sessions.session_client import get_session
                s = get_session()

    # logger.info(alpha_list)
    if len(alpha_list) == 0:
        if usage != "submit":
            return {"next": [], "decay": []}
        else:
            return {"check": []}

    # logger.info(response.json())
    if usage != "submit":
        for j in range(len(alpha_list)):
            alpha_id = alpha_list[j]["id"]
            name = alpha_list[j]["name"]
            dateCreated = alpha_list[j]["dateCreated"]
            sharpe = alpha_list[j]["is"]["sharpe"]
            fitness = alpha_list[j]["is"]["fitness"]
            turnover = alpha_list[j]["is"]["turnover"]
            margin = alpha_list[j]["is"]["margin"]
            longCount = alpha_list[j]["is"]["longCount"]
            shortCount = alpha_list[j]["is"]["shortCount"]
            decay = alpha_list[j]["settings"]["decay"]
            exp = alpha_list[j]['regular']['code']
            region = alpha_list[j]["settings"]["region"]

            concentrated_weight = next(
                (check.get('value', 0) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "CONCENTRATED_WEIGHT"), 0)
            sub_universe_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "LOW_SUB_UNIVERSE_SHARPE"), 99)
            two_year_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if check["name"] == "LOW_2Y_SHARPE"),
                99)
            ladder_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "IS_LADDER_SHARPE"), 99)

            conditions = ((longCount > 100 or shortCount > 100) and
                          (concentrated_weight < 0.2) and
                        #   (abs(sub_universe_sharpe) > sharpe_th / 1.66) and
                          (abs(sub_universe_sharpe) > math.sqrt(1000/3000) * sharpe) and
                          (abs(two_year_sharpe) > sharpe_th) and
                          (abs(ladder_sharpe) > sharpe_th) and
                          (not (region == "CHN" and sharpe < 0))
                          )
            # if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
            if conditions:
                if sharpe < 0:
                    exp = "-%s" % exp
                rec = [alpha_id, exp, sharpe, turnover, fitness, margin, longCount, shortCount, dateCreated, decay]
                # logger.info(rec)
                if turnover > 0.7:
                    rec.append(decay * 4)
                    decay_alphas.append(rec)
                elif turnover > 0.6:
                    rec.append(decay * 3 + 3)
                    decay_alphas.append(rec)
                elif turnover > 0.5:
                    rec.append(decay * 3)
                    decay_alphas.append(rec)
                elif turnover > 0.4:
                    rec.append(decay * 2)
                    decay_alphas.append(rec)
                elif turnover > 0.35:
                    rec.append(decay + 4)
                    decay_alphas.append(rec)
                elif turnover > 0.3:
                    rec.append(decay + 2)
                    decay_alphas.append(rec)
                else:
                    next_alphas.append(rec)
        output_dict = {"next": next_alphas, "decay": decay_alphas}
        logger.info("count: %d" % (len(next_alphas) + len(decay_alphas)))
    else:
        for alpha_detail in alpha_list:
            id = alpha_detail["id"]
            type = alpha_detail["type"]
            author = alpha_detail["author"]
            instrumentType = alpha_detail["settings"]["instrumentType"]
            region = alpha_detail["settings"]["region"]
            universe = alpha_detail["settings"]["universe"]
            delay = alpha_detail["settings"]["delay"]
            decay = alpha_detail["settings"]["decay"]
            neutralization = alpha_detail["settings"]["neutralization"]
            truncation = alpha_detail["settings"]["truncation"]
            pasteurization = alpha_detail["settings"]["pasteurization"]
            unitHandling = alpha_detail["settings"]["unitHandling"]
            nanHandling = alpha_detail["settings"]["nanHandling"]
            language = alpha_detail["settings"]["language"]
            visualization = alpha_detail["settings"]["visualization"]
            code = alpha_detail["regular"]["code"]
            description = alpha_detail["regular"]["description"]
            operatorCount = alpha_detail["regular"]["operatorCount"]
            dateCreated = alpha_detail["dateCreated"]
            dateSubmitted = alpha_detail["dateSubmitted"]
            dateModified = alpha_detail["dateModified"]
            name = alpha_detail["name"]
            favorite = alpha_detail["favorite"]
            hidden = alpha_detail["hidden"]
            color = alpha_detail["color"]
            category = alpha_detail["category"]
            tags = alpha_detail["tags"]
            classifications = alpha_detail["classifications"]
            grade = alpha_detail["grade"]
            stage = alpha_detail["stage"]
            status = alpha_detail["status"]
            pnl = alpha_detail["is"]["pnl"]
            bookSize = alpha_detail["is"]["bookSize"]
            longCount = alpha_detail["is"]["longCount"]
            shortCount = alpha_detail["is"]["shortCount"]
            turnover = alpha_detail["is"]["turnover"]
            returns = alpha_detail["is"]["returns"]
            drawdown = alpha_detail["is"]["drawdown"]
            margin = alpha_detail["is"]["margin"]
            fitness = alpha_detail["is"]["fitness"]
            sharpe = alpha_detail["is"]["sharpe"]
            startDate = alpha_detail["is"]["startDate"]
            checks = alpha_detail["is"]["checks"]
            os = alpha_detail["os"]
            train = alpha_detail["train"]
            test = alpha_detail["test"]
            prod = alpha_detail["prod"]
            competitions = alpha_detail["competitions"]
            themes = alpha_detail["themes"]
            team = alpha_detail["team"]
            checks_df = pd.DataFrame(checks)
            pyramids = next(
                ([y['name'] for y in item['pyramids']] for item in checks if item['name'] == 'MATCHES_PYRAMID'), None)

            if any(checks_df["result"] == "FAIL"):
                # 最基础的项目不通过
                from .alpha_manager import set_alpha_properties
                set_alpha_properties(s, id, color='RED')
                continue
            else:
                # 通过了最基础的项目
                # 把全部的信息以字典的形式返回
                rec = {"id": id, "type": type, "author": author, "instrumentType": instrumentType, "region": region,
                       "universe": universe, "delay": delay, "decay": decay, "neutralization": neutralization,
                       "truncation": truncation, "pasteurization": pasteurization, "unitHandling": unitHandling,
                       "nanHandling": nanHandling, "language": language, "visualization": visualization, "code": code,
                       "description": description, "operatorCount": operatorCount, "dateCreated": dateCreated,
                       "dateSubmitted": dateSubmitted, "dateModified": dateModified, "name": name, "favorite": favorite,
                       "hidden": hidden, "color": color, "category": category, "tags": tags,
                       "classifications": classifications, "grade": grade, "stage": stage, "status": status, "pnl": pnl,
                       "bookSize": bookSize, "longCount": longCount, "shortCount": shortCount, "turnover": turnover,
                       "returns": returns, "drawdown": drawdown, "margin": margin, "fitness": fitness, "sharpe": sharpe,
                       "startDate": startDate, "checks": checks, "os": os, "train": train, "test": test, "prod": prod,
                       "competitions": competitions, "themes": themes, "team": team, "pyramids": pyramids}
                check_alphas.append(rec)
        output_dict = {"check": check_alphas}

    # 超过了限制
    if usage == 'submit' and count >= 9900:
        if len(output_dict['check']) < len(alpha_list):
            # 那么就再来一遍
            output_dict = get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th,
                                     region, universe, delay, instrumentType, alpha_num, usage, tag, color_exclude)
        else:
            raise Exception("Too many alphas to check!! over 10000, universe: %s, region: %s" % (universe, region))

    return output_dict
