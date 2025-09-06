"""
作者：e.e.
微信：Enkidu_lin
日期：2025.08.24
"""

import time
import os
import sys
import logging
import numpy as np
import pandas as pd
from config import RECORDS_PATH, REGION_LIST
from machine_lib_ee import get_alphas, set_alpha_properties, batch_set_alpha_properties, load_user_config, load_digging_config
from session_client import get_session
from datetime import datetime, timedelta
from collections import defaultdict
import json
from alpha_record_manager import (
    is_alpha_in_records
)

# 导入数据库管理器
try:
    from database.db_manager import FactorDatabaseManager
except ImportError:
    # 如果导入失败，添加路径
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from database.db_manager import FactorDatabaseManager

# 导入相关性检查器（已移动到独立脚本）
# from correlation_checker_independent import IndependentCorrelationChecker


brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")

class OptimizedChecker:
    def __init__(self, mode=None, batch_size=50, sharpe_threshold=None, fitness_threshold=None, start_date=None):
        # 配置专用的logger
        self.logger = logging.getLogger('check_optimized')
        
        # 加载配置
        self.config = load_digging_config()
        
        # 优先使用传入参数，其次从配置文件读取，最后使用默认值
        self.mode = mode or self.config.get('mode', "PPAC")
        
        # 验证并设置用户指定的起始日期
        self.override_start_date = None
        if start_date:
            try:
                # 验证日期格式
                datetime.strptime(start_date, '%Y-%m-%d')
                self.override_start_date = start_date
                self.logger.info(f"📅 用户指定起始日期: {start_date}")
            except ValueError:
                self.logger.error(f"❌ 无效的日期格式: {start_date}，期望格式: YYYY-MM-DD")
                raise ValueError(f"无效的日期格式: {start_date}，期望格式: YYYY-MM-DD")
        
        # 根据模式设置默认阈值
        mode_defaults = {
            "CONSULTANT": {"sharpe": 1.58, "fitness": 1.0},
            "USER": {"sharpe": 1.25, "fitness": None},
            "PPAC": {"sharpe": 1.0, "fitness": None}
        }
        
        default_config = mode_defaults.get(self.mode, mode_defaults["PPAC"])
        
        # 阈值优先级：传入参数 > 配置文件 > 模式默认值
        self.sharpe_threshold = sharpe_threshold or self.config.get('sharpe_threshold', default_config["sharpe"])
        self.fitness_threshold = fitness_threshold or self.config.get('fitness_threshold', default_config["fitness"])
        
        self.batch_size = int(self.config.get('check_batch_size', batch_size))  # 批量处理大小
        
        # 从配置文件读取基本配置
        self.enable_smart_delay = self.config.get('enable_smart_delay', True)  # 启用智能延迟
        self.smart_retry_enabled = self.config.get('smart_retry_enabled', True)  # 智能重试开关
        self.exponential_backoff_max = int(self.config.get('exponential_backoff_max', 60))  # 指数退避最大延迟
        
        self.session = None
        
        self.api_delay = float(self.config.get('api_retry_delay', 1))
        self.max_retries = int(self.config.get('api_max_retries', 3))
        
        # 打印配置信息
        self.logger.info(f"✅ 简化版检查器配置:")
        self.logger.info(f"  🎯 检查模式: {self.mode}")
        self.logger.info(f"  📊 Sharpe阈值: {self.sharpe_threshold}")
        self.logger.info(f"  📈 Fitness阈值: {self.fitness_threshold if self.fitness_threshold is not None else '不使用'}")
        self.logger.info(f"  📊 批次大小: {self.batch_size}")
        self.logger.info(f"  🧠 智能延迟: {'启用' if self.enable_smart_delay else '禁用'}")
        self.logger.info(f"  🔄 智能重试: {'启用' if self.smart_retry_enabled else '禁用'}")
        self.logger.info(f"  ⏰ 指数退避上限: {self.exponential_backoff_max}s")
        
    def initialize_session(self):
        """初始化会话（使用统一会话管理器）"""
        if self.session is None:
            try:
                self.session = get_session()
                self.logger.info("✅ 会话初始化完成 (使用统一会话管理器)")
            except Exception as e:
                self.logger.error(f"❌ 统一会话管理器失败: {e}")
                # 使用SessionClient
                try:
                    from session_client import get_session
                    self.session = get_session()
                    self.logger.info("✅ 会话初始化完成 (使用SessionClient)")
                except Exception as e2:
                    self.logger.error(f"❌ SessionClient失败: {e2}")
                    self.logger.error("💡 请确保SessionKeeper正在运行并维护有效会话")
                    raise

    def batch_check_alphas(self, alphas, submitable_alpha_file):
        """批量检查Alpha - 流式处理模式"""
        self.logger.info(f"\n🔍 开始流式检查 {len(alphas)} 个Alpha...")
        
        # 初始化数据库管理器
        db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        db = FactorDatabaseManager(db_path)
        
        # 1. 过滤已检查的Alpha
        # 注意：颜色状态过滤已在API获取阶段统一处理，这里只需处理检查记录
        valid_alphas = []
        skipped_checked = 0
        
        for alpha in alphas:
            alpha_id = alpha['id']
            tags = alpha['tags']
            tag = tags[0] if len(tags) == 1 else ''
            
            # 检查是否已经检查过 - 使用alpha_record_manager
            if is_alpha_in_records(alpha_id, tag, "checked"):
                skipped_checked += 1
                self.logger.info(f"  ⏭️  Alpha {alpha_id}: 已检查过，跳过")
                continue
                
            valid_alphas.append(alpha)
            self.logger.info(f"  📝 Alpha {alpha_id}: 待检查 (标签: {tag})")
        
        self.logger.info(f"\n📊 业务逻辑过滤统计:")
        self.logger.info(f"  ⏭️  已检查跳过: {skipped_checked} 个")
        self.logger.info(f"  ✅ 需要检查: {len(valid_alphas)} 个")
        self.logger.info(f"  💡 颜色状态过滤已在API获取阶段统一处理")
        
        if not valid_alphas:
            self.logger.info(f"📝 没有需要检查的Alpha")
            return
        
        # 2. 流式处理：分批获取相关性并立即处理结果
        self.logger.info(f"\n🔄 开始流式处理模式...")
        batch_num = 0
        
        # 按批次处理Alpha
        for i in range(0, len(valid_alphas), self.batch_size):
            batch_alphas = valid_alphas[i:i + self.batch_size]
            batch_num += 1
            
            try:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"📦 流式处理批次 {batch_num}: {len(batch_alphas)} 个Alpha")
                self.logger.info(f"📋 批次进度: {i+1}-{min(i+len(batch_alphas), len(valid_alphas))}/{len(valid_alphas)}")
                self.logger.info(f"{'='*60}")
                
                # 处理当前批次（不再依赖返回值）
                self.process_alpha_batch(batch_alphas, submitable_alpha_file, batch_num)
                
                self.logger.info(f"\n📊 批次 {batch_num} 处理完成")
                
                # 批次间休息
                if i + self.batch_size < len(valid_alphas):
                    batch_delay = 3.0
                    self.logger.info(f"\n⏸️  批次间休息...")
                    for remaining in range(int(batch_delay), 0, -1):
                        progress = f"[批次{batch_num+1}准备中] 休息 {remaining} 秒..."
                        print(f"  {progress}", end='\r', flush=True)
                        time.sleep(1)
                    print(f"                                          ", end='\r')
                
            except KeyboardInterrupt:
                self.logger.info(f"\n⚠️  用户中断处理，已处理 {batch_num} 个批次")
                raise
            except Exception as e:
                self.logger.error(f"❌ 批次 {batch_num} 处理异常: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # 最终统计
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"🎉 流式检查完成:")
        self.logger.info(f"  📊 处理批次: {batch_num} 个")
        self.logger.info(f"  📊 处理Alpha: {len(valid_alphas)} 个")
        self.logger.info(f"  🔄 API失败的Alpha将在下轮循环中重试")
        self.logger.info(f"{'='*60}")

    def process_alpha_batch(self, batch_alphas, submitable_alpha_file, batch_num):
        """处理单个Alpha批次的相关性检查和结果保存"""
        self.logger.info(f"🔍 第三阶段: 处理批次 {batch_num} ({len(batch_alphas)} 个Alpha)...")
        
        # 初始化数据库管理器
        db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
        db = FactorDatabaseManager(db_path)
        
        submitable_alphas = []
        # 简化后只有可提交的Alpha，相关性检测由submit处理
        
        for alpha in batch_alphas:
            alpha_id = alpha['id']
            # 设置默认相关性值，表示需要submit检查
            alpha['self_corr'] = 999.0  # 表示需要检查自相关性
            alpha['prod_corr'] = 999.0  # 表示需要检查生产相关性
            alpha['aggressive_mode'] = False  # 不再使用此字段判断
            # 设置color为YELLOW，表示等待相关性检查
            alpha['color'] = 'YELLOW'
            submitable_alphas.append(alpha)
        
        # 立即保存结果
        self.logger.info(f"\n💾 第四阶段: 保存批次结果...")
        self.logger.info(f"  ✅ 可提交: {len(submitable_alphas)} 个")
        self.logger.info(f"  📝 注意: 相关性检测已委托给submit处理")
        
        # 1. 更新可提交Alpha数据库
        if submitable_alphas:
            self.logger.info(f"    📊 准备更新可提交Alpha数据库...")
            
            # 过滤已存在的因子，避免重复插入
            new_submitable_alphas = []
            skipped_existing = 0
            
            for alpha in submitable_alphas:
                alpha_id = alpha['id']
                if db.is_alpha_submitable(alpha_id):
                    skipped_existing += 1
                    self.logger.info(f"    ⏭️  Alpha {alpha_id}: 已存在于可提交数据库中，跳过插入")
                else:
                    new_submitable_alphas.append(alpha)
            
            self.logger.info(f"    📊 重复检查结果: {len(submitable_alphas)} 个检查通过，{skipped_existing} 个已存在，{len(new_submitable_alphas)} 个需要插入")
            
            # 使用数据库事务保证数据一致性
            try:
                if new_submitable_alphas:
                    # 转换为DataFrame格式
                    submitable_df = pd.DataFrame(new_submitable_alphas)
                    
                    # 批量添加到数据库
                    success_count = 0
                    for _, row in submitable_df.iterrows():
                        alpha_data = row.to_dict()
                        
                        # 字段名映射：API驼峰命名 -> 数据库下划线命名
                        field_mapping = {
                            'id': 'alpha_id',
                            'instrumentType': 'instrument_type',
                            'unitHandling': 'unit_handling', 
                            'nanHandling': 'nan_handling',
                            'operatorCount': 'operator_count',
                            'dateCreated': 'date_created',
                            'dateSubmitted': 'date_submitted', 
                            'dateModified': 'date_modified',
                            'bookSize': 'book_size',
                            'longCount': 'long_count',
                            'shortCount': 'short_count',
                            'startDate': 'start_date'
                        }
                        
                        # 应用字段名映射
                        for old_name, new_name in field_mapping.items():
                            if old_name in alpha_data:
                                alpha_data[new_name] = alpha_data.pop(old_name)
                        
                        # 只保留核心字段，避免存储过多复杂数据
                        core_fields = {
                            'alpha_id', 'type', 'author', 'instrument_type', 'region', 'universe',
                            'delay', 'decay', 'neutralization', 'truncation', 'pasteurization',
                            'unit_handling', 'nan_handling', 'language', 'visualization', 'code',
                            'description', 'operator_count', 'date_created', 'date_submitted',
                            'date_modified', 'name', 'favorite', 'hidden', 'color', 'category',
                            'tags', 'grade', 'stage', 'status', 'pnl', 'book_size', 'long_count',
                            'short_count', 'turnover', 'returns', 'drawdown', 'margin', 'fitness',
                            'sharpe', 'start_date', 'aggressive_mode', 'self_corr', 'prod_corr'
                        }
                        
                        # 过滤字段
                        filtered_data = {}
                        for key, value in alpha_data.items():
                            if key in core_fields:
                                filtered_data[key] = value
                        
                        # 数据类型转换：复杂对象转换为JSON字符串
                        complex_fields = ['tags']  # 只处理tags字段，其他复杂字段已过滤掉
                        for field in complex_fields:
                            if field in filtered_data and filtered_data[field] is not None:
                                if isinstance(filtered_data[field], (list, dict)):
                                    filtered_data[field] = json.dumps(filtered_data[field], ensure_ascii=False)
                                elif not isinstance(filtered_data[field], str):
                                    filtered_data[field] = str(filtered_data[field])
                        
                        # 处理None值和布尔值
                        for key in list(filtered_data.keys()):
                            if filtered_data[key] is None:
                                filtered_data[key] = ''
                            elif isinstance(filtered_data[key], bool):
                                filtered_data[key] = 1 if filtered_data[key] else 0
                            elif isinstance(filtered_data[key], (int, float)):
                                filtered_data[key] = filtered_data[key]
                            else:
                                filtered_data[key] = str(filtered_data[key])
                        
                        if db.add_submitable_alpha(filtered_data):
                            success_count += 1
                    
                    if success_count == len(new_submitable_alphas):
                        self.logger.info(f"    📊 数据库更新成功: 添加了 {success_count} 个新的可提交Alpha")
                        
                        # 获取当前数据库中的总数
                        current_df = db.get_submitable_alphas()
                        total_count = len(current_df)
                        self.logger.info(f"    📊 数据库中当前共有 {total_count} 个可提交Alpha")
                    else:
                        self.logger.info(f"    ⚠️  部分数据库更新失败: 成功 {success_count}/{len(new_submitable_alphas)}")
                else:
                    self.logger.info(f"    📝 所有因子都已存在于数据库中，无需插入新数据")
                    
            except Exception as e:
                self.logger.info(f"    ❌ 数据库操作异常: {e}")
                self.logger.info(f"    🔄 跳过本批次可提交Alpha保存，等待下轮重试")
            
            # 批量设置Alpha为YELLOW（包括新插入的和已存在的因子）
            # 对于已存在的因子，也需要标记为YELLOW，因为可能之前标记失败了
            yellow_ids = [alpha['id'] for alpha in submitable_alphas]  # 所有通过检查的因子
            if skipped_existing > 0:
                self.logger.info(f"    🎨 设置 {len(yellow_ids)} 个Alpha为YELLOW (包括 {len(new_submitable_alphas)} 个新插入的和 {skipped_existing} 个已存在的)...")
            else:
                self.logger.info(f"    🎨 设置 {len(yellow_ids)} 个Alpha为YELLOW...")
            self.batch_set_alpha_properties(yellow_ids, color='YELLOW')
        
    def batch_set_alpha_properties(self, alpha_ids, **properties):
        """批量设置Alpha属性 - 智能选择API"""
        if not alpha_ids:
            return 0, 0
            
        self.logger.info(f"      🎨 开始批量设置 {len(alpha_ids)} 个Alpha属性: {properties}")
        
        # 检查是否只设置颜色（批量API只支持颜色设置）
        is_color_only = len(properties) == 1 and 'color' in properties
        
        if is_color_only:
            self.logger.info(f"      📋 检测到仅设置颜色，使用批量API...")
            
            # 准备批量API数据格式
            alpha_data = [{"id": alpha_id, "color": properties['color']} for alpha_id in alpha_ids]
            
            try:
                result = batch_set_alpha_properties(self.session, alpha_data, max_batch_size=50)
                
                success_count = result["success"]
                failed_count = result["failed"]
                
                self.logger.info(f"      📊 批量设置完成:")
                self.logger.info(f"        ✅ 成功: {success_count}/{len(alpha_ids)} 个")
                
                if failed_count > 0:
                    self.logger.info(f"        ❌ 失败: {failed_count}/{len(alpha_ids)} 个")
                
                # 显示详细信息
                for detail in result["details"]:
                    self.logger.info(f"        📋 {detail}")
                
                return success_count, failed_count
                
            except Exception as e:
                self.logger.error(f"      ❌ 批量API异常: {e}")
                self.logger.info(f"      🔄 回退到单个设置模式...")
                
                # 回退到单个设置
                return self._fallback_individual_set(alpha_ids, **properties)
        else:
            # 设置了其他属性（name、tags等），必须使用单个API
            self.logger.info(f"      📋 检测到复杂属性设置，使用单个API...")
            return self._fallback_individual_set(alpha_ids, **properties)
    
    def _fallback_individual_set(self, alpha_ids, **properties):
        """回退方案：使用单个API设置"""
        success_count = 0
        failed_count = 0
        
        for i, alpha_id in enumerate(alpha_ids):
            try:
                success = self._set_alpha_properties_with_retry(alpha_id, **properties)
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                
                if i % 10 == 0 or i == len(alpha_ids) - 1:
                    self.logger.info(f"      ✅ 单个设置进度: {i+1}/{len(alpha_ids)} (成功: {success_count}, 失败: {failed_count})")
                
                # 适当延迟避免API限制
                if i < len(alpha_ids) - 1:
                    time.sleep(1.0)
                    
            except Exception as e:
                failed_count += 1
                self.logger.error(f"      ❌ Alpha {alpha_id} 设置异常: {e}")
        
        return success_count, failed_count
    def _set_alpha_properties_with_retry(self, alpha_id, **properties):
        """带重试机制的Alpha属性设置
        正确处理HTTP 429速率限制：通过等待而不是重新登录
        """
        max_retries = 3
        base_delay = 60  # 基础等待时间60秒
        
        for attempt in range(max_retries + 1):
            try:
                # 尝试设置Alpha属性
                result = set_alpha_properties(self.session, alpha_id, **properties)
                
                # 检查是否为速率限制
                if result == 'RATE_LIMITED':
                    if attempt < max_retries:
                        # 计算等待时间：指数退避策略
                        wait_time = base_delay * (2 ** attempt)  # 60s, 120s, 240s
                        self.logger.info(f"      ⏰ Alpha {alpha_id} 遇到速率限制 (尝试 {attempt + 1}/{max_retries + 1})")
                        self.logger.info(f"      ⏳ 等待 {wait_time} 秒后重试...")
                        
                        # 倒计时等待
                        for remaining in range(int(wait_time), 0, -1):
                            minutes, seconds = divmod(remaining, 60)
                            if minutes > 0:
                                time_str = f"{minutes}:{seconds:02d}"
                            else:
                                time_str = f"{seconds}s"
                            print(f"      ⏳ 速率限制等待: {time_str}", end='\r', flush=True)
                            time.sleep(1)
                        print(f"                                    ", end='\r')  # 清除倒计时
                        
                        self.logger.info(f"      🔄 等待完成，重试 Alpha {alpha_id}...")
                        continue
                    else:
                        self.logger.warning(f"      ❌ Alpha {alpha_id} 达到最大重试次数，仍然速率限制")
                        return False
                
                # 返回成功或失败结果
                return result == True
                
            except Exception as e:
                if attempt < max_retries:
                    self.logger.warning(f"      ⚠️  Alpha {alpha_id} 设置异常 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    self.logger.info(f"      ⏳ 等待 30 秒后重试...")
                    time.sleep(30)
                    continue
                else:
                    self.logger.error(f"      ❌ Alpha {alpha_id} 设置失败，已达最大重试次数: {e}")
                    return False
        
        return False

    def run_check_cycle(self):
        """运行一次检查周期"""
        try:
            self.logger.info(f"🚀 初始化检查环境...")
            self.initialize_session()
            
            start_date_file = os.path.join(RECORDS_PATH, 'start_date.txt')
            submitable_alpha_file = os.path.join(RECORDS_PATH, 'submitable_alpha.csv')
            
            # 生成检查时间段 - 优先级：命令行参数 > 数据库 > 文件 > 默认值
            db_path = os.path.join(os.path.dirname(RECORDS_PATH), 'database', 'factors.db')
            db = FactorDatabaseManager(db_path)
            
            if self.override_start_date:
                # 用户指定了起始日期，使用此日期并更新到数据库
                start_date = self.override_start_date
                self.logger.info(f"📅 使用用户指定开始日期: {start_date}")
                try:
                    # 将用户指定的日期写入数据库
                    db.set_system_config('start_date', start_date)
                    self.logger.info(f"📅 已将用户指定日期更新到数据库")
                    # 清除用户指定日期，避免影响后续循环
                    self.override_start_date = None
                    self.logger.info(f"📅 已清除用户指定日期，后续循环将使用数据库日期")
                except Exception as e:
                    self.logger.warning(f"📅 更新数据库失败: {e}")
            else:
                # 用户未指定日期，从数据库或文件读取
                try:
                    start_date = db.get_system_config('start_date')
                    if start_date:
                        self.logger.info(f"📅 从数据库读取开始日期: {start_date}")
                    else:
                        # 如果数据库中没有，尝试从文件读取
                        try:
                            with open(start_date_file, 'r') as f:
                                start_date = f.read().strip()
                            self.logger.info(f"📅 从文件读取开始日期: {start_date}")
                            # 将读取的日期写入数据库
                            db.set_system_config('start_date', start_date)
                        except FileNotFoundError:
                            start_date = '2024-10-07'
                            self.logger.info(f"📅 使用默认开始日期: {start_date}")
                            # 将默认日期写入数据库
                            db.set_system_config('start_date', start_date)
                except Exception as e:
                    self.logger.warning(f"📅 数据库读取失败: {e}, 使用文件读取")
                    try:
                        with open(start_date_file, 'r') as f:
                            start_date = f.read().strip()
                        self.logger.info(f"📅 从文件读取开始日期: {start_date}")
                    except FileNotFoundError:
                        start_date = '2024-10-07'
                        self.logger.info(f"📅 使用默认开始日期: {start_date}")
            
            end_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            
            self.logger.info(f"📅 检查时间段: {start_date} 到 {end_date}")
            
            # 使用动态配置的阈值
            sh_th = self.sharpe_threshold
            self.logger.info(f"📊 检查模式: {self.mode}, 夏普比阈值: {sh_th}")
            if self.fitness_threshold is not None:
                self.logger.info(f"📈 Fitness阈值: {self.fitness_threshold}")
            else:
                self.logger.info(f"📈 不使用Fitness阈值过滤")
            
            total_checked = 0
            total_regions = len(REGION_LIST)
            
            for region_idx, region in enumerate(REGION_LIST):
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"🌍 [{region_idx+1}/{total_regions}] 检查地区: {region}")
                self.logger.info(f"{'='*80}")
                
                region_start_time = time.time()
                
                # 获取需要检查的Alpha
                self.logger.info(f"🔍 获取地区 {region} 的Alpha列表...")
                # fitness阈值：如果不使用则传入None，让get_alphas函数不添加fitness过滤
                fitness_th = self.fitness_threshold  # None表示不使用fitness过滤
                # 获取Alpha（排除RED，然后统一过滤其他已处理状态）
                need_to_check = get_alphas(
                    start_date, end_date, sh_th, fitness_th, 10, 10,
                    region=region, universe="", delay='', instrumentType='',
                    alpha_num=9999, usage="submit", tag='', color_exclude='RED', s=self.session
                )
                
                # 统一过滤所有已处理状态的Alpha（GREEN、YELLOW、BLUE、PURPLE）
                if need_to_check['check']:
                    original_count = len(need_to_check['check'])
                    
                    # 统计各种状态的Alpha数量（用于日志）
                    green_count = sum(1 for alpha in need_to_check['check'] if alpha.get('color') == 'GREEN')
                    yellow_count = sum(1 for alpha in need_to_check['check'] if alpha.get('color') == 'YELLOW')
                    blue_count = sum(1 for alpha in need_to_check['check'] if alpha.get('color') == 'BLUE')
                    purple_count = sum(1 for alpha in need_to_check['check'] if alpha.get('color') == 'PURPLE')
                    
                    # 统一过滤已处理状态
                    need_to_check['check'] = [alpha for alpha in need_to_check['check'] 
                                             if alpha.get('color') not in ['GREEN', 'YELLOW', 'BLUE', 'PURPLE']]
                    filtered_count = len(need_to_check['check'])
                    
                    # 显示过滤统计
                    if original_count > filtered_count:
                        self.logger.info(f"  🔽 API过滤统计 ({original_count} → {filtered_count}):")
                        if green_count > 0:
                            self.logger.info(f"    🟢 过滤掉 {green_count} 个GREEN状态的Alpha")
                        if yellow_count > 0:
                            self.logger.info(f"    🟡 过滤掉 {yellow_count} 个YELLOW状态的Alpha")
                        if blue_count > 0:
                            self.logger.info(f"    🔵 过滤掉 {blue_count} 个BLUE状态的Alpha")
                        if purple_count > 0:
                            self.logger.info(f"    🟣 过滤掉 {purple_count} 个PURPLE状态的Alpha（厂字型）")
                        self.logger.info(f"  📊 剩余待检查Alpha: {filtered_count} 个")
                
                if not need_to_check['check']:
                    self.logger.info(f"  📝 {region} 地区没有需要检查的Alpha")
                    continue
                
                region_alpha_count = len(need_to_check['check'])
                total_checked += region_alpha_count
                self.logger.info(f"  📊 {region} 地区找到 {region_alpha_count} 个Alpha需要检查")
                
                # 显示Alpha概览
                alpha_tags = defaultdict(int)
                for alpha in need_to_check['check']:
                    tags = alpha.get('tags', [])
                    tag = tags[0] if tags else 'Unknown'
                    alpha_tags[tag] += 1
                
                self.logger.info(f"  📋 Alpha标签分布:")
                for tag, count in alpha_tags.items():
                    self.logger.info(f"    {tag}: {count} 个")
                
                # 批量检查
                self.batch_check_alphas(need_to_check['check'], submitable_alpha_file)
                
                region_time = time.time() - region_start_time
                self.logger.info(f"  ⏱️  地区 {region} 处理完成，耗时: {region_time:.2f}s")
                
            # 如果start_date距离当前日期超过2天，则更新start_date
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            if (datetime.now().date() - start_date_obj).days > 2:
                new_date = (start_date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
                
                # 数据库版本：更新start_date到数据库
                try:
                    db.set_system_config('start_date', new_date)
                    self.logger.info(f"📅 数据库更新开始日期为: {new_date} (区域内向前推进1天)")
                except Exception as e:
                    self.logger.info(f"📅 数据库更新失败: {e}，回退到文件更新")
                    # 如果数据库更新失败，回退到文件写入
                    try:
                        with open(start_date_file, 'w') as f:
                            f.write(new_date)
                        self.logger.info(f"📅 文件更新开始日期为: {new_date} (区域内向前推进1天)")
                    except Exception as fe:
                        self.logger.info(f"📅 文件更新也失败: {fe}")
                
                # 更新start_date以便下一个区域使用
                start_date = new_date
            
            self.logger.info(f"\n📊 本次检查周期总结:")
            self.logger.info(f"  🌍 检查地区: {total_regions} 个")
            self.logger.info(f"  📋 处理Alpha: {total_checked} 个")
            
        except Exception as e:
            self.logger.error(f"❌ 检查周期异常: {e}")
            import traceback
            traceback.print_exc()

def main():
    """主函数"""
    import argparse
    import logging.handlers
    from datetime import datetime
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='优化版Alpha检查器')
    parser.add_argument('--mode', choices=['CONSULTANT', 'USER', 'PPAC'], 
                       help='检查模式: CONSULTANT(sharpe≥1.58,fitness≥1), USER(sharpe≥1.25,无fitness), PPAC(sharpe≥1.0,fitness可选)')
    parser.add_argument('--sharpe-threshold', type=float, 
                       help='Sharpe阈值 (覆盖模式默认值)')
    parser.add_argument('--fitness-threshold', type=float, 
                       help='Fitness阈值 (可选，仅在指定时使用)')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='批处理大小 (默认: 50)')
    parser.add_argument('--start-date', type=str,
                       help='起始检查日期 (格式: YYYY-MM-DD, 如: 2025-01-01)')
    
    args = parser.parse_args()
    
    # 配置专用的logger，避免与session_manager的logging冲突
    logger = logging.getLogger('check_optimized')
    logger.setLevel(logging.INFO)
    
    # 只在没有handler时添加，避免重复
    if not logger.handlers:
        # 检查是否作为子进程运行（通过检查stdout是否被重定向）
        import sys
        is_subprocess = not sys.stdout.isatty()
        
        if is_subprocess:
            # 作为子进程运行，使用简单的StreamHandler输出到stdout
            # 这些输出会被父进程重定向到日志文件
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            logger.info(f"📝 优化检查器日志系统已启动 (子进程模式)")
            logger.info(f"  📤 输出重定向: 通过父进程管理")
            logger.info(f"  🆔 进程ID: {os.getpid()}")
            logger.info(f"  💾 编码: UTF-8")
        else:
            # 独立运行模式，创建自己的日志文件
            # 确保logs目录存在
            log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
            os.makedirs(log_dir, exist_ok=True)
            
            # 生成唯一的日志文件名（基于启动时间和PID）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"check_optimized_{timestamp}_{os.getpid()}.log")
            
            # 创建轮转文件处理器：最大10MB，保留3个文件
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,          # 保留3个备份文件
                encoding='utf-8'
            )
            
            # 重写doRollover方法，在轮转时记录信息
            original_doRollover = file_handler.doRollover
            def doRollover_with_log():
                original_doRollover()
                # 轮转后记录信息（使用新文件）
                file_handler.emit(file_handler.makeRecord(
                    logger.name, logging.INFO, __file__, 0, 
                    f"🔄 日志文件已轮转，当前文件: {os.path.basename(log_file)}", 
                    (), None
                ))
            file_handler.doRollover = doRollover_with_log
            
            # 控制台处理器（独立模式下也添加控制台输出）
            console_handler = logging.StreamHandler(sys.stdout)
            
            # 设置格式
            file_formatter = logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            console_formatter = logging.Formatter('%(message)s')
            file_handler.setFormatter(file_formatter)
            console_handler.setFormatter(console_formatter)
            
            # 添加处理器
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            
            # 防止日志传播到root logger，避免重复输出
            logger.propagate = False
            
            logger.info(f"📝 优化检查器日志系统已启动 (独立模式)")
            logger.info(f"  📁 日志文件: {log_file}")
            logger.info(f"  🔄 轮转设置: 3个文件 × 10MB")
            logger.info(f"  💾 编码: UTF-8")
        
        # 设置防止日志传播（两种模式都需要）
        logger.propagate = False
    
    logger.info("🚀 启动优化版Alpha检查器...")
    
    # 创建检查器实例 - 优先使用命令行参数
    checker = OptimizedChecker(
        mode=args.mode,
        batch_size=args.batch_size,
        sharpe_threshold=args.sharpe_threshold,
        fitness_threshold=args.fitness_threshold,
        start_date=args.start_date
    )
    
    # 持续运行
    while True:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🕒 开始新的检查周期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"{'='*60}")
            
            start_time = time.time()
            
            checker.run_check_cycle()
            
            end_time = time.time()
            
            if end_time - start_time > 300:
                logger.info(f"🕒 检查周期完成，耗时: {end_time - start_time:.2f}s，超过五分钟，跳过五分钟等待")
                continue
            
            logger.info(f"\n✅ 检查周期完成，{300 - (end_time - start_time):.2f}秒后开始新的检查周期...")
            # 5分钟倒计时等待
            time.sleep(300 - (end_time - start_time))
            logger.info(f"🚀 开始新的检查周期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except KeyboardInterrupt:
            logger.info("\n👋 收到中断信号，正在退出...")
            break
        except Exception as e:
            logger.error(f"❌ 主循环异常: {e}")
            logger.info("⏳ 等待100秒后重试...")
            # 100秒倒计时等待
            for remaining in range(100, 0, -1):
                minutes, seconds = divmod(remaining, 60)
                print(f"⏳ 异常恢复倒计时: {minutes:02d}:{seconds:02d}", end='\r', flush=True)
                time.sleep(1)
            print(f"                                         ", end='\r')  # 清除进度显示
            logger.info(f"🔄 重新启动检查器...")

if __name__ == '__main__':
    main() 