#!/usr/bin/env python3
"""
独立会话保持器 - 专门负责登录认证和会话维护
作者：Assistant
日期：2025.09.06

核心功能：
1. 定期自动登录认证
2. 将有效cookies保存到数据库
3. 监控会话状态
4. 其他脚本只需从数据库读取cookies使用

设计理念：
- 单一职责：只负责认证和cookie维护
- 独立运行：作为后台服务独立运行
- 数据库存储：统一的cookie存储和分发
- 故障恢复：自动重试和错误处理
"""

import os
import sys
import time
import json
import requests
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import logging

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入项目模块
import importlib.util
config_path = os.path.join(os.path.dirname(__file__), 'config.py')
spec = importlib.util.spec_from_file_location("project_config", config_path)
config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_module)
ROOT_PATH = config_module.ROOT_PATH

from database.db_manager import FactorDatabaseManager

# 设置日志系统（参考correlation_checker_independent的方式）
import logging
import logging.handlers

def setup_session_keeper_logger():
    """设置SessionKeeper日志系统"""
    logger = logging.getLogger('session_keeper')
    logger.setLevel(logging.INFO)
    
    # 只在没有handler时添加，避免重复
    if not logger.handlers:
        # 检查是否作为子进程运行（通过检查stdout是否被重定向）
        is_subprocess = not sys.stdout.isatty()
        
        if is_subprocess:
            # 作为子进程运行，使用简单的StreamHandler输出到stdout
            # 这些输出会被父进程重定向到日志文件
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            logger.info(f"📝 SessionKeeper日志系统已启动 (子进程模式)")
            logger.info(f"  📤 输出重定向: 通过父进程管理")
            logger.info(f"  🆔 进程ID: {os.getpid()}")
            logger.info(f"  💾 编码: UTF-8")
        else:
            # 独立运行模式，创建自己的日志文件
            # 确保logs目录存在
            project_root = Path(__file__).parent.parent
            log_dir = project_root / 'logs'
            log_dir.mkdir(exist_ok=True)
            
            # 生成唯一的日志文件名（基于启动时间和PID）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"session_keeper_{timestamp}_{os.getpid()}.log"
            
            # 创建轮转文件处理器：最大10MB，保留3个文件
            file_handler = logging.handlers.RotatingFileHandler(
                str(log_file), 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,          # 保留3个备份文件
                encoding='utf-8'
            )
            
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
            
            logger.info(f"📝 SessionKeeper日志系统已启动 (独立模式)")
            logger.info(f"  📁 日志文件: {log_file}")
            logger.info(f"  🔄 轮转策略: 10MB/文件，保留3个备份")
            logger.info(f"  🆔 进程ID: {os.getpid()}")
            logger.info(f"  💾 编码: UTF-8")
    
    return logger

logger = setup_session_keeper_logger()


class SessionKeeper:
    """独立会话保持器"""
    
    def __init__(self):
        """初始化会话保持器"""
        self.db_manager = None
        self.current_session = None
        self.session_expires_at = None
        self.refresh_threshold = 30 * 60  # 提前30分钟刷新
        self.session_duration = 3 * 60 * 60  # 3小时
        
        # 运行状态
        self.running = False
        self.last_refresh_time = None
        self.refresh_count = 0
        self.error_count = 0
        
        # 初始化数据库
        self._init_database()
        
        logger.info("🔧 会话保持器初始化完成")
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.db_manager = FactorDatabaseManager()
            logger.info("✅ 数据库连接已建立")
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            raise Exception("数据库连接失败，会话保持器无法启动")
    
    def _load_user_credentials(self) -> tuple:
        """加载用户凭据 - 使用与原系统一致的解析方式"""
        user_info_file = Path(ROOT_PATH) / 'config' / 'user_info.txt'
        if not user_info_file.exists():
            raise Exception("用户配置文件不存在：config/user_info.txt")
        
        # 使用与machine_lib_ee.py相同的解析逻辑
        with open(user_info_file, 'r', encoding='utf-8') as f:
            data = f.read().strip().split('\n')
            data = {line.split(': ')[0]: line.split(': ')[1] for line in data if ': ' in line}
        
        if 'username' not in data or 'password' not in data:
            raise Exception("用户配置文件缺少username或password字段")
        
        # 去除引号 - 与原系统一致
        username = data['username'][1:-1]  # 去除首尾的单引号
        password = data['password'][1:-1]  # 去除首尾的单引号
        
        return username, password
    
    def create_new_session(self) -> requests.Session:
        """创建新的登录会话 - 使用与原系统一致的认证方式"""
        logger.info("🔐 开始创建新的登录会话...")
        
        try:
            username, password = self._load_user_credentials()
            
            # 创建会话 - 使用与machine_lib_ee.py相同的方式
            session = requests.Session()
            session.auth = (username, password)  # 使用Basic Auth
            
            # 执行登录 - 与原系统保持一致
            response = session.post('https://api.worldquantbrain.com/authentication')
            
            # 检查响应状态码 - 原系统期望201
            if response.status_code != 201:
                raise Exception(f"登录失败：HTTP {response.status_code} - {response.text}")
            
            # 检查登录错误 - 与原系统一致的错误检查
            content = response.content.decode('utf-8')
            if "INVALID_CREDENTIALS" in content:
                raise Exception("用户名或密码错误")
            
            # 设置过期时间
            current_time = time.time()
            self.session_expires_at = current_time + self.session_duration
            
            logger.info("✅ 新会话创建成功")
            logger.info(f"📅 会话过期时间: {datetime.fromtimestamp(self.session_expires_at)}")
            
            return session
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"登录请求失败: {e}")
        except Exception as e:
            raise Exception(f"登录失败: {e}")
    
    def save_session_to_database(self, session: requests.Session):
        """保存会话cookies到数据库"""
        try:
            session_data = {
                'cookies': dict(session.cookies),
                'headers': dict(session.headers),
                'expires_at': self.session_expires_at,
                'created_at': time.time(),
                'created_by': 'session_keeper',
                'refresh_count': self.refresh_count
            }
            
            # 保存到数据库
            success = self.db_manager.set_config(
                key='active_session_cookies',
                value=json.dumps(session_data, default=str),
                description='活跃会话Cookies（由SessionKeeper维护）'
            )
            
            if success:
                logger.info("💾 会话cookies已保存到数据库")
                return True
            else:
                logger.error("❌ 数据库保存失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 保存会话cookies失败: {e}")
            return False
    
    def refresh_session(self) -> bool:
        """刷新会话"""
        try:
            logger.info("🔄 开始刷新会话...")
            self.refresh_count += 1
            
            # 创建新会话
            new_session = self.create_new_session()
            
            # 保存到数据库
            if self.save_session_to_database(new_session):
                self.current_session = new_session
                self.last_refresh_time = time.time()
                self.error_count = 0  # 重置错误计数
                
                logger.info(f"✅ 会话刷新成功 (第{self.refresh_count}次)")
                return True
            else:
                logger.error("❌ 会话保存失败")
                return False
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ 会话刷新失败: {e} (错误次数: {self.error_count})")
            return False
    
    def is_session_expired(self) -> bool:
        """检查会话是否需要刷新"""
        if not self.session_expires_at:
            return True
        
        current_time = time.time()
        return current_time >= (self.session_expires_at - self.refresh_threshold)
    
    def get_session_status(self) -> Dict[str, Any]:
        """获取会话状态信息"""
        current_time = time.time()
        
        status = {
            'running': self.running,
            'refresh_count': self.refresh_count,
            'error_count': self.error_count,
            'last_refresh': datetime.fromtimestamp(self.last_refresh_time).strftime('%Y-%m-%d %H:%M:%S') if self.last_refresh_time else None,
            'expires_at': datetime.fromtimestamp(self.session_expires_at).strftime('%Y-%m-%d %H:%M:%S') if self.session_expires_at else None,
            'time_left_minutes': max(0, (self.session_expires_at - current_time) // 60) if self.session_expires_at else 0,
            'is_expired': self.is_session_expired(),
            'has_session': self.current_session is not None
        }
        
        return status
    
    def _check_existing_session(self) -> bool:
        """检查数据库中是否已有有效的会话cookies"""
        try:
            # 从数据库加载现有会话数据
            existing_data = self.db_manager.get_config('active_session_cookies')
            if not existing_data:
                logger.info("📋 数据库中未找到现有会话cookies")
                return False
            
            session_data = json.loads(existing_data)
            expires_at = session_data.get('expires_at')
            created_at = session_data.get('created_at', 0)
            
            if not expires_at:
                logger.info("⚠️ 现有会话缺少过期时间信息")
                return False
            
            current_time = time.time()
            time_left = expires_at - current_time
            
            # 检查是否过期（提前5分钟判断）
            if time_left <= 300:  # 5分钟
                logger.info(f"⏰ 现有会话即将过期或已过期 (剩余: {time_left//60:.0f}分钟)")
                return False
            
            # 会话仍然有效，设置相关状态
            self.session_expires_at = expires_at
            self.refresh_count = session_data.get('refresh_count', 0)
            
            logger.info(f"✅ 发现有效会话:")
            logger.info(f"   创建时间: {datetime.fromtimestamp(created_at)}")
            logger.info(f"   过期时间: {datetime.fromtimestamp(expires_at)}")
            logger.info(f"   剩余时间: {time_left//60:.0f}分钟")
            logger.info(f"   刷新次数: {self.refresh_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"⚠️ 检查现有会话时出错: {e}")
            return False
    
    def check_and_refresh_if_needed(self):
        """检查并在需要时刷新会话"""
        if self.is_session_expired():
            logger.info("⏰ 定时检查：会话即将过期，开始刷新...")
            self.refresh_session()
        else:
            logger.info("⏰ 定时检查：会话仍然有效")
    
    def start_keeper(self):
        """启动会话保持器"""
        logger.info("🚀 启动会话保持器...")
        self.running = True
        
        try:
            # 检查是否已有有效的会话cookies
            if self._check_existing_session():
                logger.info("✅ 发现有效的现有会话，无需重新登录")
            else:
                logger.info("🔄 未发现有效会话或会话已过期，开始初始登录...")
                if not self.refresh_session():
                    logger.error("❌ 初始登录失败，会话保持器启动失败")
                    return False
            
            logger.info("✅ 会话保持器启动成功")
            logger.info("📅 定时检查：每15分钟检查会话状态")
            
            # 主循环 - 使用简单的时间循环替代schedule
            last_check_time = time.time()
            check_interval = 15 * 60  # 15分钟
            status_interval = 60 * 60  # 1小时
            last_status_time = time.time()
            
            while self.running:
                try:
                    current_time = time.time()
                    
                    # 每15分钟检查一次会话
                    if current_time - last_check_time >= check_interval:
                        self.check_and_refresh_if_needed()
                        last_check_time = current_time
                    
                    # 每小时输出一次状态
                    if current_time - last_status_time >= status_interval:
                        status = self.get_session_status()
                        logger.info(f"📊 会话状态: {status}")
                        last_status_time = current_time
                    
                    # 每分钟检查一次循环条件
                    time.sleep(60)
                        
                except KeyboardInterrupt:
                    logger.info("⏹️ 收到停止信号...")
                    break
                except Exception as e:
                    logger.error(f"⚠️ 主循环异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 会话保持器启动失败: {e}")
            return False
        finally:
            self.running = False
            logger.info("🔻 会话保持器已停止")
    
    def stop_keeper(self):
        """停止会话保持器"""
        logger.info("🛑 正在停止会话保持器...")
        self.running = False
    
    def force_refresh(self):
        """强制刷新会话"""
        logger.info("🔄 强制刷新会话...")
        return self.refresh_session()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='独立会话保持器')
    parser.add_argument('--action', choices=['start', 'status', 'refresh'], 
                       default='start', help='操作类型')
    parser.add_argument('--daemon', action='store_true', help='后台运行模式')
    
    args = parser.parse_args()
    
    keeper = SessionKeeper()
    
    if args.action == 'start':
        if args.daemon:
            logger.info("🌙 后台模式启动...")
            # 这里可以添加守护进程逻辑
        
        try:
            keeper.start_keeper()
        except KeyboardInterrupt:
            logger.info("⏹️ 用户中断，正在停止...")
            keeper.stop_keeper()
    
    elif args.action == 'status':
        status = keeper.get_session_status()
        print(f"📊 会话保持器状态:")
        for key, value in status.items():
            print(f"   {key}: {value}")
    
    elif args.action == 'refresh':
        success = keeper.force_refresh()
        if success:
            print("✅ 强制刷新成功")
        else:
            print("❌ 强制刷新失败")


if __name__ == "__main__":
    main()
