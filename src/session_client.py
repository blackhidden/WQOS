#!/usr/bin/env python3
"""
会话客户端 - 从数据库获取有效cookies创建会话
作者：Assistant
日期：2025.09.06

核心功能：
1. 从数据库读取SessionKeeper维护的cookies
2. 创建requests.Session对象
3. 检查cookies有效性
4. 提供简单的接口给其他脚本使用

设计理念：
- 只读模式：不负责登录认证，只使用cookies
- 轻量级：最小化依赖和复杂度
- 容错性：cookies失效时提供友好的错误信息
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
from typing import Optional, Dict, Any
import logging

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from database.db_manager import FactorDatabaseManager

# 设置日志
logger = logging.getLogger(__name__)


class SessionClient:
    """会话客户端 - 只负责使用cookies，不负责认证"""
    
    def __init__(self):
        """初始化会话客户端"""
        self.db_manager = None
        self._init_database()
        
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.db_manager = FactorDatabaseManager()
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            raise Exception("无法连接数据库，请确保SessionKeeper正在运行")
    
    def load_cookies_from_database(self) -> Optional[Dict[str, Any]]:
        """从数据库加载cookies"""
        try:
            cookies_json = self.db_manager.get_config('active_session_cookies')
            if not cookies_json:
                return None
            
            return json.loads(cookies_json)
            
        except Exception as e:
            logger.error(f"❌ 加载cookies失败: {e}")
            return None
    
    def is_cookies_valid(self, cookies_data: Dict[str, Any]) -> bool:
        """检查cookies是否有效（未过期）"""
        try:
            expires_at = cookies_data.get('expires_at')
            if not expires_at:
                return False
            
            current_time = time.time()
            # 提前5分钟判断过期，给请求留出时间
            return current_time < (expires_at - 300)
            
        except Exception:
            return False
    
    def create_session_from_cookies(self, cookies_data: Dict[str, Any]) -> requests.Session:
        """从cookies数据创建会话对象"""
        try:
            session = requests.Session()
            
            # 设置cookies
            cookies = cookies_data.get('cookies', {})
            for name, value in cookies.items():
                session.cookies.set(name, value)
            
            # 设置headers
            headers = cookies_data.get('headers', {})
            if headers:
                session.headers.update(headers)
            
            return session
            
        except Exception as e:
            raise Exception(f"创建会话失败: {e}")
    
    def test_session(self, session: requests.Session) -> bool:
        """测试会话是否可用"""
        try:
            response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
            return response.status_code == 200
        except Exception:
            return False
    
    def get_session(self) -> requests.Session:
        """获取有效的会话对象（主要接口）"""
        try:
            # 1. 从数据库加载cookies
            cookies_data = self.load_cookies_from_database()
            if not cookies_data:
                raise Exception("未找到有效的会话cookies，请确保SessionKeeper正在运行")
            
            # 2. 检查cookies是否过期
            if not self.is_cookies_valid(cookies_data):
                raise Exception("会话cookies已过期，请等待SessionKeeper自动刷新")
            
            # 3. 创建会话对象
            session = self.create_session_from_cookies(cookies_data)
            
            # 4. 测试会话有效性
            if not self.test_session(session):
                raise Exception("会话cookies无效，请检查SessionKeeper状态")
            
            return session
            
        except Exception as e:
            logger.error(f"❌ 获取会话失败: {e}")
            raise e
    
    def get_cookies(self) -> Optional[requests.cookies.RequestsCookieJar]:
        """获取cookies对象（用于异步操作）"""
        try:
            session = self.get_session()
            return session.cookies
        except Exception:
            return None
    
    def get_session_info(self) -> Dict[str, Any]:
        """获取会话信息"""
        try:
            cookies_data = self.load_cookies_from_database()
            if not cookies_data:
                return {'status': 'no_cookies', 'message': '未找到会话cookies'}
            
            expires_at = cookies_data.get('expires_at')
            created_at = cookies_data.get('created_at')
            refresh_count = cookies_data.get('refresh_count', 0)
            
            current_time = time.time()
            time_left = expires_at - current_time if expires_at else 0
            
            return {
                'status': 'active' if self.is_cookies_valid(cookies_data) else 'expired',
                'created_at': datetime.fromtimestamp(created_at).strftime('%Y-%m-%d %H:%M:%S') if created_at else None,
                'expires_at': datetime.fromtimestamp(expires_at).strftime('%Y-%m-%d %H:%M:%S') if expires_at else None,
                'time_left_minutes': max(0, time_left // 60),
                'refresh_count': refresh_count,
                'created_by': cookies_data.get('created_by', 'unknown')
            }
            
        except Exception as e:
            return {'status': 'error', 'message': str(e)}


# 全局实例（单例模式）
_session_client = None

def get_session_client() -> SessionClient:
    """获取会话客户端实例"""
    global _session_client
    if _session_client is None:
        _session_client = SessionClient()
    return _session_client

def get_session() -> requests.Session:
    """便捷函数：获取有效会话"""
    return get_session_client().get_session()

def get_session_cookies():
    """便捷函数：获取会话cookies"""
    return get_session_client().get_cookies()

def get_session_info() -> Dict[str, Any]:
    """便捷函数：获取会话信息"""
    return get_session_client().get_session_info()


def main():
    """测试函数"""
    try:
        client = SessionClient()
        
        # 获取会话信息
        info = client.get_session_info()
        print(f"📊 会话信息: {json.dumps(info, indent=2, ensure_ascii=False)}")
        
        # 尝试获取会话
        session = client.get_session()
        print(f"✅ 会话获取成功: {type(session)}")
        
        # 测试API调用
        response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
        print(f"🔍 API测试: HTTP {response.status_code}")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")


if __name__ == "__main__":
    main()
