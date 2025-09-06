"""
作者：e.e. 
微信：Enkidu_lin
日期：2025.08.24
"""

import os
import sys
import time
import json
import requests
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import threading
import logging

# 导入项目模块
from config import RECORDS_PATH, ROOT_PATH

# 导入数据库管理器
try:
    from database.db_manager import FactorDatabaseManager
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from database.db_manager import FactorDatabaseManager

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UnifiedSessionManager:
    """统一会话管理器
    
    功能：
    1. 统一登录和会话管理
    2. 自动会话刷新（每3小时）
    3. Cookie/会话数据持久化存储
    4. 多组件共享同一会话
    5. 会话状态监控和恢复
    """
    
    # 单例模式
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        
        # 会话配置
        self.session_duration = 3 * 60 * 60  # 3小时（秒）
        self.refresh_threshold = 30 * 60     # 提前30分钟刷新
        
        # 存储配置
        self.use_database = True  # 优先使用数据库存储
        self.fallback_to_file = True  # 数据库失败时使用文件
        
        # 会话数据
        self.session = None
        self.session_start_time = None
        self.session_expires_at = None
        self.user_info = None
        
        # 存储路径
        self.session_file = Path(RECORDS_PATH) / 'session_data.pickle'
        self.cookie_file = Path(RECORDS_PATH) / 'session_cookies.json'
        
        # 数据库管理器
        self.db_manager = None
        self._init_database()
        
        # 后台刷新线程
        self._refresh_thread = None
        self._stop_refresh = threading.Event()
        
        print("✅ 统一会话管理器初始化完成")
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            db_path = os.path.join(ROOT_PATH, 'database', 'factors.db')
            self.db_manager = FactorDatabaseManager(db_path)
            print("✅ 数据库连接初始化成功")
        except Exception as e:
            print(f"⚠️ 数据库连接初始化失败: {e}")
            self.use_database = False
    
    def _load_user_credentials(self) -> tuple[str, str]:
        """加载用户凭据"""
        try:
            config_path = os.path.join(ROOT_PATH, 'config', 'user_info.txt')
            with open(config_path, 'r') as f:
                data = {}
                for line in f:
                    line = line.strip()
                    if ': ' in line:
                        key, value = line.split(': ', 1)
                        data[key.strip()] = value.strip().strip("'\"")
                
                username = data.get('username', '')
                password = data.get('password', '')
                
                if not username or not password:
                    raise ValueError("用户名或密码为空")
                
                return username, password
                
        except Exception as e:
            raise Exception(f"加载用户凭据失败: {e}")
    
    def _create_new_session(self) -> requests.Session:
        """创建新的登录会话"""
        print("🔐 正在创建新的登录会话...")
        
        try:
            username, password = self._load_user_credentials()
            
            # 创建会话
            session = requests.Session()
            session.auth = (username, password)
            
            # 执行登录
            response = session.post('https://api.worldquantbrain.com/authentication')
            
            if response.status_code != 201:
                raise Exception(f"登录失败: HTTP {response.status_code} - {response.text}")
            
            # 检查登录错误
            content = response.content.decode('utf-8')
            if "INVALID_CREDENTIALS" in content:
                raise Exception("用户名或密码错误")
            
            # 设置会话时间
            current_time = time.time()
            self.session_start_time = current_time
            self.session_expires_at = current_time + self.session_duration
            
            print("✅ 新会话创建成功")
            logger.info(f"Session created at {datetime.fromtimestamp(current_time)}")
            
            return session
            
        except Exception as e:
            print(f"❌ 会话创建失败: {e}")
            raise
    
    def _save_session_data(self):
        """保存会话数据到持久化存储"""
        if not self.session:
            return
        
        session_data = {
            'cookies': dict(self.session.cookies),
            'headers': dict(self.session.headers),
            'auth': self.session.auth,
            'session_start_time': self.session_start_time,
            'session_expires_at': self.session_expires_at,
            'user_info': self.user_info,
            'saved_at': time.time()
        }
        
        # 优先保存到数据库
        if self.use_database and self.db_manager:
            try:
                self._save_to_database(session_data)
                print("💾 会话数据已保存到数据库")
                return
            except Exception as e:
                print(f"⚠️ 保存到数据库失败: {e}")
                if not self.fallback_to_file:
                    return
        
        # 备选：保存到文件
        if self.fallback_to_file:
            try:
                self._save_to_file(session_data)
                print("💾 会话数据已保存到文件")
            except Exception as e:
                print(f"❌ 保存会话数据失败: {e}")
    
    def _save_to_database(self, session_data: Dict[str, Any]):
        """保存会话数据到数据库"""
        # 将会话数据序列化为JSON
        json_data = json.dumps(session_data, default=str)
        
        # 保存到system_config表
        success = self.db_manager.set_config(
            key='unified_session_data',
            value=json_data,
            description='统一会话管理器的会话数据'
        )
        
        if not success:
            raise Exception("数据库保存失败")
    
    def _save_to_file(self, session_data: Dict[str, Any]):
        """保存会话数据到文件"""
        # 创建目录
        self.session_file.parent.mkdir(exist_ok=True)
        
        # 保存pickle文件
        with open(self.session_file, 'wb') as f:
            pickle.dump(session_data, f)
        
        # 单独保存cookies（JSON格式，便于调试）
        cookies_data = {
            'cookies': session_data['cookies'],
            'saved_at': session_data['saved_at'],
            'expires_at': session_data['session_expires_at']
        }
        
        with open(self.cookie_file, 'w') as f:
            json.dump(cookies_data, f, indent=2, default=str)
    
    def _load_session_data(self) -> Optional[Dict[str, Any]]:
        """从持久化存储加载会话数据"""
        # 优先从数据库加载
        if self.use_database and self.db_manager:
            try:
                data = self._load_from_database()
                if data:
                    print("📂 从数据库加载会话数据")
                    return data
            except Exception as e:
                print(f"⚠️ 从数据库加载失败: {e}")
        
        # 备选：从文件加载
        if self.fallback_to_file:
            try:
                data = self._load_from_file()
                if data:
                    print("📂 从文件加载会话数据")
                    return data
            except Exception as e:
                print(f"⚠️ 从文件加载失败: {e}")
        
        return None
    
    def _load_from_database(self) -> Optional[Dict[str, Any]]:
        """从数据库加载会话数据"""
        json_str = self.db_manager.get_config('unified_session_data')
        if not json_str:
            return None
        
        return json.loads(json_str)
    
    def _load_from_file(self) -> Optional[Dict[str, Any]]:
        """从文件加载会话数据"""
        if not self.session_file.exists():
            return None
        
        with open(self.session_file, 'rb') as f:
            return pickle.load(f)
    
    def _restore_session_from_data(self, session_data: Dict[str, Any]) -> bool:
        """从保存的数据恢复会话"""
        try:
            # 创建新会话对象
            session = requests.Session()
            
            # 恢复cookies
            session.cookies.update(session_data['cookies'])
            
            # 恢复headers
            session.headers.update(session_data['headers'])
            
            # 恢复认证信息
            if session_data.get('auth'):
                session.auth = tuple(session_data['auth'])
            
            # 恢复时间信息
            self.session_start_time = session_data['session_start_time']
            self.session_expires_at = session_data['session_expires_at']
            self.user_info = session_data.get('user_info')
            
            # 测试会话是否有效
            if self._test_session(session):
                self.session = session
                print("✅ 会话恢复成功")
                return True
            else:
                print("⚠️ 保存的会话已失效")
                return False
                
        except Exception as e:
            print(f"❌ 会话恢复失败: {e}")
            return False
    
    def _test_session(self, session: requests.Session) -> bool:
        """测试会话是否有效"""
        try:
            # 使用一个轻量级的API端点测试
            response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
            return response.status_code == 200
        except Exception:
            return False
    
    def _is_session_expired(self) -> bool:
        """检查会话是否过期或即将过期"""
        if not self.session_expires_at:
            return True
        
        current_time = time.time()
        # 提前刷新，避免在使用过程中过期
        return current_time >= (self.session_expires_at - self.refresh_threshold)
    
    def _start_background_refresh(self):
        """启动后台自动刷新线程"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            return
        
        def refresh_loop():
            while not self._stop_refresh.is_set():
                try:
                    # 检查是否需要刷新
                    if self._is_session_expired():
                        print("🔄 会话即将过期，开始自动刷新...")
                        self.refresh_session()
                    
                    # 每5分钟检查一次
                    self._stop_refresh.wait(300)
                    
                except Exception as e:
                    print(f"❌ 后台刷新异常: {e}")
                    # 出错时等待更长时间再重试
                    self._stop_refresh.wait(600)
        
        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        self._refresh_thread.start()
        print("🔄 后台会话刷新线程已启动")
    
    def get_session(self) -> requests.Session:
        """获取有效的会话对象
        
        这是所有组件应该使用的主要接口
        """
        # 如果当前会话存在且未过期，直接返回
        if self.session and not self._is_session_expired():
            return self.session
        
        # 尝试从持久化存储恢复会话
        if not self.session:
            session_data = self._load_session_data()
            if session_data and not self._is_data_expired(session_data):
                if self._restore_session_from_data(session_data):
                    self._start_background_refresh()
                    return self.session
        
        # 需要刷新会话，使用分布式锁机制
        if self.refresh_session():
            self._start_background_refresh()
            return self.session
        
        # 刷新失败，抛出异常
        raise Exception("无法获取有效会话：刷新失败")
    
    def _is_data_expired(self, session_data: Dict[str, Any]) -> bool:
        """检查保存的会话数据是否过期"""
        expires_at = session_data.get('session_expires_at')
        if not expires_at:
            return True
        
        current_time = time.time()
        return current_time >= expires_at
    
    def _acquire_refresh_lock(self, timeout_seconds: int = 60) -> bool:
        """获取会话刷新分布式锁
        
        Args:
            timeout_seconds: 锁超时时间（秒）
            
        Returns:
            bool: 是否成功获取锁
        """
        if not self.use_database or not self.db_manager:
            # 没有数据库时使用本地锁（单进程内有效）
            return True
        
        try:
            current_time = time.time()
            lock_expires_at = current_time + timeout_seconds
            process_id = f"{os.getpid()}_{threading.current_thread().ident}"
            
            lock_data = {
                'process_id': process_id,
                'acquired_at': current_time,
                'expires_at': lock_expires_at,
                'status': 'refreshing'
            }
            
            # 检查是否已有锁
            existing_lock = self.db_manager.get_config('session_refresh_lock')
            if existing_lock:
                try:
                    lock_info = json.loads(existing_lock)
                    # 检查锁是否过期
                    if lock_info.get('expires_at', 0) > current_time:
                        # 锁未过期，检查是否是当前进程
                        if lock_info.get('process_id') == process_id:
                            print(f"🔒 当前进程已持有刷新锁")
                            return True
                        else:
                            print(f"🔒 其他进程正在刷新会话 (PID: {lock_info.get('process_id')})")
                            return False
                except (json.JSONDecodeError, KeyError):
                    # 锁数据损坏，清除并重新获取
                    pass
            
            # 尝试获取锁
            success = self.db_manager.set_config(
                key='session_refresh_lock',
                value=json.dumps(lock_data, default=str),
                description=f'会话刷新锁 - PID: {process_id}'
            )
            
            if success:
                print(f"🔒 成功获取会话刷新锁 (PID: {process_id})")
                return True
            else:
                print(f"🔒 获取会话刷新锁失败")
                return False
                
        except Exception as e:
            print(f"❌ 获取刷新锁异常: {e}")
            return False
    
    def _release_refresh_lock(self):
        """释放会话刷新分布式锁"""
        if not self.use_database or not self.db_manager:
            return
        
        try:
            process_id = f"{os.getpid()}_{threading.current_thread().ident}"
            
            # 检查锁是否属于当前进程
            existing_lock = self.db_manager.get_config('session_refresh_lock')
            if existing_lock:
                try:
                    lock_info = json.loads(existing_lock)
                    if lock_info.get('process_id') != process_id:
                        print(f"⚠️ 尝试释放不属于当前进程的锁")
                        return
                except (json.JSONDecodeError, KeyError):
                    pass
            
            # 清除锁
            success = self.db_manager.set_config(
                key='session_refresh_lock',
                value='',
                description='已释放的会话刷新锁'
            )
            
            if success:
                print(f"🔓 成功释放会话刷新锁 (PID: {process_id})")
            else:
                print(f"⚠️ 释放会话刷新锁失败")
                
        except Exception as e:
            print(f"❌ 释放刷新锁异常: {e}")
    
    def _wait_for_other_refresh(self, max_wait_seconds: int = 120) -> bool:
        """等待其他进程完成会话刷新
        
        Args:
            max_wait_seconds: 最大等待时间（秒）
            
        Returns:
            bool: 是否检测到刷新完成
        """
        if not self.use_database or not self.db_manager:
            return False
        
        print("⏳ 等待其他进程完成会话刷新...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                # 检查锁状态
                existing_lock = self.db_manager.get_config('session_refresh_lock')
                if not existing_lock:
                    print("✅ 其他进程已完成刷新")
                    return True
                
                lock_info = json.loads(existing_lock)
                if lock_info.get('expires_at', 0) <= time.time():
                    print("✅ 刷新锁已过期，认为刷新完成")
                    return True
                
                # 检查会话是否已更新
                session_data = self._load_session_data()
                if session_data and not self._is_data_expired(session_data):
                    print("✅ 检测到新的有效会话")
                    return True
                
                # 等待一段时间后重新检查
                time.sleep(5)
                
            except Exception as e:
                print(f"⚠️ 等待刷新时出现异常: {e}")
                time.sleep(5)
        
        print("⏰ 等待刷新超时")
        return False
    
    def refresh_session(self) -> bool:
        """手动刷新会话（带分布式锁）"""
        # 尝试获取刷新锁
        if not self._acquire_refresh_lock():
            # 未获取到锁，等待其他进程完成刷新
            if self._wait_for_other_refresh():
                # 其他进程已完成刷新，尝试加载新会话
                try:
                    session_data = self._load_session_data()
                    if session_data and not self._is_data_expired(session_data):
                        if self._restore_session_from_data(session_data):
                            print("✅ 使用其他进程刷新的会话")
                            return True
                except Exception as e:
                    print(f"⚠️ 加载其他进程刷新的会话失败: {e}")
            
            # 等待超时或加载失败，尝试强制获取锁
            print("⚠️ 等待其他进程刷新超时，尝试强制刷新")
            if not self._acquire_refresh_lock(timeout_seconds=30):
                print("❌ 无法获取刷新锁，刷新失败")
                return False
        
        # 获取到锁，执行刷新
        try:
            print("🔄 正在刷新会话...")
            old_session = self.session
            
            # 创建新会话
            self.session = self._create_new_session()
            self._save_session_data()
            
            # 关闭旧会话
            if old_session:
                try:
                    old_session.close()
                except:
                    pass
            
            print("✅ 会话刷新成功")
            return True
            
        except Exception as e:
            print(f"❌ 会话刷新失败: {e}")
            return False
        finally:
            # 无论成功还是失败都要释放锁
            self._release_refresh_lock()
    
    def get_session_info(self) -> Dict[str, Any]:
        """获取会话状态信息"""
        if not self.session_start_time:
            return {'status': 'no_session'}
        
        current_time = time.time()
        time_left = self.session_expires_at - current_time if self.session_expires_at else 0
        
        # 获取刷新锁状态
        lock_info = self._get_refresh_lock_info()
        
        return {
            'status': 'active' if self.session else 'inactive',
            'start_time': datetime.fromtimestamp(self.session_start_time).strftime('%Y-%m-%d %H:%M:%S'),
            'expires_at': datetime.fromtimestamp(self.session_expires_at).strftime('%Y-%m-%d %H:%M:%S') if self.session_expires_at else None,
            'time_left_minutes': max(0, time_left // 60),
            'is_expired': self._is_session_expired(),
            'user_info': self.user_info,
            'refresh_lock': lock_info
        }
    
    def _get_refresh_lock_info(self) -> Dict[str, Any]:
        """获取刷新锁状态信息"""
        if not self.use_database or not self.db_manager:
            return {'available': True, 'reason': 'no_database'}
        
        try:
            existing_lock = self.db_manager.get_config('session_refresh_lock')
            if not existing_lock:
                return {'available': True, 'reason': 'no_lock'}
            
            lock_info = json.loads(existing_lock)
            current_time = time.time()
            
            if lock_info.get('expires_at', 0) <= current_time:
                return {'available': True, 'reason': 'lock_expired'}
            
            return {
                'available': False,
                'process_id': lock_info.get('process_id'),
                'acquired_at': datetime.fromtimestamp(lock_info.get('acquired_at', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'expires_at': datetime.fromtimestamp(lock_info.get('expires_at', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'time_left_seconds': max(0, lock_info.get('expires_at', 0) - current_time)
            }
            
        except Exception as e:
            return {'available': False, 'error': str(e)}
    
    def invalidate_session(self):
        """手动失效当前会话"""
        print("🗑️ 手动失效会话...")
        
        if self.session:
            try:
                self.session.close()
            except:
                pass
            self.session = None
        
        # 停止后台刷新
        if self._refresh_thread:
            self._stop_refresh.set()
        
        # 清除持久化数据
        self._clear_stored_session()
        
        # 重置状态
        self.session_start_time = None
        self.session_expires_at = None
        self.user_info = None
        
        print("✅ 会话已失效")
    
    def _clear_stored_session(self):
        """清除持久化的会话数据"""
        # 清除数据库中的会话数据
        if self.use_database and self.db_manager:
            try:
                self.db_manager.set_config('unified_session_data', '', '已清除的会话数据')
                # 同时清除刷新锁
                self.db_manager.set_config('session_refresh_lock', '', '已清除的刷新锁')
            except:
                pass
        
        # 清除文件中的会话数据
        try:
            if self.session_file.exists():
                self.session_file.unlink()
            if self.cookie_file.exists():
                self.cookie_file.unlink()
        except:
            pass
    
    def __del__(self):
        """析构函数：停止后台线程"""
        if hasattr(self, '_stop_refresh'):
            self._stop_refresh.set()


# 全局单例实例
_session_manager = None

def get_session_manager() -> UnifiedSessionManager:
    """获取全局会话管理器实例"""
    global _session_manager
    if _session_manager is None:
        _session_manager = UnifiedSessionManager()
    return _session_manager

def get_session() -> requests.Session:
    """便捷函数：获取有效的会话对象"""
    return get_session_manager().get_session()

def refresh_session() -> bool:
    """便捷函数：刷新会话"""
    return get_session_manager().refresh_session()

def get_session_info() -> Dict[str, Any]:
    """便捷函数：获取会话状态信息"""
    return get_session_manager().get_session_info()

def get_session_cookies() -> Optional[requests.cookies.RequestsCookieJar]:
    """便捷函数：获取当前有效会话的cookies
    
    主要用于异步操作需要复用cookies的场景
    """
    try:
        session = get_session()
        return session.cookies if session else None
    except Exception:
        return None

def invalidate_session():
    """便捷函数：失效当前会话"""
    get_session_manager().invalidate_session()


# 向后兼容的login函数
def login() -> requests.Session:
    """向后兼容的login函数
    
    现在使用统一会话管理器
    """
    return get_session()


if __name__ == "__main__":
    # 测试代码
    print("🧪 测试统一会话管理器...")
    
    try:
        # 获取会话管理器
        manager = get_session_manager()
        
        # 获取会话
        session = manager.get_session()
        print(f"✅ 会话获取成功: {type(session)}")
        
        # 显示会话信息
        info = manager.get_session_info()
        print(f"📊 会话信息: {info}")
        
        # 测试会话是否有效
        response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
        print(f"🌐 API测试: HTTP {response.status_code}")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
