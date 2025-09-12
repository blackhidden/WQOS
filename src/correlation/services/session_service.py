"""
会话服务 - 负责API会话管理和请求处理
"""

import time
import requests
from sessions.session_client import get_session


class SessionService:
    """会话服务"""
    
    def __init__(self, config_manager, logger):
        """初始化会话服务"""
        self.config = config_manager
        self.logger = logger
        self.session = None
    
    def initialize_session(self):
        """初始化会话（使用统一会话管理器）"""
        if self.session is None:
            try:
                self.session = get_session()
                self.logger.info(f"✅ 会话初始化完成 (使用SessionClient)")
                return True
            except Exception as e:
                self.logger.error(f"❌ SessionClient失败: {e}")
                self.logger.error(f"💡 请确保SessionKeeper正在运行并维护有效会话")
                return False
        return True
    
    def wait_get(self, url: str, max_retries: int = 10, message: str = None) -> requests.Response:
        """发送带有重试机制的GET请求"""
        retries = 0
        while retries < max_retries:
            while True:
                response = self.session.get(url)
                retry_after = response.headers.get("Retry-After", "0")
                try:
                    retry_after_num = float(retry_after)
                    if retry_after_num == 0:
                        break
                    self.logger.info(f"⏰ API限制，等待 {retry_after_num} 秒...") if message is None else self.logger.info(f"⏰ {message}，等待 {retry_after_num} 秒...")
                    time.sleep(retry_after_num)
                except (ValueError, TypeError):
                    # 如果Retry-After头无法解析，跳出内循环
                    break
            
            if response.status_code < 400:
                break
            elif response.status_code in (401, 403):
                # 专门处理认证错误 - 获取最新会话
                self.logger.warning(f"⚠️ 认证失败 (状态码: {response.status_code})，尝试获取最新会话...")
                try:
                    # 使用SessionClient获取最新会话（SessionKeeper会自动维护）
                    self.logger.info(f"🔄 获取最新会话...")
                    new_session = get_session()
                    
                    # 更新当前会话
                    self.session = new_session
                    self.logger.info(f"✅ 会话更新成功，继续请求...")
                    # 不增加重试计数，直接重试
                    continue
                    
                except Exception as e:
                    self.logger.error(f"❌ 会话更新过程中出现错误: {e}")
                    wait_time = 2 ** retries
                    self.logger.warning(f"⏰ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    retries += 1
            else:
                wait_time = 2 ** retries
                self.logger.warning(f"⚠️ 请求失败 (状态码: {response.status_code})，{wait_time}秒后重试...")
                time.sleep(wait_time)
                retries += 1
        
        return response
