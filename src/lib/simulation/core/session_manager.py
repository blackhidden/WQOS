"""
统一会话管理器 (Unified Session Manager)
作者：e.e.
日期：2025.09.08

提供统一的会话管理功能，包括：
- 异步会话初始化
- 自动会话刷新
- 错误重试机制
- 连接池管理
"""

import asyncio
import aiohttp
import yarl
from typing import Optional, Dict, Any


class UnifiedSessionManager:
    """统一会话管理器 - 整合单模拟和多模拟的会话管理逻辑"""
    
    def __init__(self):
        self.session = None
        self.cookies = None
        self.logger = None
        self._initialized = False
    
    @property
    def cookie_jar(self):
        """访问底层session的cookie_jar"""
        if self.session:
            return self.session.cookie_jar
        return None
        
    def set_logger(self, logger):
        """设置日志记录器"""
        self.logger = logger
        
    async def initialize(self) -> bool:
        """初始化异步会话
        
        Returns:
            bool: 初始化是否成功
        """
        if self._initialized and self.session and not self.session.closed:
            return True
            
        try:
            if self.logger:
                self.logger.info("🔄 初始化统一会话管理器...")
            
            # 获取会话管理器和cookies
            from sessions.session_client import get_session as get_session_manager, get_session_cookies
            
            unified_session_manager = get_session_manager()
            self.cookies = get_session_cookies()
            
            if not self.cookies:
                raise Exception("无法获取有效的会话cookies")
            
            # 创建异步会话
            cookie_jar = aiohttp.CookieJar()
            
            # 将 requests cookies 转换为 aiohttp cookies
            cookie_dict = {}
            for cookie_name, cookie_value in self.cookies.items():
                cookie_dict[cookie_name] = cookie_value
            
            # 更新 cookie jar
            if cookie_dict:
                cookie_jar.update_cookies(cookie_dict, response_url=yarl.URL("https://api.worldquantbrain.com"))
            
            # 创建会话
            timeout = aiohttp.ClientTimeout(total=120, connect=30)
            self.session = aiohttp.ClientSession(
                cookie_jar=cookie_jar,
                timeout=timeout,
                connector=aiohttp.TCPConnector(limit=50, limit_per_host=20)
            )
            
            self._initialized = True
            
            if self.logger:
                self.logger.info("✅ 统一会话管理器初始化成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 统一会话管理器初始化失败: {e}")
            return False
    
    async def refresh_session_cookies(self) -> bool:
        """刷新会话cookies
        
        Returns:
            bool: 刷新是否成功
        """
        try:
            if self.logger:
                self.logger.info("🔄 刷新会话cookies...")
            
            from sessions.session_client import get_session
            
            # 直接获取最新有效会话（SessionKeeper自动维护）
            new_session = get_session()
            if not new_session or not new_session.cookies:
                raise Exception("获取新会话失败")
            
            # 更新现有会话的cookies
            if self.session and not self.session.closed:
                cookie_dict = {}
                for cookie in new_session.cookies:
                    cookie_dict[cookie.name] = cookie.value
                
                if cookie_dict:
                    import yarl
                    self.session.cookie_jar.clear()  # 清空现有cookies
                    self.session.cookie_jar.update_cookies(
                        cookie_dict, response_url=yarl.URL("https://api.worldquantbrain.com")
                    )
                    if self.logger:
                        self.logger.info(f"✅ aiohttp cookies更新完成，包含{len(cookie_dict)}个cookie")
                else:
                    raise Exception("新会话cookies为空")
            else:
                if self.logger:
                    self.logger.warning("⚠️ aiohttp会话无效，需要重新初始化")
                # 重新初始化会话
                await self.initialize()
            
            if self.logger:
                self.logger.info("✅ 会话cookies刷新成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 会话cookies刷新失败: {e}")
            return False
    
    def request_with_retry(self, method: str, url: str, max_retries: int = 3, **kwargs):
        """带重试机制的HTTP请求 - 返回上下文管理器
        
        Args:
            method: HTTP方法
            url: 请求URL
            max_retries: 最大重试次数
            **kwargs: 其他请求参数
            
        Returns:
            RequestContextManager: 请求上下文管理器
        """
        return RequestContextManager(self, method, url, max_retries, **kwargs)
    
    def post(self, url: str, **kwargs):
        """POST请求方法 - 兼容原有的session_manager.post()调用
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数（json, data等）
            
        Returns:
            RequestContextManager: 请求上下文管理器（用于async with）
        """
        return self.request_with_retry('POST', url, **kwargs)
    
    def get(self, url: str, **kwargs):
        """GET请求方法 - 兼容原有的session_manager.get()调用
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            RequestContextManager: 请求上下文管理器（用于async with）
        """
        return self.request_with_retry('GET', url, **kwargs)
    
    def patch(self, url: str, **kwargs):
        """PATCH请求方法 - 兼容原有的session_manager.patch()调用
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数（json, data等）
            
        Returns:
            RequestContextManager: 请求上下文管理器（用于async with）
        """
        return self.request_with_retry('PATCH', url, **kwargs)
    
    async def close(self):
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            if self.logger:
                self.logger.info("🔒 统一会话管理器已关闭")
        self._initialized = False


class RequestContextManager:
    """HTTP请求上下文管理器 - 支持重试和会话刷新"""
    
    def __init__(self, session_manager, method: str, url: str, max_retries: int = 3, **kwargs):
        self.session_manager = session_manager
        self.method = method
        self.url = url
        self.max_retries = max_retries
        self.kwargs = kwargs
        self.response = None
        
    async def __aenter__(self):
        """进入上下文管理器 - 执行带重试的请求"""
        for attempt in range(self.max_retries + 1):
            try:
                if not self.session_manager.session or self.session_manager.session.closed:
                    await self.session_manager.initialize()
                
                self.response = await self.session_manager.session.request(
                    self.method, self.url, **self.kwargs
                )
                
                # 检查是否需要刷新会话
                if self.response.status in [401, 403]:
                    if self.session_manager.logger:
                        self.session_manager.logger.warning(f"⚠️ HTTP {self.response.status} - 尝试刷新会话")
                    
                    self.response.close()  # 关闭当前响应
                    
                    if await self.session_manager.refresh_session_cookies():
                        # 重试请求
                        continue
                    else:
                        if self.session_manager.logger:
                            self.session_manager.logger.error("❌ 会话刷新失败，无法继续请求")
                        raise Exception("会话刷新失败")
                
                return self.response
                    
            except asyncio.TimeoutError:
                if self.session_manager.logger:
                    self.session_manager.logger.warning(f"⏱️ 请求超时 (尝试 {attempt + 1}/{self.max_retries + 1})")
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # 指数退避
                
            except Exception as e:
                if self.session_manager.logger:
                    self.session_manager.logger.warning(f"❌ 请求异常 (尝试 {attempt + 1}/{self.max_retries + 1}): {e}")
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(1)
        
        raise Exception("请求重试次数超限")
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器 - 关闭响应"""
        if self.response and not self.response.closed:
            self.response.close()
    
    async def post_json(self, url: str, data: Dict[Any, Any], **kwargs) -> Optional[Dict]:
        """POST JSON请求的便捷方法
        
        Args:
            url: 请求URL
            data: JSON数据
            **kwargs: 其他请求参数
            
        Returns:
            Optional[Dict]: 响应JSON数据或None
        """
        try:
            async with await self.request_with_retry('POST', url, json=data, **kwargs) as response:
                if response:
                    return await response.json()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ POST JSON请求失败: {e}")
        return None
    
    async def get_json(self, url: str, **kwargs) -> Optional[Dict]:
        """GET JSON请求的便捷方法
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            Optional[Dict]: 响应JSON数据或None
        """
        try:
            async with await self.request_with_retry('GET', url, **kwargs) as response:
                if response:
                    return await response.json()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ GET JSON请求失败: {e}")
        return None
    
    async def close(self):
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            if self.logger:
                self.logger.info("🔒 统一会话管理器已关闭")
        self._initialized = False
