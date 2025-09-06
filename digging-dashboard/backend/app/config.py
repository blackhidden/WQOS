"""
应用配置管理
"""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import validator


def _auto_detect_project_root() -> str:
    """自动检测项目根目录
    
    检测逻辑：
    1. 从当前文件向上查找包含特征文件的目录
    2. 特征文件：src/machine_lib_ee.py, config/digging_config.txt 等
    3. 支持开发环境和部署环境的不同路径结构
    """
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)
    
    # 从当前目录向上查找项目根目录
    max_depth = 10
    for _ in range(max_depth):
        # 检查是否包含项目特征文件/目录
        indicators = [
            os.path.join(current_dir, "src", "machine_lib_ee.py"),
            os.path.join(current_dir, "config", "digging_config.txt"),
            os.path.join(current_dir, "database"),
            os.path.join(current_dir, "src", "unified_digging_scheduler.py")
        ]
        
        # 如果找到任何一个特征文件，认为这是项目根目录
        if any(os.path.exists(indicator) for indicator in indicators):
            print(f"🎯 自动检测到项目根目录: {current_dir}")
            return current_dir
        
        # 向上一级目录
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # 已经到达文件系统根目录
            break
        current_dir = parent_dir
    
    # 如果自动检测失败，返回一个合理的默认值
    fallback_path = "/app"  # Docker容器的标准路径
    print(f"⚠️ 无法自动检测项目根目录，使用默认路径: {fallback_path}")
    return fallback_path


class Settings(BaseSettings):
    """应用设置"""
    
    # 应用基础配置
    app_name: str = "WorldQuant Alpha Digging Dashboard"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # 服务器配置
    host: str = "0.0.0.0"
    port: int = int(os.environ.get("PORT", "8088"))
    
    # 数据库配置
    database_url: str = "sqlite:///./dashboard.db"
    
    # JWT认证配置
    secret_key: str = os.environ.get("SECRET_KEY", "WQ-Alpha-Digging-Dashboard-2025-Secret-Key-Production")
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480  # 8小时
    
    # CORS配置
    allowed_origins: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ]
    
    # 挖掘脚本配置 - 动态路径检测
    project_root: str = os.environ.get('PROJECT_ROOT', None) or _auto_detect_project_root()
    
    @property
    def digging_script_path(self) -> str:
        return os.path.join(self.project_root, "src", "unified_digging_scheduler.py")
    
    @property
    def digging_config_path(self) -> str:
        return os.path.join(self.project_root, "config", "digging_config.txt")
    
    @property
    def original_db_path(self) -> str:
        return os.path.join(self.project_root, "database", "factors.db")
    
    # 日志配置
    log_level: str = "INFO"
    log_file: str = "dashboard.log"
    
    # 安全配置
    max_login_attempts: int = 5
    login_attempt_window: int = 300  # 5分钟
    
    # 进程监控配置
    process_check_interval: int = 5  # 秒
    log_tail_lines: int = 1000
    
    @validator("allowed_origins", pre=True)
    def assemble_cors_origins(cls, v):
        if isinstance(v, str):
            return [i.strip() for i in v.split(",")]
        return v
    
    @validator("secret_key")
    def validate_secret_key(cls, v):
        if v == "your-super-secret-jwt-key-change-this-in-production":
            print("⚠️  WARNING: 使用默认密钥，请在生产环境中修改SECRET_KEY")
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# 全局设置实例
settings = Settings()


def get_settings() -> Settings:
    """获取应用设置"""
    return settings
