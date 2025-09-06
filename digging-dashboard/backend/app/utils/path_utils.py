"""
项目路径检测工具
"""

import os
import sys
from pathlib import Path


def detect_project_root() -> str:
    """
    自动检测项目根目录
    
    检测逻辑：
    1. 检查环境变量 PROJECT_ROOT
    2. 从当前文件向上查找包含特定标识文件的目录
    3. 容器环境默认为 /app
    4. 最后回退到相对路径计算
    
    Returns:
        str: 项目根目录的绝对路径
    """
    
    # 1. 优先使用环境变量
    env_root = os.environ.get('PROJECT_ROOT')
    if env_root and os.path.exists(env_root):
        return os.path.abspath(env_root)
    
    # 2. 容器环境检测
    if os.path.exists('/app') and os.path.exists('/app/src'):
        return '/app'
    
    # 3. 通过特征文件检测项目根目录
    # 这些文件/目录是项目根目录的特征标识
    project_markers = [
        'src',
        'config',
        'database',
        'digging-dashboard',
        'README.md',
        'requirements.txt'
    ]
    
    # 从当前文件开始向上查找
    current_path = Path(__file__).resolve()
    
    # 向上查找最多10级目录
    for _ in range(10):
        current_path = current_path.parent
        
        # 检查是否包含足够多的项目标识文件/目录
        marker_count = 0
        for marker in project_markers:
            if (current_path / marker).exists():
                marker_count += 1
        
        # 如果找到3个或以上标识，认为是项目根目录
        if marker_count >= 3:
            return str(current_path)
        
        # 如果已经到达文件系统根目录，停止查找
        if current_path.parent == current_path:
            break
    
    # 4. 回退方案：基于当前文件位置计算
    # backend/app/utils/path_utils.py -> ../../../../
    current_file = Path(__file__).resolve()
    fallback_root = current_file.parent.parent.parent.parent.parent
    
    if fallback_root.exists():
        return str(fallback_root)
    
    # 5. 最后的回退方案
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..'))


def get_script_path(script_name: str) -> str:
    """
    获取脚本文件的完整路径
    
    Args:
        script_name: 脚本名称（不含.py后缀）
        
    Returns:
        str: 脚本文件的完整路径
    """
    project_root = detect_project_root()
    return os.path.join(project_root, 'src', f'{script_name}.py')


def get_config_path(config_name: str = 'digging_config.txt') -> str:
    """
    获取配置文件的完整路径
    
    Args:
        config_name: 配置文件名
        
    Returns:
        str: 配置文件的完整路径
    """
    project_root = detect_project_root()
    return os.path.join(project_root, 'config', config_name)


def get_log_path(log_name: str) -> str:
    """
    获取日志文件的完整路径
    
    Args:
        log_name: 日志文件名
        
    Returns:
        str: 日志文件的完整路径
    """
    project_root = detect_project_root()
    return os.path.join(project_root, 'logs', log_name)


def ensure_directory(path: str) -> None:
    """
    确保目录存在，如不存在则创建
    
    Args:
        path: 目录路径
    """
    os.makedirs(path, exist_ok=True)


if __name__ == "__main__":
    # 测试路径检测
    root = detect_project_root()
    print(f"检测到的项目根目录: {root}")
    print(f"src目录是否存在: {os.path.exists(os.path.join(root, 'src'))}")
    print(f"config目录是否存在: {os.path.exists(os.path.join(root, 'config'))}")
    print(f"database目录是否存在: {os.path.exists(os.path.join(root, 'database'))}")
