"""
相关性检查日志工具
"""

import os
import sys
import logging
import logging.handlers
from datetime import datetime


def setup_correlation_logger(name='correlation_checker'):
    """设置相关性检查器专用日志系统"""
    logger = logging.getLogger(name)
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
            
            logger.info(f"📝 相关性检查器日志系统已启动 (子进程模式)")
            logger.info(f"  📤 输出重定向: 通过父进程管理")
            logger.info(f"  🆔 进程ID: {os.getpid()}")
            logger.info(f"  💾 编码: UTF-8")
        else:
            # 独立运行模式，创建自己的日志文件
            # 确保logs目录存在
            log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
            os.makedirs(log_dir, exist_ok=True)
            
            # 生成唯一的日志文件名（基于启动时间和PID）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"correlation_checker_{timestamp}_{os.getpid()}.log")
            
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
            
            logger.info(f"📝 相关性检查器日志系统已启动 (独立模式)")
            logger.info(f"  📁 日志文件: {log_file}")
            logger.info(f"  🔄 轮转设置: 3个文件 × 10MB")
            logger.info(f"  💾 编码: UTF-8")
        
        # 设置防止日志传播（两种模式都需要）
        logger.propagate = False
    
    return logger
