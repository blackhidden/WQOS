"""
日志管理服务
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional, AsyncGenerator
from datetime import datetime, timedelta
import aiofiles
import re

from app.core.exceptions import ValidationError


class LogService:
    """日志管理服务"""
    
    def __init__(self):
        self.project_root = "/Users/enkidu/Pyproject/WorldQuant"
        self.log_files = {
            "unified_digging": os.path.join(self.project_root, "logs", "unified_digging.log"),
            "check_optimized": os.path.join(self.project_root, "test_check_optimized.log"),
            "session_manager": os.path.join(self.project_root, "logs", "session_manager.log"),
            "submit_daemon": os.path.join(self.project_root, "logs", "submit_daemon.log"),
            "correlation_checker": os.path.join(self.project_root, "logs", "correlation_checker.log")
        }
        
        # 确保日志目录存在
        self._ensure_log_directories()
    
    def _ensure_log_directories(self):
        """确保日志目录存在"""
        log_dirs = set()
        for log_file in self.log_files.values():
            log_dirs.add(os.path.dirname(log_file))
        
        for log_dir in log_dirs:
            os.makedirs(log_dir, exist_ok=True)
    
    def get_available_log_sources(self) -> List[str]:
        """获取可用的日志源"""
        return list(self.log_files.keys())
    
    async def get_logs(
        self, 
        source: str = "unified_digging",
        limit: int = 100, 
        offset: int = 0,
        level: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        search_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取日志"""
        
        if source not in self.log_files:
            raise ValidationError(f"无效的日志源: {source}")
        
        log_file = self.log_files[source]
        
        if not os.path.exists(log_file):
            return {
                "logs": [],
                "total": 0,
                "source": source,
                "has_more": False
            }
        
        try:
            all_logs = await self._read_log_file(log_file)
            
            # 应用过滤器
            filtered_logs = self._filter_logs(
                all_logs, level, start_time, end_time, search_text
            )
            
            # 按时间倒序排列（最新的在前）
            # 处理None时间戳，将None放在最后
            filtered_logs.sort(
                key=lambda x: x.get("timestamp") or "0000-00-00T00:00:00", 
                reverse=True
            )
            
            # 分页
            total = len(filtered_logs)
            start_idx = offset
            end_idx = offset + limit
            page_logs = filtered_logs[start_idx:end_idx]
            
            return {
                "logs": page_logs,
                "total": total,
                "source": source,
                "has_more": end_idx < total
            }
            
        except Exception as e:
            raise ValidationError(f"读取日志失败: {str(e)}")
    
    async def _read_log_file(self, log_file: str) -> List[Dict[str, Any]]:
        """异步读取日志文件"""
        logs = []
        
        try:
            async with aiofiles.open(log_file, 'r', encoding='utf-8') as f:
                line_number = 0
                async for line in f:
                    line_number += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    log_entry = self._parse_log_line(line, line_number)
                    if log_entry:
                        logs.append(log_entry)
        
        except Exception as e:
            print(f"读取日志文件失败 {log_file}: {str(e)}")
        
        return logs
    
    def _parse_log_line(self, line: str, line_number: int) -> Optional[Dict[str, Any]]:
        """解析日志行"""
        try:
            # 尝试解析JSON格式的结构化日志
            if line.startswith('{') and line.endswith('}'):
                log_data = json.loads(line)
                return {
                    "id": line_number,
                    "timestamp": log_data.get("timestamp"),
                    "level": log_data.get("level", "INFO"),
                    "logger": log_data.get("logger"),
                    "message": log_data.get("message", ""),
                    "module": log_data.get("module"),
                    "function": log_data.get("function"),
                    "line": log_data.get("line"),
                    "raw": line,
                    "structured": True,
                    "details": log_data
                }
            
            # 尝试解析标准格式的日志（时间戳 - 级别 - 消息）
            timestamp_pattern = r'(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[.\d]*[Z]?)'
            level_pattern = r'(DEBUG|INFO|WARNING|ERROR|CRITICAL)'
            
            # 匹配格式：YYYY-MM-DD HH:MM:SS - LEVEL - MESSAGE
            match = re.match(rf'^{timestamp_pattern}.*?{level_pattern}.*?-\s*(.*)$', line)
            if match:
                timestamp_str, level, message = match.groups()
                return {
                    "id": line_number,
                    "timestamp": timestamp_str,
                    "level": level,
                    "logger": None,
                    "message": message.strip(),
                    "module": None,
                    "function": None,
                    "line": None,
                    "raw": line,
                    "structured": False,
                    "details": {}
                }
            
            # 简单的级别匹配
            level_match = re.search(rf'\b({level_pattern})\b', line)
            level = level_match.group(1) if level_match else "INFO"
            
            # 时间戳匹配
            timestamp_match = re.search(timestamp_pattern, line)
            timestamp = timestamp_match.group(1) if timestamp_match else None
            
            return {
                "id": line_number,
                "timestamp": timestamp,
                "level": level,
                "logger": None,
                "message": line,
                "module": None,
                "function": None,
                "line": None,
                "raw": line,
                "structured": False,
                "details": {}
            }
            
        except json.JSONDecodeError:
            # 普通文本日志
            return {
                "id": line_number,
                "timestamp": None,
                "level": "INFO",
                "logger": None,
                "message": line,
                "module": None,
                "function": None,
                "line": None,
                "raw": line,
                "structured": False,
                "details": {}
            }
        except Exception as e:
            print(f"解析日志行失败: {str(e)}")
            return None
    
    def _filter_logs(
        self,
        logs: List[Dict[str, Any]],
        level: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        search_text: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """过滤日志"""
        
        filtered = logs
        
        # 按级别过滤
        if level:
            level_priority = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
            target_priority = level_priority.get(level.upper(), 1)
            
            filtered = [
                log for log in filtered 
                if level_priority.get(log.get("level", "INFO"), 1) >= target_priority
            ]
        
        # 按时间过滤
        if start_time or end_time:
            time_filtered = []
            for log in filtered:
                log_time = self._parse_log_timestamp(log.get("timestamp"))
                if log_time:
                    if start_time and log_time < start_time:
                        continue
                    if end_time and log_time > end_time:
                        continue
                time_filtered.append(log)
            filtered = time_filtered
        
        # 按文本搜索过滤
        if search_text:
            search_text = search_text.lower()
            filtered = [
                log for log in filtered
                if search_text in log.get("message", "").lower() or
                   search_text in log.get("raw", "").lower()
            ]
        
        return filtered
    
    def _parse_log_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """解析日志时间戳"""
        if not timestamp_str:
            return None
        
        # 尝试多种时间格式
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",      # ISO格式带微秒和Z
            "%Y-%m-%dT%H:%M:%S.%f",       # ISO格式带微秒
            "%Y-%m-%dT%H:%M:%SZ",         # ISO格式带Z
            "%Y-%m-%dT%H:%M:%S",          # ISO格式
            "%Y-%m-%d %H:%M:%S.%f",       # 标准格式带微秒
            "%Y-%m-%d %H:%M:%S",          # 标准格式
            "%m/%d/%Y %H:%M:%S",          # 美式格式
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        return None
    
    async def stream_logs(
        self, 
        source: str = "unified_digging",
        follow: bool = True
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """实时流式读取日志"""
        
        if source not in self.log_files:
            raise ValidationError(f"无效的日志源: {source}")
        
        log_file = self.log_files[source]
        
        if not os.path.exists(log_file):
            # 等待文件创建
            while follow and not os.path.exists(log_file):
                await asyncio.sleep(1)
        
        try:
            # 先读取现有内容的最后几行
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # 返回最后10行
                    for line in lines[-10:]:
                        log_entry = self._parse_log_line(line.strip(), 0)
                        if log_entry:
                            yield log_entry
            
            if not follow:
                return
            
            # 实时监控新增内容
            last_size = os.path.getsize(log_file) if os.path.exists(log_file) else 0
            line_number = 0
            
            while True:
                try:
                    if os.path.exists(log_file):
                        current_size = os.path.getsize(log_file)
                        
                        if current_size > last_size:
                            async with aiofiles.open(log_file, 'r', encoding='utf-8') as f:
                                await f.seek(last_size)
                                async for line in f:
                                    line_number += 1
                                    line = line.strip()
                                    if line:
                                        log_entry = self._parse_log_line(line, line_number)
                                        if log_entry:
                                            yield log_entry
                            
                            last_size = current_size
                    
                    await asyncio.sleep(1)  # 每秒检查一次
                    
                except Exception as e:
                    print(f"流式读取日志错误: {str(e)}")
                    await asyncio.sleep(5)  # 错误后等待更长时间
        
        except Exception as e:
            raise ValidationError(f"流式读取日志失败: {str(e)}")
    
    def get_log_stats(self) -> Dict[str, Any]:
        """获取日志统计信息"""
        stats = {}
        
        for source, log_file in self.log_files.items():
            if os.path.exists(log_file):
                file_stat = os.stat(log_file)
                stats[source] = {
                    "exists": True,
                    "size": file_stat.st_size,
                    "size_mb": round(file_stat.st_size / 1024 / 1024, 2),
                    "modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    "lines": self._count_lines(log_file)
                }
            else:
                stats[source] = {
                    "exists": False,
                    "size": 0,
                    "size_mb": 0,
                    "modified": None,
                    "lines": 0
                }
        
        return stats
    
    def _count_lines(self, file_path: str) -> int:
        """计算文件行数"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except:
            return 0


# 全局实例
log_service = LogService()
