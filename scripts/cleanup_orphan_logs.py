#!/usr/bin/env python3
"""
清理孤儿日志文件脚本

功能：
1. 扫描logs目录下的所有日志文件
2. 检查哪些日志文件在数据库中没有对应记录
3. 清理这些孤儿日志文件
4. 显示清理报告
"""

import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set

# 添加项目路径
project_root = Path(__file__).parent.parent
backend_path = project_root / "digging-dashboard" / "backend"
sys.path.insert(0, str(backend_path))

# 导入数据库工具
from sqlalchemy import create_engine, text

def get_database_engine():
    """获取数据库引擎"""
    try:
        # 使用Dashboard的数据库配置
        dashboard_db_path = project_root / "digging-dashboard" / "backend" / "dashboard.db"
        if not dashboard_db_path.exists():
            # 如果dashboard.db不存在，尝试当前目录
            dashboard_db_path = project_root / "digging-dashboard" / "dashboard.db"
        
        if not dashboard_db_path.exists():
            print(f"❌ 数据库文件不存在: {dashboard_db_path}")
            print("💡 请确保Dashboard已经初始化并创建了数据库")
            return None
        
        engine = create_engine(f"sqlite:///{dashboard_db_path}")
        return engine
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def scan_log_directory() -> Dict[str, List[str]]:
    """扫描日志目录，返回所有日志文件"""
    logs_dir = project_root / "logs"
    if not logs_dir.exists():
        print(f"❌ 日志目录不存在: {logs_dir}")
        return {}
    
    log_files = {}
    total_files = 0
    total_size = 0
    
    print(f"📂 扫描日志目录: {logs_dir}")
    
    for file_path in logs_dir.glob("*.log"):
        if file_path.is_file():
            # 解析文件名格式: script_type_YYYYMMDD_HHMMSS_pid.log
            file_name = file_path.name
            size = file_path.stat().st_size
            total_size += size
            total_files += 1
            
            # 提取脚本类型
            parts = file_name.split('_')
            if len(parts) >= 4:
                if parts[0] == "check" and parts[1] == "optimized":
                    script_type = "check_optimized"
                    date_part = parts[2]
                    time_part = parts[3]
                    pid_part = parts[4].replace('.log', '')
                elif parts[0] == "correlation" and parts[1] == "checker":
                    script_type = "correlation_checker"
                    date_part = parts[2]
                    time_part = parts[3]
                    pid_part = parts[4].replace('.log', '')
                elif parts[0] == "unified" and parts[1] == "digging":
                    script_type = "unified_digging"
                    date_part = parts[2]
                    time_part = parts[3]
                    pid_part = parts[4].replace('.log', '')
                else:
                    script_type = "unknown"
                    date_part = ""
                    time_part = ""
                    pid_part = ""
            else:
                script_type = "unknown"
                date_part = ""
                time_part = ""
                pid_part = ""
            
            if script_type not in log_files:
                log_files[script_type] = []
            
            log_files[script_type].append({
                'path': str(file_path),
                'name': file_name,
                'size': size,
                'date': date_part,
                'time': time_part,
                'pid': pid_part,
                'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
            })
    
    print(f"📊 发现 {total_files} 个日志文件，总大小: {total_size / 1024 / 1024:.2f} MB")
    
    # 按脚本类型显示统计
    for script_type, files in log_files.items():
        type_size = sum(f['size'] for f in files)
        print(f"  📄 {script_type}: {len(files)} 个文件，{type_size / 1024 / 1024:.2f} MB")
    
    return log_files

def get_database_log_paths(engine) -> Set[str]:
    """获取数据库中记录的所有日志文件路径"""
    try:
        # 使用原生SQL查询，避免ORM模型导入问题
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT log_file_path 
                FROM digging_processes 
                WHERE log_file_path IS NOT NULL 
                AND log_file_path != ''
            """))
            
            log_paths = set()
            for row in result:
                log_file_path = row[0]
                if log_file_path:
                    # 获取绝对路径
                    log_path = Path(log_file_path)
                    if not log_path.is_absolute():
                        log_path = project_root / log_file_path
                    log_paths.add(str(log_path.resolve()))
            
            print(f"📋 数据库中有 {len(log_paths)} 个日志文件记录")
            return log_paths
        
    except Exception as e:
        print(f"❌ 查询数据库失败: {e}")
        return set()

def identify_orphan_logs(log_files: Dict[str, List[str]], db_log_paths: Set[str]) -> List[Dict]:
    """识别孤儿日志文件"""
    orphan_logs = []
    
    for script_type, files in log_files.items():
        for file_info in files:
            file_path = Path(file_info['path']).resolve()
            
            # 检查是否在数据库中有记录
            if str(file_path) not in db_log_paths:
                orphan_logs.append({
                    'script_type': script_type,
                    'path': str(file_path),
                    'name': file_info['name'],
                    'size': file_info['size'],
                    'modified': file_info['modified']
                })
    
    return orphan_logs

def cleanup_orphan_logs(orphan_logs: List[Dict], dry_run: bool = True) -> Dict:
    """清理孤儿日志文件"""
    cleaned_count = 0
    cleaned_size = 0
    errors = []
    
    print(f"\n{'🔍 预览清理操作' if dry_run else '🗑️ 开始清理孤儿日志文件'}...")
    
    for log_info in orphan_logs:
        file_path = Path(log_info['path'])
        size_mb = log_info['size'] / 1024 / 1024
        
        if dry_run:
            print(f"  {'📄' if log_info['script_type'] != 'unknown' else '❓'} "
                  f"{log_info['name']} ({size_mb:.2f} MB) - "
                  f"修改时间: {log_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            try:
                if file_path.exists():
                    file_path.unlink()
                    cleaned_count += 1
                    cleaned_size += log_info['size']
                    print(f"  ✅ 已删除: {log_info['name']} ({size_mb:.2f} MB)")
                else:
                    print(f"  ⚠️ 文件不存在: {log_info['name']}")
            except Exception as e:
                error_msg = f"删除失败: {log_info['name']} - {e}"
                errors.append(error_msg)
                print(f"  ❌ {error_msg}")
    
    return {
        'cleaned_count': cleaned_count,
        'cleaned_size': cleaned_size,
        'errors': errors
    }

def generate_cleanup_report(orphan_logs: List[Dict], cleanup_result: Dict = None):
    """生成清理报告"""
    print(f"\n📊 清理报告")
    print("=" * 50)
    
    # 按脚本类型统计孤儿日志
    type_stats = {}
    total_orphan_size = 0
    
    for log_info in orphan_logs:
        script_type = log_info['script_type']
        size = log_info['size']
        
        if script_type not in type_stats:
            type_stats[script_type] = {'count': 0, 'size': 0}
        
        type_stats[script_type]['count'] += 1
        type_stats[script_type]['size'] += size
        total_orphan_size += size
    
    print(f"🔍 发现 {len(orphan_logs)} 个孤儿日志文件，总大小: {total_orphan_size / 1024 / 1024:.2f} MB")
    
    for script_type, stats in type_stats.items():
        print(f"  📄 {script_type}: {stats['count']} 个文件，{stats['size'] / 1024 / 1024:.2f} MB")
    
    if cleanup_result:
        print(f"\n✅ 清理完成:")
        print(f"  🗑️ 已删除: {cleanup_result['cleaned_count']} 个文件")
        print(f"  💾 释放空间: {cleanup_result['cleaned_size'] / 1024 / 1024:.2f} MB")
        
        if cleanup_result['errors']:
            print(f"  ❌ 错误: {len(cleanup_result['errors'])} 个")
            for error in cleanup_result['errors']:
                print(f"    - {error}")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="清理孤儿日志文件")
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='预览模式，不实际删除文件 (默认)')
    parser.add_argument('--execute', action='store_true',
                       help='执行实际删理操作')
    parser.add_argument('--days', type=int, default=0,
                       help='只清理超过指定天数的文件 (0=全部)')
    
    args = parser.parse_args()
    
    # 如果指定了 --execute，则关闭预览模式
    dry_run = not args.execute
    
    print("🧹 孤儿日志文件清理工具")
    print("=" * 50)
    
    # 1. 获取数据库引擎
    engine = get_database_engine()
    if not engine:
        return 1
    
    try:
        # 2. 扫描日志目录
        log_files = scan_log_directory()
        if not log_files:
            print("✅ 没有发现日志文件")
            return 0
        
        # 3. 获取数据库中的日志路径
        db_log_paths = get_database_log_paths(engine)
        
        # 4. 识别孤儿日志文件
        orphan_logs = identify_orphan_logs(log_files, db_log_paths)
        
        if not orphan_logs:
            print("✅ 没有发现孤儿日志文件")
            return 0
        
        # 5. 按日期过滤
        if args.days > 0:
            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=args.days)
            orphan_logs = [log for log in orphan_logs if log['modified'] < cutoff_date]
            print(f"🗓️ 过滤条件: 超过 {args.days} 天的文件")
        
        if not orphan_logs:
            print(f"✅ 没有发现符合条件的孤儿日志文件")
            return 0
        
        # 6. 清理孤儿日志
        cleanup_result = cleanup_orphan_logs(orphan_logs, dry_run)
        
        # 7. 生成报告
        generate_cleanup_report(orphan_logs, cleanup_result if not dry_run else None)
        
        if dry_run:
            print(f"\n💡 预览完成。使用 --execute 参数执行实际清理操作")
        
        return 0
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        if engine:
            engine.dispose()

if __name__ == "__main__":
    exit(main())
