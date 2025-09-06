#!/usr/bin/env python3
"""
快速清理数据库中使用了不可用操作符的因子表达式记录

使用方法:
1. 扫描模式: python quick_cleanup_invalid_operators.py --scan
2. 删除模式: python quick_cleanup_invalid_operators.py --delete
3. 预览模式: python quick_cleanup_invalid_operators.py --preview
"""

import os
import sys
import sqlite3
import argparse
from datetime import datetime

# 不可用的操作符列表
INVALID_OPERATORS = [
    # ts_ops 不可用 (6个)
    'ts_ir', 'ts_min_diff', 'ts_max_diff', 'ts_returns', 'ts_skewness', 'ts_kurtosis',
    # basic_ops 不可用 (4个)
    'log_diff', 's_log_1p', 'fraction', 'scale_down',
    # group_ops 不可用 (1个)
    'group_normalize'
]

def get_db_path():
    """获取数据库路径"""
    # 尝试多个可能的路径
    possible_paths = [
        'database/factors.db',
        'factors.db',
        os.path.join(os.path.dirname(__file__), 'database', 'factors.db'),
        os.path.join(os.path.dirname(__file__), 'factors.db')
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    print("❌ 找不到数据库文件！请确认数据库路径")
    print("   尝试过的路径:")
    for path in possible_paths:
        print(f"   - {path}")
    sys.exit(1)

def scan_invalid_expressions(db_path):
    """扫描包含无效操作符的表达式"""
    print(f"🔍 扫描数据库: {db_path}")
    print(f"🚫 检查 {len(INVALID_OPERATORS)} 个不可用操作符: {', '.join(INVALID_OPERATORS)}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 查询总记录数
        cursor.execute("SELECT COUNT(*) FROM factor_expressions")
        total_count = cursor.fetchone()[0]
        print(f"📊 总记录数: {total_count:,}")
        
        # 构建查询条件
        conditions = []
        params = []
        for op in INVALID_OPERATORS:
            conditions.append("expression LIKE ?")
            params.append(f"%{op}%")
        
        query = f"""
        SELECT id, expression, dataset_id, region, step, created_at
        FROM factor_expressions 
        WHERE {' OR '.join(conditions)}
        ORDER BY created_at DESC
        """
        
        cursor.execute(query, params)
        invalid_records = cursor.fetchall()
        
        print(f"🚫 发现无效记录: {len(invalid_records):,} 个")
        if total_count > 0:
            print(f"📈 无效率: {len(invalid_records)/total_count*100:.2f}%")
        
        # 统计各操作符的使用情况
        if invalid_records:
            print(f"\n📋 按操作符统计:")
            op_stats = {}
            for record in invalid_records:
                expression = record[1]
                for op in INVALID_OPERATORS:
                    if op in expression:
                        op_stats[op] = op_stats.get(op, 0) + 1
            
            for op, count in sorted(op_stats.items(), key=lambda x: x[1], reverse=True):
                print(f"  {op}: {count:,} 个")
        
        conn.close()
        return invalid_records
        
    except Exception as e:
        print(f"❌ 扫描失败: {e}")
        return []

def preview_records(records, limit=10):
    """预览记录"""
    print(f"\n👀 预览前 {min(limit, len(records))} 个无效记录:")
    print("-" * 80)
    
    for i, record in enumerate(records[:limit]):
        record_id, expression, dataset_id, region, step, created_at = record
        print(f"{i+1:2d}. ID: {record_id} | {dataset_id} | {region} | 第{step}阶 | {created_at}")
        
        # 显示表达式（截断长表达式）
        display_expr = expression[:60] + "..." if len(expression) > 60 else expression
        print(f"    表达式: {display_expr}")
        
        # 找出使用的无效操作符
        used_ops = [op for op in INVALID_OPERATORS if op in expression]
        print(f"    无效操作符: {', '.join(used_ops)}")
        print()

def delete_records(db_path, records, confirm=False):
    """删除记录"""
    if not records:
        print("✅ 没有需要删除的记录")
        return True
    
    print(f"🗑️  准备删除 {len(records):,} 个无效记录...")
    
    if not confirm:
        print("⚠️  这是危险操作！将永久删除数据")
        response = input("👉 输入 'YES' 来确认删除: ").strip()
        if response != 'YES':
            print("❌ 用户取消删除")
            return False
    
    # 创建备份
    backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    try:
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"💾 已创建备份: {backup_path}")
    except Exception as e:
        print(f"⚠️  备份创建失败: {e}")
        response = input("👉 是否继续删除？输入 'YES' 确认: ").strip()
        if response != 'YES':
            print("❌ 用户取消删除")
            return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 开始事务
        cursor.execute("BEGIN TRANSACTION")
        
        # 批量删除
        record_ids = [str(record[0]) for record in records]
        placeholders = ','.join(['?' for _ in record_ids])
        delete_query = f"DELETE FROM factor_expressions WHERE id IN ({placeholders})"
        
        cursor.execute(delete_query, record_ids)
        deleted_count = cursor.rowcount
        
        # 提交事务
        cursor.execute("COMMIT")
        conn.close()
        
        print(f"✅ 成功删除 {deleted_count:,} 个无效记录")
        return True
        
    except Exception as e:
        print(f"❌ 删除失败: {e}")
        try:
            cursor.execute("ROLLBACK")
            conn.close()
        except:
            pass
        return False

def main():
    parser = argparse.ArgumentParser(description='清理数据库中的无效操作符记录')
    parser.add_argument('--scan', action='store_true', help='扫描模式：只查看无效记录统计')
    parser.add_argument('--preview', action='store_true', help='预览模式：显示无效记录详情')
    parser.add_argument('--delete', action='store_true', help='删除模式：删除无效记录')
    parser.add_argument('--db', help='指定数据库路径')
    
    args = parser.parse_args()
    
    if not any([args.scan, args.preview, args.delete]):
        print("❌ 请指定操作模式：--scan, --preview, 或 --delete")
        parser.print_help()
        return
    
    # 获取数据库路径
    db_path = args.db if args.db else get_db_path()
    
    print("🧹 数据库无效操作符清理工具")
    print("=" * 60)
    
    # 扫描无效记录
    invalid_records = scan_invalid_expressions(db_path)
    
    if not invalid_records:
        print("✅ 数据库很干净，没有发现无效记录！")
        return
    
    # 根据模式执行操作
    if args.scan:
        print(f"\n📊 扫描完成，发现 {len(invalid_records):,} 个无效记录")
        
    elif args.preview:
        preview_records(invalid_records)
        
    elif args.delete:
        preview_records(invalid_records, limit=5)
        success = delete_records(db_path, invalid_records)
        if success:
            print(f"\n🎉 清理完成！")
        else:
            print(f"\n❌ 清理失败！")

if __name__ == '__main__':
    main()