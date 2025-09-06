#!/usr/bin/env python3
"""
快速修复数据库中dataset_id格式不一致的脚本
专门针对服务器上发现的异常数据格式
"""

import os
import sys
import sqlite3

def fix_server_data():
    """修复服务器上的异常数据"""
    db_path = 'database/factors.db'
    
    print("🔧 快速修复服务器数据格式问题")
    print("="*50)
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("BEGIN TRANSACTION")
        
        # 定义修复规则
        fix_rules = [
            {
                'from': ('analyst4_usa_2step', 'USA', 2),
                'to': ('analyst4', 'USA', 2),
                'description': 'analyst4_usa_2step_USA_2step → analyst4_USA_2step'
            },
            {
                'from': ('fundamental2_usa_1step', 'USA', 1),
                'to': ('fundamental2', 'USA', 1),
                'description': 'fundamental2_usa_1step_USA_1step → fundamental2_USA_1step'
            },
            {
                'from': ('fundamental2_usa_2step', 'USA', 2),
                'to': ('fundamental2', 'USA', 2),
                'description': 'fundamental2_usa_2step_USA_2step → fundamental2_USA_2step'
            }
        ]
        
        total_processed = 0
        
        for rule in fix_rules:
            from_dataset, from_region, from_step = rule['from']
            to_dataset, to_region, to_step = rule['to']
            
            print(f"\n📝 处理: {rule['description']}")
            
            # 1. 检查源数据
            cursor = conn.execute("""
                SELECT COUNT(*) FROM factor_expressions 
                WHERE dataset_id = ? AND region = ? AND step = ?
            """, (from_dataset, from_region, from_step))
            
            source_count = cursor.fetchone()[0]
            if source_count == 0:
                print(f"   ℹ️  没有需要处理的数据")
                continue
            
            print(f"   📊 找到 {source_count:,} 条需要处理的记录")
            
            # 2. 获取所有需要移动的表达式
            cursor = conn.execute("""
                SELECT expression FROM factor_expressions 
                WHERE dataset_id = ? AND region = ? AND step = ?
            """, (from_dataset, from_region, from_step))
            
            expressions = [row[0] for row in cursor.fetchall()]
            
            moved_count = 0
            duplicate_count = 0
            
            for expression in expressions:
                # 3. 检查目标位置是否已存在
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM factor_expressions 
                    WHERE expression = ? AND dataset_id = ? AND region = ? AND step = ?
                """, (expression, to_dataset, to_region, to_step))
                
                target_exists = cursor.fetchone()[0] > 0
                
                if target_exists:
                    # 目标已存在，删除源记录
                    conn.execute("""
                        DELETE FROM factor_expressions 
                        WHERE expression = ? AND dataset_id = ? AND region = ? AND step = ?
                    """, (expression, from_dataset, from_region, from_step))
                    duplicate_count += 1
                else:
                    # 目标不存在，更新源记录
                    conn.execute("""
                        UPDATE factor_expressions 
                        SET dataset_id = ?, region = ?, step = ?
                        WHERE expression = ? AND dataset_id = ? AND region = ? AND step = ?
                    """, (to_dataset, to_region, to_step, expression, from_dataset, from_region, from_step))
                    moved_count += 1
            
            print(f"   ✅ 移动: {moved_count:,} 条, 删除重复: {duplicate_count:,} 条")
            total_processed += moved_count + duplicate_count
        
        # 提交事务
        conn.execute("COMMIT")
        conn.close()
        
        print(f"\n🎉 修复完成!")
        print(f"📊 总处理记录: {total_processed:,} 条")
        
        return True
        
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        print(f"❌ 修复失败: {e}")
        return False

def verify_fix():
    """验证修复结果"""
    db_path = 'database/factors.db'
    
    print(f"\n🔍 验证修复结果")
    print("="*50)
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 检查是否还有异常格式
        abnormal_patterns = [
            'analyst4_usa_2step',
            'fundamental2_usa_1step', 
            'fundamental2_usa_2step'
        ]
        
        total_abnormal = 0
        
        for pattern in abnormal_patterns:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM factor_expressions 
                WHERE dataset_id = ?
            """, (pattern,))
            
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"❌ 仍有异常格式: {pattern} ({count:,} 条)")
                total_abnormal += count
        
        if total_abnormal == 0:
            print("✅ 所有异常格式已修复")
        else:
            print(f"❌ 仍有 {total_abnormal:,} 条异常记录")
        
        # 显示修复后的统计
        cursor = conn.execute("""
            SELECT dataset_id, region, step, COUNT(*) as count
            FROM factor_expressions 
            GROUP BY dataset_id, region, step
            ORDER BY count DESC
        """)
        
        print(f"\n📊 修复后的数据分布:")
        for dataset_id, region, step, count in cursor.fetchall():
            print(f"  - {dataset_id}_{region}_{step}step: {count:,} 条")
        
        conn.close()
        return total_abnormal == 0
        
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False

def main():
    print("🚀 快速修复服务器dataset_id格式问题")
    print("="*60)
    
    # 询问确认
    print("⚠️  这将修复以下异常格式:")
    print("  - analyst4_usa_2step_USA_2step → analyst4_USA_2step")  
    print("  - fundamental2_usa_1step_USA_1step → fundamental2_USA_1step")
    print("  - fundamental2_usa_2step_USA_2step → fundamental2_USA_2step")
    print()
    print("📝 操作说明:")
    print("  - 如果目标格式已存在相同表达式，则删除源记录")
    print("  - 如果目标格式不存在，则更新源记录")
    
    response = input("\n❓ 确认执行修复? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        success = fix_server_data()
        if success:
            verify_fix()
    else:
        print("❌ 用户取消操作")

if __name__ == "__main__":
    main()