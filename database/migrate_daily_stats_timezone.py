#!/usr/bin/env python3
"""
迁移脚本：修复 daily_submit_stats 表的时区约束问题
将 date UNIQUE 改为 (date, timezone) 复合唯一约束
"""

import os
import sys
import sqlite3
from datetime import datetime

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import ROOT_PATH

def migrate_daily_stats_timezone():
    """迁移daily_submit_stats表结构"""
    db_path = os.path.join(ROOT_PATH, 'database', 'factors.db')
    
    if not os.path.exists(db_path):
        print("❌ 数据库文件不存在，无需迁移")
        return False
    
    print("🔄 开始迁移 daily_submit_stats 表结构...")
    print("="*50)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='daily_submit_stats'
        """)
        
        if not cursor.fetchone():
            print("✅ daily_submit_stats 表不存在，无需迁移")
            conn.close()
            return True
        
        # 检查是否已经有复合唯一约束
        cursor.execute("PRAGMA table_info(daily_submit_stats)")
        columns = cursor.fetchall()
        
        cursor.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='daily_submit_stats'
        """)
        table_sql = cursor.fetchone()[0]
        
        if "UNIQUE(date, timezone)" in table_sql:
            print("✅ 表结构已经是最新版本，无需迁移")
            conn.close()
            return True
        
        print("📊 当前表结构需要更新...")
        
        # 备份现有数据
        print("💾 备份现有数据...")
        cursor.execute("SELECT * FROM daily_submit_stats")
        existing_data = cursor.fetchall()
        print(f"📋 备份了 {len(existing_data)} 条记录")
        
        # 重命名旧表
        cursor.execute("ALTER TABLE daily_submit_stats RENAME TO daily_submit_stats_old")
        print("✅ 旧表已重命名为 daily_submit_stats_old")
        
        # 创建新表结构
        cursor.execute("""
            CREATE TABLE daily_submit_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                successful_submits INTEGER DEFAULT 0,
                total_attempts INTEGER DEFAULT 0,
                timezone VARCHAR(20) DEFAULT 'UTC',
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                CHECK (successful_submits >= 0),
                CHECK (total_attempts >= successful_submits),
                UNIQUE(date, timezone)
            )
        """)
        print("✅ 创建了新的表结构（支持复合唯一约束）")
        
        # 删除旧索引（如果存在）
        print("🗑️  删除旧索引...")
        try:
            cursor.execute("DROP INDEX IF EXISTS idx_daily_submit_stats_date")
            cursor.execute("DROP INDEX IF EXISTS idx_daily_submit_stats_timezone")
            cursor.execute("DROP INDEX IF EXISTS idx_daily_submit_stats_date_timezone")
        except:
            pass
        
        # 重建索引
        cursor.execute("CREATE INDEX idx_daily_submit_stats_date ON daily_submit_stats(date)")
        cursor.execute("CREATE INDEX idx_daily_submit_stats_timezone ON daily_submit_stats(timezone)")
        cursor.execute("CREATE INDEX idx_daily_submit_stats_date_timezone ON daily_submit_stats(date, timezone)")
        print("✅ 创建了新的索引")
        
        # 恢复数据
        if existing_data:
            print("🔄 恢复数据...")
            
            # 检查数据中是否有重复的 (date, timezone) 组合
            date_timezone_pairs = set()
            deduplicated_data = []
            duplicates_found = 0
            
            for row in existing_data:
                id_val, date, successful, attempts, timezone, last_updated = row
                pair = (date, timezone)
                
                if pair in date_timezone_pairs:
                    duplicates_found += 1
                    print(f"⚠️  发现重复记录: {date} {timezone} - 将合并数据")
                    # 查找已存在的记录并合并
                    for i, existing_row in enumerate(deduplicated_data):
                        if existing_row[1] == date and existing_row[4] == timezone:
                            # 合并数据（累加）
                            new_successful = existing_row[2] + successful
                            new_attempts = existing_row[3] + attempts
                            deduplicated_data[i] = (
                                existing_row[0], date, new_successful, new_attempts, 
                                timezone, max(existing_row[5], last_updated)
                            )
                            break
                else:
                    date_timezone_pairs.add(pair)
                    deduplicated_data.append(row)
            
            if duplicates_found > 0:
                print(f"📊 处理了 {duplicates_found} 个重复记录")
            
            # 插入去重后的数据
            cursor.executemany("""
                INSERT INTO daily_submit_stats 
                (id, date, successful_submits, total_attempts, timezone, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, deduplicated_data)
            
            print(f"✅ 恢复了 {len(deduplicated_data)} 条记录")
        
        # 删除旧表
        cursor.execute("DROP TABLE daily_submit_stats_old")
        print("🗑️  删除了旧表")
        
        conn.commit()
        conn.close()
        
        print("✅ 表结构迁移完成！")
        print("\n📊 新表结构特性:")
        print("  - 支持相同日期不同时区的独立记录")
        print("  - (date, timezone) 复合唯一约束")
        print("  - 优化的索引结构")
        
        return True
        
    except Exception as e:
        print(f"❌ 迁移失败: {e}")
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return False

if __name__ == "__main__":
    migrate_daily_stats_timezone()