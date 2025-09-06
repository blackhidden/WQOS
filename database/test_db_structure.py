#!/usr/bin/env python3
"""
简化版数据库结构测试脚本
仅使用Python标准库，无需额外依赖
"""

import os
import sqlite3
import sys

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from config import ROOT_PATH, RECORDS_PATH
except ImportError:
    # 如果无法导入配置，使用默认路径
    ROOT_PATH = os.path.dirname(os.path.dirname(__file__))
    RECORDS_PATH = os.path.join(ROOT_PATH, 'records')

def test_database_creation():
    """测试数据库创建"""
    print("🔧 测试数据库结构创建...")
    
    db_path = os.path.join(ROOT_PATH, 'database', 'test_structure.db')
    schema_path = os.path.join(ROOT_PATH, 'database', 'schema.sql')
    
    # 确保数据库目录存在
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # 删除旧的测试数据库
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)
        print(f"✅ 数据库连接成功: {db_path}")
        
        # 读取并执行SQL结构
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        conn.executescript(schema_sql)
        print("✅ 数据库表结构创建成功")
        
        # 验证表是否创建成功
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['factor_expressions', 'checked_alphas', 'submitable_alphas', 'system_config']
        print(f"📊 创建的表: {tables}")
        
        for table in expected_tables:
            if table in tables:
                print(f"✅ 表 {table} 创建成功")
            else:
                print(f"❌ 表 {table} 创建失败")
        
        # 验证视图
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view'")
        views = [row[0] for row in cursor.fetchall()]
        print(f"📊 创建的视图: {views}")
        
        # 测试插入一些示例数据
        print("\n🧪 测试数据插入...")
        
        # 插入因子表达式
        cursor.execute("""
            INSERT INTO factor_expressions (expression, dataset_id, region, step)
            VALUES (?, ?, ?, ?)
        """, ("ts_rank(close, 20)", "analyst4", "USA", 1))
        
        # 插入已检查因子
        cursor.execute("""
            INSERT INTO checked_alphas (alpha_id, dataset_id, region, step)
            VALUES (?, ?, ?, ?)
        """, ("TEST123", "analyst4", "USA", 1))
        
        # 插入系统配置
        cursor.execute("""
            INSERT INTO system_config (config_key, config_value, description)
            VALUES (?, ?, ?)
        """, ("test_key", "test_value", "测试配置"))
        
        conn.commit()
        print("✅ 示例数据插入成功")
        
        # 测试查询
        print("\n📊 测试数据查询...")
        
        cursor = conn.execute("SELECT COUNT(*) FROM factor_expressions")
        expr_count = cursor.fetchone()[0]
        print(f"  因子表达式数量: {expr_count}")
        
        cursor = conn.execute("SELECT COUNT(*) FROM checked_alphas")
        checked_count = cursor.fetchone()[0]
        print(f"  已检查因子数量: {checked_count}")
        
        cursor = conn.execute("SELECT COUNT(*) FROM system_config")
        config_count = cursor.fetchone()[0]
        print(f"  系统配置数量: {config_count}")
        
        # 测试视图查询
        try:
            cursor = conn.execute("SELECT * FROM system_overview")
            overview = cursor.fetchall()
            print(f"  系统概览: {len(overview)} 个表")
            for row in overview:
                print(f"    {row[0]}: {row[1]} 条记录")
        except Exception as e:
            print(f"⚠️  视图查询失败: {e}")
        
        conn.close()
        print("\n✅ 数据库结构测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 数据库测试失败: {e}")
        return False
    finally:
        # 清理测试数据库
        if os.path.exists(db_path):
            os.remove(db_path)
            print("🧹 清理测试数据库")

def analyze_existing_files():
    """分析现有文件结构"""
    print("\n📁 分析现有文件结构...")
    
    if not os.path.exists(RECORDS_PATH):
        print(f"❌ records目录不存在: {RECORDS_PATH}")
        return
    
    files_to_migrate = []
    files_to_keep = []
    
    for filename in os.listdir(RECORDS_PATH):
        filepath = os.path.join(RECORDS_PATH, filename)
        if not os.path.isfile(filepath):
            continue
            
        # 判断是否需要迁移
        if (filename.endswith('_simulated_alpha_expression.txt') or 
            filename.endswith('_checked_alpha_id.txt') or
            filename == 'submitable_alpha.csv' or
            filename == 'start_date.txt'):
            files_to_migrate.append(filename)
        else:
            files_to_keep.append(filename)
    
    print(f"📊 需要迁移到数据库的文件 ({len(files_to_migrate)} 个):")
    for filename in sorted(files_to_migrate):
        filepath = os.path.join(RECORDS_PATH, filename)
        size = os.path.getsize(filepath)
        print(f"  - {filename} ({size/1024:.1f}KB)")
    
    print(f"\n📊 保持文件存储的文件 ({len(files_to_keep)} 个):")
    for filename in sorted(files_to_keep):
        filepath = os.path.join(RECORDS_PATH, filename)
        size = os.path.getsize(filepath)
        print(f"  - {filename} ({size/1024:.1f}KB)")

def main():
    """主函数"""
    print("="*60)
    print("  WorldQuant 数据库结构测试")
    print("  验证SQLite设计和现有文件分析")
    print("="*60)
    
    # 测试数据库结构
    success = test_database_creation()
    
    # 分析现有文件
    analyze_existing_files()
    
    if success:
        print("\n🎉 数据库结构验证成功！")
        print("💡 下一步: 在正确的Python环境中运行完整迁移脚本")
        print("   $ conda activate WorldQuant")
        print("   $ python database/migrate_to_sqlite.py")
    else:
        print("\n💥 数据库结构验证失败，请检查SQL语法")
        sys.exit(1)

if __name__ == "__main__":
    main()