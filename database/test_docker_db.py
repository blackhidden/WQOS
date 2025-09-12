#!/usr/bin/env python3
"""
作者：e.e.
日期：2025.09.10
功能：Docker环境数据库访问测试脚本
"""

import os
import sys
import sqlite3

def test_db_access():
    """测试数据库访问"""
    print("🐳 Docker环境数据库访问测试")
    print("="*50)
    
    # 检查数据库文件
    db_path = 'database/factors.db'
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        print("提示: 请先运行数据迁移脚本")
        return False
    
    print(f"✅ 数据库文件存在: {db_path}")
    
    # 检查文件大小
    size_mb = os.path.getsize(db_path) / 1024 / 1024
    print(f"📊 数据库文件大小: {size_mb:.1f} MB")
    
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)
        print("✅ 数据库连接成功")
        
        # 检查表结构
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"📋 数据库表: {', '.join(tables)}")
        
        # 检查数据
        print(f"\n📊 数据统计:")
        
        for table in ['factor_expressions', 'checked_alphas', 'submitable_alphas', 'system_config']:
            if table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  - {table}: {count:,} 条记录")
        
        # 显示一些示例数据
        print(f"\n📝 示例数据:")
        
        # 显示最新的3个因子表达式
        cursor = conn.execute("""
            SELECT expression, dataset_id, region, step 
            FROM factor_expressions 
            ORDER BY created_at DESC 
            LIMIT 3
        """)
        
        expressions = cursor.fetchall()
        if expressions:
            print("  最新因子表达式:")
            for i, (expr, dataset_id, region, step) in enumerate(expressions, 1):
                short_expr = expr[:60] + "..." if len(expr) > 60 else expr
                print(f"    {i}. {short_expr} [{dataset_id}_{region}_{step}step]")
        
        # 显示系统配置
        cursor = conn.execute("SELECT config_key, config_value FROM system_config")
        configs = cursor.fetchall()
        if configs:
            print("  系统配置:")
            for key, value in configs:
                print(f"    {key}: {value}")
        
        conn.close()
        print("\n✅ 数据库测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ 数据库访问失败: {e}")
        return False

def test_import():
    """测试模块导入"""
    print("\n🔧 模块导入测试")
    print("="*50)
    
    try:
        import pandas as pd
        print("✅ pandas 导入成功")
        print(f"   版本: {pd.__version__}")
    except ImportError:
        print("❌ pandas 导入失败")
        return False
    
    try:
        import numpy as np
        print("✅ numpy 导入成功")
        print(f"   版本: {np.__version__}")
    except ImportError:
        print("❌ numpy 导入失败")
        return False
    
    try:
        from database.db_manager import FactorDatabaseManager
        print("✅ 数据库管理器导入成功")
        
        # 测试实例化
        db = FactorDatabaseManager()
        stats = db.get_system_stats()
        print(f"✅ 数据库管理器功能正常")
        print(f"   因子表达式总数: {stats.get('total_expressions', 0):,}")
        
    except ImportError as e:
        print(f"❌ 数据库管理器导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 数据库管理器测试失败: {e}")
        return False
    
    return True

def main():
    """主函数"""
    print("🚀 Docker环境完整测试")
    print("="*60)
    
    print(f"📍 当前工作目录: {os.getcwd()}")
    print(f"🐍 Python版本: {sys.version}")
    print(f"🔧 Python路径: {sys.path[0]}")
    
    # 测试数据库访问
    db_success = test_db_access()
    
    # 测试模块导入
    import_success = test_import()
    
    print("\n" + "="*60)
    print("📋 测试结果总结:")
    print(f"  - 数据库访问: {'✅ 成功' if db_success else '❌ 失败'}")
    print(f"  - 模块导入: {'✅ 成功' if import_success else '❌ 失败'}")
    
    if db_success and import_success:
        print("\n🎉 Docker环境配置完全正常！")
        print("💡 您现在可以使用以下工具查看数据:")
        print("   1. python database/db_viewer.py     # 交互式查看器")
        print("   2. python database/quick_queries.py stats  # 快速统计")
        print("   3. sqlite3 database/factors.db     # 直接SQL查询")
    else:
        print("\n⚠️  Docker环境存在问题，请检查配置")
    
    return db_success and import_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)