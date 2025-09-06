#!/usr/bin/env python3
"""
数据库分库迁移脚本
作者：Assistant  
日期：2025.09.05

功能：
- 将主数据库中的factor_expressions表数据迁移到数据集分库
- 保留主数据库中的其他表
- 提供迁移前后的性能对比
"""

import os
import sys
import time
import argparse
from typing import List, Dict

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from database.partitioned_db_manager import PartitionedFactorManager
from database.db_manager import FactorDatabaseManager


def performance_test(db_manager, dataset_id: str, region: str, step: int, test_name: str):
    """性能测试"""
    print(f"🔬 {test_name} 性能测试...")
    
    # 测试查询性能
    start_time = time.time()
    expressions = db_manager.get_factor_expressions(dataset_id, region, step)
    query_time = time.time() - start_time
    
    print(f"  📊 查询结果: {len(expressions)} 条记录")
    print(f"  ⏱️  查询时间: {query_time:.4f} 秒")
    
    if len(expressions) > 0:
        # 测试存在性检查
        test_expr = expressions[len(expressions)//2]
        start_time = time.time()
        exists = db_manager.is_expression_exists(test_expr, dataset_id, region, step)
        check_time = time.time() - start_time
        print(f"  🔍 存在性检查: {check_time:.4f} 秒 (结果: {exists})")
    
    return query_time


def main():
    parser = argparse.ArgumentParser(description='数据库分库迁移工具')
    parser.add_argument('--db-path', default='database/factors.db', 
                       help='主数据库路径 (默认: database/factors.db)')
    parser.add_argument('--datasets', nargs='*', 
                       help='指定要迁移的数据集ID列表 (默认: 全部)')
    parser.add_argument('--test-performance', action='store_true',
                       help='迁移前后进行性能测试')
    parser.add_argument('--cleanup-main', action='store_true',
                       help='迁移后清理主数据库中的factor_expressions数据')
    parser.add_argument('--dry-run', action='store_true',
                       help='只显示迁移计划，不执行实际迁移')
    
    args = parser.parse_args()
    
    # 检查数据库文件
    if not os.path.exists(args.db_path):
        print(f"❌ 数据库文件不存在: {args.db_path}")
        return 1
    
    print("🚀 数据库分库迁移工具")
    print("=" * 50)
    
    # 初始化管理器
    print(f"📂 主数据库: {args.db_path}")
    main_db = FactorDatabaseManager(args.db_path)
    partitioned_db = PartitionedFactorManager(args.db_path)
    
    try:
        # 1. 分析当前数据
        print("\n📊 分析当前数据...")
        
        with main_db.get_connection() as conn:
            # 获取数据集统计
            cursor = conn.execute("""
                SELECT dataset_id, region, step, COUNT(*) as count
                FROM factor_expressions 
                GROUP BY dataset_id, region, step
                ORDER BY dataset_id, region, step
            """)
            
            dataset_stats = {}
            total_records = 0
            
            for row in cursor.fetchall():
                dataset_id, region, step, count = row
                if dataset_id not in dataset_stats:
                    dataset_stats[dataset_id] = {}
                if region not in dataset_stats[dataset_id]:
                    dataset_stats[dataset_id][region] = {}
                dataset_stats[dataset_id][region][step] = count
                total_records += count
        
        print(f"📈 总记录数: {total_records:,}")
        print("📋 数据集分布:")
        
        target_datasets = args.datasets if args.datasets else list(dataset_stats.keys())
        
        for dataset_id in sorted(dataset_stats.keys()):
            if dataset_id in target_datasets:
                dataset_total = sum(
                    sum(steps.values()) 
                    for steps in dataset_stats[dataset_id].values()
                )
                print(f"  ✅ {dataset_id}: {dataset_total:,} 条记录")
                
                for region in sorted(dataset_stats[dataset_id].keys()):
                    for step in sorted(dataset_stats[dataset_id][region].keys()):
                        count = dataset_stats[dataset_id][region][step]
                        print(f"    📍 {region}-Step{step}: {count:,} 条")
            else:
                dataset_total = sum(
                    sum(steps.values()) 
                    for steps in dataset_stats[dataset_id].values()
                )
                print(f"  ⏭️  {dataset_id}: {dataset_total:,} 条记录 (跳过)")
        
        if args.dry_run:
            print("\n🔍 模拟运行模式 - 不执行实际迁移")
            return 0
        
        # 2. 性能测试（迁移前）
        if args.test_performance and target_datasets:
            print("\n🔬 迁移前性能测试...")
            test_dataset = target_datasets[0]
            test_regions = list(dataset_stats[test_dataset].keys())
            test_region = test_regions[0] if test_regions else 'USA'
            test_steps = list(dataset_stats[test_dataset][test_region].keys())
            test_step = test_steps[0] if test_steps else 1
            
            old_time = performance_test(main_db, test_dataset, test_region, test_step, "主数据库")
        
        # 3. 执行迁移
        print(f"\n🔄 开始迁移 {len(target_datasets)} 个数据集...")
        
        migration_stats = partitioned_db.migrate_from_main_db(target_datasets)
        
        print("\n✅ 迁移完成!")
        print("📊 迁移统计:")
        total_migrated = 0
        for dataset_id, count in migration_stats.items():
            print(f"  {dataset_id}: {count:,} 条记录")
            total_migrated += count
        print(f"🎯 总计迁移: {total_migrated:,} 条记录")
        
        # 4. 验证迁移结果
        print("\n🔍 验证迁移结果...")
        partition_stats = partitioned_db.get_partition_stats()
        
        for dataset_id in target_datasets:
            if dataset_id in partition_stats:
                info = partition_stats[dataset_id]
                if 'error' not in info:
                    print(f"  ✅ {dataset_id}: {info['total_expressions']:,} 条记录, {info['db_size_mb']} MB")
                else:
                    print(f"  ❌ {dataset_id}: {info['error']}")
        
        # 5. 性能测试（迁移后）
        if args.test_performance and target_datasets:
            print("\n🔬 迁移后性能测试...")
            new_time = performance_test(partitioned_db, test_dataset, test_region, test_step, "分库数据库")
            
            if old_time > 0:
                improvement = ((old_time - new_time) / old_time) * 100
                print(f"\n📈 性能提升: {improvement:.1f}% (从 {old_time:.4f}s 到 {new_time:.4f}s)")
        
        # 6. 清理主数据库（可选）
        if args.cleanup_main:
            print("\n🧹 清理主数据库...")
            
            response = input("⚠️  确认要从主数据库删除已迁移的factor_expressions数据吗? (y/N): ")
            if response.lower() == 'y':
                deleted_count = partitioned_db.cleanup_main_db_expressions(target_datasets)
                print(f"✅ 清理完成: 删除 {deleted_count:,} 条记录")
            else:
                print("⏭️  跳过清理步骤")
        
        print("\n🎉 迁移流程完成!")
        print("\n💡 使用建议:")
        print("  1. 修改代码使用 PartitionedFactorManager 替代 FactorDatabaseManager")
        print("  2. 监控分库性能和存储使用情况")
        print("  3. 考虑定期压缩数据库文件")
        
    except Exception as e:
        print(f"\n❌ 迁移失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # 清理连接
        partitioned_db.close_all_connections()
    
    return 0


if __name__ == "__main__":
    exit(main())
