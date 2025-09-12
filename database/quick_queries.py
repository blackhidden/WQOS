#!/usr/bin/env python3
"""
作者：e.e.
日期：2025.09.10
功能：快速查询脚本，提供常用的数据库查询命令
"""

import os
import sys
import argparse

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from db_manager import FactorDatabaseManager

def show_stats():
    """显示统计信息"""
    print("📊 数据库统计信息")
    print("="*40)
    
    db = FactorDatabaseManager()
    
    stats = db.get_system_stats()
    
    print(f"📈 总数统计:")
    print(f"  - 因子表达式: {stats.get('total_expressions', 0):,} 条")
    print(f"  - 已检查因子: {stats.get('total_checked', 0):,} 个")
    print(f"  - 可提交因子: {stats.get('total_submitable', 0):,} 个")
    
    print(f"\n📋 各数据集分布:")
    breakdown = stats.get('expression_breakdown', [])
    for dataset_id, region, step, count in breakdown:
        print(f"  - {dataset_id}_{region}_{step}step: {count:,} 条")

def show_latest(limit=10):
    """显示最新的记录"""
    print(f"🕒 最新 {limit} 条记录")
    print("="*40)
    
    db = FactorDatabaseManager()
    
    try:
        with db.get_connection() as conn:
            # 最新的因子表达式
            print("📝 最新因子表达式:")
            cursor = conn.execute("""
                SELECT expression, dataset_id, region, step, created_at
                FROM factor_expressions 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,))
            
            for i, (expr, dataset_id, region, step, created) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2d}. {expr[:50]}{'...' if len(expr) > 50 else ''}")
                print(f"      [{dataset_id}_{region}_{step}step] {created}")
            
            # 最新的检查记录
            print(f"\n✅ 最新已检查因子:")
            cursor = conn.execute("""
                SELECT alpha_id, dataset_id, region, step, checked_at
                FROM checked_alphas 
                ORDER BY checked_at DESC 
                LIMIT ?
            """, (limit,))
            
            for i, (alpha_id, dataset_id, region, step, checked) in enumerate(cursor.fetchall(), 1):
                print(f"  {i:2d}. {alpha_id} [{dataset_id}_{region}_{step}step] {checked}")
                
    except Exception as e:
        print(f"❌ 查询失败: {e}")

def search_expressions(keyword, limit=10):
    """搜索因子表达式"""
    print(f"🔍 搜索包含 '{keyword}' 的因子表达式")
    print("="*40)
    
    db = FactorDatabaseManager()
    
    try:
        with db.get_connection() as conn:
            cursor = conn.execute("""
                SELECT expression, dataset_id, region, step, created_at
                FROM factor_expressions 
                WHERE expression LIKE ?
                ORDER BY created_at DESC 
                LIMIT ?
            """, (f"%{keyword}%", limit))
            
            results = cursor.fetchall()
            if not results:
                print(f"❌ 未找到包含 '{keyword}' 的表达式")
                return
            
            print(f"📋 找到 {len(results)} 条结果:")
            for i, (expr, dataset_id, region, step, created) in enumerate(results, 1):
                print(f"  {i:2d}. {expr}")
                print(f"      [{dataset_id}_{region}_{step}step] {created}")
                
    except Exception as e:
        print(f"❌ 搜索失败: {e}")

def show_dataset_info(dataset_id, region="", step=None):
    """显示指定数据集信息"""
    filter_desc = f"{dataset_id}"
    if region:
        filter_desc += f"_{region}"
    if step is not None:
        filter_desc += f"_{step}step"
    
    print(f"📊 数据集信息: {filter_desc}")
    print("="*40)
    
    db = FactorDatabaseManager()
    
    try:
        with db.get_connection() as conn:
            # 构建查询条件
            conditions = ["dataset_id = ?"]
            params = [dataset_id]
            
            if region:
                conditions.append("region = ?")
                params.append(region)
            
            if step is not None:
                conditions.append("step = ?")
                params.append(step)
            
            where_clause = " AND ".join(conditions)
            
            # 表达式统计
            cursor = conn.execute(f"""
                SELECT COUNT(*), MIN(created_at), MAX(created_at)
                FROM factor_expressions 
                WHERE {where_clause}
            """, params)
            
            expr_count, first_created, last_created = cursor.fetchone()
            print(f"📝 因子表达式: {expr_count:,} 条")
            if first_created:
                print(f"   首次创建: {first_created}")
                print(f"   最近创建: {last_created}")
            
            # 检查统计
            cursor = conn.execute(f"""
                SELECT COUNT(*), MIN(checked_at), MAX(checked_at)
                FROM checked_alphas 
                WHERE {where_clause}
            """, params)
            
            checked_count, first_checked, last_checked = cursor.fetchone()
            print(f"\n✅ 已检查因子: {checked_count:,} 个")
            if first_checked:
                print(f"   首次检查: {first_checked}")
                print(f"   最近检查: {last_checked}")
            
            # 检查率
            if expr_count > 0:
                check_rate = checked_count * 100.0 / expr_count
                print(f"\n📊 检查率: {check_rate:.1f}%")
            
            # 常用操作符统计
            print(f"\n🔧 常用操作符统计:")
            operators = ['ts_rank', 'ts_mean', 'ts_sum', 'rank', 'winsorize']
            
            for op in operators:
                cursor = conn.execute(f"""
                    SELECT COUNT(*) FROM factor_expressions 
                    WHERE {where_clause} AND expression LIKE ?
                """, params + [f"%{op}%"])
                
                op_count = cursor.fetchone()[0]
                if op_count > 0:
                    percentage = op_count * 100.0 / expr_count if expr_count > 0 else 0
                    print(f"   {op}: {op_count:,} 次 ({percentage:.1f}%)")
                
    except Exception as e:
        print(f"❌ 查询失败: {e}")

def show_daily_stats(limit=30, timezone_offset=None):
    """显示每日因子表达式插入统计"""
    
    # 时区说明
    tz_info = ""
    if timezone_offset is not None:
        if timezone_offset > 0:
            tz_info = f" (UTC+{timezone_offset})"
        elif timezone_offset < 0:
            tz_info = f" (UTC{timezone_offset})"
        else:
            tz_info = " (UTC)"
    else:
        tz_info = " (本地时区)"
    
    print(f"📅 每日因子表达式插入统计 (最近 {limit} 天){tz_info}")
    print("="*60)
    
    db = FactorDatabaseManager()
    
    try:
        with db.get_connection() as conn:
            # 构建时区转换的SQL
            if timezone_offset is not None:
                # 使用指定时区偏移
                date_expr = f"DATE(created_at, '{timezone_offset:+d} hours')"
            else:
                # 使用本地时区（系统默认）
                date_expr = "DATE(created_at, 'localtime')"
            
            cursor = conn.execute(f"""
                SELECT 
                    {date_expr} as date,
                    COUNT(*) as total_expressions,
                    COUNT(DISTINCT dataset_id) as unique_datasets,
                    COUNT(DISTINCT region) as unique_regions
                FROM factor_expressions 
                GROUP BY {date_expr}
                ORDER BY date DESC 
                LIMIT ?
            """, (limit,))
            
            results = cursor.fetchall()
            if not results:
                print("❌ 未找到任何因子表达式记录")
                return
            
            print(f"{'日期':<12} {'表达式数量':<10} {'数据集数':<8} {'地区数':<6}")
            print("-" * 60)
            
            total_expressions = 0
            for date, expr_count, dataset_count, region_count in results:
                total_expressions += expr_count
                print(f"{date:<12} {expr_count:<10,} {dataset_count:<8} {region_count:<6}")
            
            print("-" * 60)
            print(f"{'总计':<12} {total_expressions:<10,}")
            
            # 显示趋势信息
            if len(results) >= 2:
                recent_avg = sum(row[1] for row in results[:7]) / min(7, len(results))
                print(f"\n📊 最近平均每日: {recent_avg:.1f} 个表达式")
            
            # 时区说明
            if timezone_offset == -4:
                print(f"\n🌍 使用美国东部时间 (UTC-4)，与WorldQuant平台一致")
            elif timezone_offset is None:
                print(f"\n🌍 使用本地系统时区，可能与平台时区不同")
                print(f"💡 建议使用: --et (美国东部时间) 与平台保持一致")
                
    except Exception as e:
        print(f"❌ 查询失败: {e}")

def show_submitable():
    """显示可提交因子"""
    print("🚀 可提交因子列表")
    print("="*40)
    
    db = FactorDatabaseManager()
    df = db.get_submitable_alphas()
    
    if df.empty:
        print("❌ 当前没有可提交的因子")
        return
    
    print(f"📋 共有 {len(df)} 个可提交因子:")
    
    # 显示关键列
    key_columns = ['alpha_id', 'sharpe', 'fitness', 'turnover', 'region', 'universe']
    available_columns = [col for col in key_columns if col in df.columns]
    
    if available_columns:
        print(df[available_columns].to_string(index=False))
    else:
        print(df.to_string(index=False))

def show_daily_submit_limit():
    """显示每日提交限额状态"""
    print("📅 每日提交限额状态")
    print("="*50)
    
    db = FactorDatabaseManager()
    
    try:
        # 导入配置和时区函数
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
        from machine_lib_ee import load_digging_config, get_current_date_with_timezone
        
        # 加载配置
        config = load_digging_config()
        daily_limit = config.get('daily_submit_limit', 0)
        limit_timezone = config.get('daily_limit_timezone', 'UTC')
        limit_behavior = config.get('daily_limit_behavior', 'wait')
        
        print(f"⚙️  配置信息:")
        print(f"  - 每日限额: {daily_limit} 个/天" + (" (无限制)" if daily_limit == 0 else ""))
        print(f"  - 时区设置: {limit_timezone}")
        print(f"  - 限额行为: {limit_behavior}")
        
        if daily_limit > 0:
            # 获取当前日期
            current_date = get_current_date_with_timezone(limit_timezone)
            
            # 获取今日统计
            daily_stats = db.get_daily_submit_stats(current_date, limit_timezone)
            today_successful = daily_stats['successful_submits']
            today_attempts = daily_stats['total_attempts']
            remaining_quota = daily_limit - today_successful
            
            print(f"\n📊 今日状态 [{current_date}]:")
            print(f"  - ✅ 成功提交: {today_successful} 个")
            print(f"  - 🎯 总尝试数: {today_attempts} 个")
            print(f"  - 📈 成功率: {today_successful/today_attempts*100:.1f}%" if today_attempts > 0 else "  - 📈 成功率: N/A")
            print(f"  - 🔄 剩余配额: {remaining_quota} 个")
            
            # 状态指示
            if remaining_quota <= 0:
                print(f"  - ⚠️  状态: 限额已用完")
            elif remaining_quota <= daily_limit * 0.2:  # 少于20%
                print(f"  - 🔸 状态: 配额不足")
            else:
                print(f"  - ✅ 状态: 配额充足")
        
        # 获取最近几天的统计
        print(f"\n📈 最近7天统计:")
        recent_stats = db.get_recent_daily_stats(7)
        if recent_stats:
            print(f"{'日期':>12} {'成功':>6} {'尝试':>6} {'成功率':>8} {'剩余配额':>8}")
            print("-" * 50)
            for stat in recent_stats:
                date = stat['date']
                successful = stat['successful_submits']
                attempts = stat['total_attempts']
                success_rate = stat['success_rate']
                remaining = daily_limit - successful if daily_limit > 0 else "无限制"
                
                print(f"{date:>12} {successful:>6} {attempts:>6} {success_rate:>7}% {str(remaining):>8}")
        else:
            print("  暂无历史数据")
    
    except Exception as e:
        print(f"❌ 获取每日限额状态失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='WorldQuant 因子数据库快速查询工具')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # stats命令
    stats_parser = subparsers.add_parser('stats', help='显示数据库统计信息')
    
    # latest命令
    latest_parser = subparsers.add_parser('latest', help='显示最新记录')
    latest_parser.add_argument('--limit', type=int, default=10, help='显示数量 (默认10)')
    
    # search命令
    search_parser = subparsers.add_parser('search', help='搜索因子表达式')
    search_parser.add_argument('keyword', help='搜索关键词')
    search_parser.add_argument('--limit', type=int, default=10, help='显示数量 (默认10)')
    
    # dataset命令
    dataset_parser = subparsers.add_parser('dataset', help='显示数据集信息')
    dataset_parser.add_argument('dataset_id', help='数据集ID (如: analyst4)')
    dataset_parser.add_argument('--region', help='地区 (如: USA)')
    dataset_parser.add_argument('--step', type=int, choices=[1, 2], help='步骤 (1或2)')
    
    # submitable命令
    submitable_parser = subparsers.add_parser('submitable', help='显示可提交因子')
    
    # daily命令
    daily_parser = subparsers.add_parser('daily', help='显示每日因子表达式插入统计')
    daily_parser.add_argument('--limit', type=int, default=30, help='显示天数 (默认30天)')
    daily_parser.add_argument('--et', action='store_true', help='使用美国东部时间 (UTC-4)，与平台一致')
    daily_parser.add_argument('--utc', action='store_true', help='使用UTC时间')
    daily_parser.add_argument('--tz', type=int, help='指定时区偏移 (小时)，如 +8 或 -4')
    
    # daily-limit命令
    daily_limit_parser = subparsers.add_parser('daily-limit', help='显示每日提交限额状态')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'stats':
            show_stats()
        elif args.command == 'latest':
            show_latest(args.limit)
        elif args.command == 'search':
            search_expressions(args.keyword, args.limit)
        elif args.command == 'dataset':
            show_dataset_info(args.dataset_id, args.region or "", args.step)
        elif args.command == 'submitable':
            show_submitable()
        elif args.command == 'daily':
            # 解析时区参数
            timezone_offset = None
            if args.et:
                timezone_offset = -4  # 美国东部时间
            elif args.utc:
                timezone_offset = 0   # UTC时间
            elif args.tz is not None:
                timezone_offset = args.tz  # 用户指定的时区
            
            show_daily_stats(args.limit, timezone_offset)
        elif args.command == 'daily-limit':
            show_daily_submit_limit()
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()