#!/usr/bin/env python3
"""
查看和分析失败的因子表达式
"""

import os
import sys
import argparse
import pandas as pd
import json
from typing import Optional

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from db_manager import FactorDatabaseManager

def view_failed_expressions(dataset_id: Optional[str] = None, 
                          region: Optional[str] = None,
                          step: Optional[int] = None,
                          failure_reason: Optional[str] = None,
                          limit: int = 50):
    """查看失败的因子表达式"""
    
    # 初始化数据库管理器
    db_path = os.path.join(os.path.dirname(__file__), 'factors.db')
    db = FactorDatabaseManager(db_path)
    
    print("🔍 查询失败的因子表达式...")
    print(f"📊 过滤条件: dataset_id={dataset_id}, region={region}, step={step}")
    print(f"📊 失败原因: {failure_reason}")
    print(f"📊 显示记录数: {limit}")
    print("=" * 80)
    
    # 获取失败表达式列表
    failed_expressions = db.get_failed_expressions(
        dataset_id=dataset_id,
        region=region,
        step=step,
        failure_reason=failure_reason,
        limit=limit
    )
    
    if not failed_expressions:
        print("✅ 没有找到符合条件的失败表达式记录")
        return
    
    # 转换为DataFrame以便更好地显示
    df = pd.DataFrame(failed_expressions)
    
    print(f"📋 找到 {len(failed_expressions)} 条失败记录:\n")
    
    # 解析error_details中的message作为主要失败原因
    def extract_error_message(error_details):
        """从error_details中提取message（支持JSON和Python dict格式）"""
        if not error_details:
            return "Unknown error"
        try:
            # 首先尝试JSON解析
            error_data = json.loads(error_details)
            return error_data.get('message', 'Unknown error')
        except (json.JSONDecodeError, TypeError):
            try:
                # 如果JSON解析失败，尝试使用eval解析Python字典格式
                # 注意：这里使用eval是因为数据来自可信的数据库
                if isinstance(error_details, str) and error_details.startswith('{'):
                    error_data = eval(error_details)
                    if isinstance(error_data, dict):
                        return error_data.get('message', 'Unknown error')
                # 如果已经是字典
                elif isinstance(error_details, dict):
                    return error_details.get('message', 'Unknown error')
            except:
                pass
            # 如果都失败了，返回原始字符串的前100个字符
            return str(error_details)[:100] if error_details else "Unknown error"
    
    # 添加提取的错误消息列
    df['error_message'] = df['error_details'].apply(extract_error_message)
    
    # 按错误消息分组显示
    for message in df['error_message'].unique():
        message_df = df[df['error_message'] == message]
        print(f"\n🔥 错误消息: {message} ({len(message_df)} 条)")
        print("-" * 80)
        
        for idx, row in message_df.head(10).iterrows():  # 每个错误最多显示10条
            print(f"📅 {row['created_at']}")
            print(f"🎯 数据集: {row['dataset_id']} | 地区: {row['region']} | 步骤: {row['step']}")
            print(f"📝 表达式: {row['expression'][:120]}{'...' if len(row['expression']) > 120 else ''}")
            
            # 显示完整的API响应（如果有的话）
            if row['error_details']:
                try:
                    error_data = json.loads(row['error_details'])
                    if 'id' in error_data:
                        print(f"🔗 模拟ID: {error_data['id']}")
                    if 'type' in error_data:
                        print(f"📊 类型: {error_data['type']}")
                    if 'status' in error_data:
                        print(f"⚠️  状态: {error_data['status']}")
                except (json.JSONDecodeError, TypeError):
                    print(f"💬 原始错误: {row['error_details'][:150]}{'...' if len(str(row['error_details'])) > 150 else ''}")
            print()
        
        if len(message_df) > 10:
            print(f"   ... 还有 {len(message_df) - 10} 条记录（使用 --limit 参数查看更多）")

def show_failure_stats():
    """显示失败统计信息"""
    
    # 初始化数据库管理器
    db_path = os.path.join(os.path.dirname(__file__), 'factors.db')
    db = FactorDatabaseManager(db_path)
    
    print("📊 失败表达式统计信息")
    print("=" * 60)
    
    # 获取所有失败表达式进行自定义统计
    failed_expressions = db.get_failed_expressions(limit=10000)
    
    if not failed_expressions:
        print("✅ 没有找到失败表达式记录")
        return
    
    df = pd.DataFrame(failed_expressions)
    
    # 解析error_details中的message
    def extract_error_message(error_details):
        if not error_details:
            return "Unknown error"
        try:
            # 首先尝试JSON解析
            error_data = json.loads(error_details)
            return error_data.get('message', 'Unknown error')
        except (json.JSONDecodeError, TypeError):
            try:
                # 如果JSON解析失败，尝试使用eval解析Python字典格式
                if isinstance(error_details, str) and error_details.startswith('{'):
                    error_data = eval(error_details)
                    if isinstance(error_data, dict):
                        return error_data.get('message', 'Unknown error')
                # 如果已经是字典
                elif isinstance(error_details, dict):
                    return error_details.get('message', 'Unknown error')
            except:
                pass
            # 如果都失败了，返回原始字符串的前100个字符
            return str(error_details)[:100] if error_details else "Unknown error"
    
    df['error_message'] = df['error_details'].apply(extract_error_message)
    
    print(f"🔢 总失败记录数: {len(df)}")
    print(f"🔢 唯一失败表达式数: {df['expression'].nunique()}")
    
    # 最近24小时的失败数
    recent_failures = len(df[df['created_at'] >= (pd.Timestamp.now() - pd.Timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')])
    print(f"🔢 最近24小时失败数: {recent_failures}")
    
    print("\n📋 按具体错误消息统计 (Top 10):")
    error_message_stats = df.groupby('error_message').agg({
        'expression': ['count', 'nunique'],
        'dataset_id': 'nunique'
    }).round(2)
    error_message_stats.columns = ['total_count', 'unique_expressions', 'affected_datasets']
    error_message_stats = error_message_stats.sort_values('total_count', ascending=False).head(10)
    
    for error_msg, row in error_message_stats.iterrows():
        # 确保错误消息不被截断
        display_msg = error_msg if len(error_msg) <= 80 else error_msg[:77] + "..."
        print(f"   {display_msg}: {int(row['total_count'])} 次, {int(row['unique_expressions'])} 个唯一表达式, {int(row['affected_datasets'])} 个数据集")
    
    print("\n📋 按数据集统计 (Top 10):")
    dataset_stats = df.groupby(['dataset_id', 'region', 'step']).agg({
        'expression': ['count', 'nunique'],
        'error_message': 'nunique'
    }).round(2)
    dataset_stats.columns = ['total_count', 'unique_expressions', 'failure_types']
    dataset_stats = dataset_stats.sort_values('total_count', ascending=False).head(10)
    
    for (dataset_id, region, step), row in dataset_stats.iterrows():
        print(f"   {dataset_id} ({region}, Step {step}): {int(row['total_count'])} 次失败, {int(row['unique_expressions'])} 个唯一表达式, {int(row['failure_types'])} 种失败类型")
    
    print("\n📊 表达式长度统计:")
    expr_lengths = df['expression'].str.len()
    print(f"   平均长度: {expr_lengths.mean():.1f} 字符")
    print(f"   最短长度: {expr_lengths.min()} 字符") 
    print(f"   最长长度: {expr_lengths.max()} 字符")
    
    print("\n🔍 最常见的错误模式:")
    # 分析错误消息的模式
    error_patterns = {}
    for msg in df['error_message'].unique():
        if 'Invalid data field' in msg:
            error_patterns['Invalid data field'] = error_patterns.get('Invalid data field', 0) + len(df[df['error_message'] == msg])
        elif 'syntax' in msg.lower():
            error_patterns['Syntax error'] = error_patterns.get('Syntax error', 0) + len(df[df['error_message'] == msg])
        elif 'undefined' in msg.lower() or 'unknown' in msg.lower():
            error_patterns['Undefined/Unknown'] = error_patterns.get('Undefined/Unknown', 0) + len(df[df['error_message'] == msg])
        elif 'end of input' in msg.lower():
            error_patterns['Unexpected end of input'] = error_patterns.get('Unexpected end of input', 0) + len(df[df['error_message'] == msg])
        else:
            error_patterns['Other'] = error_patterns.get('Other', 0) + len(df[df['error_message'] == msg])
    
    for pattern, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(df)) * 100
        print(f"   {pattern}: {count} 次 ({percentage:.1f}%)")

def export_failed_expressions(output_file: str, **filters):
    """导出失败表达式到CSV文件"""
    
    # 初始化数据库管理器
    db_path = os.path.join(os.path.dirname(__file__), 'factors.db')
    db = FactorDatabaseManager(db_path)
    
    print(f"📤 导出失败表达式到 {output_file}...")
    
    # 获取所有失败表达式（不限制数量）
    failed_expressions = db.get_failed_expressions(limit=10000, **filters)
    
    if not failed_expressions:
        print("❌ 没有找到失败表达式记录")
        return
    
    # 转换为DataFrame并解析错误消息
    df = pd.DataFrame(failed_expressions)
    
    # 解析error_details中的message
    def extract_error_message(error_details):
        if not error_details:
            return "Unknown error"
        try:
            # 首先尝试JSON解析
            error_data = json.loads(error_details)
            return error_data.get('message', 'Unknown error')
        except (json.JSONDecodeError, TypeError):
            try:
                # 如果JSON解析失败，尝试使用eval解析Python字典格式
                if isinstance(error_details, str) and error_details.startswith('{'):
                    error_data = eval(error_details)
                    if isinstance(error_data, dict):
                        return error_data.get('message', 'Unknown error')
                # 如果已经是字典
                elif isinstance(error_details, dict):
                    return error_details.get('message', 'Unknown error')
            except:
                pass
            # 如果都失败了，返回原始字符串的前100个字符
            return str(error_details)[:100] if error_details else "Unknown error"
    
    def extract_simulation_id(error_details):
        if not error_details:
            return ""
        try:
            # 首先尝试JSON解析
            error_data = json.loads(error_details)
            return error_data.get('id', '')
        except (json.JSONDecodeError, TypeError):
            try:
                # 如果JSON解析失败，尝试使用eval解析Python字典格式
                if isinstance(error_details, str) and error_details.startswith('{'):
                    error_data = eval(error_details)
                    if isinstance(error_data, dict):
                        return error_data.get('id', '')
                # 如果已经是字典
                elif isinstance(error_details, dict):
                    return error_details.get('id', '')
            except:
                pass
            return ""
    
    # 添加解析后的列
    df['error_message'] = df['error_details'].apply(extract_error_message)
    df['simulation_id'] = df['error_details'].apply(extract_simulation_id)
    
    # 重新排列列顺序，将重要信息放在前面
    column_order = ['created_at', 'dataset_id', 'region', 'step', 'error_message', 
                    'expression', 'failure_reason', 'simulation_id', 'error_details']
    df = df.reindex(columns=column_order)
    
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"✅ 成功导出 {len(failed_expressions)} 条记录到 {output_file}")
    print(f"📋 导出文件包含以下列: {', '.join(df.columns.tolist())}")

def cleanup_old_failures(days: int = 30):
    """清理旧的失败记录"""
    
    # 初始化数据库管理器
    db_path = os.path.join(os.path.dirname(__file__), 'factors.db')
    db = FactorDatabaseManager(db_path)
    
    print(f"🧹 清理 {days} 天前的失败记录...")
    
    deleted_count = db.cleanup_old_failed_expressions(days)
    
    print(f"✅ 已清理 {deleted_count} 条旧记录")

def main():
    parser = argparse.ArgumentParser(description='查看和分析失败的因子表达式')
    parser.add_argument('--action', choices=['view', 'stats', 'export', 'cleanup'], 
                       default='view', help='操作类型')
    parser.add_argument('--dataset-id', help='数据集ID过滤')
    parser.add_argument('--region', help='地区过滤')
    parser.add_argument('--step', type=int, help='步骤过滤')
    parser.add_argument('--failure-reason', help='失败原因过滤')
    parser.add_argument('--limit', type=int, default=50, help='显示记录数限制')
    parser.add_argument('--output', help='导出文件路径 (用于export)')
    parser.add_argument('--days', type=int, default=30, help='清理天数 (用于cleanup)')
    
    args = parser.parse_args()
    
    if args.action == 'view':
        view_failed_expressions(
            dataset_id=args.dataset_id,
            region=args.region,
            step=args.step,
            failure_reason=args.failure_reason,
            limit=args.limit
        )
    elif args.action == 'stats':
        show_failure_stats()
    elif args.action == 'export':
        if not args.output:
            print("❌ 导出操作需要指定 --output 参数")
            return
        export_failed_expressions(
            args.output,
            dataset_id=args.dataset_id,
            region=args.region,
            step=args.step,
            failure_reason=args.failure_reason
        )
    elif args.action == 'cleanup':
        cleanup_old_failures(args.days)

if __name__ == '__main__':
    main()
