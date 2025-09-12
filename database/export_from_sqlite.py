#!/usr/bin/env python3
"""
作者：e.e.
日期：2025.09.10
功能：将SQLite数据库数据导出到文本文件格式
"""

import os
import sys
import sqlite3
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import RECORDS_PATH, ROOT_PATH

class FactorDataExporter:
    def __init__(self, db_path='database/factors.db'):
        """初始化导出器"""
        self.db_path = os.path.join(ROOT_PATH, db_path)
        self.records_path = RECORDS_PATH
        self.conn = None
        
        # 确保导出目录存在
        os.makedirs(self.records_path, exist_ok=True)
        
    def connect_db(self):
        """连接数据库"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            print(f"✅ 成功连接数据库: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
            
    def export_factor_expressions(self):
        """导出因子表达式数据"""
        print("\n🔄 开始导出因子表达式数据...")
        total_exported = 0
        
        try:
            # 查询所有表达式数据，按数据集分组
            cursor = self.conn.execute("""
                SELECT dataset_id, region, step, expression 
                FROM factor_expressions 
                ORDER BY dataset_id, region, step, created_at
            """)
            
            # 按文件分组数据
            file_groups = {}
            for row in cursor.fetchall():
                dataset_id, region, step, expression = row
                filename = f"{dataset_id}_{region.lower()}_{step}step_simulated_alpha_expression.txt"
                
                if filename not in file_groups:
                    file_groups[filename] = []
                file_groups[filename].append(expression)
            
            # 写入文件
            for filename, expressions in file_groups.items():
                file_path = os.path.join(self.records_path, filename)
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        for expression in expressions:
                            f.write(expression + '\n')
                    
                    count = len(expressions)
                    total_exported += count
                    print(f"✅ {filename}: 导出 {count} 条表达式")
                    
                except Exception as e:
                    print(f"❌ 导出文件 {filename} 失败: {e}")
            
            print(f"📊 因子表达式导出完成，总计: {total_exported} 条")
            
        except Exception as e:
            print(f"❌ 导出因子表达式失败: {e}")
        
    def export_checked_alphas(self):
        """导出已检查因子数据"""
        print("\n🔄 开始导出已检查因子数据...")
        total_exported = 0
        
        try:
            # 查询所有已检查数据，按数据集分组
            cursor = self.conn.execute("""
                SELECT dataset_id, region, step, alpha_id 
                FROM checked_alphas 
                ORDER BY dataset_id, region, step, checked_at
            """)
            
            # 按文件分组数据
            file_groups = {}
            for row in cursor.fetchall():
                dataset_id, region, step, alpha_id = row
                filename = f"{dataset_id}_{region.lower()}_{step}step_checked_alpha_id.txt"
                
                if filename not in file_groups:
                    file_groups[filename] = []
                file_groups[filename].append(alpha_id)
            
            # 写入文件
            for filename, alpha_ids in file_groups.items():
                file_path = os.path.join(self.records_path, filename)
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        for alpha_id in alpha_ids:
                            f.write(alpha_id + '\n')
                    
                    count = len(alpha_ids)
                    total_exported += count
                    print(f"✅ {filename}: 导出 {count} 个Alpha ID")
                    
                except Exception as e:
                    print(f"❌ 导出文件 {filename} 失败: {e}")
            
            print(f"📊 已检查因子导出完成，总计: {total_exported} 条")
            
        except Exception as e:
            print(f"❌ 导出已检查因子失败: {e}")
        
    def export_submitable_alphas(self):
        """导出可提交因子数据"""
        print("\n🔄 开始导出可提交因子数据...")
        
        try:
            # 查询所有可提交因子数据
            df = pd.read_sql_query("""
                SELECT * FROM submitable_alphas 
                ORDER BY created_at
            """, self.conn)
            
            if df.empty:
                print("⚠️  数据库中没有可提交因子数据")
                return
            
            # 转换列名：下划线转驼峰（反向映射）
            column_mapping = {}
            for col in df.columns:
                # 将下划线命名转换为驼峰命名
                camel_case = ''.join(word.capitalize() if i > 0 else word for i, word in enumerate(col.split('_')))
                column_mapping[col] = camel_case
            
            df = df.rename(columns=column_mapping)
            
            # 特殊处理：alpha_id -> id
            if 'alpha_id' in df.columns:
                df = df.rename(columns={'alpha_id': 'id'})
            
            # 处理复杂字段：JSON字符串转回对象
            complex_fields = ['tags', 'checks', 'os', 'train', 'test', 'prod', 
                           'competitions', 'themes', 'team', 'pyramids', 'classifications']
            
            for field in complex_fields:
                if field in df.columns:
                    # 尝试解析JSON字符串
                    def parse_json(value):
                        if pd.isna(value) or value == '':
                            return []
                        try:
                            return json.loads(value)
                        except:
                            return value
                    
                    df[field] = df[field].apply(parse_json)
            
            # 写入CSV文件
            csv_path = os.path.join(self.records_path, 'submitable_alpha.csv')
            df.to_csv(csv_path, index=False)
            
            count = len(df)
            print(f"✅ 可提交因子导出完成: {count} 条 -> submitable_alpha.csv")
            
        except Exception as e:
            print(f"❌ 导出可提交因子失败: {e}")
            
    def export_config(self):
        """导出配置数据"""
        print("\n🔄 开始导出配置数据...")
        
        try:
            # 查询系统配置
            cursor = self.conn.execute("""
                SELECT config_key, config_value FROM system_config
            """)
            
            configs = dict(cursor.fetchall())
            
            # 导出开始日期
            if 'start_date' in configs:
                start_date_path = os.path.join(self.records_path, 'start_date.txt')
                with open(start_date_path, 'w', encoding='utf-8') as f:
                    f.write(configs['start_date'])
                print(f"✅ 开始日期配置导出完成: {configs['start_date']}")
            
            # 导出其他配置（可选）
            for key, value in configs.items():
                if key != 'start_date':
                    config_file = os.path.join(self.records_path, f'{key}.txt')
                    with open(config_file, 'w', encoding='utf-8') as f:
                        f.write(str(value))
                    print(f"✅ 配置 {key} 导出完成")
                    
        except Exception as e:
            print(f"❌ 导出配置数据失败: {e}")
        
    def export_database_stats(self):
        """导出数据库统计信息"""
        print("\n📊 数据库统计信息:")
        
        try:
            # 查询各表数据量
            tables = ['factor_expressions', 'checked_alphas', 'submitable_alphas', 'system_config']
            
            for table in tables:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} 条记录")
            
            # 显示数据分布统计
            print("\n📈 数据分布统计:")
            cursor = self.conn.execute("SELECT * FROM system_overview")
            for row in cursor.fetchall():
                print(f"  {row[0]}: {row[1]} 条记录，最新更新: {row[2]}")
                
        except Exception as e:
            print(f"❌ 获取数据库统计失败: {e}")
            
    def create_backup_info(self):
        """创建备份信息文件"""
        try:
            backup_info = {
                "export_time": datetime.now().isoformat(),
                "database_path": self.db_path,
                "records_path": self.records_path,
                "export_type": "database_to_files",
                "description": "从SQLite数据库导出到文本文件格式"
            }
            
            backup_info_path = os.path.join(self.records_path, 'export_info.json')
            with open(backup_info_path, 'w', encoding='utf-8') as f:
                json.dump(backup_info, f, indent=2, ensure_ascii=False)
            
            print(f"✅ 导出信息已保存到: export_info.json")
            
        except Exception as e:
            print(f"❌ 创建备份信息失败: {e}")
            
    def run_export(self):
        """执行完整导出流程"""
        print("🚀 开始数据导出...")
        
        if not self.connect_db():
            return False
            
        try:
            # 1. 导出各类数据
            self.export_factor_expressions()
            self.export_checked_alphas()
            self.export_submitable_alphas()
            self.export_config()
            
            # 2. 显示统计信息
            self.export_database_stats()
            
            # 3. 创建备份信息
            self.create_backup_info()
            
            print("\n✅ 数据导出完成！")
            print(f"📍 导出位置: {self.records_path}")
            print("📝 注意: 导出的文件可以用于服务器迁移或数据备份")
            
            return True
            
        except Exception as e:
            print(f"❌ 导出过程中发生错误: {e}")
            return False
            
        finally:
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    print("=" * 60)
    print("  WorldQuant 因子系统数据导出工具")
    print("  从SQLite数据库导出到文本文件格式")
    print("=" * 60)
    
    exporter = FactorDataExporter()
    success = exporter.run_export()
    
    if success:
        print("\n🎉 导出成功！数据已保存到文本文件格式。")
        print("📋 导出文件包括:")
        print("  • *_simulated_alpha_expression.txt - 因子表达式")
        print("  • *_checked_alpha_id.txt - 已检查因子")
        print("  • submitable_alpha.csv - 可提交因子")
        print("  • start_date.txt - 开始日期配置")
        print("  • export_info.json - 导出信息")
    else:
        print("\n💥 导出失败，请检查错误信息并重试。")
        sys.exit(1)

if __name__ == "__main__":
    main() 