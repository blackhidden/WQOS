#!/usr/bin/env python3
"""
作者：e.e.
日期：2025.09.10
功能：性能测试脚本，对比文件操作和数据库操作的性能
"""

import os
import sys
import time
import random
import pandas as pd
from typing import List
import tempfile
import shutil

# 添加src目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import RECORDS_PATH
from database.db_manager import FactorDatabaseManager

class PerformanceTester:
    """性能测试器"""
    
    def __init__(self):
        self.db_manager = FactorDatabaseManager('database/test_factors.db')
        self.temp_dir = tempfile.mkdtemp()
        self.test_data_size = 1000  # 测试数据量
        
    def cleanup(self):
        """清理测试环境"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # 删除测试数据库
        test_db_path = os.path.join(os.path.dirname(__file__), '..', 'database', 'test_factors.db')
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
    
    def generate_test_data(self) -> tuple:
        """生成测试数据"""
        # 生成测试因子表达式
        expressions = []
        for i in range(self.test_data_size):
            expressions.append(f"ts_rank(close, {random.randint(5, 50)}) + ts_mean(volume, {random.randint(10, 100)})")
        
        # 生成测试Alpha ID
        alpha_ids = [f"TEST{i:06d}" for i in range(self.test_data_size)]
        
        # 生成测试可提交因子数据
        submitable_data = []
        for i in range(100):  # 较少的可提交因子
            submitable_data.append({
                'alpha_id': f"SUB{i:06d}",
                'sharpe': random.uniform(1.0, 3.0),
                'fitness': random.uniform(0.5, 1.0),
                'turnover': random.uniform(0.1, 0.5),
                'region': 'USA',
                'universe': 'TOP3000'
            })
        
        return expressions, alpha_ids, submitable_data
    
    def test_file_operations(self, expressions: List[str], alpha_ids: List[str], submitable_data: List[dict]) -> dict:
        """测试文件操作性能"""
        results = {}
        
        # 1. 测试写入因子表达式
        start_time = time.time()
        expr_file = os.path.join(self.temp_dir, 'test_expressions.txt')
        with open(expr_file, 'w') as f:
            for expr in expressions:
                f.write(expr + '\n')
        results['file_write_expressions'] = time.time() - start_time
        
        # 2. 测试读取因子表达式
        start_time = time.time()
        with open(expr_file, 'r') as f:
            loaded_expressions = [line.strip() for line in f if line.strip()]
        results['file_read_expressions'] = time.time() - start_time
        
        # 3. 测试检查表达式是否存在
        start_time = time.time()
        test_expr = expressions[len(expressions)//2]  # 中间的表达式
        exists = test_expr in loaded_expressions
        results['file_check_expression_exists'] = time.time() - start_time
        
        # 4. 测试写入Alpha ID
        start_time = time.time()
        alpha_file = os.path.join(self.temp_dir, 'test_alphas.txt')
        with open(alpha_file, 'w') as f:
            for alpha_id in alpha_ids:
                f.write(alpha_id + '\n')
        results['file_write_alphas'] = time.time() - start_time
        
        # 5. 测试读取Alpha ID
        start_time = time.time()
        with open(alpha_file, 'r') as f:
            loaded_alphas = [line.strip() for line in f if line.strip()]
        results['file_read_alphas'] = time.time() - start_time
        
        # 6. 测试CSV操作
        start_time = time.time()
        csv_file = os.path.join(self.temp_dir, 'test_submitable.csv')
        df = pd.DataFrame(submitable_data)
        df.to_csv(csv_file, index=False)
        results['file_write_csv'] = time.time() - start_time
        
        start_time = time.time()
        loaded_df = pd.read_csv(csv_file)
        results['file_read_csv'] = time.time() - start_time
        
        # 7. 测试CSV查询
        start_time = time.time()
        filtered_df = loaded_df[loaded_df['sharpe'] > 2.0]
        results['file_filter_csv'] = time.time() - start_time
        
        return results
    
    def test_database_operations(self, expressions: List[str], alpha_ids: List[str], submitable_data: List[dict]) -> dict:
        """测试数据库操作性能"""
        results = {}
        
        # 1. 测试批量写入因子表达式
        start_time = time.time()
        self.db_manager.add_factor_expressions_batch(expressions, 'test_dataset', 'USA', 1)
        results['db_write_expressions'] = time.time() - start_time
        
        # 2. 测试读取因子表达式
        start_time = time.time()
        loaded_expressions = self.db_manager.get_factor_expressions('test_dataset', 'USA', 1)
        results['db_read_expressions'] = time.time() - start_time
        
        # 3. 测试检查表达式是否存在
        start_time = time.time()
        test_expr = expressions[len(expressions)//2]
        exists = self.db_manager.is_expression_exists(test_expr, 'test_dataset', 'USA', 1)
        results['db_check_expression_exists'] = time.time() - start_time
        
        # 4. 测试批量写入Alpha ID
        start_time = time.time()
        self.db_manager.add_checked_alphas_batch(alpha_ids, 'test_dataset', 'USA', 1)
        results['db_write_alphas'] = time.time() - start_time
        
        # 5. 测试读取Alpha ID
        start_time = time.time()
        loaded_alphas = self.db_manager.get_checked_alphas('test_dataset', 'USA', 1)
        results['db_read_alphas'] = time.time() - start_time
        
        # 6. 测试写入可提交因子
        start_time = time.time()
        for data in submitable_data:
            self.db_manager.add_submitable_alpha(data)
        results['db_write_submitable'] = time.time() - start_time
        
        # 7. 测试读取可提交因子
        start_time = time.time()
        loaded_df = self.db_manager.get_submitable_alphas()
        results['db_read_submitable'] = time.time() - start_time
        
        # 8. 测试查询过滤
        start_time = time.time()
        with self.db_manager.get_connection() as conn:
            filtered_df = pd.read_sql_query("SELECT * FROM submitable_alphas WHERE sharpe > 2.0", conn)
        results['db_filter_submitable'] = time.time() - start_time
        
        return results
    
    def run_performance_test(self):
        """运行性能测试"""
        print("🚀 开始性能测试...")
        print(f"📊 测试数据量: {self.test_data_size} 条记录")
        print("="*60)
        
        # 生成测试数据
        print("📋 生成测试数据...")
        expressions, alpha_ids, submitable_data = self.generate_test_data()
        
        # 测试文件操作
        print("📁 测试文件操作性能...")
        file_results = self.test_file_operations(expressions, alpha_ids, submitable_data)
        
        # 测试数据库操作
        print("🗄️  测试数据库操作性能...")
        db_results = self.test_database_operations(expressions, alpha_ids, submitable_data)
        
        # 输出对比结果
        self.print_comparison_results(file_results, db_results)
        
        # 清理
        self.cleanup()
    
    def print_comparison_results(self, file_results: dict, db_results: dict):
        """打印对比结果"""
        print("\n📊 性能对比结果:")
        print("="*60)
        
        operations = [
            ('写入因子表达式', 'file_write_expressions', 'db_write_expressions'),
            ('读取因子表达式', 'file_read_expressions', 'db_read_expressions'),
            ('检查表达式存在', 'file_check_expression_exists', 'db_check_expression_exists'),
            ('写入Alpha ID', 'file_write_alphas', 'db_write_alphas'),
            ('读取Alpha ID', 'file_read_alphas', 'db_read_alphas'),
            ('写入可提交因子', 'file_write_csv', 'db_write_submitable'),
            ('读取可提交因子', 'file_read_csv', 'db_read_submitable'),
        ]
        
        total_file_time = 0
        total_db_time = 0
        
        print(f"{'操作类型':<15} {'文件操作(s)':<12} {'数据库操作(s)':<15} {'性能提升':<10}")
        print("-"*60)
        
        for op_name, file_key, db_key in operations:
            file_time = file_results.get(file_key, 0)
            db_time = db_results.get(db_key, 0)
            
            total_file_time += file_time
            total_db_time += db_time
            
            if file_time > 0 and db_time > 0:
                improvement = file_time / db_time
                improvement_str = f"{improvement:.1f}x" if improvement > 1 else f"0.{int(improvement*10)}x"
            else:
                improvement_str = "N/A"
            
            print(f"{op_name:<15} {file_time:<12.4f} {db_time:<15.4f} {improvement_str:<10}")
        
        print("-"*60)
        overall_improvement = total_file_time / total_db_time if total_db_time > 0 else 1
        print(f"{'总计':<15} {total_file_time:<12.4f} {total_db_time:<15.4f} {overall_improvement:.1f}x")
        
        # 额外的数据库优势
        print("\n🎯 数据库额外优势:")
        if 'db_filter_submitable' in db_results:
            print(f"  - SQL查询过滤: {db_results['db_filter_submitable']:.4f}s")
        print("  - 并发安全性: 支持多进程同时访问")
        print("  - 数据一致性: 事务保证")
        print("  - 索引查询: O(log n) vs O(n)")
        print("  - 关系查询: 支持复杂JOIN操作")
        
        # 推荐
        print(f"\n💡 结论:")
        if overall_improvement > 1:
            print(f"  数据库操作比文件操作快 {overall_improvement:.1f} 倍，强烈推荐迁移！")
        else:
            print(f"  性能相近，但数据库在并发和查询方面有显著优势，推荐迁移。")

def main():
    """主函数"""
    print("="*60)
    print("  WorldQuant 因子系统性能测试")
    print("  文件操作 vs 数据库操作")
    print("="*60)
    
    tester = PerformanceTester()
    try:
        tester.run_performance_test()
    except Exception as e:
        print(f"❌ 性能测试失败: {e}")
        tester.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()