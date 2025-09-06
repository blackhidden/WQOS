#!/usr/bin/env python3
"""
数据集字段导出脚本
作者：e.e.
日期：2025.08.24

功能：
- 获取指定数据集的字段信息
- 处理字段（matrix和vector类型）
- 将字段信息导出到JSON文件
- 支持批量导出多个数据集
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

# 添加src目录到Python路径
current_dir = Path(__file__).parent
src_dir = current_dir.parent / 'src'
sys.path.insert(0, str(src_dir))

try:
    from machine_lib_ee import init_session, login, get_datafields, process_datafields
    from config import ROOT_PATH, RECORDS_PATH
except ImportError as e:
    print(f"❌ 导入模块失败: {e}")
    print("请确保在正确的目录下运行此脚本")
    sys.exit(1)


class DatasetFieldsExporter:
    """数据集字段导出器"""
    
    def __init__(self, records_dir: str = None):
        """初始化导出器"""
        self.records_dir = Path(records_dir) if records_dir else Path(RECORDS_PATH)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        
        # 确保session已初始化
        print("🔐 正在初始化session...")
        try:
            self.session = init_session()
            print("✅ Session初始化成功")
        except Exception as e:
            print(f"❌ Session初始化失败: {e}")
            sys.exit(1)
    
    def get_dataset_fields(self, dataset_id: str, region: str = 'USA', 
                          universe: str = 'TOP3000', delay: int = 1) -> dict:
        """获取数据集的字段信息"""
        print(f"🔍 正在获取数据集 {dataset_id} 的字段信息...")
        print(f"   🌍 地区: {region}")
        print(f"   🎛️ 宇宙: {universe}")
        print(f"   ⏰ 延迟: {delay}")
        
        try:
            # 获取原始字段信息
            start_time = time.time()
            df = get_datafields(
                self.session, 
                dataset_id=dataset_id,
                region=region,
                universe=universe,
                delay=delay
            )
            fetch_time = time.time() - start_time
            
            if df.empty:
                print("⚠️  未获取到任何字段信息")
                return {
                    'dataset_id': dataset_id,
                    'region': region,
                    'universe': universe,
                    'delay': delay,
                    'fetch_time': fetch_time,
                    'total_fields': 0,
                    'raw_fields': [],
                    'processed_fields': {
                        'matrix': [],
                        'vector': []
                    },
                    'error': 'No fields returned from API'
                }
            
            # 统计字段信息
            total_fields = len(df)
            matrix_fields = df[df['type'] == 'MATRIX']['id'].tolist() if 'type' in df.columns else []
            vector_fields = df[df['type'] == 'VECTOR']['id'].tolist() if 'type' in df.columns else []
            other_fields = df[~df['type'].isin(['MATRIX', 'VECTOR'])]['id'].tolist() if 'type' in df.columns else []
            
            print(f"📊 字段统计:")
            print(f"   总计: {total_fields:,} 个字段")
            print(f"   Matrix: {len(matrix_fields):,} 个")
            print(f"   Vector: {len(vector_fields):,} 个")
            print(f"   其他: {len(other_fields):,} 个")
            print(f"   获取耗时: {fetch_time:.2f}秒")
            
            # 统计字段信息
            print("📊 字段统计:")
            print(f"   总计: {total_fields:,} 个字段")
            print(f"   Matrix: {len(matrix_fields):,} 个")
            print(f"   Vector: {len(vector_fields):,} 个")
            print(f"   其他: {len(other_fields):,} 个")
            print(f"   获取耗时: {fetch_time:.2f}秒")
            
            # 处理字段（为简化版本准备）
            print("🔧 正在处理字段...")
            start_time = time.time()
            
            # 处理matrix字段
            processed_matrix = process_datafields(df, "matrix")
            
            # 处理vector字段
            processed_vector = process_datafields(df, "vector")
            
            process_time = time.time() - start_time
            print(f"   处理耗时: {process_time:.2f}秒")
            print(f"   处理后Matrix字段: {len(processed_matrix):,} 个")
            print(f"   处理后Vector字段: {len(processed_vector):,} 个")
            
            # 构建结果
            result = {
                'dataset_id': dataset_id,
                'region': region,
                'universe': universe,
                'delay': delay,
                'fetch_time': fetch_time,
                'total_fields': total_fields,
                'raw_fields': {
                    'all': df.to_dict('records'),
                    'matrix': matrix_fields,
                    'vector': vector_fields,
                    'other': other_fields
                },
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_script': 'export_dataset_fields.py',
                    'api_version': 'v1'
                }
            }
            
            # 为简化版本准备数据（包含处理后的字段）
            result['_simplified_fields'] = []
            for field in df.to_dict('records'):
                if 'id' in field:
                    field_info = {
                        'id': field['id'],
                        'description': field.get('description', ''),
                        'type': field.get('type', ''),
                        'processed_fields': []
                    }
                    
                    # 根据字段类型添加处理后的字段
                    if field.get('type') == 'MATRIX':
                        # 为matrix字段生成处理后的表达式
                        field_expr = f"winsorize(ts_backfill({field['id']}, 120), std=4)"
                        field_info['processed_fields'].append(field_expr)
                    elif field.get('type') == 'VECTOR':
                        # 为vector字段生成处理后的表达式
                        vec_ops = ["vec_avg", "vec_sum", "vec_ir", "vec_max", "vec_count", "vec_skewness", "vec_stddev", "vec_choose"]
                        for vec_op in vec_ops:
                            if vec_op == "vec_choose":
                                field_info['processed_fields'].extend([
                                    f"{vec_op}({field['id']}, nth=-1)",
                                    f"{vec_op}({field['id']}, nth=0)"
                                ])
                            else:
                                field_info['processed_fields'].append(f"{vec_op}({field['id']})")
                    
                    result['_simplified_fields'].append(field_info)
            
            return result
            
        except Exception as e:
            print(f"❌ 获取字段信息失败: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'dataset_id': dataset_id,
                'region': region,
                'universe': universe,
                'delay': delay,
                'error': str(e),
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_script': 'export_dataset_fields.py',
                    'api_version': 'v1'
                }
            }
    
    def export_to_json(self, dataset_id: str, fields_data: dict, 
                      output_dir: str = None, simplified_only: bool = False) -> str:
        """将字段信息导出到JSON文件"""
        if output_dir is None:
            output_dir = self.records_dir
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if simplified_only:
            filename = f"{dataset_id}_fields_simplified_{timestamp}.json"
        else:
            filename = f"{dataset_id}_fields_{timestamp}.json"
        filepath = output_path / filename
        
        try:
            # 如果只需要简化版本，创建简化数据结构
            if simplified_only:
                simplified_data = {
                    'dataset_id': fields_data['dataset_id'],
                    'region': fields_data['region'],
                    'universe': fields_data['universe'],
                    'delay': fields_data['delay'],
                    'total_fields': fields_data['total_fields'],
                    'fields': fields_data.get('_simplified_fields', []),
                    'metadata': fields_data['metadata']
                }
                export_data = simplified_data
            else:
                export_data = fields_data
            
            # 写入JSON文件
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            file_type = "简化版" if simplified_only else "完整版"
            print(f"💾 {file_type}字段信息已导出到: {filepath}")
            
            # 计算文件大小
            file_size = filepath.stat().st_size
            if file_size > 1024 * 1024:  # 大于1MB
                size_str = f"{file_size / (1024 * 1024):.2f} MB"
            else:
                size_str = f"{file_size / 1024:.2f} KB"
            print(f"📁 文件大小: {size_str}")
            
            return str(filepath)
            
        except Exception as e:
            print(f"❌ 导出JSON文件失败: {e}")
            return None
    
    def export_dataset(self, dataset_id: str, region: str = 'USA', 
                      universe: str = 'TOP3000', delay: int = 1,
                      output_dir: str = None, export_simplified: bool = True) -> dict:
        """导出单个数据集的字段信息"""
        print(f"\n{'='*60}")
        print(f"🚀 开始导出数据集: {dataset_id}")
        print(f"{'='*60}")
        
        # 获取字段信息
        fields_data = self.get_dataset_fields(dataset_id, region, universe, delay)
        
        # 导出到JSON
        if 'error' not in fields_data:
            results = {}
            
            # 导出完整版本
            full_output_file = self.export_to_json(dataset_id, fields_data, output_dir, simplified_only=False)
            if full_output_file:
                results['full'] = full_output_file
                print(f"✅ 完整版导出完成")
            else:
                print(f"❌ 完整版导出失败")
            
            # 导出简化版本
            if export_simplified:
                simplified_output_file = self.export_to_json(dataset_id, fields_data, output_dir, simplified_only=True)
                if simplified_output_file:
                    results['simplified'] = simplified_output_file
                    print(f"✅ 简化版导出完成")
                else:
                    print(f"❌ 简化版导出失败")
            
            if results:
                print(f"✅ 数据集 {dataset_id} 导出完成")
                return results
            else:
                print(f"❌ 数据集 {dataset_id} 导出失败")
                return {}
        else:
            print(f"❌ 数据集 {dataset_id} 获取字段失败: {fields_data['error']}")
            return {}
    
    def batch_export(self, dataset_ids: list, region: str = 'USA', 
                    universe: str = 'TOP3000', delay: int = 1,
                    output_dir: str = None) -> dict:
        """批量导出多个数据集的字段信息"""
        print(f"\n{'='*60}")
        print(f"🚀 开始批量导出 {len(dataset_ids)} 个数据集")
        print(f"{'='*60}")
        
        results = {}
        success_count = 0
        failed_count = 0
        
        for i, dataset_id in enumerate(dataset_ids, 1):
            print(f"\n📊 进度: {i}/{len(dataset_ids)}")
            
            try:
                export_results = self.export_dataset(
                    dataset_id, region, universe, delay, output_dir
                )
                
                if export_results:
                    results[dataset_id] = {
                        'status': 'success',
                        'output_files': export_results
                    }
                    success_count += 1
                else:
                    results[dataset_id] = {
                        'status': 'failed',
                        'error': 'Export failed'
                    }
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ 导出数据集 {dataset_id} 时发生异常: {e}")
                results[dataset_id] = {
                    'status': 'error',
                    'error': str(e)
                }
                failed_count += 1
            
            # 添加延迟，避免API限制
            if i < len(dataset_ids):
                print("⏳ 等待2秒后继续下一个数据集...")
                time.sleep(2)
        
        # 打印总结
        print(f"\n{'='*60}")
        print(f"📊 批量导出完成")
        print(f"   ✅ 成功: {success_count} 个")
        print(f"   ❌ 失败: {failed_count} 个")
        print(f"   📁 输出目录: {output_dir or self.records_dir}")
        print(f"{'='*60}")
        
        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='导出WorldQuant数据集的字段信息到JSON文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 导出单个数据集（默认导出完整版和简化版）
  python export_dataset_fields.py fundamental6
  
  # 导出多个数据集
  python export_dataset_fields.py fundamental6 fundamental7 fundamental8
  
  # 指定地区和宇宙
  python export_dataset_fields.py fundamental6 --region USA --universe TOP3000
  
  # 指定输出目录
  python export_dataset_fields.py fundamental6 --output-dir ./custom_output
  
  # 只导出完整版本（不导出简化版）
  python export_dataset_fields.py fundamental6 --no-simplified
  
  # 批量导出配置文件中的数据集
  python export_dataset_fields.py --config-file ./config/dataset.json
        """
    )
    
    parser.add_argument('dataset_ids', nargs='*', 
                       help='要导出的数据集ID列表')
    
    parser.add_argument('--region', default='USA',
                       help='地区 (默认: USA)')
    
    parser.add_argument('--universe', default='TOP3000',
                       help='宇宙 (默认: TOP3000)')
    
    parser.add_argument('--delay', type=int, default=1,
                       help='延迟 (默认: 1)')
    
    parser.add_argument('--output-dir', 
                       help='输出目录 (默认: records目录)')
    
    parser.add_argument('--config-file',
                       help='包含数据集列表的配置文件路径')
    
    parser.add_argument('--batch', action='store_true',
                       help='启用批量导出模式')
    
    parser.add_argument('--no-simplified', action='store_true',
                       help='不导出简化版本（只导出完整版本）')
    
    args = parser.parse_args()
    
    # 检查参数
    if not args.dataset_ids and not args.config_file:
        parser.error("必须指定数据集ID或配置文件")
    
    # 初始化导出器
    try:
        exporter = DatasetFieldsExporter(args.output_dir)
    except Exception as e:
        print(f"❌ 初始化导出器失败: {e}")
        sys.exit(1)
    
    # 获取数据集列表
    dataset_ids = []
    
    if args.config_file:
        # 从配置文件读取数据集列表
        try:
            with open(args.config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # 尝试从不同路径获取数据集ID
            if 'results' in config_data:
                dataset_ids = [item['id'] for item in config_data['results']]
            elif 'datasets' in config_data:
                dataset_ids = config_data['datasets']
            else:
                print(f"⚠️  配置文件 {args.config_file} 中未找到数据集列表")
                print("请检查配置文件格式")
                sys.exit(1)
                
            print(f"📋 从配置文件读取到 {len(dataset_ids)} 个数据集")
            
        except Exception as e:
            print(f"❌ 读取配置文件失败: {e}")
            sys.exit(1)
    
    # 添加命令行参数中的数据集ID
    dataset_ids.extend(args.dataset_ids)
    
    # 去重
    dataset_ids = list(dict.fromkeys(dataset_ids))
    
    if not dataset_ids:
        print("❌ 没有找到要导出的数据集")
        sys.exit(1)
    
    print(f"🎯 准备导出 {len(dataset_ids)} 个数据集:")
    for i, dataset_id in enumerate(dataset_ids, 1):
        print(f"   {i:2d}. {dataset_id}")
    
    # 执行导出
    export_simplified = not args.no_simplified
    
    if len(dataset_ids) == 1:
        # 单个数据集
        exporter.export_dataset(
            dataset_ids[0], 
            args.region, 
            args.universe, 
            args.delay,
            args.output_dir,
            export_simplified
        )
    else:
        # 多个数据集
        exporter.batch_export(
            dataset_ids,
            args.region,
            args.universe,
            args.delay,
            args.output_dir
        )
    
    print("\n🎉 导出完成！")


if __name__ == '__main__':
    main()
