# 数据集字段导出脚本使用示例

## 快速开始

### 1. 导出单个数据集
```bash
# 导出 fundamental14 数据集
python scripts/export_dataset_fields.py fundamental14
```

### 2. 查看帮助信息
```bash
python scripts/export_dataset_fields.py --help
```

### 3. 导出多个数据集
```bash
# 一次性导出多个数据集
python scripts/export_dataset_fields.py fundamental14 fundamental15 fundamental16
```

### 4. 指定自定义参数
```bash
# 指定地区、宇宙和输出目录
python scripts/export_dataset_fields.py fundamental14 \
  --region USA \
  --universe TOP3000 \
  --delay 1 \
  --output-dir ./custom_output
```

### 5. 控制导出版本
```bash
# 默认导出完整版和简化版
python scripts/export_dataset_fields.py fundamental14

# 只导出完整版本（不导出简化版）
python scripts/export_dataset_fields.py fundamental14 --no-simplified
```

## 实际使用场景

### 场景1: 数据集分析
```bash
# 导出数据集进行离线分析
python scripts/export_dataset_fields.py fundamental14
# 输出文件: records/fundamental14_fields_20250103_143022.json
```

### 场景2: 批量导出
```bash
# 从配置文件批量导出
python scripts/export_dataset_fields.py --config-file ./config/dataset.json
```

### 场景3: 自定义输出
```bash
# 导出到指定目录
python scripts/export_dataset_fields.py fundamental6 --output-dir ./exports
```

## 输出文件示例

### 完整版本（默认）
包含所有原始字段信息：

```json
{
  "dataset_id": "fundamental14",
  "region": "USA",
  "universe": "TOP3000",
  "delay": 1,
  "fetch_time": 2.45,
  "total_fields": 208,
  "raw_fields": {
    "all": [...],
    "matrix": [...],
    "vector": [...],
    "other": [...]
  },
  "metadata": {
    "export_timestamp": "2025-01-03T14:30:22",
    "export_script": "export_dataset_fields.py",
    "api_version": "v1"
  }
}
```

### 简化版本
包含字段ID、描述、类型和处理后的字段：

```json
{
  "dataset_id": "fundamental14",
  "region": "USA",
  "universe": "TOP3000",
  "delay": 1,
  "total_fields": 208,
  "fields": [
    {
      "id": "field_id",
      "description": "field description",
      "type": "VECTOR",
      "processed_fields": [
        "vec_avg(field_id)",
        "vec_sum(field_id)",
        "vec_ir(field_id)",
        "vec_max(field_id)",
        "vec_count(field_id)",
        "vec_skewness(field_id)",
        "vec_stddev(field_id)",
        "vec_choose(field_id, nth=-1)",
        "vec_choose(field_id, nth=0)"
      ]
    }
  ],
  "metadata": {
    "export_timestamp": "2025-01-03T14:30:22",
    "export_script": "export_dataset_fields.py",
    "api_version": "v1"
  }
}
```

## 注意事项

1. **API限制**: 脚本会自动处理API速率限制
2. **文件大小**: 大型数据集可能产生较大的JSON文件
3. **网络连接**: 确保网络连接稳定
4. **权限**: 确保输出目录有写入权限

## 故障排除

如果遇到问题，请运行测试脚本：
```bash
python scripts/test_export_fields.py
```

这将检查所有必要的组件是否正常工作。
