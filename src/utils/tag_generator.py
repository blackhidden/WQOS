"""
标签生成器模块 (Tag Generator)
作者：e.e.
微信：Enkidu_lin
日期：2025.08.24

功能：
- 生成新格式的tag名称
- 验证tag格式
- 解析tag信息
- 支持两种配置模式（数据集模式和推荐字段模式）

新tag格式：region_delay_instrumentType_universe_(dataset_id or recommended_name)_step{number}
示例：USA_1_EQUITY_TOP3000_analyst11_step1
"""

import re
from typing import Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class TagConfig:
    """Tag配置信息"""
    region: str
    delay: int
    instrument_type: str
    universe: str
    dataset_id: Optional[str] = None
    recommended_name: Optional[str] = None
    use_recommended_fields: bool = False


class TagGenerator:
    """标签生成器"""
    
    # 新tag格式的正则表达式
    TAG_PATTERN = re.compile(
        r'^([A-Z]+)_(\d+)_([A-Z]+)_([A-Z0-9]+)_([a-zA-Z0-9_]+)_step(\d+)$'
    )
    
    @staticmethod
    def generate_tag(config: TagConfig, step: int) -> str:
        """
        生成新格式的tag名称
        
        Args:
            config: Tag配置信息
            step: 步骤编号 (1, 2, 3...)
            
        Returns:
            格式化的tag名称
            
        Raises:
            ValueError: 当配置无效时
        """
        # 验证基础参数
        if not config.region:
            raise ValueError("region不能为空")
        if config.delay < 0:
            raise ValueError("delay不能为负数")
        if not config.instrument_type:
            raise ValueError("instrument_type不能为空")
        if not config.universe:
            raise ValueError("universe不能为空")
        if step < 1:
            raise ValueError("step必须大于0")
        
        # 确定数据源标识
        if config.use_recommended_fields:
            if not config.recommended_name:
                raise ValueError("使用推荐字段模式时必须提供recommended_name")
            data_source = config.recommended_name
        else:
            if not config.dataset_id:
                raise ValueError("使用数据集模式时必须提供dataset_id")
            data_source = config.dataset_id
        
        # 构建tag名称
        parts = [
            config.region.upper(),
            str(config.delay),
            config.instrument_type.upper(),
            config.universe.upper(),
            data_source,
            f"step{step}"
        ]
        
        return "_".join(parts)
    
    @staticmethod
    def parse_tag(tag_name: str) -> Dict[str, any]:
        """
        解析tag名称获取配置信息
        
        Args:
            tag_name: tag名称
            
        Returns:
            解析后的配置信息字典
            
        Raises:
            ValueError: 当tag格式无效时
        """
        if not TagGenerator.validate_tag(tag_name):
            raise ValueError(f"无效的tag格式: {tag_name}")
        
        match = TagGenerator.TAG_PATTERN.match(tag_name)
        if not match:
            raise ValueError(f"tag格式不匹配: {tag_name}")
        
        region, delay, instrument_type, universe, data_source, step = match.groups()
        
        return {
            "region": region,
            "delay": int(delay),
            "instrument_type": instrument_type,
            "universe": universe,
            "data_source": data_source,
            "step": int(step),
            "original_tag": tag_name
        }
    
    @staticmethod
    def validate_tag(tag_name: str) -> bool:
        """
        验证tag名称格式
        
        Args:
            tag_name: 要验证的tag名称
            
        Returns:
            是否为有效格式
        """
        if not tag_name or not isinstance(tag_name, str):
            return False
        
        return bool(TagGenerator.TAG_PATTERN.match(tag_name))
    
    @staticmethod
    def get_base_tag(tag_name: str) -> str:
        """
        获取不包含step的基础tag
        
        Args:
            tag_name: 完整的tag名称
            
        Returns:
            基础tag名称 (不包含step部分)
        """
        if not TagGenerator.validate_tag(tag_name):
            raise ValueError(f"无效的tag格式: {tag_name}")
        
        # 移除最后的"_step{number}"部分
        parts = tag_name.split("_")
        base_parts = parts[:-1]  # 去掉最后一个"step{number}"
        
        return "_".join(base_parts)
    
    @staticmethod
    def create_step_tag(base_tag: str, step: int) -> str:
        """
        从基础tag创建指定步骤的tag
        
        Args:
            base_tag: 基础tag (不包含step)
            step: 步骤编号
            
        Returns:
            完整的tag名称
        """
        if step < 1:
            raise ValueError("step必须大于0")
        
        return f"{base_tag}_step{step}"
    
    @staticmethod
    def is_same_config(tag1: str, tag2: str) -> bool:
        """
        判断两个tag是否来自同一配置
        
        Args:
            tag1: 第一个tag
            tag2: 第二个tag
            
        Returns:
            是否来自同一配置
        """
        try:
            base1 = TagGenerator.get_base_tag(tag1)
            base2 = TagGenerator.get_base_tag(tag2)
            return base1 == base2
        except ValueError:
            return False
    
    @staticmethod
    def convert_old_tag_to_new(old_tag: str, config: TagConfig) -> str:
        """
        将旧格式tag转换为新格式
        
        Args:
            old_tag: 旧格式tag (如: fundamental6_usa_1step)
            config: 新的配置信息
            
        Returns:
            新格式的tag
        """
        # 解析旧tag获取step信息
        old_parts = old_tag.split("_")
        
        # 提取step编号
        step = 1  # 默认值
        for part in old_parts:
            if part.endswith("step"):
                step_str = part.replace("step", "")
                if step_str.isdigit():
                    step = int(step_str)
                elif part == "1step":
                    step = 1
                elif part == "2step":
                    step = 2
                elif part == "3step":
                    step = 3
                break
        
        return TagGenerator.generate_tag(config, step)


# 便捷函数
def generate_tag_for_dataset(region: str, delay: int, instrument_type: str, 
                           universe: str, dataset_id: str, step: int) -> str:
    """为数据集模式生成tag"""
    config = TagConfig(
        region=region,
        delay=delay,
        instrument_type=instrument_type,
        universe=universe,
        dataset_id=dataset_id,
        use_recommended_fields=False
    )
    return TagGenerator.generate_tag(config, step)


def generate_tag_for_recommended(region: str, delay: int, instrument_type: str,
                               universe: str, recommended_name: str, step: int) -> str:
    """为推荐字段模式生成tag"""
    config = TagConfig(
        region=region,
        delay=delay,
        instrument_type=instrument_type,
        universe=universe,
        recommended_name=recommended_name,
        use_recommended_fields=True
    )
    return TagGenerator.generate_tag(config, step)


# 示例使用
if __name__ == "__main__":
    # 测试数据集模式
    dataset_config = TagConfig(
        region="USA",
        delay=1,
        instrument_type="EQUITY",
        universe="TOP3000",
        dataset_id="analyst11",
        use_recommended_fields=False
    )
    
    dataset_tag = TagGenerator.generate_tag(dataset_config, 1)
    print(f"数据集模式tag: {dataset_tag}")
    # 输出: USA_1_EQUITY_TOP3000_analyst11_step1
    
    # 测试推荐字段模式
    recommended_config = TagConfig(
        region="USA",
        delay=1,
        instrument_type="EQUITY",
        universe="TOP3000",
        recommended_name="custom_fields",
        use_recommended_fields=True
    )
    
    recommended_tag = TagGenerator.generate_tag(recommended_config, 2)
    print(f"推荐字段模式tag: {recommended_tag}")
    # 输出: USA_1_EQUITY_TOP3000_custom_fields_step2
    
    # 测试解析
    parsed = TagGenerator.parse_tag(dataset_tag)
    print(f"解析结果: {parsed}")
    
    # 测试验证
    print(f"tag有效性: {TagGenerator.validate_tag(dataset_tag)}")
    
    # 测试基础tag
    base_tag = TagGenerator.get_base_tag(dataset_tag)
    print(f"基础tag: {base_tag}")
    
    # 测试步骤tag创建
    step2_tag = TagGenerator.create_step_tag(base_tag, 2)
    print(f"第2步tag: {step2_tag}")
