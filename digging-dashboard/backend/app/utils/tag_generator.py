"""
标签生成器 - 后端工具模块
从 src/tag_generator.py 移植而来，适配后端使用
"""

from typing import Dict, Optional
from dataclasses import dataclass
import re


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
    def generate_tag(
        region: str,
        delay: int,
        instrument_type: str,
        universe: str,
        dataset_id: Optional[str] = None,
        recommended_name: Optional[str] = None,
        step: str = "step1"
    ) -> str:
        """
        生成新格式的tag名称 - 兼容原API
        
        Args:
            region: 地区
            delay: 延迟天数
            instrument_type: 工具类型
            universe: 股票池
            dataset_id: 数据集ID (可选)
            recommended_name: 推荐字段名称 (可选)
            step: 步骤 (如 "step1", "step2" 或 "1")
            
        Returns:
            格式化的tag名称
        """
        # 验证基础参数
        if not region:
            raise ValueError("region不能为空")
        if delay < 0:
            raise ValueError("delay不能为负数")
        if not instrument_type:
            raise ValueError("instrument_type不能为空")
        if not universe:
            raise ValueError("universe不能为空")
        
        # 确定数据源标识
        if recommended_name:
            data_source = recommended_name
        elif dataset_id:
            data_source = dataset_id
        else:
            raise ValueError("必须提供dataset_id或recommended_name")
        
        # 解析step编号
        if isinstance(step, str):
            if step.startswith("step"):
                step_num = int(step.replace("step", ""))
            else:
                step_num = int(step)
        else:
            step_num = int(step)
        
        # 构建tag名称
        parts = [
            region.upper(),
            str(delay),
            instrument_type.upper(),
            universe.upper(),
            data_source,
            f"step{step_num}"
        ]
        
        return "_".join(parts)
    
    @staticmethod
    def generate_tag_from_config(config: TagConfig, step: int) -> str:
        """
        从配置对象生成tag
        
        Args:
            config: Tag配置信息
            step: 步骤编号 (1, 2, 3...)
            
        Returns:
            格式化的tag名称
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
    return TagGenerator.generate_tag_from_config(config, step)


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
    return TagGenerator.generate_tag_from_config(config, step)