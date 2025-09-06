# 统一会话管理器使用指南

## 📋 概述

统一会话管理器是一个集中式的登录和会话管理解决方案，旨在解决项目中多个组件重复登录导致的API限制问题。

### 🎯 主要目标
- **减少登录API调用**：各组件共享同一会话，避免重复登录
- **自动会话维护**：每3小时自动刷新会话，保持持续可用
- **持久化存储**：会话数据保存到数据库或文件，程序重启后可恢复
- **统一管理**：所有组件使用相同的会话管理接口

## ✨ 核心特性

### 🔄 会话共享
- 多个组件共享同一个会话实例
- 避免每个组件单独登录
- 大幅减少登录API调用次数

### ⏰ 自动刷新
- 会话持续时间：3小时
- 提前30分钟开始刷新
- 后台线程自动维护
- 支持手动强制刷新

### 💾 持久化存储
- **优先**: 数据库存储 (`system_config`表)
- **备选**: 文件存储 (pickle + JSON格式)
- 程序重启后自动恢复会话
- 支持跨进程会话共享

### 🛡️ 错误处理
- 会话失效自动重新登录
- 网络错误自动重试
- 详细的错误日志和恢复机制

## 🚀 快速开始

### 1. 基本用法

```python
# 最简单的用法：获取会话
from session_manager import get_session

session = get_session()
response = session.get('https://api.worldquantbrain.com/users/self')
```

### 2. 替换现有登录代码

```python
# 原来的代码
from machine_lib_ee import login
session = login()

# 新的代码
from session_manager import get_session  
session = get_session()
```

### 3. 会话状态监控

```python
from session_manager import get_session_info

info = get_session_info()
print(f"会话状态: {info['status']}")
print(f"剩余时间: {info['time_left_minutes']} 分钟")
```

## 🔧 组件集成

### correlation_checker_independent.py

```python
# 修改 initialize_session 方法
def initialize_session(self):
    """初始化会话（使用统一会话管理器）"""
    if self.session is None:
        try:
            from session_manager import get_session
            self.session = get_session()
            print("✅ 会话初始化完成 (使用统一会话管理器)")
            return True
        except Exception as e:
            print(f"❌ 会话初始化失败: {e}")
            return False
    return True
```

### check_optimized.py

```python
# 在类初始化中
def __init__(self, mode=None, batch_size=50):
    # ... 现有初始化代码 ...
    
    # 使用统一会话管理器
    from session_manager import get_session
    self.session = get_session()
    print("✅ 优化检查器会话初始化完成")
```

### submit_daemon.py

```python
# 替换所有 login() 调用
from session_manager import get_session

def process_submit_batch(csv_path, batch_size=20):
    session = get_session()  # 替换 login()
    # ... 其余代码保持不变
```

### unified_digging_scheduler.py

```python
# 选项1：完全替换SessionManager
from session_manager import get_session

class UnifiedDiggingScheduler:
    def __init__(self):
        self.session = get_session()
        
# 选项2：与现有SessionManager共存
def get_unified_session():
    from session_manager import get_session
    return get_session()
```

## 🛠️ 管理工具

### 命令行工具

```bash
# 查看会话状态
python src/session_cli.py status

# 刷新会话
python src/session_cli.py refresh

# 测试会话有效性
python src/session_cli.py test

# 清除当前会话
python src/session_cli.py clear

# 查看详细信息
python src/session_cli.py info

# 持续监控会话状态
python src/session_cli.py monitor

# 性能基准测试
python src/session_cli.py benchmark
```

### 集成测试工具

```bash
# 运行完整的集成测试
python src/session_integration.py
```

## 📊 API参考

### 核心函数

#### `get_session() -> requests.Session`
获取有效的会话对象。这是最主要的接口函数。

```python
session = get_session()
response = session.get('https://api.worldquantbrain.com/users/self')
```

#### `get_session_info() -> Dict[str, Any]`
获取会话状态信息。

```python
info = get_session_info()
# 返回: {
#     'status': 'active',
#     'start_time': '2025-01-08 10:30:00', 
#     'expires_at': '2025-01-08 13:30:00',
#     'time_left_minutes': 150,
#     'is_expired': False,
#     'user_info': {...}
# }
```

#### `refresh_session() -> bool`
手动刷新会话。

```python
success = refresh_session()
if success:
    print("会话刷新成功")
```

#### `invalidate_session()`
手动失效当前会话。

```python
invalidate_session()  # 清除当前会话，下次调用get_session()会重新登录
```

### 高级API

#### `get_session_manager() -> UnifiedSessionManager`
获取会话管理器实例（单例模式）。

```python
manager = get_session_manager()
session = manager.get_session()
info = manager.get_session_info()
```

### UnifiedSessionManager类

主要方法：
- `get_session()`: 获取会话
- `refresh_session()`: 刷新会话
- `get_session_info()`: 获取状态信息
- `invalidate_session()`: 失效会话

配置属性：
- `session_duration`: 会话持续时间（默认3小时）
- `refresh_threshold`: 刷新阈值（默认30分钟）
- `use_database`: 是否使用数据库存储（默认True）
- `fallback_to_file`: 是否使用文件备份（默认True）

## ⚙️ 配置说明

### 数据库配置

会话数据存储在`system_config`表中：
```sql
-- 查看会话数据
SELECT * FROM system_config WHERE config_key = 'unified_session_data';

-- 清除会话数据
UPDATE system_config SET config_value = '' WHERE config_key = 'unified_session_data';
```

### 文件配置

备用文件存储位置：
- `records/session_data.pickle`: 完整会话数据（pickle格式）
- `records/session_cookies.json`: Cookie数据（JSON格式，便于调试）

### 环境配置

确保用户凭据文件存在：
```
config/user_info.txt:
username: 'your_username'
password: 'your_password'
```

## 📈 性能优化

### 效果对比

| 场景 | 传统方式 | 统一管理器 | 改善 |
|------|----------|------------|------|
| 单次获取 | 2秒登录 | <10ms | 200倍 |
| 10个组件同时启动 | 20秒 | 2秒 | 10倍 |
| 每日登录API调用 | 100-500次 | 8次 | 12-60倍 |

### 最佳实践

1. **及早初始化**：在程序启动时获取一次会话
2. **复用会话**：多个操作使用同一会话对象
3. **监控状态**：定期检查会话状态，及时发现问题
4. **错误处理**：捕获会话异常，实现优雅降级

```python
# 推荐的使用模式
class MyComponent:
    def __init__(self):
        self.session = get_session()  # 初始化时获取
        
    def do_api_call(self):
        # 直接使用，无需重新获取
        response = self.session.get(url)
        return response
```

## 🔍 故障排除

### 常见问题

#### 1. 会话获取失败
```
❌ 会话初始化失败: 用户名或密码错误
```
**解决方案**：
- 检查 `config/user_info.txt` 文件
- 确认用户名密码正确
- 检查网络连接

#### 2. 数据库连接失败
```
⚠️ 数据库连接初始化失败: no such table: system_config
```
**解决方案**：
- 运行数据库迁移脚本
- 检查数据库文件权限
- 使用文件备份模式

#### 3. 会话频繁过期
```
⚠️ 保存的会话已失效
```
**解决方案**：
- 检查系统时间是否正确
- 确认网络连接稳定
- 查看后台刷新线程是否正常运行

### 调试工具

#### 1. 详细日志
```python
import logging
logging.basicConfig(level=logging.DEBUG)

from session_manager import get_session
session = get_session()
```

#### 2. 会话状态检查
```bash
python src/session_cli.py info
```

#### 3. API测试
```bash
python src/session_cli.py test
```

## 📋 迁移检查清单

### 迁移前准备
- [ ] 备份现有代码
- [ ] 确认数据库schema包含system_config表  
- [ ] 测试用户凭据配置

### 组件迁移
- [ ] correlation_checker_independent.py
- [ ] check_optimized.py  
- [ ] submit_daemon.py
- [ ] unified_digging_scheduler.py

### 功能验证
- [ ] 会话共享测试
- [ ] 会话持久化测试
- [ ] API访问权限测试
- [ ] 自动刷新测试
- [ ] 性能基准测试

### 生产部署
- [ ] 监控会话状态
- [ ] 设置告警机制
- [ ] 文档更新
- [ ] 团队培训

## 🔮 未来规划

### 短期改进
- [ ] 支持多用户会话管理
- [ ] 添加会话使用统计
- [ ] 实现会话池管理
- [ ] 支持分布式会话共享

### 长期规划
- [ ] Web界面管理面板
- [ ] 实时监控Dashboard
- [ ] 集成到CI/CD流程
- [ ] 支持OAuth等现代认证方式

## 📞 支持与贡献

### 问题反馈
如遇到问题，请提供：
1. 错误信息和堆栈跟踪
2. 会话状态信息 (`session_cli.py info`)
3. 系统环境信息

### 贡献指南
欢迎提交：
- Bug修复
- 性能优化
- 功能增强
- 文档改进

---

