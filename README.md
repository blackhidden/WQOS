# WorldQuant Alpha 挖掘仪表板

## 📖 简介
这是一个用于WorldQuant Brain平台的Alpha因子挖掘工作流管理工具，提供友好的Web界面来管理和监控挖掘过程。
请自行阅读开源许可，请勿以任何形式将代码打包售卖！

## 🎯 主要功能
- **自动挖掘**：基于配置参数自动挖掘Alpha因子
- **智能筛选**：两阶段质量检测和相关性分析，自动筛选高质量因子
- **实时监控**：查看挖掘进度和系统状态
- **因子管理**：查看、筛选已挖掘的Alpha因子
- **Web界面**：简单易用的图形化操作界面

## 🚀 快速开始

### ⚠️ 重要提醒
使用前请确保：
1. 拥有有效的WorldQuant Brain账号
2. 网络连接正常
3. Windows、macOS、Linux系统均可运行

### 📋 启动步骤
1. **查看启动指南**：
打开 [Windows启动指南](digging-dashboard/Windows启动指南.md)

## 📚 技术文档

### 核心组件详解
- **[Alpha因子智能筛选系统](docs/ALPHA_SCREENING_SYSTEM_GUIDE.md)** - 了解两阶段因子筛选流程的工作原理和技术细节

### 重构与优化指南
- **[因子挖掘调度器重构指南](docs/DIGGING_SCHEDULER_REFACTOR_GUIDE.md)** - 挖掘脚本模块化重构详解，从1072行巨型文件到15个清晰模块
- **[相关性检查器重构指南](docs/CORRELATION_CHECKER_REFACTOR_GUIDE.md)** - 相关性检查脚本重构详解，从1965行单文件到分层模块架构

### 项目更新记录
- **[更新日志](docs/更新日志.md)** - 项目版本更新记录和重要修复说明

## 📁 项目结构
```
WorldQuant/
├── src/                          				# 核心脚本
│   ├── check_optimized.py                     # ⭐ 第一轮筛选，将通过的因子标记为YELLOW
│   ├── correlation_checker_independent.py     # ⭐ 本地计算相关性，筛选RA以及PPAC因子（已重构）
│   ├── unified_digging_scheduler.py           # ⭐ 因子挖掘调度器（已重构，从1072行→176行）
│   ├── correlation/                           # 相关性检查模块（重构后）
│   │   ├── core/, checkers/, processors/      # 分层模块架构
│   │   └── services/, utils/, data/           # 职责分离的组件
│   ├── digging/                               # 挖掘调度模块（重构后）
│   │   ├── core/, executors/, services/       # 模块化执行器
│   │   └── utils/                             # 工具组件
│   └── 其他辅助脚本
├── config/                       # 配置文件
├── database/                     # 数据库文件
├── docs/                         # 技术文档（含重构指南）
├── digging-dashboard/            # Web仪表板
│   ├── Windows启动指南.md        # ⭐ 详细启动说明
│   ├── frontend/                 # 前端界面
│   └── backend/                  # 后端服务
└── README.md                     # 本文件
```

## 💖 支持项目

如果这个项目对您有帮助，欢迎请我吃KFC！您的支持是我继续完善项目的动力。

<div align="center">
  <img src="img/48e4fdf5114cf55b8252e3b7eb6b5347.png" alt="赞赏码" width="200"/>
  <p><em>扫码支持项目发展</em></p>
</div>

---

<div align="center">
  <p>⭐ 如果觉得项目有用，请给个Star支持一下！</p>
</div>
