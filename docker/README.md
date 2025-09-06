# WorldQuant Digging Dashboard - Docker 部署指南

## 📋 概述

本目录包含 WorldQuant Digging Dashboard 的 Docker 部署配置，支持：

- **后端服务**：FastAPI 应用（端口 8088）
- **前端服务**：Nginx 静态文件服务（端口 80/443）
- **原有脚本**：挖掘脚本容器化部署

## 🏗️ 架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Nginx         │    │  FastAPI        │    │  Scripts        │
│   (Frontend)    │───▶│   (Backend)     │───▶│  (Workers)      │
│   Port: 80/443  │    │   Port: 8088    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Static Files   │    │   Database      │    │   Logs/Records  │
│  (build/)       │    │  (SQLite)       │    │   (Volumes)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 快速开始

### 1. 准备环境

确保已安装 Docker 和 Docker Compose：

```bash
docker --version
docker-compose --version
```

### 2. 构建前端（如果需要）

```bash
cd digging-dashboard/frontend
npm install
npm run build
```

### 3. 配置环境变量

```bash
# 复制环境变量模板
cp docker/env.dashboard.example docker/.env.dashboard

# 编辑配置（可选）
vim docker/.env.dashboard
```

### 4. 启动服务

#### 仅启动后端：
```bash
docker-compose --profile dashboard up dashboard-backend
```

#### 启动后端 + 前端：
```bash
docker-compose --profile dashboard up
```

#### 后台运行：
```bash
docker-compose --profile dashboard up -d
```

### 5. 访问服务

- **前端界面**：http://localhost
- **后端API**：http://localhost:8088
- **API文档**：http://localhost:8088/docs

## 📁 文件结构

```
docker/
├── Dockerfile.dashboard      # ✅ Dashboard 后端镜像（当前使用）
├── Dockerfile               # ❌ 已弃用，保留仅为向后兼容
├── docker-compose.yml       # 服务编排配置
├── init-dashboard.sh        # 数据库初始化脚本（包含因子数据库+面板数据库）
├── init-db.sh              # 传统数据库初始化脚本
├── env.dashboard.example    # 环境变量模板
└── README.md               # 本文档
```

### ⚠️ 重要说明

- **`Dockerfile`** 已弃用，请使用 `Dockerfile.dashboard`
- **数据库初始化**：`init-dashboard.sh` 现在会同时初始化两个数据库：
  - 因子数据库：`/app/database/factors.db`
  - 面板数据库：`/app/digging-dashboard/backend/dashboard.db`

## ⚙️ 配置说明

### 环境变量

主要配置项在 `.env.dashboard` 中：

```bash
# 服务配置
HOST=0.0.0.0
PORT=8088
DEBUG=false

# 数据库配置
DATABASE_URL=sqlite:///./dashboard.db

# 认证配置
SECRET_KEY=your-secret-key-here
```

### 卷挂载

| 宿主机路径 | 容器路径 | 说明 |
|-----------|---------|------|
| `../logs` | `/app/logs` | 日志文件 |
| `../records` | `/app/records` | 记录文件 |
| `../database` | `/app/database` | 数据库文件 |
| `../config` | `/app/config` | 配置文件 |

### 端口映射

| 服务 | 容器端口 | 宿主机端口 | 说明 |
|-----|---------|-----------|------|
| dashboard-backend | 8088 | 8088 | FastAPI服务 |
| dashboard-frontend | 80 | 80 | HTTP服务 |
| dashboard-frontend | 443 | 443 | HTTPS服务 |

## 🔧 常用命令

### 查看日志
```bash
# 查看后端日志
docker-compose logs -f dashboard-backend

# 查看前端日志
docker-compose logs -f dashboard-frontend

# 查看所有服务日志
docker-compose logs -f
```

### 重启服务
```bash
# 重启后端
docker-compose restart dashboard-backend

# 重启所有服务
docker-compose restart
```

### 进入容器
```bash
# 进入后端容器
docker-compose exec dashboard-backend bash

# 进入前端容器
docker-compose exec dashboard-frontend sh
```

### 清理资源
```bash
# 停止服务
docker-compose --profile dashboard down

# 停止并删除卷
docker-compose --profile dashboard down -v

# 删除镜像
docker-compose --profile dashboard down --rmi all
```

## 🔍 故障排除

### 1. 后端启动失败

检查日志：
```bash
docker-compose logs dashboard-backend
```

常见问题：
- **数据库初始化失败**：检查 `/app/database/` 目录权限
- **因子数据库缺失**：容器会自动运行 `database/migrate_to_sqlite.py`
- **面板数据库缺失**：容器会自动运行 `init_db.py`
- 端口被占用
- 权限问题

### 2. 前端无法访问

检查：
- Nginx 配置是否正确
- 前端构建文件是否存在
- 端口是否被占用

### 3. API 代理失败

检查：
- 后端服务是否正常运行
- Nginx upstream 配置
- 网络连接

## 📊 监控

### 健康检查

后端服务包含健康检查端点：
```bash
curl http://localhost:8088/health
```

### 性能监控

查看容器资源使用：
```bash
docker stats
```

## 🔒 生产环境

### 安全配置

1. **更改默认密钥**：
   ```bash
   SECRET_KEY=your-production-secret-key
   ```

2. **启用 HTTPS**：
   - 配置 SSL 证书
   - 更新 nginx.conf 中的 HTTPS 配置

3. **限制访问**：
   - 配置防火墙规则
   - 设置访问白名单

### 性能优化

1. **资源限制**：
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '1.0'
         memory: 1G
   ```

2. **日志轮转**：
   ```yaml
   logging:
     options:
       max-size: "10m"
       max-file: "3"
   ```

## 📚 相关文档

- [FastAPI 文档](https://fastapi.tiangolo.com/)
- [Docker Compose 文档](https://docs.docker.com/compose/)
- [Nginx 文档](https://nginx.org/en/docs/)
