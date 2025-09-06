#!/bin/bash

# ====================================================================
# WorldQuant Digging Dashboard - 数据库初始化脚本
# ====================================================================

set -e  # 遇到错误立即退出

echo "🚀 开始初始化 WorldQuant Digging Dashboard..."

# ====================================================================
# 环境检查
# ====================================================================
echo "📋 检查环境..."

# 检查Python环境
if ! command -v python &> /dev/null; then
    echo "❌ Python 未找到"
    exit 1
fi

echo "✅ Python 版本: $(python --version)"

# 检查必要目录
REQUIRED_DIRS=("/app/logs" "/app/records" "/app/database" "/app/digging-dashboard/backend")
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "📁 创建目录: $dir"
        mkdir -p "$dir"
    fi
done

# ====================================================================
# 数据库初始化
# ====================================================================
echo "🗄️ 初始化数据库..."

# 1. 初始化因子数据库
echo "📊 检查因子数据库..."
cd /app
if [ -f "database/factors.db" ]; then
    echo "✅ 因子数据库已存在: database/factors.db"
else
    echo "🔧 初始化因子数据库..."
    python database/migrate_to_sqlite.py
    echo "✅ 因子数据库初始化完成"
fi

# 2. 初始化面板数据库  
echo "📊 检查面板数据库..."
cd /app/digging-dashboard/backend
if [ -f "dashboard.db" ]; then
    echo "✅ 面板数据库已存在: dashboard.db"
else
    echo "🔧 初始化面板数据库..."
    python init_db.py
    echo "✅ 面板数据库初始化完成"
fi

# ====================================================================
# 权限设置
# ====================================================================
echo "🔐 设置权限..."

# 确保日志目录可写
chmod -R 755 /app/logs
chmod -R 755 /app/records
chmod -R 755 /app/database

# 确保数据库文件可写
if [ -f "dashboard.db" ]; then
    chmod 664 dashboard.db
fi

# 确保因子数据库可写
if [ -f "/app/database/factors.db" ]; then
    chmod 664 /app/database/factors.db
fi

# ====================================================================
# 健康检查
# ====================================================================
echo "🏥 执行健康检查..."

# 检查主项目依赖
MAIN_PROJECT_FILES=("/app/src/unified_digging_scheduler.py" "/app/config/digging_config.txt")
for file in "${MAIN_PROJECT_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "⚠️  警告: 主项目文件不存在: $file"
    fi
done

# 检查后端依赖
BACKEND_FILES=("/app/digging-dashboard/backend/app/main.py" "/app/digging-dashboard/backend/run.py")
for file in "${BACKEND_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ 错误: 后端文件不存在: $file"
        exit 1
    fi
done

echo "✅ 健康检查通过"

# ====================================================================
# 完成初始化
# ====================================================================
echo "🎉 WorldQuant Digging Dashboard 初始化完成!"
echo ""
echo "📊 面板数据库: /app/digging-dashboard/backend/dashboard.db"
echo "📊 因子数据库: /app/database/factors.db"
echo "📁 日志目录: /app/logs"
echo "📁 记录目录: /app/records"
echo "🌐 服务将在端口 8088 启动"
echo ""
echo "🚀 准备启动后端服务..."
