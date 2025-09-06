#!/bin/bash

# WorldQuant 数据库初始化脚本
# 在容器启动时自动检查并初始化数据库

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 WorldQuant 数据库初始化检查...${NC}"

# 设置数据库路径
DB_PATH="/app/database/factors.db"
MIGRATION_SCRIPT="/app/database/migrate_to_sqlite.py"
TEST_SCRIPT="/app/database/test_docker_db.py"

# 检查数据库是否存在
if [ -f "$DB_PATH" ]; then
    echo -e "${GREEN}✅ 数据库文件已存在: $DB_PATH${NC}"
    
    # 验证数据库完整性
    echo -e "${BLUE}🔍 验证数据库完整性...${NC}"
    if python "$TEST_SCRIPT" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ 数据库验证通过${NC}"
        
        # 检查是否需要增量迁移
        echo -e "${BLUE}🔄 检查是否有新数据需要迁移...${NC}"
        NEED_MIGRATION=false
        
        # 检查各种表达式文件的修改时间
        for file in /app/records/*_simulated_alpha_expression.txt; do
            if [ -f "$file" ]; then
                # 比较文件修改时间和数据库修改时间
                if [ "$file" -nt "$DB_PATH" ]; then
                    echo -e "${YELLOW}📝 发现新数据: $(basename "$file")${NC}"
                    NEED_MIGRATION=true
                fi
            fi
        done
        
        # 检查checked_alpha_id文件
        for file in /app/records/*_checked_alpha_id.txt; do
            if [ -f "$file" ]; then
                if [ "$file" -nt "$DB_PATH" ]; then
                    echo -e "${YELLOW}📝 发现新检查记录: $(basename "$file")${NC}"
                    NEED_MIGRATION=true
                fi
            fi
        done
        
        # 检查submitable_alpha.csv
        if [ -f "/app/records/submitable_alpha.csv" ]; then
            if [ "/app/records/submitable_alpha.csv" -nt "$DB_PATH" ]; then
                echo -e "${YELLOW}📝 发现新提交数据: submitable_alpha.csv${NC}"
                NEED_MIGRATION=true
            fi
        fi
        
        if [ "$NEED_MIGRATION" = true ]; then
            echo -e "${BLUE}🔄 执行增量数据迁移...${NC}"
            if python "$MIGRATION_SCRIPT"; then
                echo -e "${GREEN}✅ 增量迁移完成${NC}"
            else
                echo -e "${YELLOW}⚠️  增量迁移失败，但继续运行${NC}"
            fi
        else
            echo -e "${GREEN}✅ 无新数据，跳过迁移${NC}"
        fi
        
        # 显示数据库统计信息
        echo -e "${BLUE}📊 当前数据库统计:${NC}"
        python /app/database/quick_queries.py stats || echo -e "${YELLOW}⚠️  无法获取统计信息${NC}"
    else
        echo -e "${YELLOW}⚠️  数据库验证失败，尝试重新初始化...${NC}"
        rm -f "$DB_PATH"
    fi
fi

# 如果数据库不存在，执行迁移
if [ ! -f "$DB_PATH" ]; then
    echo -e "${YELLOW}📂 数据库文件不存在，开始自动初始化...${NC}"
    
    # 确保数据库目录存在
    mkdir -p "$(dirname "$DB_PATH")"
    
    # 检查是否有数据源文件
    if [ -f "/app/records/analyst4_usa_1step_simulated_alpha_expression.txt" ] || \
       [ -f "/app/records/analyst4_usa_2step_simulated_alpha_expression.txt" ] || \
       [ -f "/app/records/fundamental2_usa_1step_simulated_alpha_expression.txt" ]; then
        
        echo -e "${BLUE}📥 发现数据源文件，执行自动迁移...${NC}"
        if python "$MIGRATION_SCRIPT"; then
            echo -e "${GREEN}✅ 数据库初始化完成！${NC}"
            
            # 显示迁移结果
            echo -e "${BLUE}📊 迁移结果统计:${NC}"
            python /app/database/quick_queries.py stats
        else
            echo -e "${RED}❌ 数据库迁移失败${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}⚠️  未发现数据源文件，创建空数据库...${NC}"
        
        # 创建空数据库结构
        python -c "
import sqlite3
import os

schema_path = '/app/database/schema.sql'
db_path = '$DB_PATH'

if os.path.exists(schema_path):
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    conn = sqlite3.connect(db_path)
    conn.executescript(schema_sql)
    conn.close()
    print('✅ 空数据库结构创建成功')
else:
    print('❌ 未找到数据库结构文件')
    exit(1)
"
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ 空数据库创建完成${NC}"
        else
            echo -e "${RED}❌ 空数据库创建失败${NC}"
            exit 1
        fi
    fi
fi

echo -e "${GREEN}🎉 数据库初始化检查完成！${NC}"

# 如果提供了额外参数，执行原始命令
if [ $# -gt 0 ]; then
    echo -e "${BLUE}🚀 启动主程序: $@${NC}"
    exec "$@"
fi