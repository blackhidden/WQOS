# ====================================================================
# ⚠️  已弃用 - DEPRECATED
# ====================================================================
# 此Dockerfile已不再使用，请使用 Dockerfile.dashboard
# 保留此文件仅为了向后兼容性，未来版本将删除
# 
# 当前有效的Docker文件：
# - Dockerfile.dashboard: WorldQuant Dashboard容器镜像
# ====================================================================

FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制requirements文件并安装Python依赖
COPY config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制项目文件
COPY . .

# 创建必要目录
RUN mkdir -p records database

# 设置权限
RUN chmod +x src/*.py docker/init-db.sh

# 默认命令（将被docker-compose覆盖）
CMD ["python", "--version"] 