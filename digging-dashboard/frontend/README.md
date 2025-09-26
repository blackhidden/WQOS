# 前端界面说明

## 🚀 快速启动

### 环境配置
项目已优化环境配置，支持两种模式：

#### 本地开发
```bash
npm start
```
- 使用 `.env` 配置文件
- 连接本地后端服务：`http://localhost:8088/api`
- 适合日常开发和调试

#### 生产构建
```bash
npm run build
```
- 使用 `.env.production` 配置文件
- 连接线上服务：`https://wqe.ineed.asia/api`
- 用于打包部署

### Windows 用户启动步骤
1. 打开命令行，进入此目录
2. 安装依赖（只需第一次）：`npm install`
3. 启动界面：`npm start`
4. 浏览器会自动打开 http://localhost:3000

## 🔧 使用说明
- 确保后端服务已启动（端口8088）
- 默认登录信息：用户名 `admin`，密码 `admin123`
- **无需手动修改环境文件**：开发用`npm start`，构建用`npm run build`

## ❓ 遇到问题？
1. 确保Node.js已正确安装
2. 确保后端服务正在运行
3. 查看命令行的错误信息

---
**💡 提示：请参考上级目录的《Windows启动指南.md》获取完整的使用说明**
