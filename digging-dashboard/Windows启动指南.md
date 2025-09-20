# WorldQuant Alpha 挖掘仪表板 - Windows 启动指南

## 📖 适用说明
本指南专为**无代码基础**或**低代码基础**的同学设计，提供详细的Windows环境部署步骤。

🔧 **有代码基础的同学**：可以自行选择喜欢的部署方式，`docker/` 目录下提供了完整的后端Docker容器化部署方案。

## ⚠️ 重要安全警告

> **🚨 账号安全提醒**
> 
> 对GitHub等开源平台不熟悉的同学，**强烈不建议**将项目提交到自己的开源仓库！
> 
> **风险说明**：
> - 可能因配置文件修改不当导致WorldQuant账号信息泄露
> - 账号泄露可能导致平台封号等严重后果
> - 本项目已在 `.gitignore` 中屏蔽配置文件，但仍需谨慎操作
> 
> **免责声明**：因违规操作导致的任何后果（包括但不限于账号封禁），开发者概不负责！

## 📋 准备工作

### ⚠️ 系统要求
- Windows 10/11 操作系统
- 至少 4GB 可用磁盘空间
- 稳定的网络连接

### 🔧 重要：设置UTF-8编码（必须先做）
为了确保日志文件中的emoji和中文字符正常显示，需要先设置Windows使用UTF-8编码：

#### 方法一：设置环境变量（推荐）
1. 按 `Win + R`，输入 `sysdm.cpl`，回车
2. 点击"高级"选项卡，然后点击"环境变量"
3. 在"系统变量"区域，点击"新建"
4. 变量名输入：`PYTHONIOENCODING`
5. 变量值输入：`utf-8`
6. 点击"确定"保存
7. 再次点击"新建"，添加第二个变量：
   - 变量名：`PYTHONUTF8`
   - 变量值：`1`
8. 点击"确定"保存所有设置
9. **重启电脑**使设置生效

#### 方法二：修改系统区域设置（备选）
1. 按 `Win + R`，输入 `intl.cpl`，回车
2. 点击"管理"选项卡
3. 在"非Unicode程序的语言"部分，点击"更改系统区域设置"
4. 勾选"Beta版：使用Unicode UTF-8提供全球语言支持"
5. 点击"确定"，**重启电脑**

**⚠️ 重要提醒：完成编码设置并重启电脑后，再继续后续步骤！**

### 1. 安装 Python
1. 访问 [Python官网](https://www.python.org/downloads/) 下载 Python 3.12，**不要使用3.13会有依赖不支持**
2. 安装时**务必勾选** "Add Python to PATH"
3. 安装完成后，按 `Win + R`，输入 `cmd`，回车
4. 输入 `python --version`，如果显示版本号说明安装成功

### 2. 安装 Node.js
1. 访问 [Node.js官网](https://nodejs.org/) 下载 LTS 版本
2. 双击安装，一路点击"下一步"
3. 安装完成后，在命令行输入 `node --version`，显示版本号说明成功

### 3. 安装 Rust（必需）
1. 访问 [Rust官网](https://rustup.rs/) 下载 rustup-init.exe
2. 运行安装程序，选择默认安装选项
3. 安装完成后，重新打开命令行窗口
4. 输入 `rustc --version`，显示版本号说明成功
5. **重要**：某些Python包（如cryptography）需要Rust编译环境

## 🚀 启动步骤

### 第一步：下载项目
1. 将整个项目文件夹下载到本地（比如 `D:\WorldQuant\`）
2. 确保项目结构如下：
   ```
   D:\WorldQuant\
   ├── src\
   ├── config\
   ├── database\
   ├── digging-dashboard\
   │   ├── frontend\
   │   └── backend
   └── ...
   ```

### 第二步：配置用户信息
1. 打开 `config` 文件夹
2. 复制 `user_info.txt.example` 并重命名为 `user_info.txt`
3. 用记事本打开 `user_info.txt`，填入你的WorldQuant账号信息：
   ```
   username=你的邮箱
   password=你的密码
   ```

### 第三步：初始化数据库

**重要说明**：本项目使用两个独立的数据库：
- **因子数据库**：存储挖掘的Alpha因子数据（SQLite）
- **面板数据库**：存储Web界面的用户和配置数据

#### 3.1 创建因子数据库
1. 进入项目根目录：
   ```cmd
   cd /d D:\WorldQuant
   ```
2. 创建因子数据库（如果不存在）：
   ```cmd
   python database\migrate_to_sqlite.py
   ```
3. 看到"✅ 数据库创建完成"说明成功

#### 3.2 初始化面板数据库
1. 进入后端目录：
   ```cmd
   cd digging-dashboard\backend
   ```
2. 初始化面板数据库：
   ```cmd
   python init_db.py
   ```
3. 看到"✅ 数据库初始化完成"和默认登录信息说明成功

### 第四步：安装Python依赖
1. 按 `Win + R`，输入 `cmd`，回车打开命令行
2. 进入项目根目录：
   ```cmd
   cd /d D:\WorldQuant
   ```
3. 安装所有依赖（只需第一次运行）：
   ```cmd
   pip install -r config\requirements.txt
   ```
4. 等待安装完成（可能需要几分钟）

### 第五步：启动后端服务
1. 进入后端目录：
   ```cmd
   cd /d D:\WorldQuant\digging-dashboard\backend
   ```
2. 复制配置文件：
   ```cmd
   copy env.example .env
   ```
3. 启动后端：
   ```cmd
   python run.py
   ```
4. 看到 `🎯 自动检测到项目根目录` 和服务启动信息说明成功

### 第六步：启动前端界面
1. **保持后端运行**，再打开一个新的命令行窗口
2. 进入前端目录：
   ```cmd
   cd /d D:\WorldQuant\digging-dashboard\frontend
   ```
3. 安装依赖（只需第一次运行）：
   ```cmd
   npm install
   ```
4. 复制环境文件（只需第一次运行）：
   ```cmd
   copy .env.example .env
   ```

4. 启动前端：
   ```cmd
   npm start
   ```
   - 自动使用本地开发配置，连接本地后端服务
   - 无需手动修改任何配置文件

5. 浏览器会自动打开 `http://localhost:3000`

### 💡 环境配置说明
- **本地开发**：使用 `npm start`，自动连接本地后端（8088端口）
- **生产构建**：使用 `npm run build`，自动使用线上API配置
- **无需手动修改配置文件**，系统会根据命令自动选择对应环境

## 🎉 使用界面

### 登录
面板默认登录信息为：
- 用户名：`admin`
- 密码：`admin123`

**安全提醒**：生产环境中请务必修改默认密码！运行manage_users.py根据提示进行修改。
```cmd
cd digging-dashboard\backend
python manage_users.py
```

### 数据库文件位置
- **因子数据库**：`database/factors.db`
- **面板数据库**：`digging-dashboard/backend/dashboard.db`
- **备份建议**：定期备份这两个数据库文件

### 功能说明
- **仪表板**：查看运行状态和系统概览
- **因子状态**：查看挖掘出的Alpha因子详情
- **配置管理**：修改挖掘参数和系统设置

#### 🔄 进程管理 - 重要启动顺序

> **⚠️ 关键提醒：脚本启动顺序**
> 
> 在进程管理页面启动挖掘脚本时，**必须严格按照以下顺序**：
> 
> 1. **首先启动**：`会话保持器 (Session Keeper)` 🔑
> 2. **然后启动**：其他挖掘脚本，以及配置页面的同步配置也是通过会话保持器提供的认证信息
> 
> **为什么这样做？**
> - 会话保持器负责维护与WorldQuant平台的登录状态
> - 其他脚本依赖会话保持器提供的认证信息
> - 如果顺序错误，挖掘脚本将无法正常工作
> 
> **⭐ 建议操作流程**：
> 1. 进入"进程管理"页面
> 2. 找到"会话保持器"，点击"启动"
> 3. 等待状态变为"运行中"（绿色），日志显示：✅ 新会话创建成功
> 4. 再启动需要的挖掘脚本

### 挖掘脚本课题
为增加使用门槛，开源版将设置一道简单的课题：挖掘脚本1-3阶的因子构建留空，请各位同学自行补充。
没有补充将无法使用因子挖掘脚本！

## ❓ 常见问题

### Q1：`python` 命令不识别
**解决**：重新安装Python，确保勾选"Add Python to PATH"

### Q2：`npm` 命令不识别  
**解决**：重新安装Node.js

### Q3：依赖安装失败
**解决**：
1. 确保已安装Rust（cryptography包需要）
2. 确保网络连接正常
3. 尝试使用国内镜像：`pip install -r config\requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/`
4. 如果cryptography安装失败，尝试：`pip install --upgrade pip setuptools wheel`
5. 如果仍然失败，尝试逐个安装主要依赖：`pip install pandas numpy requests aiohttp`

### Q4：数据库初始化失败
**解决**：
1. 确保已安装所有Python依赖
2. 检查项目目录权限
3. 手动创建database目录：`mkdir database`
4. 重新运行初始化命令

### Q5：后端启动失败
**解决**：
1. 确保已完成数据库初始化（第三步）
2. 确保已安装所有Python依赖（第四步）
3. 检查 `config\user_info.txt` 是否正确填写
4. 检查网络连接是否正常

### Q6：前端页面打不开
**解决**：
1. 确保后端服务正在运行
2. 检查浏览器地址是否为 `http://localhost:3000`

### Q7：挖掘脚本启动失败
**解决**：
1. 确保WorldQuant账号信息正确
2. 检查网络连接
3. 查看进程管理页面的日志信息

### Q8：Windows系统兼容性问题
如遇到类似 `module 'os' has no attribute 'setsid'` 的错误：
**解决**：
1. 该问题已在最新版本中修复
2. 系统会自动检测Windows平台并使用兼容的进程管理方式
3. 如仍有问题，请确保使用最新版本的代码

### Q9：路径错误问题
如遇到类似 `can't open file 'D:\Users\enkidu\...'` 的路径错误：
**解决**：
1. 该问题已修复，系统现在会自动检测项目根目录
2. 无需手动设置PROJECT_ROOT环境变量
3. 系统支持Windows、Linux、macOS和Docker环境的自动路径检测
4. 如仍有路径问题，请检查项目文件结构是否完整

## 📞 需要帮助？

如果遇到问题：
1. 查看命令行的错误信息
2. 检查网络连接
3. 确认WorldQuant账号可以正常登录
4. 重启电脑后重试

---

**🎊 现在您可以开始使用 WorldQuant Alpha 挖掘仪表板了！**

