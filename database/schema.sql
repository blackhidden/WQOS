-- ====================================================================
-- WorldQuant Alpha 因子系统 SQLite 数据库结构设计
-- ====================================================================
-- 创建日期: 2025-01-15
-- 目标: 替换文本文件存储，提升查询性能和并发安全性

-- ====================================================================
-- 1. 因子表达式表 - 替换 *_simulated_alpha_expression.txt 文件
-- ====================================================================
CREATE TABLE factor_expressions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    expression TEXT NOT NULL UNIQUE,           -- 因子表达式内容
    dataset_id VARCHAR(50) NOT NULL,           -- 数据集ID (analyst4, fundamental2等)
    region VARCHAR(10) NOT NULL,               -- 地区 (USA, GLB等)
    step INTEGER NOT NULL,                     -- 步骤 (1或2)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- 复合索引，优化按数据集和地区查询
    UNIQUE(expression, dataset_id, region, step)
);

-- ====================================================================
-- 2. 已检查因子表 - 替换 *_checked_alpha_id.txt 文件
-- ====================================================================
CREATE TABLE checked_alphas (
    alpha_id VARCHAR(100) PRIMARY KEY,         -- Alpha ID
    dataset_id VARCHAR(50) NOT NULL,           -- 数据集ID
    region VARCHAR(10) NOT NULL,               -- 地区
    step INTEGER NOT NULL,                     -- 步骤
    checked_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- 3. 可提交因子表 - 替换 submitable_alpha.csv 文件
-- ====================================================================
CREATE TABLE submitable_alphas (
    alpha_id VARCHAR(100) PRIMARY KEY,         -- Alpha ID
    type VARCHAR(50),                          -- 类型
    author VARCHAR(100),                       -- 作者
    instrument_type VARCHAR(20),               -- 工具类型
    region VARCHAR(10),                        -- 地区
    universe VARCHAR(20),                      -- 宇宙
    delay INTEGER,                             -- 延迟
    decay INTEGER,                             -- 衰减
    neutralization VARCHAR(50),                -- 中性化
    truncation REAL,                           -- 截断
    pasteurization VARCHAR(50),                -- 巴氏杀菌
    unit_handling VARCHAR(50),                 -- 单位处理
    nan_handling VARCHAR(50),                  -- NaN处理
    language VARCHAR(20),                      -- 语言
    visualization TEXT,                        -- 可视化
    code TEXT,                                 -- 代码
    description TEXT,                          -- 描述
    operator_count INTEGER,                    -- 操作符数量
    date_created DATETIME,                     -- 创建日期
    date_submitted DATETIME,                   -- 提交日期
    date_modified DATETIME,                    -- 修改日期
    name VARCHAR(200),                         -- 名称
    favorite BOOLEAN DEFAULT FALSE,            -- 收藏
    hidden BOOLEAN DEFAULT FALSE,              -- 隐藏
    color VARCHAR(20),                         -- 颜色
    category VARCHAR(50),                      -- 分类
    tags TEXT,                                 -- 标签
    classifications TEXT,                      -- 分类
    grade VARCHAR(10),                         -- 等级
    stage VARCHAR(20),                         -- 阶段
    status VARCHAR(20),                        -- 状态
    pnl REAL,                                  -- 盈亏
    book_size REAL,                            -- 账面规模
    long_count INTEGER,                        -- 多头数量
    short_count INTEGER,                       -- 空头数量
    turnover REAL,                             -- 换手率
    returns REAL,                              -- 收益
    drawdown REAL,                             -- 回撤
    margin REAL,                               -- 保证金
    fitness REAL,                              -- 适应度
    sharpe REAL,                               -- 夏普比率
    start_date DATE,                           -- 开始日期
    checks TEXT,                               -- 检查
    os TEXT,                                   -- 操作系统
    train TEXT,                                -- 训练
    test TEXT,                                 -- 测试
    prod TEXT,                                 -- 生产
    competitions TEXT,                         -- 竞赛
    themes TEXT,                               -- 主题
    team TEXT,                                 -- 团队
    pyramids TEXT,                             -- 金字塔
    aggressive_mode BOOLEAN,                   -- 激进模式
    self_corr REAL,                            -- 自相关
    prod_corr REAL,                            -- 生产相关
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- 注意：notified_alphas.txt 文件不需要存储到数据库中
-- 这类通知日志文件保持文件存储方式即可
-- ====================================================================

-- ====================================================================
-- 4. 失败表达式表 - 存储模拟失败的因子表达式（仅记录真正无法模拟的表达式）
-- ====================================================================
CREATE TABLE failed_expressions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    expression TEXT NOT NULL,                  -- 失败的因子表达式
    dataset_id VARCHAR(50) NOT NULL,           -- 数据集ID (analyst4, fundamental2等)
    region VARCHAR(10) NOT NULL,               -- 地区 (USA, GLB等)
    step INTEGER NOT NULL,                     -- 步骤 (1或2)
    failure_reason TEXT,                       -- 失败原因 (如: "Unexpected end of input", "Syntax error")
    error_details TEXT,                        -- 详细错误信息 (API返回的原始错误)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- 避免重复记录同样的表达式在同一条件下的相同错误
    UNIQUE(expression, dataset_id, region, step, failure_reason)
);

-- ====================================================================
-- 5. 系统配置表 - 替换 start_date.txt 等配置文件
-- ====================================================================
CREATE TABLE system_config (
    config_key VARCHAR(100) PRIMARY KEY,       -- 配置键
    config_value TEXT NOT NULL,                -- 配置值
    description TEXT,                          -- 描述
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 插入默认配置
INSERT INTO system_config (config_key, config_value, description) VALUES 
('start_date', '2025-07-27', '因子挖掘开始日期'),
('db_version', '1.0', '数据库版本'),
('migration_date', datetime('now'), '数据迁移日期');

-- ====================================================================
-- 创建索引 - 优化查询性能
-- ====================================================================

-- 因子表达式索引
CREATE INDEX idx_expressions_dataset_region_step ON factor_expressions(dataset_id, region, step);
CREATE INDEX idx_expressions_created ON factor_expressions(created_at);

-- 已检查因子索引  
CREATE INDEX idx_checked_dataset_region_step ON checked_alphas(dataset_id, region, step);
CREATE INDEX idx_checked_date ON checked_alphas(checked_at);

-- 可提交因子索引
CREATE INDEX idx_submitable_region_universe ON submitable_alphas(region, universe);
CREATE INDEX idx_submitable_sharpe ON submitable_alphas(sharpe);
CREATE INDEX idx_submitable_created ON submitable_alphas(created_at);

-- 失败表达式索引
CREATE INDEX idx_failed_expressions_dataset_region_step ON failed_expressions(dataset_id, region, step);
CREATE INDEX idx_failed_expressions_reason ON failed_expressions(failure_reason);
CREATE INDEX idx_failed_expressions_created ON failed_expressions(created_at);

-- 系统配置索引
CREATE INDEX idx_config_key ON system_config(config_key);

-- ====================================================================
-- 性能优化视图
-- ====================================================================

-- 因子表达式统计视图
CREATE VIEW factor_expression_stats AS
SELECT 
    dataset_id,
    region,
    step,
    COUNT(*) as expression_count,
    MAX(created_at) as latest_created
FROM factor_expressions 
GROUP BY dataset_id, region, step;

-- 已检查因子统计视图
CREATE VIEW checked_alpha_stats AS
SELECT 
    dataset_id,
    region,
    step,
    COUNT(*) as checked_count,
    MAX(checked_at) as latest_checked
FROM checked_alphas 
GROUP BY dataset_id, region, step;

-- 失败表达式统计视图
CREATE VIEW failed_expression_stats AS
SELECT 
    dataset_id,
    region,
    step,
    failure_reason,
    failure_stage,
    COUNT(*) as failure_count,
    COUNT(DISTINCT expression) as unique_expressions,
    MAX(created_at) as latest_failure
FROM failed_expressions 
GROUP BY dataset_id, region, step, failure_reason, failure_stage;

-- 失败表达式按原因统计视图
CREATE VIEW failure_reason_stats AS
SELECT 
    failure_reason,
    failure_stage,
    COUNT(*) as total_failures,
    COUNT(DISTINCT expression) as unique_failed_expressions,
    COUNT(DISTINCT dataset_id) as affected_datasets,
    AVG(LENGTH(expression)) as avg_expression_length,
    MAX(created_at) as latest_occurrence
FROM failed_expressions 
GROUP BY failure_reason, failure_stage
ORDER BY total_failures DESC;

-- 失败表达式按数据集统计视图
CREATE VIEW failure_dataset_stats AS
SELECT 
    dataset_id,
    region,
    step,
    COUNT(*) as total_failures,
    COUNT(DISTINCT expression) as unique_failed_expressions,
    COUNT(DISTINCT failure_reason) as failure_types,
    MAX(created_at) as latest_failure
FROM failed_expressions 
GROUP BY dataset_id, region, step
ORDER BY total_failures DESC;

-- ====================================================================
-- 4. 每日提交统计表 - 记录每天成功提交的因子数量
-- ====================================================================
CREATE TABLE daily_submit_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,                           -- 日期 YYYY-MM-DD
    successful_submits INTEGER DEFAULT 0,         -- 当日成功提交数量
    total_attempts INTEGER DEFAULT 0,             -- 当日总尝试数量
    timezone VARCHAR(20) DEFAULT 'UTC',           -- 使用的时区
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束检查
    CHECK (successful_submits >= 0),
    CHECK (total_attempts >= successful_submits),
    
    -- 复合唯一约束：日期+时区组合唯一
    UNIQUE(date, timezone)
);

-- 创建日期索引
CREATE INDEX idx_daily_submit_stats_date ON daily_submit_stats(date);

-- 每日提交统计视图
CREATE VIEW daily_submit_overview AS
SELECT 
    date,
    successful_submits,
    total_attempts,
    timezone,
    CASE 
        WHEN total_attempts > 0 THEN ROUND(successful_submits * 100.0 / total_attempts, 1)
        ELSE 0
    END as success_rate,
    last_updated
FROM daily_submit_stats
ORDER BY date DESC;

-- 系统状态总览视图
CREATE VIEW system_overview AS
SELECT 
    'factor_expressions' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as latest_update
FROM factor_expressions
UNION ALL
SELECT 
    'checked_alphas' as table_name,
    COUNT(*) as record_count,
    MAX(checked_at) as latest_update
FROM checked_alphas
UNION ALL
SELECT 
    'submitable_alphas' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as latest_update
FROM submitable_alphas
UNION ALL
SELECT 
    'daily_submit_stats' as table_name,
    COUNT(*) as record_count,
    MAX(last_updated) as latest_update
FROM daily_submit_stats;