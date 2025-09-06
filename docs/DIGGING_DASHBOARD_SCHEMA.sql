-- ====================================================================
-- 挖掘控制面板数据库设计
-- ====================================================================

-- 用户认证表（唯一用户）
CREATE TABLE IF NOT EXISTS dashboard_user (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- 配置模板表
CREATE TABLE IF NOT EXISTS digging_config_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_name VARCHAR(100) NOT NULL,
    description TEXT,
    
    -- 模式选择
    use_recommended_fields BOOLEAN NOT NULL,
    
    -- 基础配置
    region VARCHAR(20) NOT NULL,
    universe VARCHAR(50) NOT NULL,
    delay INTEGER NOT NULL,
    instrument_type VARCHAR(50) DEFAULT 'EQUITY',
    max_trade VARCHAR(20) DEFAULT 'OFF',
    n_jobs INTEGER DEFAULT 3,
    
    -- 数据集模式配置
    dataset_id VARCHAR(100),
    
    -- 推荐字段模式配置
    recommended_name VARCHAR(100),
    recommended_fields TEXT,  -- JSON格式的字段列表
    
    -- 元数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER REFERENCES dashboard_user(id),
    
    -- 生成的tag名称
    tag_name VARCHAR(200) GENERATED ALWAYS AS (
        CASE 
            WHEN use_recommended_fields = 1 THEN 
                region || '_' || delay || '_' || instrument_type || '_' || universe || '_' || recommended_name || '_step'
            ELSE 
                region || '_' || delay || '_' || instrument_type || '_' || universe || '_' || dataset_id || '_step'
        END
    ) STORED
);

-- 挖掘进程表
CREATE TABLE IF NOT EXISTS digging_processes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_template_id INTEGER REFERENCES digging_config_templates(id),
    
    -- 进程信息
    process_id INTEGER,
    status VARCHAR(20) NOT NULL DEFAULT 'stopped', -- running, stopped, error, starting
    tag_name VARCHAR(200) NOT NULL,
    
    -- 时间信息
    started_at TIMESTAMP,
    stopped_at TIMESTAMP,
    
    -- 日志和输出
    log_file_path VARCHAR(500),
    error_message TEXT,
    
    -- 进程统计
    total_expressions INTEGER DEFAULT 0,
    completed_expressions INTEGER DEFAULT 0,
    
    -- 元数据
    started_by INTEGER REFERENCES dashboard_user(id),
    notes TEXT
);

-- 配置使用历史表
CREATE TABLE IF NOT EXISTS config_usage_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_template_id INTEGER REFERENCES digging_config_templates(id),
    process_id INTEGER REFERENCES digging_processes(id),
    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    used_by INTEGER REFERENCES dashboard_user(id)
);

-- 系统日志表
CREATE TABLE IF NOT EXISTS dashboard_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level VARCHAR(10) NOT NULL, -- INFO, WARNING, ERROR
    message TEXT NOT NULL,
    context TEXT, -- JSON格式的上下文信息
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER REFERENCES dashboard_user(id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_config_templates_name ON digging_config_templates(template_name);
CREATE INDEX IF NOT EXISTS idx_config_templates_created_at ON digging_config_templates(created_at);
CREATE INDEX IF NOT EXISTS idx_processes_status ON digging_processes(status);
CREATE INDEX IF NOT EXISTS idx_processes_started_at ON digging_processes(started_at);
CREATE INDEX IF NOT EXISTS idx_usage_history_used_at ON config_usage_history(used_at);
CREATE INDEX IF NOT EXISTS idx_dashboard_logs_created_at ON dashboard_logs(created_at);

-- 插入默认用户（密码需要在应用中设置）
INSERT OR IGNORE INTO dashboard_user (username, password_hash, email) 
VALUES ('admin', '', 'admin@worldquant.local');

-- 示例配置模板
INSERT OR IGNORE INTO digging_config_templates (
    template_name, description, use_recommended_fields, region, universe, 
    delay, instrument_type, max_trade, n_jobs, dataset_id
) VALUES (
    'Default USA Equity', 
    '默认美股配置模板', 
    0, 'USA', 'TOP3000', 1, 'EQUITY', 'OFF', 3, 'fundamental6'
);

INSERT OR IGNORE INTO digging_config_templates (
    template_name, description, use_recommended_fields, region, universe,
    delay, instrument_type, max_trade, n_jobs, recommended_name, recommended_fields
) VALUES (
    'Recommended Fields Template',
    '推荐字段配置模板',
    1, 'USA', 'TOP3000', 1, 'EQUITY', 'OFF', 3, 'analyst11',
    '["close", "volume", "market_cap", "pe_ratio"]'
);
