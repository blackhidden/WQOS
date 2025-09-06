/**
 * 进程管理相关类型定义
 */

export interface ProcessStatus {
  status: 'running' | 'stopped' | 'error' | 'starting' | 'stopping';
  pid?: number;
  config_id?: number;
  start_time?: string;
  uptime?: number;
  memory_usage?: number;
  cpu_usage?: number;
  tag?: string;
  error_message?: string;
}

export interface DiggingProcess {
  id: number;
  config_template_id?: number;
  process_id?: number;
  status: string;
  tag_name: string;
  started_at?: string;
  stopped_at?: string;
  log_file_path?: string;
  error_message?: string;
}

export interface ProcessLog {
  id: number;
  timestamp?: string;
  level: string;
  message: string;
  logger?: string;
  module?: string;
  function?: string;
  line?: number;
  raw: string;
  structured: boolean;
  details: any;
}

export interface LogResponse {
  logs: ProcessLog[];
  total: number;
  limit: number;
  offset: number;
}

// 进程状态标签映射
export const PROCESS_STATUS_LABELS = {
  running: '运行中',
  stopped: '已停止',
  error: '错误',
  starting: '启动中',
  stopping: '停止中',
};

// 进程状态颜色映射
export const PROCESS_STATUS_COLORS = {
  running: 'green',
  stopped: 'red',
  error: 'red',
  starting: 'orange',
  stopping: 'orange',
};