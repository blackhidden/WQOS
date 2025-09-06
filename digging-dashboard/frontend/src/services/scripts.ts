/**
 * 脚本管理API服务
 */

import { api } from './api';

export interface ScriptStatus {
  id: string | number;
  script_type: string;
  status: 'running' | 'stopped';
  script_name: string;
  pid?: number;
  tag?: string;
  started_at?: string;
  stopped_at?: string;
  log_file?: string;
  needs_config?: boolean;
  config_required?: string;
}

export interface ScriptTypes {
  [key: string]: string;
}

export interface ScriptStartResponse {
  status: string;
  pid: number;
  script_type: string;
  script_name: string;
  tag: string;
  log_file: string;
  start_time: string;
}

export interface ScriptStopResponse {
  status: string;
  pid: number;
  script_type: string;
  script_name: string;
  tag: string;
  terminate_method: string;
  stop_time: string;
}

export interface LogResponse {
  content: string;
  total_lines: number;
  current_offset: number;
  returned_lines: number;
  has_more?: boolean;
  log_files?: Array<{
    file: string;
    lines: number;
    size: number;
    error?: string;
    mode?: string;
  }>;
  mode?: 'realtime' | 'full';
}

export const scriptsAPI = {
  // 获取支持的脚本类型
  getScriptTypes: () => api.get<ScriptTypes>('/scripts/types'),
  
  // 获取所有脚本状态
  getAllScriptsStatus: () => api.get<{scripts: ScriptStatus[], script_types: ScriptTypes}>('/scripts/status'),
  
  // 启动脚本（向后兼容，内部调用带参数版本）
  startScript: (scriptType: string) => 
    api.post<ScriptStartResponse>(`/scripts/start/${scriptType}`, {}),
  
  // 启动脚本（带参数）
  startScriptWithParams: (scriptType: string, params: any) => 
    api.post<ScriptStartResponse>(`/scripts/start/${scriptType}`, params),
  
  // 停止脚本（按类型）
  stopScript: (scriptType: string, force: boolean = false) => 
    api.post<ScriptStopResponse>(`/scripts/stop/${scriptType}?force=${force}`),
  
  // 停止特定任务
  stopTask: (taskId: number, force: boolean = false) => 
    api.post<ScriptStopResponse>(`/scripts/stop/task/${taskId}?force=${force}`),
  
  // 获取任务日志 
  getTaskLogs: (taskId: number, offset: number = -1000, limit: number = 1000, includeRotated: boolean = false) => 
    api.get<LogResponse>(`/scripts/logs/task/${taskId}?offset=${offset}&limit=${limit}&include_rotated=${includeRotated}`),
  
  // 删除任务
  deleteTask: (taskId: number) => api.delete(`/scripts/task/${taskId}`),
};
