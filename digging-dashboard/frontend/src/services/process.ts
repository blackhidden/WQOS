/**
 * 进程管理API服务
 */

import { apiClient } from './api';
import { DiggingConfig } from '../types/config';

// 获取进程状态
export const getProcessStatus = () => {
  return apiClient.get('/process/status');
};

// 获取所有进程状态统计
export const getAllProcessesStatus = () => {
  return apiClient.get('/process/status/all');
};

// 启动进程
export const startProcess = (config: DiggingConfig) => {
  return apiClient.post('/process/start', config);
};

// 从模板启动进程
export const startProcessFromTemplate = (templateId: number, stage: number = 1, n_jobs: number = 5) => {
  return apiClient.post(`/process/start-template/${templateId}?stage=${stage}&n_jobs=${n_jobs}`);
};

// 停止进程
export const stopProcess = (force: boolean = false) => {
  return apiClient.post('/process/stop', { force });
};

// 重启进程
export const restartProcess = (config: DiggingConfig) => {
  return apiClient.post('/process/restart', config);
};

// 获取进程日志
export const getProcessLogs = (limit: number = 100, offset: number = 0) => {
  return apiClient.get(`/process/logs?limit=${limit}&offset=${offset}`);
};