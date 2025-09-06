/**
 * 配置管理API服务
 */

import { apiClient } from './api';
import { ConfigTemplate, DiggingConfig } from '../types/config';

// 获取配置模板列表
export const getConfigTemplates = (skip: number = 0, limit: number = 100) => {
  return apiClient.get(`/config/templates?skip=${skip}&limit=${limit}`);
};

// 创建配置模板
export const createConfigTemplate = (templateData: any) => {
  return apiClient.post('/config/templates', templateData);
};

// 获取单个配置模板
export const getConfigTemplate = (templateId: number) => {
  return apiClient.get(`/config/templates/${templateId}`);
};

// 更新配置模板
export const updateConfigTemplate = (templateId: number, templateData: any) => {
  return apiClient.put(`/config/templates/${templateId}`, templateData);
};

// 删除配置模板
export const deleteConfigTemplate = (templateId: number) => {
  return apiClient.delete(`/config/templates/${templateId}`);
};

// 验证配置
export const validateConfig = (configData: DiggingConfig) => {
  return apiClient.post('/config/validate', configData);
};

// 生成tag
export const generateTag = (configData: any) => {
  return apiClient.post('/config/generate-tag', configData);
};

// 获取当前配置
export const getCurrentConfig = () => {
  return apiClient.get('/config/current');
};

// 获取字段选项
export const getFieldOptions = () => {
  return apiClient.get('/config/field-options');
};

// 获取WorldQuant配置选项
export const getWorldQuantOptions = () => {
  return apiClient.get('/config/worldquant-options');
};

// 根据工具类型获取地区列表
export const getRegionsForInstrument = (instrumentType: string) => {
  return apiClient.get(`/config/regions/${instrumentType}`);
};

// 根据工具类型和地区获取股票池列表
export const getUniversesForRegion = (instrumentType: string, region: string) => {
  return apiClient.get(`/config/universes/${instrumentType}/${region}`);
};

// 根据工具类型和地区获取延迟选项
export const getDelaysForRegion = (instrumentType: string, region: string) => {
  return apiClient.get(`/config/delays/${instrumentType}/${region}`);
};

// 根据工具类型和地区获取中性化选项
export const getNeutralizationsForRegion = (instrumentType: string, region: string) => {
  return apiClient.get(`/config/neutralizations/${instrumentType}/${region}`);
};

// 同步WorldQuant配置到数据库
export const syncWorldQuantConfig = () => {
  return apiClient.post('/config/sync-worldquant-config');
};

// 获取WorldQuant配置同步历史
export const getWorldQuantSyncHistory = (limit: number = 10) => {
  return apiClient.get(`/config/worldquant-sync-history?limit=${limit}`);
};

// 获取WorldQuant配置状态
export const getWorldQuantConfigStatus = () => {
  return apiClient.get('/config/worldquant-config-status');
};