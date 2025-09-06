/**
 * Alpha管理API服务
 */

import { apiClient } from './api';

export interface AlphaItem {
  alpha_id: string;
  tags: string;
  fitness: number;
  sharpe: number;
  correlation_value?: number; // 相关性值，仅在normal和ppac页签显示
  aggressive_mode?: boolean; // 激进模式标识
}

export interface AlphaListResponse {
  data: AlphaItem[];
  total: number;
  page: number;
  page_size: number;
  tab: string;
}

export interface AlphaStatistics {
  ppac_count: number;
  normal_count: number;
  pending_count: number;
  red_count: number;
  total_count: number;
}

export interface ManualRemoveRequest {
  alpha_ids: string[];
}

export interface ManualRemoveResponse {
  success: boolean;
  message: string;
  removed_count: number;
  failed_alphas: string[];
  total_requested: number;
}

export const alphasAPI = {
  /**
   * 获取可提交Alpha列表
   */
  getSubmitableAlphas: async (
    tab: 'ppac' | 'normal' | 'pending',
    page: number = 1,
    pageSize: number = 50
  ): Promise<AlphaListResponse> => {
    const response = await apiClient.get('/alphas/submitable', {
      params: {
        tab,
        page,
        page_size: pageSize,
      },
    });
    return response.data;
  },

  /**
   * 获取Alpha统计信息
   */
  getStatistics: async (): Promise<AlphaStatistics> => {
    const response = await apiClient.get('/alphas/statistics');
    return response.data;
  },

  /**
   * 手动从数据库移除Alpha
   */
  removeAlphas: async (request: ManualRemoveRequest): Promise<ManualRemoveResponse> => {
    const response = await apiClient.post('/alphas/remove', request);
    return response.data;
  },
};

export default alphasAPI;
