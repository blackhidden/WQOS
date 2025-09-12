/**
 * 数据集相关API服务
 */

import { api } from './api';

// 数据集字段接口
export interface DatasetField {
  id: string;
  description: string;
  type: string;
  [key: string]: any; // 允许其他字段
}

// 数据集字段响应接口
export interface DatasetFieldsResponse {
  dataset_id: string;
  region: string;
  universe: string;
  delay: number;
  total_fields: number;
  raw_fields: DatasetField[];
  fetch_time?: number;
  error?: string;
}

// 数据集字段进度响应接口
export interface DatasetFieldsProgressDetails {
  page_count: number;
  current_count: number;
  estimated_total?: number;
  elapsed_time: number;
  estimated_remaining_time?: number;
}

export interface DatasetFieldsProgressResponse {
  task_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number; // 0-100
  message: string;
  data?: DatasetFieldsResponse;
  details?: DatasetFieldsProgressDetails;
}

// 数据集API
export const datasetAPI = {
  /**
   * 获取数据集字段信息 (同步方式)
   */
  getDatasetFields: async (
    dataset_id: string,
    region: string = 'USA',
    universe: string = 'TOP3000',
    delay: number = 1,
    instrument_type: string = 'EQUITY'
  ): Promise<DatasetFieldsResponse> => {
    const response = await api.get('/dataset/fields', {
      params: {
        dataset_id,
        region,
        universe,
        delay,
        instrument_type
      }
    });
    return response.data;
  },

  /**
   * 启动异步获取数据集字段任务
   */
  startDatasetFieldsFetch: async (
    dataset_id: string,
    region: string = 'USA',
    universe: string = 'TOP3000',
    delay: number = 1,
    instrument_type: string = 'EQUITY'
  ): Promise<DatasetFieldsProgressResponse> => {
    const response = await api.post('/dataset/fields/async', null, {
      params: {
        dataset_id,
        region,
        universe,
        delay,
        instrument_type
      }
    });
    return response.data;
  },

  /**
   * 获取数据集字段任务进度
   */
  getDatasetFieldsProgress: async (task_id: string): Promise<DatasetFieldsProgressResponse> => {
    const response = await api.get(`/dataset/fields/progress/${task_id}`);
    return response.data;
  }
};
