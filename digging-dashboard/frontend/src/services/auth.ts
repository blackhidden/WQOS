/**
 * 认证相关API服务
 */

import { api } from './api';
import { LoginRequest, TokenResponse, UserInfo } from '../types/auth';

export const authAPI = {
  /**
   * 用户登录
   */
  login: async (loginData: LoginRequest): Promise<TokenResponse> => {
    const response = await api.post<TokenResponse>('/auth/login', loginData);
    return response.data;
  },

  /**
   * 用户登出
   */
  logout: async (): Promise<void> => {
    await api.post('/auth/logout');
  },

  /**
   * 获取当前用户信息
   */
  getCurrentUser: async (): Promise<UserInfo> => {
    const response = await api.get<UserInfo>('/auth/me');
    return response.data;
  },

  /**
   * 验证令牌有效性
   */
  verifyToken: async (): Promise<{ valid: boolean; user_id: number; username: string }> => {
    const response = await api.post('/auth/verify-token');
    return response.data;
  },
};
