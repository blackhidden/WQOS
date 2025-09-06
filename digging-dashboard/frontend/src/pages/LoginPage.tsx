/**
 * 登录页面
 */

import React, { useEffect } from 'react';
import { Form, Input, Button, Alert, Typography } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useDispatch, useSelector } from 'react-redux';

import { RootState, AppDispatch } from '../store';
import { loginAsync, clearError } from '../store/authSlice';
import { LoginRequest } from '../types/auth';

const { Title, Text } = Typography;

const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  const dispatch = useDispatch<AppDispatch>();
  const { loading, error, isAuthenticated } = useSelector((state: RootState) => state.auth);

  // 如果已经登录，重定向到首页
  useEffect(() => {
    if (isAuthenticated) {
      navigate('/dashboard');
    }
  }, [isAuthenticated, navigate]);

  // 清除错误信息
  useEffect(() => {
    dispatch(clearError());
  }, [dispatch]);

  const handleSubmit = async (values: LoginRequest) => {
    const result = await dispatch(loginAsync(values));
    if (result.type === 'auth/login/fulfilled') {
      navigate('/dashboard');
    }
  };

  return (
    <div className="login-container">
      <div className="login-form-wrapper">
        <div className="login-logo">
          <Title level={3} style={{ color: '#1890ff', marginBottom: 8 }}>
            WorldQuant Alpha
          </Title>
          <Text type="secondary">挖掘控制面板</Text>
        </div>

        {error && (
          <Alert
            message="登录失败"
            description={error}
            type="error"
            closable
            onClose={() => dispatch(clearError())}
            style={{ marginBottom: 24 }}
          />
        )}

        <Form
          name="login"
          size="large"
          onFinish={handleSubmit}
          autoComplete="off"
        >
          <Form.Item
            name="username"
            rules={[
              { required: true, message: '请输入用户名' },
              { min: 3, message: '用户名至少3个字符' }
            ]}
          >
            <Input
              prefix={<UserOutlined />}
              placeholder="用户名"
              autoComplete="username"
            />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[
              { required: true, message: '请输入密码' },
              { min: 6, message: '密码至少6个字符' }
            ]}
          >
            <Input.Password
              prefix={<LockOutlined />}
              placeholder="密码"
              autoComplete="current-password"
            />
          </Form.Item>

          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              block
            >
              登录
            </Button>
          </Form.Item>
        </Form>
      </div>
    </div>
  );
};

export default LoginPage;
