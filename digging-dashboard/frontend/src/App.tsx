import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Provider } from 'react-redux';
import { ConfigProvider, App as AntApp } from 'antd';
import zhCN from 'antd/locale/zh_CN';

import { store } from './store';
import { ProtectedRoute } from './components/Auth/ProtectedRoute';
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import ConfigPage from './pages/ConfigPage';
import ProcessManagementPage from './pages/ProcessManagementPage';
import AlphaStatusPage from './pages/AlphaStatusPage';

import './App.css';

const App: React.FC = () => {
  return (
    <Provider store={store}>
      <ConfigProvider 
        locale={zhCN}
        theme={{
          token: {
            colorPrimary: '#1890ff',
            colorSuccess: '#52c41a',
            colorWarning: '#faad14',
            colorError: '#f5222d',
            borderRadius: 6,
          },
        }}
      >
        <Router>
          <AntApp>
            <div className="App">
              <Routes>
              {/* 登录页面 */}
              <Route path="/login" element={<LoginPage />} />
              
              {/* 受保护的页面 */}
              <Route path="/" element={
                <ProtectedRoute>
                  <Navigate to="/dashboard" replace />
                </ProtectedRoute>
              } />
              
              <Route path="/dashboard" element={
                <ProtectedRoute>
                  <DashboardPage />
                </ProtectedRoute>
              } />
              
              <Route path="/config" element={
                <ProtectedRoute>
                  <ConfigPage />
                </ProtectedRoute>
              } />
              
              <Route path="/process-management" element={
                <ProtectedRoute>
                  <ProcessManagementPage />
                </ProtectedRoute>
              } />
              
              <Route path="/alpha-status" element={
                <ProtectedRoute>
                  <AlphaStatusPage />
                </ProtectedRoute>
              } />
              
              {/* 默认重定向 */}
              <Route path="*" element={<Navigate to="/dashboard" replace />} />
              </Routes>
            </div>
          </AntApp>
        </Router>
      </ConfigProvider>
    </Provider>
  );
};

export default App;