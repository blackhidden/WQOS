/**
 * 主控制面板布局组件
 */

import React, { useState, useEffect } from 'react';
import { Layout, Menu, Avatar, Dropdown, Button, Typography, Drawer } from 'antd';
import { 
  SettingOutlined, 
  MonitorOutlined, 
  FileTextOutlined, 
  HistoryOutlined,
  DashboardOutlined,
  LogoutOutlined,
  UserOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  CodeOutlined,
  BarChartOutlined,
  GithubOutlined
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useSelector, useDispatch } from 'react-redux';
import type { MenuProps } from 'antd';

import { RootState, AppDispatch } from '../../store';
import { logout } from '../../store/authSlice';

const { Header, Sider, Content } = Layout;
const { Text } = Typography;

interface DashboardLayoutProps {
  children: React.ReactNode;
}

export const DashboardLayout: React.FC<DashboardLayoutProps> = ({ children }) => {
  const [collapsed, setCollapsed] = useState(false);
  const [mobileMenuVisible, setMobileMenuVisible] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const dispatch = useDispatch<AppDispatch>();
  
  const { user } = useSelector((state: RootState) => state.auth);

  // 检测屏幕尺寸
  useEffect(() => {
    const checkScreenSize = () => {
      const mobile = window.innerWidth <= 768;
      setIsMobile(mobile);
      if (mobile) {
        setCollapsed(true); // 移动端默认收起侧边栏
      }
    };

    checkScreenSize();
    window.addEventListener('resize', checkScreenSize);
    return () => window.removeEventListener('resize', checkScreenSize);
  }, []);

  // 菜单项
  const menuItems: MenuProps['items'] = [
    {
      key: '/dashboard',
      icon: <DashboardOutlined />,
      label: '控制面板',
    },
    {
      key: '/config',
      icon: <SettingOutlined />,
      label: '配置管理',
    },
    {
      key: '/process-management',
      icon: <MonitorOutlined />,
      label: '进程管理',
    },
    {
      key: '/alpha-status',
      icon: <BarChartOutlined />,
      label: 'Alpha状态',
    },
  ];

  // 用户菜单
  const userMenuItems: MenuProps['items'] = [
    {
      key: 'github',
      icon: <GithubOutlined />,
      label: '开源项目地址',
      onClick: () => {
        window.open('https://github.com/Yao-lin101/WorldQuant', '_blank');
      },
    },
    {
      type: 'divider',
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: '退出登录',
      onClick: () => {
        dispatch(logout());
        navigate('/login');
      },
    },
  ];

  const handleMenuClick = ({ key }: { key: string }) => {
    navigate(key);
    // 移动端点击菜单后关闭抽屉
    if (isMobile) {
      setMobileMenuVisible(false);
    }
  };

  const toggleMenu = () => {
    if (isMobile) {
      setMobileMenuVisible(!mobileMenuVisible);
    } else {
      setCollapsed(!collapsed);
    }
  };

  return (
    <Layout className="dashboard-layout">
      <Header className="dashboard-header">
        <div className="dashboard-logo">
          <Button
            type="text"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={toggleMenu}
            style={{ marginRight: isMobile ? 8 : 16 }}
          />
          <span className="dashboard-logo-text">
            {isMobile ? 'WQ Alpha' : 'WorldQuant Alpha 挖掘控制面板'}
          </span>
        </div>
        
        <div className="dashboard-user-info">
          <Text type="secondary">欢迎，</Text>
          <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
            <div style={{ cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8 }}>
              <Avatar 
                size="small" 
                src="https://q.qlogo.cn/headimg_dl?dst_uin=1017789696&spec=640&img_type=jpg"
                icon={<UserOutlined />}
              />
              <Text strong>{user?.username}</Text>
            </div>
          </Dropdown>
        </div>
      </Header>
      
      <Layout style={{ paddingTop: '64px' }}>
        {/* 桌面端侧边栏 */}
        {!isMobile && (
          <Sider 
            trigger={null}
            collapsible 
            collapsed={collapsed}
            className="dashboard-sidebar"
            width={250}
            collapsedWidth={80}
          >
            <Menu
              theme="light"
              mode="inline"
              selectedKeys={[location.pathname]}
              items={menuItems}
              onClick={handleMenuClick}
              style={{ height: '100%', borderRight: 0 }}
            />
          </Sider>
        )}
        
        {/* 移动端抽屉导航 */}
        <Drawer
          title="导航菜单"
          placement="left"
          onClose={() => setMobileMenuVisible(false)}
          open={mobileMenuVisible && isMobile}
          bodyStyle={{ padding: 0 }}
          width={250}
          className="mobile-menu-drawer"
        >
          <Menu
            theme="light"
            mode="inline"
            selectedKeys={[location.pathname]}
            items={menuItems}
            onClick={handleMenuClick}
            style={{ border: 0 }}
          />
        </Drawer>
        
        <Layout>
          <Content 
            className="dashboard-content"
            style={{
              marginLeft: !isMobile ? (collapsed ? '80px' : '250px') : '0px',
              transition: 'margin-left 0.2s ease'
            }}
          >
            {children}
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
};
