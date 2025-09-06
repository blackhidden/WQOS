/**
 * 主控制台页面
 */

import React, { useEffect, useState } from 'react';
import {
  Card,
  Row,
  Col,
  Statistic,
  Typography,
  Timeline,
  Table,
  Tag,
  Button,
  Space,
  Alert,
  Progress,
  List,
  Avatar
} from 'antd';
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  SettingOutlined,
  MonitorOutlined,
  FileTextOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  ApiOutlined,
  BugOutlined
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useDispatch, useSelector } from 'react-redux';

import { DashboardLayout } from '../components/Layout/DashboardLayout';
import { RootState, AppDispatch } from '../store';
import { getProcessStatusAsync, getAllProcessesStatusAsync } from '../store/processSlice';
import { fetchConfigTemplatesAsync } from '../store/configSlice';

const { Title, Text, Paragraph } = Typography;

const DashboardPage: React.FC = () => {
  const navigate = useNavigate();
  const dispatch = useDispatch<AppDispatch>();
  
  const { currentProcess, allProcesses } = useSelector((state: RootState) => state.process);
  const { templates } = useSelector((state: RootState) => state.config);
  const { user } = useSelector((state: RootState) => state.auth);

  // 动态计时状态
  const [dynamicUptime, setDynamicUptime] = useState<number>(0);
  const [dynamicProcesses, setDynamicProcesses] = useState<any[]>([]);

  useEffect(() => {
    dispatch(getProcessStatusAsync());
    dispatch(getAllProcessesStatusAsync());
    dispatch(fetchConfigTemplatesAsync());
    
    // 每30秒刷新一次状态
    const interval = setInterval(() => {
      dispatch(getProcessStatusAsync());
      dispatch(getAllProcessesStatusAsync());
    }, 30000);
    
    return () => clearInterval(interval);
  }, [dispatch]);

  // 当API数据更新时，更新动态计时的初始值
  useEffect(() => {
    if (allProcesses) {
      setDynamicUptime(allProcesses.max_uptime || 0);
      setDynamicProcesses(allProcesses.processes.map(process => ({
        ...process,
        currentUptime: process.uptime
      })));
    }
  }, [allProcesses]);

  // 每秒更新动态计时
  useEffect(() => {
    const timer = setInterval(() => {
      if (allProcesses && allProcesses.total_processes > 0) {
        setDynamicProcesses(prev => {
          const updated = prev.map(process => ({
            ...process,
            currentUptime: process.currentUptime + 1
          }));
          
          // 动态计算最长运行时间
          const maxUptime = Math.max(...updated.map(p => p.currentUptime));
          setDynamicUptime(maxUptime);
          
          return updated;
        });
      } else {
        // 如果没有运行的进程，重置计时
        setDynamicUptime(0);
        setDynamicProcesses([]);
      }
    }, 1000);

    return () => clearInterval(timer);
  }, [allProcesses?.total_processes]);

  const formatUptime = (seconds?: number): string => {
    if (!seconds) return '0秒';
    
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    if (hours > 0) {
      return `${hours}小时${minutes}分${secs}秒`;
    } else if (minutes > 0) {
      return `${minutes}分${secs}秒`;
    } else {
      return `${secs}秒`;
    }
  };

  const formatMemory = (mb?: number): string => {
    if (!mb) return '0 MB';
    if (mb > 1024) {
      return `${(mb / 1024).toFixed(2)} GB`;
    }
    return `${mb.toFixed(2)} MB`;
  };

  const getStatusIcon = (status?: string) => {
    switch (status) {
      case 'running':
        return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'stopped':
        return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      case 'error':
        return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      default:
        return <ClockCircleOutlined style={{ color: '#faad14' }} />;
    }
  };

  const getStatusColor = (status?: string) => {
    switch (status) {
      case 'running':
        return '#52c41a';
      case 'stopped':
        return '#ff4d4f';
      case 'error':
        return '#ff4d4f';
      default:
        return '#faad14';
    }
  };

  const quickActions = [
    {
      title: '配置管理',
      description: '创建和管理挖掘配置模板',
      icon: <SettingOutlined style={{ fontSize: '24px', color: '#1890ff' }} />,
      path: '/config',
    },
    {
      title: '进程监控',
      description: '监控和控制挖掘进程',
      icon: <MonitorOutlined style={{ fontSize: '24px', color: '#52c41a' }} />,
      path: '/monitor',
    },
  ];

  const recentTemplates = templates.slice(0, 5);

  const systemHealth = [
    {
      name: '后端API',
      status: 'healthy',
      icon: <ApiOutlined />,
      description: '所有API服务正常运行'
    },
    {
      name: '数据库',
      status: 'healthy',
      icon: <DatabaseOutlined />,
      description: 'SQLite数据库连接正常'
    },
    {
      name: '会话管理',
      status: 'healthy',
      icon: <CheckCircleOutlined />,
      description: '统一会话管理器运行正常'
    },
  ];

  return (
    <DashboardLayout>
      <div style={{ padding: '24px' }}>
        {/* 欢迎信息 */}
        <Row style={{ marginBottom: '24px' }}>
          <Col span={24}>
            <Card>
              <Row align="middle">
                <Col flex={1}>
                  <Title level={2} style={{ margin: 0 }}>
                    欢迎回来，{user?.username}！
                  </Title>
                  <Paragraph style={{ margin: '8px 0 0 0', color: '#666' }}>
                    WorldQuant Alpha 挖掘控制台 - 管理您的因子挖掘工作流
                  </Paragraph>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>

        {/* 状态概览 */}
        <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
          <Col xs={24} sm={12} md={6}>
            <Card>
              <Statistic
                title="运行进程数"
                value={allProcesses?.total_processes || 0}
                prefix={getStatusIcon(allProcesses?.status)}
                suffix="个"
                valueStyle={{ color: getStatusColor(allProcesses?.status) }}
              />
            </Card>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <Card>
              <Statistic
                title="配置模板"
                value={templates.length}
                prefix={<SettingOutlined />}
                suffix="个"
              />
            </Card>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <Card>
              <Statistic
                title="总内存使用"
                value={allProcesses?.total_memory_usage ? formatMemory(allProcesses.total_memory_usage) : '0 MB'}
                prefix={<MonitorOutlined />}
              />
              {allProcesses?.total_memory_usage && (
                <Progress
                  percent={Math.min((allProcesses.total_memory_usage / 1024) * 100, 100)}
                  showInfo={false}
                  size="small"
                  style={{ marginTop: '8px' }}
                />
              )}
            </Card>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <Card>
              <Statistic
                title="最长运行时间"
                value={formatUptime(dynamicUptime)}
                prefix={<ClockCircleOutlined />}
              />
            </Card>
          </Col>
        </Row>

        {/* 运行进程详情 */}
        {allProcesses && allProcesses.total_processes > 0 && (
          <Row style={{ marginBottom: '24px' }}>
            <Col span={24}>
              <Card title="运行中的进程" extra={<MonitorOutlined />}>
                <List
                  dataSource={dynamicProcesses}
                  renderItem={(process) => (
                    <List.Item>
                      <List.Item.Meta
                        avatar={
                          <Avatar 
                            style={{ backgroundColor: '#52c41a' }}
                            icon={<PlayCircleOutlined />}
                          />
                        }
                        title={
                          <Space>
                            <Text strong>{process.tag || `进程 ${process.pid}`}</Text>
                            <Tag color="blue">{process.script_type}</Tag>
                            <Tag color="green">PID: {process.pid}</Tag>
                          </Space>
                        }
                        description={
                          <Space size="large">
                            <Text type="secondary">
                              运行时间: {formatUptime(process.currentUptime)}
                            </Text>
                            <Text type="secondary">
                              内存: {formatMemory(process.memory_usage)}
                            </Text>
                            <Text type="secondary">
                              CPU: {process.cpu_usage.toFixed(1)}%
                            </Text>
                            <Text type="secondary">
                              启动时间: {new Date(process.start_time).toLocaleString()}
                            </Text>
                          </Space>
                        }
                      />
                    </List.Item>
                  )}
                />
              </Card>
            </Col>
          </Row>
        )}

        <Row gutter={[16, 16]}>
          {/* 快速操作 */}
          <Col xs={24} lg={12}>
            <Card title="快速操作" extra={<SettingOutlined />}>
              <List
                dataSource={quickActions}
                renderItem={(item) => (
                  <List.Item
                    actions={[
                      <Button 
                        type="link" 
                        onClick={() => navigate(item.path)}
                      >
                        进入
                      </Button>
                    ]}
                  >
                    <List.Item.Meta
                      avatar={<Avatar icon={item.icon} />}
                      title={item.title}
                      description={item.description}
                    />
                  </List.Item>
                )}
              />
            </Card>
          </Col>

          {/* 最近配置模板 */}
          <Col xs={24} lg={12}>
            <Card 
              title="最近配置模板" 
              extra={
                <Button 
                  type="link" 
                  onClick={() => navigate('/config')}
                >
                  查看全部
                </Button>
              }
            >
              {recentTemplates.length > 0 ? (
                <List
                  dataSource={recentTemplates}
                  renderItem={(template) => (
                    <List.Item>
                      <List.Item.Meta
                        title={
                          <Space>
                            <Text strong>{template.name}</Text>
                            <Tag color="blue" style={{ fontFamily: 'monospace', fontSize: '11px' }}>
                              {template.tag_preview}
                            </Tag>
                          </Space>
                        }
                        description={
                          <Space>
                            <Text type="secondary">{template.description}</Text>
                            <Tag color={template.config_data.use_recommended_fields ? 'green' : 'orange'}>
                              {template.config_data.use_recommended_fields ? '推荐字段' : '数据集'}
                            </Tag>
                          </Space>
                        }
                      />
                    </List.Item>
                  )}
                />
              ) : (
                <Text type="secondary">暂无配置模板</Text>
              )}
            </Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginTop: '16px' }}>
          {/* 系统健康状态 */}
          <Col xs={24} lg={12}>
            <Card title="系统健康状态" extra={<CheckCircleOutlined style={{ color: '#52c41a' }} />}>
              <List
                dataSource={systemHealth}
                renderItem={(item) => (
                  <List.Item>
                    <List.Item.Meta
                      avatar={<Avatar icon={item.icon} style={{ backgroundColor: '#52c41a' }} />}
                      title={
                        <Space>
                          <Text>{item.name}</Text>
                          <Tag color="green">正常</Tag>
                        </Space>
                      }
                      description={item.description}
                    />
                  </List.Item>
                )}
              />
            </Card>
          </Col>

          {/* 活动时间线 */}
          <Col xs={24} lg={12}>
            <Card title="最近活动" extra={<ClockCircleOutlined />}>
              <Timeline>
                <Timeline.Item color="green">
                  <Text type="secondary">{new Date().toLocaleString()}</Text>
                  <br />
                  <Text>系统状态检查 - 正常</Text>
                </Timeline.Item>
                {currentProcess?.start_time && (
                  <Timeline.Item color="blue">
                    <Text type="secondary">
                      {new Date(currentProcess.start_time).toLocaleString()}
                    </Text>
                    <br />
                    <Text>挖掘进程启动</Text>
                  </Timeline.Item>
                )}
                <Timeline.Item color="gray">
                  <Text type="secondary">{user?.last_login ? new Date(user.last_login).toLocaleString() : '未知'}</Text>
                  <br />
                  <Text>用户登录</Text>
                </Timeline.Item>
              </Timeline>
            </Card>
          </Col>
        </Row>
      </div>
    </DashboardLayout>
  );
};

export default DashboardPage;