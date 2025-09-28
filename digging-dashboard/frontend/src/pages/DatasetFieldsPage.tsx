/**
 * 数据集字段查看页面
 */

import React, { useState } from 'react';
import {
  Card,
  Table,
  Button,
  Space,
  Typography,
  Row,
  Col,
  Input,
  Select,
  Form,
  Spin,
  App,
  Empty,
  Descriptions,
  Tag,
  Progress,
  Alert
} from 'antd';
import {
  SearchOutlined,
  DownloadOutlined,
  DatabaseOutlined,
  ReloadOutlined
} from '@ant-design/icons';

import { DashboardLayout } from '../components/Layout/DashboardLayout';
import { datasetAPI, DatasetFieldsResponse, DatasetFieldsProgressDetails } from '../services/dataset';
import { createDatasetFieldsProgressWebSocket, DatasetFieldsProgressWebSocket } from '../services/websocket';
import { useDispatch, useSelector } from 'react-redux';
import { RootState, AppDispatch } from '../store';
import { fetchWorldQuantOptionsAsync, setDynamicOptions } from '../store/configSlice';

const { Title, Text } = Typography;
const { Option } = Select;

interface DatasetFieldsPageState {
  loading: boolean;
  data: DatasetFieldsResponse | null;
  progress: {
    visible: boolean;
    taskId: string | null;
    progress: number;
    status: string;
    message: string;
    details?: DatasetFieldsProgressDetails;
  };
  searchParams: {
    dataset_id: string;
    region: string;
    universe: string;
    delay: number;
  };
}

const DatasetFieldsPage: React.FC = () => {
  const { message } = App.useApp();
  const dispatch = useDispatch<AppDispatch>();
  const { worldQuantOptions, dynamicOptions } = useSelector((state: RootState) => state.config);
  const [form] = Form.useForm();
  const [wsClient, setWsClient] = React.useState<DatasetFieldsProgressWebSocket | null>(null);
  
  const [state, setState] = useState<DatasetFieldsPageState>({
    loading: false,
    data: null,
    progress: {
      visible: false,
      taskId: null,
      progress: 0,
      status: '',
      message: '',
      details: undefined
    },
    searchParams: {
      dataset_id: '',
      region: 'USA',
      universe: 'TOP3000',
      delay: 1
    }
  });

  // 加载WorldQuant选项
  React.useEffect(() => {
    dispatch(fetchWorldQuantOptionsAsync());
  }, [dispatch]);

  // 组件卸载时清理WebSocket连接
  React.useEffect(() => {
    return () => {
      if (wsClient) {
        wsClient.disconnect();
      }
    };
  }, [wsClient]);

  // 当worldQuantOptions加载完成后，初始化默认的EQUITY选项
  React.useEffect(() => {
    if (worldQuantOptions?.regions?.EQUITY) {
      const defaultInstrumentType = 'EQUITY';
      const defaultRegion = 'USA';
      
      const regions = worldQuantOptions.regions[defaultInstrumentType] || [];
      const universes = worldQuantOptions.universes?.instrumentType?.[defaultInstrumentType]?.region?.[defaultRegion] || [];
      const delays = worldQuantOptions.delays?.instrumentType?.[defaultInstrumentType]?.region?.[defaultRegion] || [];
      
      dispatch(setDynamicOptions({
        regions,
        universes,
        delays,
        neutralizations: [] // 数据集字段页面不需要neutralizations
      }));
    }
  }, [worldQuantOptions, dispatch]);

  // 处理地区变化
  const handleRegionChange = (region: string) => {
    const instrumentType = 'EQUITY'; // 数据集字段页面固定使用EQUITY
    if (instrumentType && region && worldQuantOptions) {
      const universeOptions = worldQuantOptions.universes?.instrumentType?.[instrumentType]?.region?.[region] || [];
      const delayOptions = worldQuantOptions.delays?.instrumentType?.[instrumentType]?.region?.[region] || [];
      
      dispatch(setDynamicOptions({
        regions: worldQuantOptions.regions?.[instrumentType] || [],
        universes: universeOptions,
        delays: delayOptions,
        neutralizations: []
      }));
      
      // 设置默认值
      form.setFieldsValue({
        universe: universeOptions.length > 0 ? universeOptions[0]?.value : undefined,
        delay: delayOptions.length > 0 ? delayOptions[0]?.value : undefined
      });
    }
  };

  // 使用WebSocket或轮询来跟踪进度
  const trackProgress = async (taskId: string) => {
    let pollInterval: NodeJS.Timeout | null = null;
    let pollTimeout: NodeJS.Timeout | null = null;
    let isCompleted = false; // 防止重复处理完成状态
    
    // 清理所有定时器和WebSocket连接
    const cleanup = () => {
      if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
      }
      if (pollTimeout) {
        clearTimeout(pollTimeout);
        pollTimeout = null;
      }
      if (wsClient) {
        wsClient.disconnect();
        setWsClient(null);
      }
    };

    // 进度更新处理函数
    const handleProgressUpdate = (progressData: any) => {
      if (isCompleted) return; // 如果已完成，忽略后续更新
      
      setState(prev => ({
        ...prev,
        progress: {
          visible: true,
          taskId: taskId,
          progress: progressData.progress,
          status: progressData.status,
          message: progressData.message,
          details: progressData.details
        }
      }));
    };

    // 任务完成处理函数
    const handleTaskComplete = (data: any) => {
      if (isCompleted) return; // 防止重复处理
      isCompleted = true;
      
      cleanup(); // 立即清理所有资源
      
      setState(prev => ({
        ...prev,
        loading: false,
        progress: { ...prev.progress, visible: false },
        data: data
      }));

      if (data) {
        message.success(`成功获取数据集 ${data.dataset_id} 的 ${data.total_fields} 个字段`);
      }
    };

    // 错误处理函数
    const handleError = (error: string) => {
      if (isCompleted) return; // 防止重复处理
      isCompleted = true;
      
      cleanup(); // 立即清理所有资源
      
      setState(prev => ({ 
        ...prev, 
        loading: false,
        progress: { ...prev.progress, visible: false }
      }));
      message.error(`获取字段信息失败: ${error}`);
    };

    // 轮询函数
    const startPolling = () => {
      if (isCompleted) return;
      
      pollInterval = setInterval(async () => {
        if (isCompleted) {
          cleanup();
          return;
        }
        
        try {
          const progressResponse = await datasetAPI.getDatasetFieldsProgress(taskId);
          
          handleProgressUpdate({
            progress: progressResponse.progress,
            status: progressResponse.status,
            message: progressResponse.message,
            details: progressResponse.details
          });

          // 如果任务完成或失败，停止轮询
          if (progressResponse.status === 'completed' || progressResponse.status === 'failed') {
            if (progressResponse.status === 'completed') {
              handleTaskComplete(progressResponse.data);
            } else {
              handleError(progressResponse.message);
            }
          }
        } catch (error: any) {
          handleError(error.response?.data?.detail || error.message);
        }
      }, 2000); // 每2秒轮询一次

      // 设置超时
      pollTimeout = setTimeout(() => {
        handleError('获取字段信息超时，请重试');
      }, 120000); // 2分钟超时
    };

    // 尝试WebSocket连接
    try {
      const ws = createDatasetFieldsProgressWebSocket(
        taskId,
        handleProgressUpdate,
        handleTaskComplete,
        handleError
      );
      
      // 设置WebSocket连接超时
      const wsTimeout = setTimeout(() => {
        console.log('WebSocket连接超时，回退到轮询模式');
        ws.disconnect();
        if (!isCompleted) {
          startPolling();
        }
      }, 3000); // 3秒超时，比较快速的回退
      
      ws.connect().then(() => {
        clearTimeout(wsTimeout);
        setWsClient(ws);
        console.log('WebSocket连接成功');
        
        // WebSocket连接成功，设置总超时
        pollTimeout = setTimeout(() => {
          handleError('获取字段信息超时，请重试');
        }, 120000);
      }).catch((wsError) => {
        console.error('WebSocket连接失败，回退到轮询模式:', wsError);
        clearTimeout(wsTimeout);
        if (!isCompleted) {
          startPolling();
        }
      });
      
    } catch (wsError) {
      console.error('WebSocket初始化失败，使用轮询模式:', wsError);
      startPolling();
    }
    
    // 返回清理函数，供外部调用
    return cleanup;
  };

  // 搜索数据集字段
  const handleSearch = async () => {
    try {
      const values = await form.validateFields();
      
      // 清理之前的连接（如果有的话）
      if (wsClient) {
        wsClient.disconnect();
        setWsClient(null);
      }
      
      setState(prev => ({ 
        ...prev, 
        loading: true,
        searchParams: values,
        data: null,
        progress: {
          visible: true,
          taskId: null,
          progress: 0,
          status: 'pending',
          message: '正在启动任务...',
          details: undefined
        }
      }));

      // 启动异步获取任务
      const taskResponse = await datasetAPI.startDatasetFieldsFetch(
        values.dataset_id,
        values.region,
        values.universe,
        values.delay,
        'EQUITY' // 数据集字段页面固定使用EQUITY
      );

      // 开始跟踪进度（WebSocket优先，轮询备选）
      await trackProgress(taskResponse.task_id);
      
    } catch (error: any) {
      setState(prev => ({ 
        ...prev, 
        loading: false,
        progress: { ...prev.progress, visible: false }
      }));
      message.error(`启动任务失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 导出字段列表
  const handleExport = () => {
    if (!state.data || !state.data.raw_fields) {
      message.warning('没有可导出的数据');
      return;
    }

    try {
      // 构建导出数据 - 只包含字段信息
      const exportData = state.data.raw_fields.map(field => ({
        id: field.id,
        description: field.description,
        type: field.type
      }));

      // 创建下载链接
      const blob = new Blob([JSON.stringify(exportData, null, 2)], {
        type: 'application/json'
      });
      const url = URL.createObjectURL(blob);
      
      // 生成文件名
      const filename = `${state.data.dataset_id}_${state.data.region}_${state.data.universe}_${state.data.delay}.json`;
      
      // 创建下载链接并触发下载
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      message.success(`已导出 ${state.data.total_fields} 个字段到 ${filename}`);
      
    } catch (error) {
      message.error('导出失败');
      console.error('Export error:', error);
    }
  };

  // 清空结果
  const handleClear = () => {
    // 清理WebSocket连接
    if (wsClient) {
      wsClient.disconnect();
      setWsClient(null);
    }
    
    setState(prev => ({
      ...prev,
      loading: false,
      data: null,
      progress: {
        visible: false,
        taskId: null,
        progress: 0,
        status: '',
        message: '',
        details: undefined
      }
    }));
    form.resetFields();
  };

  // 表格列定义
  const columns = [
    {
      title: '字段ID',
      dataIndex: 'id',
      key: 'id',
      width: 250,
      render: (text: string) => (
        <Text code copyable={{ text }}>{text}</Text>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
      render: (text: string) => text || '-',
    },
    {
      title: '类型',
      dataIndex: 'type',
      key: 'type',
      width: 120,
      render: (type: string) => {
        const getColor = () => {
          switch (type) {
            case 'MATRIX': return 'blue';
            case 'VECTOR': return 'green';
            case 'SCALAR': return 'orange';
            default: return 'default';
          }
        };
        return <Tag color={getColor()}>{type}</Tag>;
      },
    },
  ];

  return (
    <DashboardLayout>
      <div style={{ padding: '24px' }}>
        <Row justify="space-between" align="middle" style={{ marginBottom: '24px' }}>
          <Col>
            <Title level={2}>
              <DatabaseOutlined /> 数据集字段查看
            </Title>
            <Text type="secondary">查看WorldQuant数据集的字段信息</Text>
          </Col>
        </Row>

        {/* 搜索表单 */}
        <Card title="查询参数" style={{ marginBottom: '16px' }}>
          <Form
            form={form}
            layout="inline"
            initialValues={{
              region: 'USA',
              universe: 'TOP3000',
              delay: 1
            }}
          >
            <Form.Item
              name="dataset_id"
              label="数据集ID"
              rules={[{ required: true, message: '请输入数据集ID' }]}
            >
              <Input 
                placeholder="例如: fundamental6"
                style={{ width: 200 }}
              />
            </Form.Item>

               <Form.Item
                 name="region"
                 label="地区"
                 rules={[{ required: true, message: '请选择地区' }]}
               >
                 <Select style={{ width: 120 }} onChange={handleRegionChange}>
                   {dynamicOptions.regions?.map((region: any) => (
                     <Option key={region.value} value={region.value}>{region.label}</Option>
                   ))}
                 </Select>
               </Form.Item>

               <Form.Item
                 name="universe"
                 label="universe"
                 rules={[{ required: true, message: '请选择universe' }]}
               >
                 <Select style={{ width: 150 }}>
                   {dynamicOptions.universes?.map((universe: any) => (
                     <Option key={universe.value} value={universe.value}>{universe.label}</Option>
                   ))}
                 </Select>
               </Form.Item>

               <Form.Item
                 name="delay"
                 label="延迟"
                 rules={[{ required: true, message: '请选择延迟' }]}
               >
                 <Select style={{ width: 100 }}>
                   {dynamicOptions.delays?.map((delay: any) => (
                     <Option key={delay.value} value={delay.value}>{delay.label}</Option>
                   ))}
                 </Select>
               </Form.Item>

            <Form.Item>
              <Space>
                <Button
                  type="primary"
                  icon={<SearchOutlined />}
                  onClick={handleSearch}
                  loading={state.loading}
                >
                  搜索
                </Button>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={handleClear}
                  disabled={state.loading}
                >
                  清空
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </Card>

        {/* 进度显示 */}
        {state.progress.visible && (
          <Card title="获取进度" style={{ marginBottom: '16px' }}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <Progress 
                percent={state.progress.progress} 
                status={state.progress.status === 'failed' ? 'exception' : 'active'}
                strokeColor={
                  state.progress.status === 'completed' ? '#52c41a' :
                  state.progress.status === 'failed' ? '#ff4d4f' : '#1890ff'
                }
                format={(percent) => `${percent}%`}
              />
              <Alert
                message={state.progress.message}
                type={
                  state.progress.status === 'completed' ? 'success' :
                  state.progress.status === 'failed' ? 'error' : 'info'
                }
                showIcon
              />
              
              {/* 详细进度信息 */}
              {state.progress.details && (
                <div style={{ fontSize: '12px', color: '#666', lineHeight: '1.4' }}>
                  <Row gutter={16}>
                    <Col span={6}>
                      <Text type="secondary">已处理页数:</Text>
                      <br />
                      <Text strong>{state.progress.details.page_count} 页</Text>
                    </Col>
                    <Col span={6}>
                      <Text type="secondary">已获取字段:</Text>
                      <br />
                      <Text strong>
                        {state.progress.details.current_count}
                        {state.progress.details.estimated_total && 
                          ` / ${state.progress.details.estimated_total}`
                        } 个
                      </Text>
                    </Col>
                    <Col span={6}>
                      <Text type="secondary">已用时间:</Text>
                      <br />
                      <Text strong>{state.progress.details.elapsed_time.toFixed(1)}s</Text>
                    </Col>
                    <Col span={6}>
                      <Text type="secondary">预计剩余:</Text>
                      <br />
                      <Text strong>
                        {state.progress.details.estimated_remaining_time ? 
                          `${state.progress.details.estimated_remaining_time.toFixed(1)}s` : 
                          '计算中...'
                        }
                      </Text>
                    </Col>
                  </Row>
                </div>
              )}
            </Space>
          </Card>
        )}

        {/* 结果展示 */}
        {state.data && (
          <>
            {/* 数据集信息 */}
            <Card 
              title="数据集信息" 
              style={{ marginBottom: '16px' }}
              extra={
                <Button
                  type="primary"
                  icon={<DownloadOutlined />}
                  onClick={handleExport}
                  disabled={!state.data.raw_fields || state.data.raw_fields.length === 0}
                >
                  导出字段列表
                </Button>
              }
            >
              <Descriptions column={4} size="small">
                <Descriptions.Item label="数据集ID">
                  <Text code>{state.data.dataset_id}</Text>
                </Descriptions.Item>
                <Descriptions.Item label="地区">
                  <Tag color="blue">{state.data.region}</Tag>
                </Descriptions.Item>
                <Descriptions.Item label="universe">
                  <Tag color="green">{state.data.universe}</Tag>
                </Descriptions.Item>
                <Descriptions.Item label="延迟">
                  <Tag color="orange">{state.data.delay}</Tag>
                </Descriptions.Item>
                <Descriptions.Item label="总字段数">
                  <Text strong style={{ color: '#1890ff' }}>
                    {state.data.total_fields.toLocaleString()} 个
                  </Text>
                </Descriptions.Item>
                <Descriptions.Item label="Matrix字段">
                  <Text strong style={{ color: '#52c41a' }}>
                    {state.data.raw_fields?.filter(f => f.type === 'MATRIX').length || 0} 个
                  </Text>
                </Descriptions.Item>
                <Descriptions.Item label="Vector字段">
                  <Text strong style={{ color: '#fa8c16' }}>
                    {state.data.raw_fields?.filter(f => f.type === 'VECTOR').length || 0} 个
                  </Text>
                </Descriptions.Item>
                <Descriptions.Item label="其他字段">
                  <Text strong style={{ color: '#722ed1' }}>
                    {state.data.raw_fields?.filter(f => f.type !== 'MATRIX' && f.type !== 'VECTOR').length || 0} 个
                  </Text>
                </Descriptions.Item>
              </Descriptions>
            </Card>

            {/* 字段列表 */}
            <Card title={`字段列表 (${state.data.total_fields.toLocaleString()} 个字段)`}>
              <Spin spinning={state.loading}>
                <Table
                  columns={columns}
                  dataSource={state.data.raw_fields || []}
                  rowKey="id"
                  pagination={{
                    pageSize: 50,
                    showSizeChanger: true,
                    showQuickJumper: true,
                    showTotal: (total, range) => 
                      `第 ${range[0]}-${range[1]} 条，共 ${total} 个字段`,
                    pageSizeOptions: ['20', '50', '100', '200']
                  }}
                  size="middle"
                  scroll={{ x: 800 }}
                  locale={{
                    emptyText: <Empty description="暂无字段数据" />
                  }}
                />
              </Spin>
            </Card>
          </>
        )}

        {/* 无数据状态 */}
        {!state.data && !state.loading && (
          <Card>
            <Empty 
              description="请输入数据集参数并点击搜索来查看字段信息"
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
          </Card>
        )}
      </div>
    </DashboardLayout>
  );
};

export default DatasetFieldsPage;
