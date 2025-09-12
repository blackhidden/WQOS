/**
 * 统一进程管理页面 - 合并脚本管理和进程监控功能
 */

import React, { useState, useEffect, useRef } from 'react';
import dayjs from 'dayjs';
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Drawer,
  Row,
  Col,
  Tooltip,
  Spin,
  App,
  Statistic,
  Progress,
  Modal,
  Input,
  Form,
  Select,
  Alert,
  InputNumber,
  Empty,
  DatePicker,
  Tabs
} from 'antd';
import {
  PlayCircleOutlined,
  StopOutlined,
  EyeOutlined,
  ReloadOutlined,
  ExclamationCircleOutlined,
  PauseCircleOutlined,
  SyncOutlined,
  DeleteOutlined

} from '@ant-design/icons';
import { useDispatch, useSelector } from 'react-redux';

import { DashboardLayout } from '../components/Layout/DashboardLayout';
import { RootState, AppDispatch } from '../store';
import { 
  getProcessStatusAsync, 
  startProcessAsync, 
  stopProcessAsync,
  startProcessFromTemplateAsync 
} from '../store/processSlice';
import { fetchConfigTemplatesAsync } from '../store/configSlice';
import { scriptsAPI, ScriptStatus, ScriptTypes, LogResponse } from '../services/scripts';

const { Title, Text } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

interface ScriptInfo extends ScriptStatus {
  // ScriptStatus 已包含所有需要的字段
}

const ProcessManagementPage: React.FC = () => {
  const { modal, message } = App.useApp();
  const dispatch = useDispatch<AppDispatch>();
  const { currentProcess, loading: processLoading } = useSelector((state: RootState) => state.process);
  const { templates } = useSelector((state: RootState) => state.config);
  
  // 脚本管理状态
  const [scripts, setScripts] = useState<ScriptInfo[]>([]);
  const [scriptTypes, setScriptTypes] = useState<ScriptTypes>({});
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState<string>('active');
  
  // 日志查看状态
  const [logDrawerVisible, setLogDrawerVisible] = useState(false);
  const [selectedScript, setSelectedScript] = useState<ScriptInfo | null>(null);
  const [logContent, setLogContent] = useState<string>('');
  const [logLoading, setLogLoading] = useState(false);
  const [logAutoRefresh, setLogAutoRefresh] = useState(true);
  const [currentLogOffset, setCurrentLogOffset] = useState<number>(0);
  const [totalLogLines, setTotalLogLines] = useState<number>(0);
  const [logViewMode, setLogViewMode] = useState<'realtime' | 'full'>('realtime'); // 日志查看模式
  const [isInitialLoad, setIsInitialLoad] = useState<boolean>(true);
  const logIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const logAutoRefreshRef = useRef<boolean>(true);
  
  // 启动模态框状态
  const [isStartModalVisible, setIsStartModalVisible] = useState(false);
  const [selectedScriptToStart, setSelectedScriptToStart] = useState<string>('');
  const [selectedCheckMode, setSelectedCheckMode] = useState<string>('PPAC');
  const [startForm] = Form.useForm();
  
  // 移除了自动刷新，改为手动刷新

  // 检查每种脚本的运行状态
  const getScriptRunningStatus = (scriptType: string) => {
    return scripts.find(script => script.script_type === scriptType)?.status === 'running';
  };

  // 同步更新日志自动刷新状态
  const updateLogAutoRefresh = (value: boolean) => {
    setLogAutoRefresh(value);
    logAutoRefreshRef.current = value;
  };

  // 加载脚本数据
  const loadScriptsData = async () => {
    setLoading(true);
    try {
      // 获取脚本状态（新格式返回数组）
      const statusResponse = await scriptsAPI.getAllScriptsStatus();
      
      // 直接使用返回的数组数据
      setScripts(statusResponse.data.scripts);
      setScriptTypes(statusResponse.data.script_types);
    } catch (error: any) {
      message.error(`加载脚本数据失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };



  // 停止脚本
  const handleStopScript = (script: ScriptInfo, force: boolean = false) => {
    const scriptName = script.script_name;
    const displayInfo = script.tag ? `${scriptName} (${script.tag})` : scriptName;
    
    modal.confirm({
      title: `确认停止${displayInfo}`,
      icon: <ExclamationCircleOutlined />,
      content: force ? '强制停止可能导致数据丢失，确定继续吗？' : '确定要停止该脚本吗？',
      okText: '确定',
      cancelText: '取消',
      onOk: () => {
        return new Promise<void>((resolve, reject) => {
          setActionLoading(script.id as number);
          
          // 使用新的按任务ID停止API
          if (typeof script.id === 'number') {
            scriptsAPI.stopTask(script.id, force)
              .then(() => {
                message.success(`${displayInfo}停止成功`);
                
                // 如果正在查看该脚本的日志，停止自动刷新
                if (selectedScript && selectedScript.id === script.id && logDrawerVisible) {
                  updateLogAutoRefresh(false);
                  if (logIntervalRef.current) {
                    clearInterval(logIntervalRef.current);
                    logIntervalRef.current = null;
                  }
                }
                
                loadScriptsData();
                resolve();
              })
              .catch((error: any) => {
                message.error(`停止失败: ${error.response?.data?.detail || error.message}`);
                reject(error);
              })
              .finally(() => {
                setActionLoading(null);
              });
          } else {
            // 回退到按类型停止（用于旧任务）
            scriptsAPI.stopScript(script.script_type, force)
              .then(() => {
                message.success(`${displayInfo}停止成功`);
                
                // 如果正在查看该脚本的日志，停止自动刷新
                if (selectedScript && selectedScript.id === script.id && logDrawerVisible) {
                  updateLogAutoRefresh(false);
                  if (logIntervalRef.current) {
                    clearInterval(logIntervalRef.current);
                    logIntervalRef.current = null;
                  }
                }
                
                loadScriptsData();
                resolve();
              })
              .catch((error: any) => {
                message.error(`停止失败: ${error.response?.data?.detail || error.message}`);
                reject(error);
              })
              .finally(() => {
                setActionLoading(null);
              });
          }
        });
      }
    });
  };

  // 删除任务
  const handleDeleteTask = (script: ScriptInfo) => {
    if (typeof script.id !== 'number') {
      message.error('无法删除该任务');
      return;
    }

    const displayInfo = script.tag ? `${script.script_name} (${script.tag})` : script.script_name;
    
    modal.confirm({
      title: `确认删除任务`,
      icon: <ExclamationCircleOutlined />,
      content: `确定要删除任务"${displayInfo}"吗？此操作将同时删除任务记录和日志文件，无法恢复。`,
      okText: '确定删除',
      okType: 'danger',
      cancelText: '取消',
      onOk: () => {
        return new Promise<void>((resolve, reject) => {
          scriptsAPI.deleteTask(script.id as number)
            .then((response) => {
              const data = response.data;
              if (data.log_deleted) {
                message.success(`任务"${displayInfo}"已删除，日志文件已清理`);
              } else {
                message.success(`任务"${displayInfo}"已删除 (${data.message})`);
              }
              
              // 如果有详细的删除信息，在控制台输出
              if (data.log_deletion_details && data.log_deletion_details.length > 0) {
                console.log('日志删除详情:', data.log_deletion_details);
              }
              
              loadScriptsData();
              resolve();
            })
            .catch((error: any) => {
              message.error(`删除失败: ${error.response?.data?.detail || error.message}`);
              reject(error);
            });
        });
      }
    });
  };

  // 加载日志内容（初始加载或增量加载）
  const loadLogContent = async (taskId: number, isInitial: boolean = false) => {
    // 检查脚本是否仍在运行（仅对增量加载进行检查）
    if (!isInitial && !isInitialLoad) {
      const currentScript = scripts.find(s => s.id === taskId);
      if (currentScript && currentScript.status === 'stopped') {
        // 脚本已停止，停止自动刷新
        updateLogAutoRefresh(false);
        if (logIntervalRef.current) {
          clearInterval(logIntervalRef.current);
          logIntervalRef.current = null;
        }
        return;
      }
    }
    
    try {
      let response: { data: LogResponse };
      
      if (isInitial || isInitialLoad) {
        // 初始加载：获取最后1000行
        const includeRotated = logViewMode === 'full';
        response = await scriptsAPI.getTaskLogs(taskId, -1000, 1000, includeRotated);
        setLogContent(response.data.content || '暂无日志内容');
        setCurrentLogOffset(response.data.total_lines);
        setTotalLogLines(response.data.total_lines);
        setIsInitialLoad(false);
        
        // 自动滚动到底部
        setTimeout(() => {
          const logElement = document.getElementById('log-content');
          if (logElement) {
            logElement.scrollTop = logElement.scrollHeight;
          }
        }, 100);
      } else {
        // 增量加载：检查是否有新内容
        // 注意：增量加载时始终只读主文件，避免性能问题
        response = await scriptsAPI.getTaskLogs(taskId, currentLogOffset, 1000, false);
        
        if (response.data.returned_lines > 0) {
          // 有新内容，追加到现有日志
          setLogContent(prevContent => prevContent + response.data.content);
          setCurrentLogOffset(response.data.current_offset + response.data.returned_lines);
          setTotalLogLines(response.data.total_lines);
          
          // 自动滚动到底部
          setTimeout(() => {
            const logElement = document.getElementById('log-content');
            if (logElement) {
              logElement.scrollTop = logElement.scrollHeight;
            }
          }, 100);
        } else {
          // 更新总行数
          setTotalLogLines(response.data.total_lines);
        }
      }
    } catch (error: any) {
      console.error('加载日志失败:', error);
      if (isInitial || isInitialLoad) {
        setLogContent('加载日志失败');
      }
    }
  };

  // 查看日志
  const handleViewLogs = async (script: ScriptInfo) => {
    if (typeof script.id !== 'number') {
      message.warning('该任务没有可查看的日志');
      return;
    }

    setSelectedScript(script);
    setLogDrawerVisible(true);
    setLogLoading(true);
    setLogViewMode('realtime'); // 默认实时模式
    updateLogAutoRefresh(script.status === 'running'); // 只有运行中的任务才自动刷新
    setIsInitialLoad(true);
    
    // 立即加载一次日志（初始加载）
    await loadLogContent(script.id, true);
    setLogLoading(false);
    
    // 启动自动刷新（仅对运行中的任务）
    if (logIntervalRef.current) {
      clearInterval(logIntervalRef.current);
    }
    
    if (script.status === 'running') {
      logIntervalRef.current = setInterval(() => {
        if (logAutoRefreshRef.current) {
          loadLogContent(script.id as number, false); // 增量加载
        }
      }, 2000); // 每2秒刷新一次日志
    }
  };
  
  // 切换日志查看模式
  const handleLogViewModeChange = (mode: 'realtime' | 'full') => {
    if (mode === logViewMode) return;
    
    setLogViewMode(mode);
    setLogContent('');
    setIsInitialLoad(true);
    setCurrentLogOffset(0);
    setTotalLogLines(0);
    
    if (selectedScript) {
      // 重新加载日志
      loadLogContent(selectedScript.id as number, true);
      
      // 如果切换到完整模式，暂停自动刷新
      if (mode === 'full') {
        updateLogAutoRefresh(false);
        if (logIntervalRef.current) {
          clearInterval(logIntervalRef.current);
          logIntervalRef.current = null;
        }
      } else if (mode === 'realtime' && selectedScript.status === 'running') {
        // 切换回实时模式且脚本在运行，启用自动刷新
        updateLogAutoRefresh(true);
        if (logIntervalRef.current) {
          clearInterval(logIntervalRef.current);
        }
        logIntervalRef.current = setInterval(() => {
          if (logAutoRefreshRef.current) {
            loadLogContent(selectedScript.id as number, false);
          }
        }, 2000);
      }
    }
  };

  // 关闭日志抽屉
  const handleCloseLogDrawer = () => {
    setLogDrawerVisible(false);
    setSelectedScript(null);
    setLogContent('');
    updateLogAutoRefresh(false);
    setCurrentLogOffset(0);
    setTotalLogLines(0);
    setIsInitialLoad(true);
    
    // 清理日志刷新定时器
    if (logIntervalRef.current) {
      clearInterval(logIntervalRef.current);
      logIntervalRef.current = null;
    }
  };

  // 切换日志自动刷新
  const toggleLogAutoRefresh = () => {
    updateLogAutoRefresh(!logAutoRefresh);
  };

  // 手动刷新日志
  const handleRefreshLogs = () => {
    if (selectedScript && typeof selectedScript.id === 'number') {
      setIsInitialLoad(true);
      loadLogContent(selectedScript.id, true);
    }
  };

  // 打开启动脚本弹窗
  const handleOpenStartModal = () => {
    setIsStartModalVisible(true);
    setSelectedScriptToStart('');
    setSelectedCheckMode('PPAC');
    startForm.resetFields();
  };

  // 处理脚本启动
  const handleStartScript = async () => {
    try {
      const values = await startForm.validateFields();
      
      if (selectedScriptToStart === 'unified_digging') {
        // 因子挖掘需要配置模板
        await dispatch(startProcessFromTemplateAsync({
          templateId: values.templateId,
          stage: values.stage,
          n_jobs: values.n_jobs,
          enable_multi_simulation: values.enable_multi_simulation || false
        }));
        message.success(`第${values.stage}阶段因子挖掘启动成功`);
      } else {
        // 其他脚本启动，检查是否为check_optimized需要传递参数
        if (selectedScriptToStart === 'check_optimized') {
          const scriptParams = {
            mode: values.mode,
            sharpe_threshold: values.sharpe_threshold,
            fitness_threshold: values.fitness_threshold,
            start_date: values.start_date ? values.start_date.format('YYYY-MM-DD') : undefined
          };
          // 过滤掉undefined和空字符串的值
          Object.keys(scriptParams).forEach(key => {
            const value = scriptParams[key as keyof typeof scriptParams];
            if (value === undefined || value === '') {
              delete scriptParams[key as keyof typeof scriptParams];
            }
          });
          
          await scriptsAPI.startScriptWithParams(selectedScriptToStart, scriptParams);
        } else {
          // 其他脚本也需要发送body，即使是空参数
          await scriptsAPI.startScriptWithParams(selectedScriptToStart, {});
        }
        message.success(`${scriptTypes[selectedScriptToStart] || selectedScriptToStart} 启动成功`);
      }
      
      setIsStartModalVisible(false);
      startForm.resetFields();
      setSelectedScriptToStart('');
      setSelectedCheckMode('PPAC');
      await loadScriptsData();
    } catch (error: any) {
      const errorMessage = error?.response?.data?.detail || error?.message || '启动失败';
      message.error(`启动失败: ${errorMessage}`);
    }
  };

  // 停止因子挖掘
  const handleStopUnifiedDigging = (force: boolean = false) => {
    modal.confirm({
      title: '确认停止因子挖掘',
      icon: <ExclamationCircleOutlined />,
      content: force ? '强制停止可能导致数据丢失，确定继续吗？' : '确定要停止因子挖掘吗？',
      okText: '确定',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dispatch(stopProcessAsync(force));
          message.success('因子挖掘停止成功');
          
          // 如果正在查看因子挖掘的日志，停止自动刷新
          if (selectedScript && selectedScript.script_type === 'unified_digging' && logDrawerVisible) {
            updateLogAutoRefresh(false);
            if (logIntervalRef.current) {
              clearInterval(logIntervalRef.current);
              logIntervalRef.current = null;
            }
          }
          
          await loadScriptsData();
        } catch (error) {
          message.error('停止失败');
        }
      }
    });
  };

  // 获取阶段标签（截取最后一部分）
  const getStageLabel = (tag: string) => {
    if (!tag) return '';
    const parts = tag.split('_');
    return parts[parts.length - 1]; // 返回最后一部分，如 "step1"
  };

  // 获取阶段颜色
  const getStageColor = (tag: string) => {
    const label = getStageLabel(tag);
    if (label.includes('step1') || label.includes('1')) return 'green';
    if (label.includes('step2') || label.includes('2')) return 'orange';
    if (label.includes('step3') || label.includes('3')) return 'red';
    return 'blue'; // 默认颜色
  };

  // 格式化函数
  const formatUptime = (seconds?: number) => {
    if (!seconds) return 'N/A';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${minutes}m`;
  };

  const formatMemory = (bytes?: number) => {
    if (!bytes) return 'N/A';
    return (bytes / 1024 / 1024).toFixed(1);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'running':
        return <PlayCircleOutlined style={{ color: '#52c41a' }} />;
      case 'stopped':
        return <PauseCircleOutlined style={{ color: '#d9d9d9' }} />;
      default:
        return <ExclamationCircleOutlined style={{ color: '#faad14' }} />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running':
        return '#52c41a';
      case 'stopped':
        return '#d9d9d9';
      default:
        return '#faad14';
    }
  };

  // 活跃任务表格列定义
  const getActiveColumns = () => [
    {
      title: '脚本名称',
      dataIndex: 'script_name',
      key: 'script_name',
      render: (scriptName: string, record: ScriptInfo) => (
        <Space direction="vertical" size="small" style={{ display: 'flex' }}>
          <Space>
            <Text strong>{scriptName}</Text>
            {record.script_type === 'unified_digging' && record.tag && (
              <Tag color={getStageColor(record.tag)}>{getStageLabel(record.tag)}</Tag>
            )}
          </Space>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'running' ? 'green' : 'default'}>
          {getStatusIcon(status)} {status === 'running' ? '运行中' : '已停止'}
        </Tag>
      ),
    },
    {
      title: '进程ID',
      dataIndex: 'pid',
      key: 'pid',
      render: (pid: number) => pid || '-',
    },
    {
      title: '启动时间',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (time: string) => time ? dayjs(time).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '运行时长',
      key: 'uptime',
      render: (_: any, record: ScriptInfo) => {
        if (record.started_at && record.status === 'running') {
          const now = dayjs();
          const startTime = dayjs(record.started_at);
          const duration = now.diff(startTime, 'minute');
          const hours = Math.floor(duration / 60);
          const minutes = duration % 60;
          return `${hours}h ${minutes}m`;
        }
        return '-';
      },
    },
    {
      title: '标签',
      dataIndex: 'tag',
      key: 'tag',
      render: (tag: string) => tag || '-',
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: any, record: ScriptInfo) => (
        <Space>
          {record.status === 'running' && (
            <Space>
              <Tooltip title="正常停止">
                <Button
                  danger
                  icon={<StopOutlined />}
                  size="small"
                  loading={actionLoading === record.id}
                  onClick={() => handleStopScript(record, false)}
                >
                  停止
                </Button>
              </Tooltip>
              <Tooltip title="强制停止">
                <Button
                  danger
                  type="primary"
                  size="small"
                  loading={actionLoading === record.id}
                  onClick={() => handleStopScript(record, true)}
                >
                  强制停止
                </Button>
              </Tooltip>
            </Space>
          )}
          <Tooltip title="查看日志">
            <Button
              icon={<EyeOutlined />}
              size="small"
              onClick={() => handleViewLogs(record)}
            >
              日志
            </Button>
          </Tooltip>
          {record.status === 'stopped' && typeof record.id === 'number' && (
            <Tooltip title="删除任务">
              <Button
                danger
                type="text"
                icon={<DeleteOutlined />}
                size="small"
                onClick={() => handleDeleteTask(record)}
              >
                删除
              </Button>
            </Tooltip>
          )}
        </Space>
      ),
    },
  ];

  // 历史任务表格列定义
  const getHistoryColumns = () => [
    {
      title: '脚本名称',
      dataIndex: 'script_name',
      key: 'script_name',
      render: (scriptName: string, record: ScriptInfo) => (
        <Space direction="vertical" size="small" style={{ display: 'flex' }}>
          <Space>
            <Text strong>{scriptName}</Text>
            {record.script_type === 'unified_digging' && record.tag && (
              <Tag color={getStageColor(record.tag)}>{getStageLabel(record.tag)}</Tag>
            )}
          </Space>
        </Space>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'running' ? 'green' : 'default'}>
          {getStatusIcon(status)} {status === 'running' ? '运行中' : '已停止'}
        </Tag>
      ),
    },
    {
      title: '启动时间',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (time: string) => time ? dayjs(time).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '停止时间',
      dataIndex: 'stopped_at',
      key: 'stopped_at',
      render: (time: string) => time ? dayjs(time).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: '运行时长',
      key: 'duration',
      render: (_: any, record: ScriptInfo) => {
        if (record.started_at && record.stopped_at) {
          const startTime = dayjs(record.started_at);
          const stopTime = dayjs(record.stopped_at);
          const duration = stopTime.diff(startTime, 'minute');
          const hours = Math.floor(duration / 60);
          const minutes = duration % 60;
          return `${hours}h ${minutes}m`;
        }
        return '-';
      },
    },
    {
      title: '标签',
      dataIndex: 'tag',
      key: 'tag',
      render: (tag: string) => tag || '-',
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: any, record: ScriptInfo) => (
        <Space>
          <Tooltip title="查看日志">
            <Button
              icon={<EyeOutlined />}
              size="small"
              onClick={() => handleViewLogs(record)}
            >
              日志
            </Button>
          </Tooltip>
          {typeof record.id === 'number' && (
            <Tooltip title="删除任务">
              <Button
                danger
                type="text"
                icon={<DeleteOutlined />}
                size="small"
                onClick={() => handleDeleteTask(record)}
              >
                删除
              </Button>
            </Tooltip>
          )}
        </Space>
      ),
    },
  ];

  // 初始化
  useEffect(() => {
    loadScriptsData();
    dispatch(getProcessStatusAsync());
    dispatch(fetchConfigTemplatesAsync());

    return () => {
      // 只清理日志刷新定时器
      if (logIntervalRef.current) {
        clearInterval(logIntervalRef.current);
      }
    };
  }, [dispatch]);

  // 获取因子挖掘的详细信息
  const unifiedDiggingScript = scripts.find(s => s.script_type === 'unified_digging');

  // 过滤活跃和历史任务
  const activeScripts = scripts.filter(script => script.status === 'running');
  const historyScripts = scripts.filter(script => script.status === 'stopped');

  return (
    <DashboardLayout>
      <div style={{ padding: '24px' }}>
        <Row justify="space-between" align="middle" style={{ marginBottom: '24px' }}>
          <Col>
            <Title level={2}>进程管理</Title>
            <Text type="secondary">统一管理所有脚本的启动、停止和监控</Text>
          </Col>
          <Col>
            <Space>
              <Button
                icon={<ReloadOutlined />}
                onClick={loadScriptsData}
                loading={loading}
              >
                刷新状态
              </Button>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleOpenStartModal}
              >
                启动脚本
              </Button>
            </Space>
          </Col>
        </Row>

        {/* 脚本管理表格 */}
        <Card >
          <Tabs 
            activeKey={activeTab} 
            onChange={setActiveTab}
            items={[
              {
                key: 'active',
                label: (
                  <span>
                    <PlayCircleOutlined />
                    活跃任务 {activeScripts.length > 0 && <Tag color="green">{activeScripts.length}</Tag>}
                  </span>
                ),
                children: (
                  <Table
                    columns={getActiveColumns()}
                    dataSource={activeScripts}
                    rowKey="id"
                    loading={loading}
                    pagination={false}
                    size="middle"
                    locale={{
                      emptyText: <Empty description="当前没有运行中的任务" />
                    }}
                  />
                )
              },
              {
                key: 'history',
                label: (
                  <span>
                    <StopOutlined />
                    历史任务 {historyScripts.length > 0 && <Tag color="blue">{historyScripts.length}</Tag>}
                  </span>
                ),
                children: (
                  <Table
                    columns={getHistoryColumns()}
                    dataSource={historyScripts}
                    rowKey="id"
                    loading={loading}
                    pagination={{
                      pageSize: 10,
                      showSizeChanger: true,
                      showQuickJumper: true,
                      showTotal: (total, range) => `第 ${range[0]}-${range[1]} 条，共 ${total} 条历史任务`
                    }}
                    size="middle"
                    locale={{
                      emptyText: <Empty description="暂无历史任务记录" />
                    }}
                  />
                )
              }
            ]}
          />
        </Card>

        {/* 日志查看抽屉 */}
        <Drawer
          title={
            <Space>
              <span>{`${selectedScript ? scriptTypes[selectedScript.script_type] : ''}日志`}</span>
              {selectedScript && (
                <Tag color={selectedScript.status === 'running' ? 'green' : 'default'}>
                  {selectedScript.status === 'running' ? '脚本运行中' : '脚本已停止'}
                </Tag>
              )}
              <Tag color={logAutoRefresh ? 'processing' : 'default'}>
                {logAutoRefresh ? '实时刷新' : '刷新已停止'}
              </Tag>
              {totalLogLines > 0 && (
                <Tag color="blue">
                  {totalLogLines.toLocaleString()} 行
                </Tag>
              )}
              <Tag color={logViewMode === 'realtime' ? 'green' : 'orange'}>
                {logViewMode === 'realtime' ? '实时模式' : '完整历史'}
              </Tag>
            </Space>
          }
          placement="right"
          width={800}
          open={logDrawerVisible}
          onClose={handleCloseLogDrawer}
          styles={{
            header: {
              paddingTop: '16px',
              paddingBottom: '16px'
            },
            body: {
              paddingTop: '0'
            }
          }}
          extra={
            <Space>
              <Tooltip title="切换查看模式">
                <Select
                  size="small"
                  value={logViewMode}
                  onChange={handleLogViewModeChange}
                  style={{ width: 120 }}
                >
                  <Option value="realtime">
                    实时模式
                  </Option>
                  <Option value="full">
                    完整历史
                  </Option>
                </Select>
              </Tooltip>
              <Tooltip title={logAutoRefresh ? '停止自动刷新' : '开启自动刷新'}>
                <Button
                  type={logAutoRefresh ? 'primary' : 'default'}
                  icon={<SyncOutlined spin={logAutoRefresh} />}
                  onClick={toggleLogAutoRefresh}
                  size="small"
                  disabled={logViewMode === 'full'} // 完整模式时禁用自动刷新
                >
                  {logAutoRefresh ? '停止' : '自动'}
                </Button>
              </Tooltip>
              <Tooltip title="手动刷新">
                <Button
                  icon={<ReloadOutlined />}
                  onClick={handleRefreshLogs}
                  size="small"
                >
                  刷新
                </Button>
              </Tooltip>
            </Space>
          }
        >
          <Spin spinning={logLoading}>
            <pre 
              id="log-content"
              style={{ 
                background: '#f5f5f5', 
                padding: '12px', 
                borderRadius: '6px', 
                fontSize: '12px',
                height: 'calc(100vh - 220px)', // 调整高度：100vh - 固定头部64px - Drawer头部和控制栏约140px - 底部间距16px
                maxHeight: 'calc(100vh - 220px)',
                overflow: 'auto',
                fontFamily: 'Monaco, Menlo, "Ubuntu Mono", monospace'
              }}
            >
              {logContent}
            </pre>
          </Spin>
        </Drawer>

        {/* 启动脚本模态框 */}
        <Modal
          title="启动脚本"
          open={isStartModalVisible}
          onOk={handleStartScript}
          onCancel={() => {
            setIsStartModalVisible(false);
            setSelectedScriptToStart('');
            setSelectedCheckMode('PPAC');
            startForm.resetFields();
          }}
          width={600}
        >
          <Form form={startForm} layout="vertical">
            
            <Form.Item
              name="scriptType"
              label="脚本类型"
              rules={[{ required: true, message: '请选择要启动的脚本' }]}
            >
              <Select 
                placeholder="选择要启动的脚本"
                onChange={(value) => setSelectedScriptToStart(value)}
              >
                {Object.entries(scriptTypes).map(([key, name]) => (
                  <Option key={key} value={key}>
                    {name}
                  </Option>
                ))}
              </Select>
            </Form.Item>

            {/* 因子挖掘的额外配置 */}
            {selectedScriptToStart === 'unified_digging' && (
              <>
                <Form.Item
                  name="templateId"
                  label="配置模板"
                  rules={[{ required: true, message: '请选择配置模板' }]}
                >
                  <Select placeholder="选择配置模板">
                    {templates.map(template => (
                      <Option key={template.id} value={template.id}>
                        {template.name} - {template.description}
                      </Option>
                    ))}
                  </Select>
                </Form.Item>

                <Form.Item
                  name="stage"
                  label="挖掘阶段"
                  rules={[{ required: true, message: '请选择挖掘阶段' }]}
                  initialValue={1}
                >
                  <Select placeholder="选择要执行的挖掘阶段">
                    <Option value={1}>第1阶段 - 一阶因子挖掘</Option>
                    <Option value={2}>第2阶段 - 二阶因子挖掘</Option>
                    <Option value={3}>第3阶段 - 三阶因子挖掘</Option>
                  </Select>
                </Form.Item>

                <Form.Item
                  name="n_jobs"
                  label="并发数 (n_jobs)"
                  rules={[{ required: true, message: '请输入并发数' }]}
                  initialValue={5}
                >
                  <InputNumber 
                    min={1} 
                    max={15} 
                    style={{ width: '100%' }}
                    placeholder="建议: 5-8"
                  />
                </Form.Item>

                <Form.Item
                  name="enable_multi_simulation"
                  label="模拟模式"
                  tooltip="多模拟模式可显著提升并发度：每10个alpha为一组，理论并发度可达n_jobs*10倍"
                  initialValue={true}
                >
                  <Select placeholder="选择模拟模式">
                    <Option value={true}>多模拟模式（需要顾问权限）</Option>
                    <Option value={false}>单模拟模式</Option>
                  </Select>
                </Form.Item>
              </>
            )}

            {/* 检查器的额外配置 */}
            {selectedScriptToStart === 'check_optimized' && (
              <>
                <Form.Item
                  name="mode"
                  label="检查模式"
                  rules={[{ required: true, message: '请选择检查模式' }]}
                  initialValue="PPAC"
                >
                  <Select 
                    placeholder="选择检查模式"
                    onChange={(value) => setSelectedCheckMode(value)}
                  >
                    <Option value="CONSULTANT">
                      CONSULTANT 模式 
                      <Text type="secondary" style={{ fontSize: '12px', marginLeft: 8 }}>
                        (Sharpe≥1.58, Fitness≥1.0)
                      </Text>
                    </Option>
                    <Option value="USER">
                      USER 模式 
                      <Text type="secondary" style={{ fontSize: '12px', marginLeft: 8 }}>
                        (Sharpe≥1.25, 不使用Fitness)
                      </Text>
                    </Option>
                    <Option value="PPAC">
                      PPAC 模式 
                      <Text type="secondary" style={{ fontSize: '12px', marginLeft: 8 }}>
                        (Sharpe≥1.0, Fitness可选)
                      </Text>
                    </Option>
                  </Select>
                </Form.Item>

                {/* 起始日期设置 - 所有模式都可用 */}
                <Form.Item
                  name="start_date"
                  label="起始检查日期"
                  tooltip="指定检查开始的日期，默认为今天。留空则从数据库读取。注意：过早的日期会因API限制导致进度缓慢"
                  initialValue={dayjs()}
                >
                  <DatePicker
                    placeholder="选择起始检查日期"
                    style={{ width: '100%' }}
                    format="YYYY-MM-DD"
                    allowClear
                  />
                </Form.Item>

                {/* 只有PPAC模式才允许自定义参数 */}
                {selectedCheckMode === 'PPAC' && (
                  <>
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="sharpe_threshold"
                          label="Sharpe阈值"
                          tooltip="覆盖模式默认值"
                        >
                          <InputNumber
                            min={0.5}
                            max={3.0}
                            step={0.01}
                            precision={2}
                            placeholder="如: 1.0"
                            style={{ width: '100%' }}
                          />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="fitness_threshold"
                          label="Fitness阈值"
                          tooltip="可选参数，仅在指定时使用"
                        >
                          <InputNumber
                            min={0.5}
                            max={3.0}
                            step={0.01}
                            precision={2}
                            placeholder="如: 1.0"
                            style={{ width: '100%' }}
                          />
                        </Form.Item>
                      </Col>
                    </Row>
                  </>
                )}
              </>
            )}
          </Form>
        </Modal>
      </div>
    </DashboardLayout>
  );
};

export default ProcessManagementPage;
