/**
 * 简化版配置管理页面 - 只包含挖掘脚本实际使用的参数
 */

import React, { useState, useEffect } from 'react';
import {
  Form,
  Input,
  Select,
  InputNumber,
  Switch,
  Button,
  Card,
  Table,
  Space,
  Typography,
  Row,
  Col,
  Tag,
  Modal,
  Popconfirm,
  Tabs,
  Alert,
  Drawer,
  Empty,
  App
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  EyeOutlined,
  ClearOutlined
} from '@ant-design/icons';
import { useDispatch, useSelector } from 'react-redux';

import { DashboardLayout } from '../components/Layout/DashboardLayout';
import { RootState, AppDispatch } from '../store';
import { 
  fetchConfigTemplatesAsync, 
  createConfigTemplateAsync,
  updateConfigTemplateAsync,
  deleteConfigTemplateAsync,
  validateConfigAsync,
  fetchFieldOptionsAsync,
  fetchWorldQuantOptionsAsync,
  syncWorldQuantConfigAsync,
  getWorldQuantConfigStatusAsync,
  getWorldQuantSyncHistoryAsync,
  setDynamicOptions
} from '../store/configSlice';
import { startProcessFromTemplateAsync } from '../store/processSlice';
import { DiggingConfig, ConfigTemplate } from '../types/config';

const { Title, Text } = Typography;
const { TextArea } = Input;
const { Option } = Select;
const { TabPane } = Tabs;

const ConfigPage: React.FC = () => {
  const { message } = App.useApp();
  const dispatch = useDispatch<AppDispatch>();
  const { templates,  worldQuantOptions, dynamicOptions, syncStatus, syncHistory, syncing, loading} = useSelector((state: RootState) => state.config);
  
  const [form] = Form.useForm();
  const [activeTab, setActiveTab] = useState('list');
  const [editingTemplate, setEditingTemplate] = useState<ConfigTemplate | null>(null);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [isCreateMode, setIsCreateMode] = useState(true);
  const [useRecommendedFields, setUseRecommendedFields] = useState(false);
  const [currentTagPreview, setCurrentTagPreview] = useState<string>('');
  const [isSyncHistoryVisible, setIsSyncHistoryVisible] = useState(false);
  const [startingProcess, setStartingProcess] = useState<number | null>(null);
  const [startModalVisible, setStartModalVisible] = useState(false);
  const [templateToStart, setTemplateToStart] = useState<ConfigTemplate | null>(null);
  const [startForm] = Form.useForm();
  const [setSelectKey] = useState(0);
  const [recommendedFieldsValue, setRecommendedFieldsValue] = useState<string[]>([]);
  const [isRecommendedFieldsModalVisible, setIsRecommendedFieldsModalVisible] = useState(false);
  const [tempRecommendedFields, setTempRecommendedFields] = useState<string[]>([]);
  const [recommendedFieldsInput, setRecommendedFieldsInput] = useState('');

  useEffect(() => {
    dispatch(fetchConfigTemplatesAsync());
    dispatch(fetchFieldOptionsAsync());
    dispatch(fetchWorldQuantOptionsAsync());
    dispatch(getWorldQuantConfigStatusAsync());
  }, [dispatch]);

  // 当worldQuantOptions加载完成后，初始化默认的EQUITY选项
  useEffect(() => {
    if (worldQuantOptions?.regions?.EQUITY) {
      // 直接从worldQuantOptions设置初始的动态选项，避免API调用
      const defaultInstrumentType = 'EQUITY';
      const defaultRegion = 'USA';
      
      const regions = worldQuantOptions.regions[defaultInstrumentType] || [];
      const universes = worldQuantOptions.universes?.instrumentType?.[defaultInstrumentType]?.region?.[defaultRegion] || [];
      const delays = worldQuantOptions.delays?.instrumentType?.[defaultInstrumentType]?.region?.[defaultRegion] || [];
      const neutralizations = worldQuantOptions.neutralizations?.instrumentType?.[defaultInstrumentType]?.region?.[defaultRegion] || [];
      
      // 更新Redux状态中的动态选项
      dispatch(setDynamicOptions({
        regions,
        universes,
        delays,
        neutralizations
      }));
      
      console.log('初始化动态选项:', {
        regions: regions.length,
        universes: universes.length,
        delays: delays.length,
        neutralizations: neutralizations.length
      });
    }
  }, [worldQuantOptions, dispatch]);

  // 处理工具类型变化
  const handleInstrumentTypeChange = (instrumentType: string) => {
    if (!worldQuantOptions) return;
    
    // 直接从worldQuantOptions获取regions，避免API调用
    const regions = worldQuantOptions.regions?.[instrumentType] || [];
    const firstRegion = regions.length > 0 ? regions[0] : undefined;
    
    // 更新Redux状态中的regions选项
    dispatch(setDynamicOptions({
      regions,
      universes: [],
      delays: [],
      neutralizations: []
    }));
    
    form.setFieldsValue({
      region: firstRegion,
      universe: undefined,
      delay: undefined,
      neutralization: undefined
    });
    
    // 如果有第一个region，立即加载其依赖选项并设置默认值
    if (firstRegion) {
      handleRegionChangeWithDefaults(firstRegion, instrumentType);
    }
  };

  // 处理地区变化
  const handleRegionChange = (region: string) => {
    const instrumentType = form.getFieldValue('instrument_type');
    if (instrumentType && region) {
      // 直接调用带默认值的处理函数
      handleRegionChangeWithDefaults(region, instrumentType);
    }
  };

  // 处理地区变化并设置默认值
  const handleRegionChangeWithDefaults = (region: string, instrumentType?: string, preserveExistingValues?: boolean) => {
    const currentInstrumentType = instrumentType || form.getFieldValue('instrument_type');
    if (currentInstrumentType && region && worldQuantOptions) {
      // 直接从已加载的worldQuantOptions获取选项，避免重复API调用
      const universeOptions = worldQuantOptions.universes?.instrumentType?.[currentInstrumentType]?.region?.[region] || [];
      const delayOptions = worldQuantOptions.delays?.instrumentType?.[currentInstrumentType]?.region?.[region] || [];
      const neutralizationOptions = worldQuantOptions.neutralizations?.instrumentType?.[currentInstrumentType]?.region?.[region] || [];
      
      // 直接更新Redux状态中的动态选项（用于下拉框显示），避免API调用
      dispatch(setDynamicOptions({
        regions: worldQuantOptions.regions?.[currentInstrumentType] || [],
        universes: universeOptions,
        delays: delayOptions,
        neutralizations: neutralizationOptions
      }));
      
      // 只有在不需要保留现有值的情况下才设置默认值
      if (!preserveExistingValues) {
        // neutralization默认选择SUBINDUSTRY，如果没有则使用第一个
        const defaultNeutralization = neutralizationOptions.find((n: any) => n.value === 'SUBINDUSTRY')?.value || 
                                     (neutralizationOptions.length > 0 ? neutralizationOptions[0]?.value : undefined);
        
        // 立即设置默认值
        form.setFieldsValue({
          universe: universeOptions.length > 0 ? universeOptions[0]?.value : undefined,
          delay: delayOptions.length > 0 ? delayOptions[0]?.value : undefined,
          neutralization: defaultNeutralization
        });
        
        console.log('设置默认值:', {
            region,
            instrumentType: currentInstrumentType,
            universe: universeOptions[0]?.value,
            delay: delayOptions[0]?.value,
            neutralization: defaultNeutralization,
            universeOptions: universeOptions.length,
            delayOptions: delayOptions.length,
            neutralizationOptions: neutralizationOptions.length
          });
      }
    }
  };

  const handleCreateNew = () => {
    setIsCreateMode(true);
    setEditingTemplate(null);
    form.resetFields();
    
    // 设置默认值
    form.setFieldsValue({
      region: 'USA',
      universe: 'TOP3000',
      instrument_type: 'EQUITY',
      delay: 1,
      decay: 6,
      max_trade: 'OFF',
      neutralization: 'SUBINDUSTRY',
      use_recommended_fields: false  // 明确设置为数据集模式
    });
    
    setCurrentTagPreview('');
    setRecommendedFieldsValue([]);
    setUseRecommendedFields(false);  // 同步状态
    setIsModalVisible(true);
  };

  const handleEdit = (template: ConfigTemplate) => {
    setIsCreateMode(false);
    setEditingTemplate(template);
    
    // 解析配置数据
    let configData: any = {};
    try {
      configData = template.config_data;
    } catch (error) {
      console.error('解析配置失败:', error);
      message.error('配置数据格式错误');
      return;
    }

    // 确保recommended_fields是正确的数组格式
    let recommendedFields = configData.recommended_fields;
    if (recommendedFields && typeof recommendedFields === 'string') {
      try {
        recommendedFields = JSON.parse(recommendedFields);
      } catch {
        // 如果JSON解析失败，尝试按行分割，然后回退到逗号分割（向后兼容）
        if (recommendedFields.includes('\n') || recommendedFields.includes('\r')) {
          // 按行分割
          recommendedFields = recommendedFields
            .split(/[\n\r]+/)
            .map((field: string) => field.trim())
            .filter((field: string) => field.length > 0 && !field.startsWith('#'))
            .map((field: string) => field.replace(/,\s*$/, '')); // 清理行末的逗号
        } else {
          // 回退到逗号分割（向后兼容）
          recommendedFields = recommendedFields.split(',').map((field: string) => field.trim().replace(/,\s*$/, '')).filter((field: string) => field.length > 0);
        }
      }
    }



    const formValues = {
      name: template.name,
      description: template.description,
      ...configData,
      recommended_fields: recommendedFields  // 覆盖处理后的字段
    };
    
    form.setFieldsValue(formValues);
    
    // 设置受控组件的值
    setRecommendedFieldsValue(recommendedFields || []);
    
    // 强制刷新Select组件
    setSelectKey(prev => prev + 1);
    
    // 如果有地区信息，需要重新加载对应的依赖选项（但保留现有值）
    if (configData.region && configData.instrument_type) {
      handleRegionChangeWithDefaults(configData.region, configData.instrument_type, true);
    }
    
    setUseRecommendedFields(configData.use_recommended_fields || false);
    setIsModalVisible(true);
  };

  const handleDelete = async (id: number) => {
    try {
      await dispatch(deleteConfigTemplateAsync(id));
      message.success('模板删除成功');
    } catch (error) {
      message.error('模板删除失败');
    }
  };

  const handleSaveTemplate = async () => {
    try {
      const values = await form.validateFields();
      
      // 确保recommended_fields是数组格式
      let recommendedFields = values.recommended_fields;
      if (recommendedFields) {
        if (typeof recommendedFields === 'string') {
          // 如果是字符串，按行分割（支持旧的逗号分割格式进行兼容）
          if (recommendedFields.includes('\n') || recommendedFields.includes('\r')) {
            // 按行分割
            recommendedFields = recommendedFields
              .split(/[\n\r]+/)
              .map((field: string) => field.trim())
              .filter((field: string) => field.length > 0 && !field.startsWith('#'))
              .map((field: string) => field.replace(/,\s*$/, '')); // 清理行末的逗号
          } else if (recommendedFields.includes("'") || recommendedFields.includes('"')) {
            // 处理带引号的格式: "'field1', 'field2', 'field3'" (向后兼容)
            const quotedMatches = recommendedFields.match(/['"`]([^'"`]+)['"`]/g);
            if (quotedMatches) {
              recommendedFields = quotedMatches.map((match: string) => 
                match.replace(/^['"`]|['"`]$/g, '').trim()
              ).filter((field: string) => field.length > 0);
            } else {
              // 回退到逗号分割（向后兼容）
              recommendedFields = recommendedFields.split(',').map((field: string) => field.trim().replace(/,\s*$/, '')).filter((field: string) => field.length > 0);
            }
          } else if (recommendedFields.includes(',')) {
            // 普通逗号分割（向后兼容）
            recommendedFields = recommendedFields.split(',').map((field: string) => field.trim().replace(/,\s*$/, '')).filter((field: string) => field.length > 0);
          } else {
            // 单个字段
            recommendedFields = [recommendedFields.trim().replace(/,\s*$/, '')].filter((field: string) => field.length > 0);
          }
        } else if (!Array.isArray(recommendedFields)) {
          // 如果不是数组也不是字符串，转换为数组
          recommendedFields = [recommendedFields].filter(Boolean);
        }
      } else {
        recommendedFields = [];
      }
      
      const templateData = {
        name: values.name,
        description: values.description || '',
        region: values.region,
        universe: values.universe,
        delay: values.delay,
        decay: values.decay,
        neutralization: values.neutralization,

        use_recommended_fields: values.use_recommended_fields,
        recommended_name: values.recommended_name,
        recommended_fields: recommendedFields,
        dataset_id: values.dataset_id,
        instrument_type: values.instrument_type,
        max_trade: values.max_trade
      };

      console.log('发送的模板数据:', templateData);
      console.log('recommended_fields类型:', typeof templateData.recommended_fields);
      console.log('recommended_fields内容:', templateData.recommended_fields);

      if (isCreateMode) {
        await dispatch(createConfigTemplateAsync(templateData));
        message.success('模板创建成功');
      } else if (editingTemplate) {
        await dispatch(updateConfigTemplateAsync({
          id: editingTemplate.id,
          data: templateData
        }));
        message.success('模板更新成功');
      }

      setIsModalVisible(false);
      form.resetFields();
      setCurrentTagPreview('');
    } catch (error) {
      message.error('保存失败');
    }
  };

  const handleStartProcess = (template: ConfigTemplate) => {
    setTemplateToStart(template);
    setStartModalVisible(true);
    // 重置表单
    startForm.setFieldsValue({
      stage: 1,
      n_jobs: 5,
      enable_multi_simulation: true
    });
  };

  const handleConfirmStart = async () => {
    try {
      const values = await startForm.validateFields();
      if (!templateToStart) return;

      setStartingProcess(templateToStart.id);
      setStartModalVisible(false);
      
      const result = await dispatch(startProcessFromTemplateAsync({
        templateId: templateToStart.id,
        stage: values.stage,
        n_jobs: values.n_jobs,
        enable_multi_simulation: values.enable_multi_simulation || false
      }));
      
      if (startProcessFromTemplateAsync.fulfilled.match(result)) {
        message.success(`第${values.stage}阶段挖掘进程启动成功！PID: ${result.payload.pid}, Tag: ${result.payload.tag}`);
      } else {
        // 处理被拒绝的情况
        const errorMessage = (result.payload as any)?.detail || result.error?.message || '启动失败';
        message.error(`启动失败: ${errorMessage}`);
      }
    } catch (error: any) {
      console.error('启动进程错误:', error);
      const errorMessage = error?.response?.data?.detail || error?.message || '启动失败';
      message.error(`启动失败: ${errorMessage}`);
    } finally {
      setStartingProcess(null);
      setTemplateToStart(null);
    }
  };

  const handleCancel = () => {
    setIsModalVisible(false);
    form.resetFields();
    setCurrentTagPreview('');
    setRecommendedFieldsValue([]);
    setUseRecommendedFields(false);  // 重置为默认状态
    // 关闭推荐字段管理窗口
    handleCloseRecommendedFieldsModal();
  };

  const validateCurrentConfig = async () => {
    try {
      const values = await form.validateFields();
      await dispatch(validateConfigAsync(values));
      message.success('配置验证通过');
    } catch (error) {
      message.error('配置验证失败');
    }
  };

  // 同步WorldQuant配置
  const handleSyncConfig = async () => {
    try {
      const result = await dispatch(syncWorldQuantConfigAsync());
      if (syncWorldQuantConfigAsync.fulfilled.match(result)) {
        message.success(`配置同步成功: ${result.payload.message}`);
        // 重新获取状态和选项
        dispatch(getWorldQuantConfigStatusAsync());
        dispatch(fetchWorldQuantOptionsAsync());
      }
    } catch (error) {
      message.error('配置同步失败');
    }
  };

  // 查看同步历史
  const handleViewSyncHistory = async () => {
    try {
      await dispatch(getWorldQuantSyncHistoryAsync(10));
      setIsSyncHistoryVisible(true);
    } catch (error) {
      message.error('获取同步历史失败');
    }
  };

  // 打开推荐字段管理窗口
  const handleOpenRecommendedFieldsModal = () => {
    setTempRecommendedFields([...recommendedFieldsValue]);
    setRecommendedFieldsInput('');
    setIsRecommendedFieldsModalVisible(true);
  };

  // 关闭推荐字段管理窗口
  const handleCloseRecommendedFieldsModal = () => {
    setIsRecommendedFieldsModalVisible(false);
    setTempRecommendedFields([]);
    setRecommendedFieldsInput('');
  };

  // 确认推荐字段修改
  const handleConfirmRecommendedFields = () => {
    setRecommendedFieldsValue([...tempRecommendedFields]);
    form.setFieldsValue({ recommended_fields: tempRecommendedFields });
    setIsRecommendedFieldsModalVisible(false);
    setRecommendedFieldsInput('');
  };

  // 添加推荐字段
  const handleAddRecommendedFields = () => {
    if (!recommendedFieldsInput.trim()) return;
    
    // 按行分割并处理输入
    const lines = recommendedFieldsInput.split(/[\n\r]+/);
    const newFields = lines
      .map((line: string) => line.trim())
      .filter((line: string) => line && !line.startsWith('#'))
      .map((line: string) => line.replace(/,\s*$/, '')); // 清理行末的逗号
    
    // 合并到临时字段列表
    const combinedFields = Array.from(new Set([...tempRecommendedFields, ...newFields]));
    setTempRecommendedFields(combinedFields);
    setRecommendedFieldsInput('');
  };

  // 删除推荐字段
  const handleRemoveRecommendedField = (fieldToRemove: string) => {
    const updatedFields = tempRecommendedFields.filter(field => field !== fieldToRemove);
    setTempRecommendedFields(updatedFields);
  };

  // 清空推荐字段
  const handleClearRecommendedFields = () => {
    setTempRecommendedFields([]);
  };

  const columns = [
    {
      title: '模板名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      render: (text: string) => text || '-',
    },
    {
      title: '配置预览',
      dataIndex: 'config_data',
      key: 'config_data',
      render: (configData: DiggingConfig) => {
        try {
          return (
            <Space wrap>
              <Tag color="blue">{configData.region || 'N/A'}</Tag>
              <Tag color="green">{configData.universe || 'N/A'}</Tag>
              <Tag color="orange">delay: {configData.delay || 'N/A'}</Tag>
            </Space>
          );
        } catch {
          return <Tag color="red">配置错误</Tag>;
        }
      },
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (text: string) => new Date(text).toLocaleString(),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: any, record: ConfigTemplate) => (
        <Space>
          <Button
            type="link"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          >
            编辑
          </Button>
          <Button
            type="link"
            icon={<PlayCircleOutlined />}
            loading={startingProcess === record.id}
            onClick={() => handleStartProcess(record)}
            disabled={startingProcess !== null}
          >
            {startingProcess === record.id ? '启动中...' : '启动'}
          </Button>
          <Popconfirm
            title="确定要删除这个模板吗？"
            onConfirm={() => handleDelete(record.id)}
          >
            <Button
              type="link"
              danger
              icon={<DeleteOutlined />}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <DashboardLayout>
      <div style={{ padding: '24px' }}>
        <Row justify="space-between" align="middle" style={{ marginBottom: '16px' }}>
          <Col>
            <Title level={2}>配置管理</Title>
          </Col>
          <Col>
            <Space>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={handleCreateNew}
              >
                新建配置模板
              </Button>
            </Space>
          </Col>
        </Row>

        {/* WorldQuant配置同步状态 */}
        <Row style={{ marginBottom: '24px' }}>
          <Col span={24}>
            <Card size="small">
              <Row justify="space-between" align="middle">
                <Col>
                  <Space>
                    <Text strong>WorldQuant配置状态:</Text>
                    <Tag color={syncStatus?.is_available ? 'green' : 'orange'}>
                      {syncStatus?.status || '未知'}
                    </Tag>
                    {syncStatus?.last_sync_time && (
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        最后同步: {new Date(syncStatus.last_sync_time).toLocaleString()}
                      </Text>
                    )}
                  </Space>
                </Col>
                <Col>
                  <Space>
                    <Button
                      loading={syncing}
                      onClick={handleSyncConfig}
                      type="primary"
                      size="small"
                    >
                      {syncing ? '同步中...' : '同步配置'}
                    </Button>
                    <Button
                      onClick={handleViewSyncHistory}
                      size="small"
                    >
                      同步历史
                    </Button>
                  </Space>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>

        <Tabs activeKey={activeTab} onChange={setActiveTab}>
          <TabPane tab="配置模板列表" key="list">
            <Card>
              <Table
                columns={columns}
                dataSource={templates}
                loading={loading}
                rowKey="id"
                pagination={{
                  pageSize: 10,
                  showSizeChanger: true,
                  showQuickJumper: true,
                }}
              />
            </Card>
          </TabPane>
        </Tabs>

        {/* 配置模板编辑模态框 */}
        <Modal
          title={isCreateMode ? '新建配置模板' : '编辑配置模板'}
          open={isModalVisible}
          onCancel={handleCancel}
          width={800}
          footer={[
            <Button key="cancel" onClick={handleCancel}>
              取消
            </Button>,
            <Button key="validate" onClick={validateCurrentConfig}>
              验证配置
            </Button>,
            <Button key="save" type="primary" onClick={handleSaveTemplate}>
              保存
            </Button>,
          ]}
        >
          <Form
            form={form}
            layout="vertical"
            onValuesChange={(changedValues) => {
              if ('use_recommended_fields' in changedValues) {
                setUseRecommendedFields(changedValues.use_recommended_fields);
              }
            }}
          >
            {/* 基本信息 */}
            <Card title="基本信息" size="small" style={{ marginBottom: '16px' }}>
              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    label="模板名称"
                    name="name"
                    rules={[{ required: true, message: '请输入模板名称' }]}
                  >
                    <Input placeholder="请输入模板名称" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item label="描述" name="description">
                    <Input placeholder="请输入描述信息" />
                  </Form.Item>
                </Col>
              </Row>
            </Card>

            {/* 参数配置 */}
            <Card title="参数配置" size="small" style={{ marginBottom: '16px' }}>
              <Row gutter={16}>
                <Col span={8}>
                  <Form.Item
                    label="Region (地区)"
                    name="region"
                    rules={[{ required: true, message: '请选择地区' }]}
                  >
                    <Select placeholder="请选择地区" onChange={handleRegionChange}>
                      {dynamicOptions.regions?.map((region: any) => (
                        <Option key={region.value} value={region.value}>{region.label}</Option>
                      ))}
                    </Select>
                  </Form.Item>
                </Col>
                <Col span={8}>
                  <Form.Item
                    label="Universe (股票池)"
                    name="universe"
                    rules={[{ required: true, message: '请选择股票池' }]}
                  >
                    <Select placeholder="请选择股票池">
                      {dynamicOptions.universes?.map((universe: any) => (
                        <Option key={universe.value} value={universe.value}>{universe.label}</Option>
                      ))}
                    </Select>
                  </Form.Item>
                </Col>
                <Col span={8}>
                  <Form.Item
                    label="Instrument Type"
                    name="instrument_type"
                  >
                    <Select placeholder="请选择工具类型" onChange={handleInstrumentTypeChange}>
                      {worldQuantOptions?.instrument_types?.map((type: any) => (
                        <Option key={type.value} value={type.value}>{type.label}</Option>
                      ))}
                    </Select>
                  </Form.Item>
                </Col>
              </Row>

              <Row gutter={16}>
                <Col span={6}>
                  <Form.Item
                    label="Delay (延迟)"
                    name="delay"
                    rules={[{ required: true, message: '请选择延迟' }]}
                  >
                    <Select placeholder="请选择延迟">
                      {dynamicOptions.delays?.map((delay: any) => (
                        <Option key={delay.value} value={delay.value}>{delay.label}</Option>
                      ))}
                    </Select>
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item
                    label="Decay (衰减)"
                    name="decay"
                    rules={[{ required: true, message: '请输入衰减' }]}
                  >
                    <InputNumber min={1} max={30} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item
                    label="Max Trade"
                    name="max_trade"
                    rules={[{ required: true, message: '请选择交易模式' }]}
                  >
                    <Select placeholder="请选择交易模式">
                      {worldQuantOptions?.max_trade?.map((trade: any) => (
                        <Option key={trade.value} value={trade.value}>{trade.label}</Option>
                      )) || [
                        <Option key="OFF" value="OFF">OFF</Option>,
                        <Option key="ON" value="ON">ON</Option>,
                        <Option key="FULL" value="FULL">FULL</Option>
                      ]}
                    </Select>
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item 
                    label="Neutralization"
                    name="neutralization"
                    rules={[{ required: true, message: '请选择中性化方式' }]}
                  >
                    <Select placeholder="请选择中性化方式">
                      {dynamicOptions.neutralizations?.map((neut: any) => (
                        <Option key={neut.value} value={neut.value}>{neut.label}</Option>
                      ))}
                    </Select>
                  </Form.Item>
                </Col>
              </Row>
            </Card>

            {/* 配置模式和数据源 */}
            <Card title="配置模式与数据源" size="small" style={{ marginBottom: '16px' }}>
              <Row gutter={16}>
                <Col span={6}>
                  <Form.Item
                    label="使用推荐字段"
                    name="use_recommended_fields"
                    valuePropName="checked"
                  >
                    <Switch
                      checkedChildren="推荐字段"
                      unCheckedChildren="数据集"
                    />
                  </Form.Item>
                </Col>
                <Col span={18}>
                  {useRecommendedFields ? (
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          label="推荐字段名称"
                          name="recommended_name"
                          rules={[{ required: true, message: '请输入推荐字段名称' }]}
                        >
                          <Input placeholder="例如: custom_fields" />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          label="推荐字段列表"
                          name="recommended_fields"
                          rules={[{ required: true, message: '请输入推荐字段' }]}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <Button
                              type="default"
                              icon={<EditOutlined />}
                              onClick={handleOpenRecommendedFieldsModal}
                              style={{ flex: 1 }}
                            >
                              管理推荐字段 ({recommendedFieldsValue.length} 个)
                            </Button>
                            <Button
                              type="link"
                              icon={<ClearOutlined />}
                              onClick={() => {
                                setRecommendedFieldsValue([]);
                                form.setFieldsValue({ recommended_fields: [] });
                              }}
                              danger
                              size="small"
                            >
                              清空
                            </Button>
                          </div>
                        </Form.Item>
                      </Col>
                    </Row>
                  ) : (
                    <Form.Item
                      label={
                        <Space>
                          <span>数据集ID</span>
                          <Button
                            type="link"
                            icon={<EyeOutlined />}
                            size="small"
                            onClick={() => {
                              const values = form.getFieldsValue();
                              const delay = values.delay || 1;
                              const instrumentType = values.instrument_type || 'EQUITY';
                              const region = values.region || 'USA';
                              const universe = values.universe || 'TOP3000';
                              
                              const url = `https://platform.worldquantbrain.com/data?delay=${delay}&instrumentType=${instrumentType}&region=${region}&universe=${universe}`;
                              window.open(url, '_blank');
                            }}
                          >
                            挑选数据集
                          </Button>
                        </Space>
                      }
                      name="dataset_id"
                      rules={[{ required: true, message: '请输入数据集ID' }]}
                    >
                      <Input placeholder="例如: analyst11, fundamental6" />
                    </Form.Item>
                  )}
                </Col>
              </Row>
            </Card>

            {/* Tag预览 */}
            {currentTagPreview && (
              <Card title="Tag预览" size="small">
                <Tag color="blue" style={{ fontSize: '14px', padding: '4px 12px' }}>
                  {currentTagPreview}
                </Tag>
              </Card>
            )}
          </Form>
        </Modal>

        {/* 同步历史抽屉 */}
        <Drawer
          title="WorldQuant配置同步历史"
          placement="right"
          onClose={() => setIsSyncHistoryVisible(false)}
          open={isSyncHistoryVisible}
          width={600}
        >
          <div>
            {syncHistory.length > 0 ? (
              <div>
                {syncHistory.map((log, index) => (
                  <Card 
                    key={log.id} 
                    size="small" 
                    style={{ marginBottom: '16px' }}
                    title={
                      <Space>
                        <Tag color={log.success ? 'green' : 'red'}>
                          {log.success ? '成功' : '失败'}
                        </Tag>
                        <Text style={{ fontSize: '14px' }}>
                          {new Date(log.sync_time).toLocaleString()}
                        </Text>
                      </Space>
                    }
                  >
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <div>
                        <Text strong>消息: </Text>
                        <Text>{log.message}</Text>
                      </div>
                      <div>
                        <Text strong>配置项数量: </Text>
                        <Text>{log.total_configs}</Text>
                      </div>
                      <div>
                        <Text strong>耗时: </Text>
                        <Text>{log.duration_seconds} 秒</Text>
                      </div>
                    </Space>
                  </Card>
                ))}
              </div>
            ) : (
              <Empty description="暂无同步历史记录" />
            )}
          </div>
        </Drawer>
        {/* 启动挖掘进程模态框 */}
        <Modal
          title="启动挖掘进程"
          open={startModalVisible}
          onOk={handleConfirmStart}
          onCancel={() => {
            setStartModalVisible(false);
            setTemplateToStart(null);
          }}
          confirmLoading={startingProcess !== null}
        >
          <Form form={startForm} layout="vertical">
            <Alert
              message="配置挖掘参数"
              description={`模板: ${templateToStart?.name || ''}`}
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
            />
            
            <Form.Item
              label="挖掘阶段"
              name="stage"
              rules={[{ required: true, message: '请选择挖掘阶段' }]}
            >
              <Select placeholder="选择要执行的挖掘阶段">
                <Select.Option value={1}>第1阶段 - 一阶因子挖掘</Select.Option>
                <Select.Option value={2}>第2阶段 - 二阶因子挖掘</Select.Option>
                <Select.Option value={3}>第3阶段 - 三阶因子挖掘</Select.Option>
              </Select>
            </Form.Item>

            <Form.Item
              label="并发数 (n_jobs)"
              name="n_jobs"
              rules={[{ required: true, message: '请输入并发数' }]}
            >
              <InputNumber 
                min={1} 
                max={15} 
                style={{ width: '100%' }}
                placeholder="建议: 5-8"
              />
            </Form.Item>

            <Form.Item
              label="模拟模式"
              name="enable_multi_simulation"
              tooltip="多模拟模式可显著提升并发度：每10个alpha为一组，理论并发度可达n_jobs*10倍"
              initialValue={true}
            >
              <Select placeholder="选择模拟模式">
                <Select.Option value={true}>多模拟模式（需要顾问权限）</Select.Option>
                <Select.Option value={false}>单模拟模式</Select.Option>
              </Select>
            </Form.Item>
          </Form>
        </Modal>

        {/* 推荐字段管理模态框 */}
        <Modal
          title="推荐字段管理"
          open={isRecommendedFieldsModalVisible}
          onCancel={handleCloseRecommendedFieldsModal}
          width={900}
          footer={[
            <Button key="cancel" onClick={handleCloseRecommendedFieldsModal}>
              取消
            </Button>,
            <Button key="clear" onClick={handleClearRecommendedFields} danger>
              清空所有
            </Button>,
            <Button key="confirm" type="primary" onClick={handleConfirmRecommendedFields}>
              确认
            </Button>,
          ]}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            {/* 当前推荐字段展示区域 */}
            <Card 
              title={`当前推荐字段 (${tempRecommendedFields.length} 个)`}
              size="small"
              style={{ flex: 1 }}
            >
              <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                {tempRecommendedFields.length > 0 ? (
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    {tempRecommendedFields.map((field, index) => (
                      <div key={index} style={{ 
                        display: 'flex', 
                        justifyContent: 'space-between', 
                        alignItems: 'center',
                        padding: '4px 8px',
                        backgroundColor: '#f5f5f5',
                        borderRadius: '4px'
                      }}>
                        <Text 
                          style={{ 
                            flex: 1, 
                            fontFamily: 'monospace', 
                            fontSize: '13px',
                            wordBreak: 'break-all'
                          }}
                        >
                          {field}
                        </Text>
                        <Button
                          type="link"
                          icon={<DeleteOutlined />}
                          size="small"
                          danger
                          onClick={() => handleRemoveRecommendedField(field)}
                        />
                      </div>
                    ))}
                  </Space>
                ) : (
                  <Empty description="暂无推荐字段" />
                )}
              </div>
            </Card>

            {/* 输入区域 */}
            <Card title="添加推荐字段" size="small">
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text type="secondary">
                  每行输入一个字段表达式，支持批量粘贴。以 # 开头的行将被忽略，行末的逗号会被自动清理。
                </Text>
                <TextArea
                  value={recommendedFieldsInput}
                  onChange={(e) => setRecommendedFieldsInput(e.target.value)}
                  placeholder="请输入推荐字段，每行一个，例如:
winsorize(ts_backfill(star_val_pe / star_val_fwd5_eps_cagr, 120), std=4)
rank(ts_sum(volume, 5))
# 这是注释，会被忽略"
                  rows={6}
                  style={{ fontFamily: 'monospace', fontSize: '13px' }}
                />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Text type="secondary">
                    当前输入: {recommendedFieldsInput.split(/[\n\r]+/).filter(line => line.trim() && !line.trim().startsWith('#')).length} 个字段
                  </Text>
                  <Space>
                    <Button
                      onClick={() => setRecommendedFieldsInput('')}
                      disabled={!recommendedFieldsInput.trim()}
                    >
                      清空输入
                    </Button>
                    <Button
                      type="primary"
                      onClick={handleAddRecommendedFields}
                      disabled={!recommendedFieldsInput.trim()}
                    >
                      添加字段
                    </Button>
                  </Space>
                </div>
              </Space>
            </Card>
          </div>
        </Modal>
      </div>
    </DashboardLayout>
  );
};

export default ConfigPage;
