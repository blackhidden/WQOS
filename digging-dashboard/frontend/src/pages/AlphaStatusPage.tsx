/**
 * Alpha状态查看页面
 */

import React, { useState, useEffect } from 'react';
import {
  Card,
  Table,
  Tabs,
  Statistic,
  Row,
  Col,
  Typography,
  Tag,
  message,
  Badge,
  Space,
  Tooltip,
  Button,
  Modal,
  Switch,
  Select,
  Grid
} from 'antd';
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  ThunderboltOutlined,
  BarChartOutlined,
  DeleteOutlined,
  ExclamationCircleOutlined
} from '@ant-design/icons';
import { alphasAPI, AlphaItem, AlphaStatistics } from '../services/alphas';
import { DashboardLayout } from '../components/Layout/DashboardLayout';

const { Title, Text } = Typography;
const { TabPane } = Tabs;
const { useBreakpoint } = Grid;

interface TableData extends AlphaItem {
  key: string;
}

const AlphaStatusPage: React.FC = () => {
  const screens = useBreakpoint();
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'ppac' | 'normal' | 'pending'>('pending');
  const [tableData, setTableData] = useState<TableData[]>([]);
  const [originalTableData, setOriginalTableData] = useState<TableData[]>([]); // 保存原始数据
  const [statistics, setStatistics] = useState<AlphaStatistics | null>(null);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 50,
    total: 0,
  });
  
  // 表格行选择状态
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [selectedAlphas, setSelectedAlphas] = useState<TableData[]>([]);
  
  // 手动移除模态框状态
  const [removeModalVisible, setRemoveModalVisible] = useState(false);
  const [removeLoading, setRemoveLoading] = useState(false);
  
  // 筛选状态
  const [showAggressive, setShowAggressive] = useState(true); // 是否显示激进因子
  const [selectedTags, setSelectedTags] = useState<string[]>([]); // 选中的标签
  const [availableTags, setAvailableTags] = useState<string[]>([]); // 可用的标签列表
  
  // 排序状态
  const [sortField, setSortField] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<'ascend' | 'descend' | null>(null);

  // 加载所有Alpha数据（用于筛选）
  const loadAllAlphaData = async (tab: 'ppac' | 'normal' | 'pending') => {
    setLoading(true);
    try {
      // 首先获取第一页数据以了解总数
      const firstPageResponse = await alphasAPI.getSubmitableAlphas(tab, 1, 100);
      const totalCount = firstPageResponse.total;
      
      // 如果总数较少，直接使用第一页数据
      if (totalCount <= 100) {
        const data: TableData[] = firstPageResponse.data.map(item => ({
          ...item,
          key: item.alpha_id,
        }));
        
        processAllData(data, totalCount);
        return;
      }
      
      // 如果数据较多，分批获取所有数据
      const allData: TableData[] = [];
      const pageSize = 200; // 使用较大的页面大小减少请求次数
      const totalPages = Math.ceil(totalCount / pageSize);
      
      // 并行获取所有页面的数据
      const promises = [];
      for (let page = 1; page <= totalPages; page++) {
        promises.push(alphasAPI.getSubmitableAlphas(tab, page, pageSize));
      }
      
      const responses = await Promise.all(promises);
      
      // 合并所有数据
      responses.forEach(response => {
        const pageData: TableData[] = response.data.map(item => ({
          ...item,
          key: item.alpha_id,
        }));
        allData.push(...pageData);
      });
      
      processAllData(allData, totalCount);
      
    } catch (error: any) {
      message.error(`加载Alpha数据失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // 处理所有数据：提取标签、设置原始数据、应用筛选
  const processAllData = (data: TableData[], totalCount: number) => {
    // 保存原始数据
    setOriginalTableData(data);
    
    // 提取所有可用的标签
    const tagsSet = new Set<string>();
    data.forEach(item => {
      if (item.tags) {
        try {
          const tagList = typeof item.tags === 'string' ? JSON.parse(item.tags) : item.tags;
          if (Array.isArray(tagList)) {
            tagList.forEach(tag => tagsSet.add(tag));
          }
        } catch {
          // 如果解析失败，直接添加原始字符串
          tagsSet.add(item.tags);
        }
      }
    });
    setAvailableTags(Array.from(tagsSet).sort());
    
    // 更新分页信息（基于原始数据总数）
    setPagination(prev => ({
      ...prev,
      current: 1, // 重置到第一页
      total: totalCount,
    }));
    
    // 应用筛选
    applyFilters(data);
  };

  // 应用筛选、排序和本地分页（带参数版本，用于实时更新）
  const applyFiltersWithParams = (
    data: TableData[] = originalTableData, 
    sortFieldParam: string | null = sortField, 
    sortOrderParam: 'ascend' | 'descend' | null = sortOrder,
    currentPage: number = pagination.current,
    pageSize: number = pagination.pageSize
  ) => {
    let filteredData = [...data];
    
    // 筛选激进因子
    if (!showAggressive) {
      filteredData = filteredData.filter(item => !item.aggressive_mode);
    }
    
    // 筛选标签
    if (selectedTags.length > 0) {
      filteredData = filteredData.filter(item => {
        if (!item.tags) return false;
        try {
          const tagList = typeof item.tags === 'string' ? JSON.parse(item.tags) : item.tags;
          if (Array.isArray(tagList)) {
            return selectedTags.some(selectedTag => tagList.includes(selectedTag));
          } else {
            return selectedTags.includes(item.tags);
          }
        } catch {
          return selectedTags.includes(item.tags);
        }
      });
    }
    
    // 全局排序（使用参数）
    if (sortFieldParam && sortOrderParam) {
      filteredData.sort((a, b) => {
        let aValue: any = a[sortFieldParam as keyof TableData];
        let bValue: any = b[sortFieldParam as keyof TableData];
        
        // 处理数值类型的排序
        if (typeof aValue === 'number' && typeof bValue === 'number') {
          const result = aValue - bValue;
          return sortOrderParam === 'ascend' ? result : -result;
        }
        
        // 处理字符串类型的排序
        if (typeof aValue === 'string' && typeof bValue === 'string') {
          const result = aValue.localeCompare(bValue);
          return sortOrderParam === 'ascend' ? result : -result;
        }
        
        // 处理null/undefined值，将其排到最后
        if (aValue == null && bValue == null) return 0;
        if (aValue == null) return sortOrderParam === 'ascend' ? 1 : -1;
        if (bValue == null) return sortOrderParam === 'ascend' ? -1 : 1;
        
        return 0;
      });
    }
    
    // 更新分页信息（基于筛选和排序后的数据）
    const filteredTotal = filteredData.length;
    const newCurrent = Math.ceil(filteredTotal / pageSize) < currentPage ? 1 : currentPage;
    
    // 本地分页：根据当前页码和页面大小截取数据
    const startIndex = (newCurrent - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const paginatedData = filteredData.slice(startIndex, endIndex);
    
    // 设置表格数据
    setTableData(paginatedData);
    
    // 更新分页状态
    setPagination(prev => ({
      ...prev,
      total: filteredTotal,
      current: newCurrent,
      pageSize: pageSize
    }));
  };

  // 应用筛选、排序和本地分页（使用当前状态）
  const applyFilters = (data: TableData[] = originalTableData) => {
    applyFiltersWithParams(data, sortField, sortOrder, pagination.current, pagination.pageSize);
  };

  // 加载统计数据
  const loadStatistics = async () => {
    try {
      const stats = await alphasAPI.getStatistics();
      setStatistics(stats);
    } catch (error: any) {
      message.error(`加载统计数据失败: ${error.message}`);
    }
  };

  // 页面加载时获取数据
  useEffect(() => {
    loadStatistics();
    loadAllAlphaData(activeTab);
  }, []);

  // 监听筛选和排序条件变化
  useEffect(() => {
    applyFilters();
  }, [showAggressive, selectedTags, sortField, sortOrder, originalTableData]);

  // 切换页签时重新加载数据
  const handleTabChange = (key: string) => {
    const tab = key as 'ppac' | 'normal' | 'pending';
    setActiveTab(tab);
    setPagination(prev => ({ ...prev, current: 1 }));
    // 清空选择状态
    setSelectedRowKeys([]);
    setSelectedAlphas([]);
    // 重置筛选和排序条件
    setShowAggressive(true);
    setSelectedTags([]);
    setSortField(null);
    setSortOrder(null);
    loadAllAlphaData(tab);
  };

  // 表格变化处理（分页、排序、筛选）
  const handleTableChange = (paginationInfo: any, filters: any, sorter: any) => {
    const { current, pageSize } = paginationInfo;
    
    // 处理排序 - 直接使用sorter参数进行排序
    let newSortField = null;
    let newSortOrder = null;
    
    if (sorter && sorter.field && sorter.order) {
      newSortField = sorter.field;
      newSortOrder = sorter.order;
    }
    
    // 更新排序状态
    setSortField(newSortField);
    setSortOrder(newSortOrder);
    
    // 更新分页状态
    setPagination(prev => ({ ...prev, current, pageSize }));
    
    // 清空选择状态（换页或排序时）
    setSelectedRowKeys([]);
    setSelectedAlphas([]);
    
    // 立即应用筛选、排序和分页（使用新的参数直接调用）
    applyFiltersWithParams(originalTableData, newSortField, newSortOrder, current, pageSize);
  };

  // 表格行选择处理
  const handleRowSelectionChange = (newSelectedRowKeys: React.Key[], newSelectedRows: TableData[]) => {
    setSelectedRowKeys(newSelectedRowKeys);
    setSelectedAlphas(newSelectedRows);
  };

  // 打开手动移除模态框
  const handleOpenRemoveModal = () => {
    if (selectedAlphas.length === 0) {
      message.warning('请先选择要移除的Alpha');
      return;
    }
    setRemoveModalVisible(true);
  };

  // 执行手动移除
  const handleRemoveAlphas = async () => {
    if (selectedAlphas.length === 0) {
      message.warning('请先选择要移除的Alpha');
      return;
    }

    setRemoveLoading(true);
    try {
      const response = await alphasAPI.removeAlphas({
        alpha_ids: selectedAlphas.map(alpha => alpha.alpha_id)
      });

      if (response.success) {
        message.success(response.message);
        
        // 刷新数据
        await loadAllAlphaData(activeTab);
        await loadStatistics();
        
        // 清空选择状态
        setSelectedRowKeys([]);
        setSelectedAlphas([]);
        
        // 关闭模态框
        setRemoveModalVisible(false);
      } else {
        message.error(response.message);
        
        // 显示失败详情
        if (response.failed_alphas && response.failed_alphas.length > 0) {
          console.error('移除失败的Alpha:', response.failed_alphas);
          Modal.error({
            title: '部分Alpha移除失败',
            content: (
              <div>
                <p>成功: {response.removed_count} 个</p>
                <p>失败: {response.failed_alphas.length} 个</p>
                <div style={{ maxHeight: '200px', overflow: 'auto' }}>
                  {response.failed_alphas.map((error, index) => (
                    <p key={index} style={{ margin: '4px 0', fontSize: '12px' }}>
                      {error}
                    </p>
                  ))}
                </div>
              </div>
            )
          });
        }
      }
    } catch (error: any) {
      message.error(`移除失败: ${error.message}`);
    } finally {
      setRemoveLoading(false);
    }
  };

  // 表格列定义
  const getColumns = () => {
    const baseColumns: any[] = [
      {
        title: 'Alpha ID',
        dataIndex: 'alpha_id',
        key: 'alpha_id',
        width: 140,
        render: (text: string, record: TableData) => (
          <Space>
            <Text 
              code 
              style={{ 
                color: '#1890ff', 
                cursor: 'pointer',
                textDecoration: 'underline'
              }}
              onClick={() => {
                const url = `https://platform.worldquantbrain.com/alpha/${text}`;
                window.open(url, '_blank');
              }}
            >
              {text}
            </Text>
            {record.aggressive_mode && (
              <Tooltip title="不推荐提交因子：早期为0，近期强势上涨">
                <Tag 
                  color="orange" 
                  style={{ 
                    fontSize: '10px',
                    lineHeight: '16px',
                    padding: '0 4px',
                    margin: 0,
                    height: '18px',
                    display: 'inline-flex',
                    alignItems: 'center'
                  }}
                >
                  ⚠️
                </Tag>
              </Tooltip>
            )}
          </Space>
        ),
      },
      {
        title: '标签',
        dataIndex: 'tags',
        key: 'tags',
        width: 150,
        render: (tags: string) => {
          if (!tags) return '-';
          try {
            const tagList = typeof tags === 'string' ? JSON.parse(tags) : tags;
            if (Array.isArray(tagList)) {
              return (
                <Space wrap>
                  {tagList.map((tag: string, index: number) => (
                    <Tag key={index} color="blue" style={{ fontSize: '12px' }}>
                      {tag}
                    </Tag>
                  ))}
                </Space>
              );
            }
          } catch {
            // 如果解析失败，直接显示原始字符串
          }
          return <Tag color="blue">{tags}</Tag>;
        },
      },
      {
        title: 'Fitness',
        dataIndex: 'fitness',
        key: 'fitness',
        width: 100,
        sorter: true, // 启用全局排序
        sortOrder: sortField === 'fitness' ? sortOrder : null,
        showSorterTooltip: false,
        render: (value: number) => (
          <Text strong style={{ color: value >= 1 ? '#52c41a' : '#1890ff' }}>
            {value?.toFixed(3) || '-'}
          </Text>
        ),
      },
      {
        title: 'Sharpe',
        dataIndex: 'sharpe',
        key: 'sharpe',
        width: 100,
        sorter: true, // 启用全局排序
        sortOrder: sortField === 'sharpe' ? sortOrder : null,
        showSorterTooltip: false,
        render: (value: number) => (
          <Text strong style={{ color: value >= 1.58 ? '#52c41a' : '#1890ff' }}>
            {value?.toFixed(3) || '-'}
          </Text>
        ),
      },
    ];

    // 根据页签类型添加相关性列
    if (activeTab === 'normal') {
      baseColumns.push({
        title: (
          <Space>
            Self Corr
            <BarChartOutlined />
          </Space>
        ) as any,
        dataIndex: 'self_corr',
        key: 'self_corr',
        width: 120,
        sorter: true, // 启用全局排序
        sortOrder: sortField === 'self_corr' ? sortOrder : null,
        showSorterTooltip: false,
        render: (value: number) => (
          <Text strong style={{ color: value < 0.7 ? '#52c41a' : '#ff4d4f' }}>
            {value?.toFixed(4) || '-'}
          </Text>
        ),
      },
      {
        title: (
        <Space>
          Power Pool Correlation
          <ThunderboltOutlined />
        </Space>
        ) as any,
        dataIndex: 'prod_corr',
        key: 'prod_corr',
        width: 120,
        sorter: true, // 启用全局排序
        sortOrder: sortField === 'prod_corr' ? sortOrder : null,
        showSorterTooltip: false,
        render: (value: number) => (
          <Text strong style={{ color: value < 0.5 ? '#52c41a' : '#ff4d4f' }}>
            {value?.toFixed(4) || '-'}
          </Text>
        ),
      });
    } else if (activeTab === 'ppac') {
      baseColumns.push({
        title: (
        <Space>
          Power Pool Correlation
          <ThunderboltOutlined />
        </Space>
        ) as any,
        dataIndex: 'correlation_value',
        key: 'correlation_value',
        width: 120,
        sorter: true, // 启用全局排序
        sortOrder: sortField === 'correlation_value' ? sortOrder : null,
        showSorterTooltip: false,
        render: (value: number) => (
          <Text strong style={{ color: value < 0.5 ? '#52c41a' : '#ff4d4f' }}>
            {value?.toFixed(4) || '-'}
          </Text>
        ),
      });
    }

    return baseColumns;
  };

  // 获取页签标题和徽章
  const getTabContent = (tab: 'ppac' | 'normal' | 'pending') => {
    if (!statistics) return { title: '', badge: 0, icon: null, color: '#1890ff' };

    const configs = {
      ppac: {
        title: 'PPAC因子',
        badge: statistics.ppac_count,
        icon: <ThunderboltOutlined />,
        color: '#1890ff'
      },
      normal: {
        title: '普通因子',
        badge: statistics.normal_count,
        icon: <CheckCircleOutlined />,
        color: '#52c41a'
      },
      pending: {
        title: '待检测因子',
        badge: statistics.pending_count,
        icon: <ClockCircleOutlined />,
        color: '#faad14'
      }
    };

    return configs[tab];
  };

  return (
    <DashboardLayout>
      <div style={{ padding: '24px' }}>
        {/* 页面标题 */}
        <div style={{ marginBottom: '24px' }}>
          <Title level={2}>
            <BarChartOutlined style={{ marginRight: '8px' }} />
            Alpha 状态查看
          </Title>
          <Text type="secondary">
            查看可提交Alpha的状态和相关性检测结果
          </Text>
        </div>

      {/* 统计卡片 */}
      {statistics && (
        <Row 
          gutter={[16, 16]} 
          style={{ marginBottom: screens.md ? '24px' : '16px' }}
        >
          <Col xs={12} sm={12} md={6}>
            <Card size={screens.md ? 'default' : 'small'}>
              <Statistic
                title="总计"
                value={statistics.total_count}
                prefix={<BarChartOutlined />}
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col xs={12} sm={12} md={6}>
            <Card size={screens.md ? 'default' : 'small'}>
              <Statistic
                title="普通因子"
                value={statistics.normal_count}
                prefix={<CheckCircleOutlined />}
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
          <Col xs={12} sm={12} md={6}>
            <Card size={screens.md ? 'default' : 'small'}>
              <Statistic
                title="PPAC因子"
                value={statistics.ppac_count}
                prefix={<ThunderboltOutlined />}
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col xs={12} sm={12} md={6}>
            <Card size={screens.md ? 'default' : 'small'}>
              <Statistic
                title="待检测因子"
                value={statistics.pending_count}
                prefix={<ClockCircleOutlined />}
                valueStyle={{ color: '#faad14' }}
              />
            </Card>
          </Col>
        </Row>
      )}

      {/* 主要内容区域 */}
      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          size="large"
        >
          {(['pending', 'normal', 'ppac'] as const).map(tab => {
            const config = getTabContent(tab);
            return (
              <TabPane
                tab={
                  <Space>
                    {config.icon}
                    {config.title}
                    <Badge 
                      count={config.badge} 
                      style={{ backgroundColor: config.color }}
                      overflowCount={9999}
                    />
                  </Space>
                }
                key={tab}
              >
                {/* 筛选工具栏 */}
                <div style={{ 
                  marginBottom: '16px', 
                  padding: screens.md ? '12px' : '8px', 
                  backgroundColor: '#fafafa', 
                  borderRadius: '6px' 
                }}>
                  {screens.md ? (
                    // 桌面端布局
                    <Row gutter={16} align="middle">
                      <Col>
                        <Space>
                          <span style={{ fontWeight: 'bold' }}>筛选条件:</span>
                        </Space>
                      </Col>
                      <Col>
                        <Space align="center">
                          <span>显示激进因子:</span>
                          <Switch 
                            checked={showAggressive}
                            onChange={setShowAggressive}
                            size="small"
                          />
                          {!showAggressive && (
                            <span style={{ color: '#ff4d4f', fontSize: '12px' }}>
                              (已隐藏 {originalTableData.filter(item => item.aggressive_mode).length} 个激进因子)
                            </span>
                          )}
                        </Space>
                      </Col>
                      <Col flex="auto">
                        <Space align="center" style={{ width: '100%' }}>
                          <span>标签筛选:</span>
                          <Select
                            mode="multiple"
                            placeholder="选择标签进行筛选"
                            value={selectedTags}
                            onChange={setSelectedTags}
                            style={{ minWidth: 350, flex: 1, maxWidth: 500 }}
                            size="small"
                            allowClear
                            maxTagCount={2}
                            maxTagPlaceholder={(omittedValues) => `+${omittedValues.length}...`}
                            dropdownStyle={{ minWidth: 400 }}
                            options={availableTags.map(tag => ({ 
                              label: tag, 
                              value: tag 
                            }))}
                          />
                          {selectedTags.length > 0 && (
                            <span style={{ color: '#1890ff', fontSize: '12px' }}>
                              (已选择 {selectedTags.length} 个标签)
                            </span>
                          )}
                        </Space>
                      </Col>
                      <Col>
                        <Button 
                          size="small" 
                          onClick={() => {
                            setShowAggressive(true);
                            setSelectedTags([]);
                            setSortField(null);
                            setSortOrder(null);
                          }}
                        >
                          重置筛选
                        </Button>
                      </Col>
                    </Row>
                  ) : (
                    // 移动端布局
                    <Space direction="vertical" style={{ width: '100%' }} size="small">
                      <div style={{ fontWeight: 'bold', fontSize: '13px' }}>筛选条件</div>
                      
                      <Space align="center" wrap>
                        <span style={{ fontSize: '12px' }}>显示激进因子:</span>
                        <Switch 
                          checked={showAggressive}
                          onChange={setShowAggressive}
                          size="small"
                        />
                      </Space>
                      
                      {!showAggressive && (
                        <div style={{ color: '#ff4d4f', fontSize: '11px' }}>
                          已隐藏 {originalTableData.filter(item => item.aggressive_mode).length} 个激进因子
                        </div>
                      )}
                      
                      <div style={{ width: '100%' }}>
                        <div style={{ fontSize: '12px', marginBottom: '4px' }}>标签筛选:</div>
                        <Select
                          mode="multiple"
                          placeholder="选择标签"
                          value={selectedTags}
                          onChange={setSelectedTags}
                          style={{ width: '100%' }}
                          size="small"
                          allowClear
                          maxTagCount={1}
                          maxTagPlaceholder={(omittedValues) => `+${omittedValues.length}个`}
                          options={availableTags.map(tag => ({ 
                            label: tag, 
                            value: tag 
                          }))}
                        />
                        {selectedTags.length > 0 && (
                          <div style={{ color: '#1890ff', fontSize: '11px', marginTop: '2px' }}>
                            已选择 {selectedTags.length} 个标签
                          </div>
                        )}
                      </div>
                      
                      <Button 
                        size="small" 
                        block
                        onClick={() => {
                          setShowAggressive(true);
                          setSelectedTags([]);
                          setSortField(null);
                          setSortOrder(null);
                        }}
                      >
                        重置筛选
                      </Button>
                    </Space>
                  )}
                </div>

                {/* 批量操作工具栏 */}
                {selectedRowKeys.length > 0 && (
                  <div style={{ marginBottom: '16px', padding: '8px', backgroundColor: '#f0f2f5', borderRadius: '6px' }}>
                    <Space>
                      <Text>已选择 {selectedRowKeys.length} 个Alpha</Text>
                      <Button 
                        type="primary" 
                        danger 
                        icon={<DeleteOutlined />}
                        onClick={handleOpenRemoveModal}
                        size="small"
                      >
                        从数据库移除
                      </Button>
                      <Button 
                        size="small"
                        onClick={() => {
                          setSelectedRowKeys([]);
                          setSelectedAlphas([]);
                        }}
                      >
                        取消选择
                      </Button>
                    </Space>
                  </div>
                )}

                <Table<TableData>
                  loading={loading}
                  dataSource={tableData}
                  columns={getColumns()}
                  rowSelection={{
                    selectedRowKeys,
                    onChange: handleRowSelectionChange,
                    preserveSelectedRowKeys: false, // 换页时不保留选择
                  }}
                  pagination={{
                    current: pagination.current,
                    pageSize: pagination.pageSize,
                    total: pagination.total,
                    showSizeChanger: !screens.xs,
                    showQuickJumper: !screens.xs,
                    showTotal: screens.md ? 
                      (total, range) => `第 ${range[0]}-${range[1]} 条 / 共 ${total} 条` :
                      (total, range) => `${range[0]}-${range[1]} / ${total}`,
                    pageSizeOptions: ['20', '50', '100', '200'],
                    size: screens.md ? 'default' : 'small',
                  }}
                  onChange={handleTableChange}
                  scroll={{ x: screens.md ? 'max-content' : 600 }}
                  size={screens.md ? 'middle' : 'small'}
                />
              </TabPane>
            );
          })}
        </Tabs>
      </Card>

      {/* 手动移除模态框 */}
      <Modal
        title={
          <Space>
            <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />
            从数据库移除Alpha
          </Space>
        }
        open={removeModalVisible}
        onOk={handleRemoveAlphas}
        onCancel={() => {
          setRemoveModalVisible(false);
        }}
        confirmLoading={removeLoading}
        okText="确认移除"
        cancelText="取消"
        okButtonProps={{ danger: true }}
        width={500}
      >
        <div style={{ marginBottom: '16px' }}>
          <Text type="warning">
            ⚠️ 确定要从数据库中移除以下Alpha吗？此操作不可逆。
          </Text>
        </div>

        <div>
          <Text strong>选中的Alpha ({selectedAlphas.length} 个)：</Text>
          <div style={{ 
            maxHeight: '150px', 
            overflow: 'auto', 
            border: '1px solid #d9d9d9', 
            borderRadius: '6px', 
            padding: '8px',
            marginTop: '8px',
            backgroundColor: '#fafafa'
          }}>
            <Space wrap>
              {selectedAlphas.map(alpha => (
                <Tag key={alpha.alpha_id} color="blue">
                  {alpha.alpha_id}
                </Tag>
              ))}
            </Space>
          </div>
        </div>
      </Modal>

      </div>
    </DashboardLayout>
  );
};

export default AlphaStatusPage;
