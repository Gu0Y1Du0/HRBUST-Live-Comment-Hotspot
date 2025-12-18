import React, { useState, useEffect } from 'react';
import { Layout, Card, List, Typography, Badge, Statistic, Row, Col, Empty } from 'antd';
import { FireOutlined, LineChartOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import axios from 'axios';

const { Header, Content, Sider } = Layout;
const { Title, Text } = Typography;

// 定义 TypeScript 接口
interface RankItem {
  room_id: string;
  heat: number;
}

interface HistoryPoint {
  ts: number;
  value: number;
}

// 配置项：后端地址
const API_BASE_URL = 'http://10.23.2.12:8000';

const App: React.FC = () => {
  // 状态管理
  const [rankList, setRankList] = useState<RankItem[]>([]);
  const [selectedRoomId, setSelectedRoomId] = useState<string | null>(null);
  const [chartData, setChartData] = useState<HistoryPoint[]>([]);
  const [loading, setLoading] = useState(false);

  // 获取排行榜数据 (每2秒轮询)
  useEffect(() => {
    const fetchRank = async () => {
      try {
        const res = await axios.get(`${API_BASE_URL}/api/rank`);
        setRankList(res.data);

        // 如果当前没有选中房间，且排行榜有数据，默认选中第一名
        if (!selectedRoomId && res.data.length > 0) {
          setSelectedRoomId(res.data[0].room_id);
        }
      } catch (error) {
        console.error("获取排行榜失败:", error);
      }
    };

    fetchRank(); // 初次执行
    const timer = setInterval(fetchRank, 2000); // 轮询
    return () => clearInterval(timer);
  }, [selectedRoomId]);

  // 获取选中房间的历史趋势 (每2秒轮询)
  useEffect(() => {
    if (!selectedRoomId) return;

    const fetchHistory = async () => {
      try {
        const res = await axios.get(`${API_BASE_URL}/api/history/${selectedRoomId}`);
        setChartData(res.data.data);
      } catch (error) {
        console.error("获取趋势失败:", error);
      }
    };

    fetchHistory();
    const timer = setInterval(fetchHistory, 2000);
    return () => clearInterval(timer);
  }, [selectedRoomId]);

  // ECharts 配置
  const getOption = () => {
    return {
      backgroundColor: 'transparent',
      // 1. 标题调整：稍微往下挪一点，不要顶头
      title: {
        text: `房间 ${selectedRoomId} 实时热度`,
        left: 'center',
        top: 10,
        textStyle: { color: '#fff', fontSize: 16 }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' }
      },
      // 2. 【关键】布局调整：大幅减小留白，让图表撑满高度
      grid: {
        top: '15%',    // 给标题留出空间
        left: '5%',    // 左边留给数值
        right: '5%',   // 右边留一点边距
        bottom: '15%', // 底部留给滑动条
        containLabel: true // 防止坐标轴文字被切掉
      },
      xAxis: {
        type: 'time',
        boundaryGap: false,
        axisLabel: {
          formatter: '{HH}:{mm}:{ss}',
          color: '#aaa' // 文字颜色变浅一点，不抢眼
        },
        splitLine: { show: false }
      },
      yAxis: {
        type: 'value',
        scale: true, // 保持脱离 0 值
        // 3. 样式优化：让网格线稍微明显一点点，看起来更像仪表盘
        splitLine: {
          show: true,
          lineStyle: { color: 'rgba(255, 255, 255, 0.1)' }
        },
        axisLabel: { color: '#aaa' }
      },
      // 4. 滑动条样式优化：放在最底部，颜色更深邃
      dataZoom: [
        {
          type: 'inside',
          start: 80,
          end: 100
        },
        {
          type: 'slider',
          show: true,
          bottom: 5, // 紧贴底部
          height: 20,
          start: 80,
          end: 100,
          borderColor: 'transparent',
          textStyle: { color: "#aaa" },
          fillerColor: "rgba(24, 144, 255, 0.3)", // 选中区域半透明蓝
          backgroundColor: 'rgba(255, 255, 255, 0.05)', // 未选中区域微亮
          handleStyle: {
            color: '#1890ff',
            shadowBlur: 3,
            shadowColor: 'rgba(0, 0, 0, 0.6)'
          }
        }
      ],
      series: [
        {
          name: '热度值',
          type: 'line',
          smooth: true, // 平滑曲线
          symbol: 'none', // 默认不显示数据点，鼠标放上去才显示
          // 5. 【视觉升级】线条加粗，渐变增强
          lineStyle: {
            width: 4, // 线条加粗！
            shadowColor: 'rgba(24, 144, 255, 0.5)', // 线条发光效果
            shadowBlur: 10
          },
          itemStyle: {
            color: '#1890ff'
          },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(24, 144, 255, 0.6)' }, // 顶部颜色更浓
                { offset: 1, color: 'rgba(24, 144, 255, 0.05)' } // 底部几乎透明
              ]
            }
          },
          data: chartData.map(item => [item.ts, item.value])
        }
      ]
    };
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Header style={{ display: 'flex', alignItems: 'center', borderBottom: '1px solid #333' }}>
        <LineChartOutlined style={{ fontSize: '24px', color: '#1890ff', marginRight: 12 }} />
        <Title level={3} style={{ color: '#fff', margin: 0 }}>HRBUST 弹幕实时监控平台</Title>
      </Header>

      <Layout>
        {/* 左侧：排行榜 */}
        <Sider width={300} style={{ borderRight: '1px solid #333', overflow: 'auto' }}>
          <div style={{ padding: '16px' }}>
            <Title level={5} style={{ color: '#aaa', marginBottom: 16 }}>
              <FireOutlined /> 实时热榜
            </Title>
            <List
              dataSource={rankList}
              renderItem={(item, index) => (
                <List.Item
                  onClick={() => setSelectedRoomId(item.room_id)}
                  style={{
                    cursor: 'pointer',
                    backgroundColor: selectedRoomId === item.room_id ? '#1f1f1f' : 'transparent',
                    padding: '12px',
                    borderRadius: '4px',
                    marginBottom: '4px',
                    border: 'none'
                  }}
                >
                  <div style={{ display: 'flex', width: '100%', alignItems: 'center' }}>
                    <Badge count={index + 1} style={{ backgroundColor: index < 3 ? '#1890ff' : '#555' }} />
                    <span style={{ marginLeft: 12, flex: 1, color: '#fff', fontWeight: 'bold' }}>
                      房间 {item.room_id}
                    </span>
                    <Text type="secondary">{item.heat} 🔥</Text>
                  </div>
                </List.Item>
              )}
            />
          </div>
        </Sider>

        {/* 右侧：图表区域 */}
        <Content style={{ padding: '24px' }}>
          {selectedRoomId ? (
            <>
              {/* 顶部指标卡 */}
              <Row gutter={16} style={{ marginBottom: 24 }}>
                <Col span={8}>
                  <Card bordered={false} style={{ background: '#141414' }}>
                    <Statistic title="当前热度" value={chartData.length > 0 ? chartData[chartData.length - 1].value : 0} prefix={<FireOutlined />} valueStyle={{ color: '#cf1322' }} />
                  </Card>
                </Col>
                <Col span={8}>
                  <Card bordered={false} style={{ background: '#141414' }}>
                    <Statistic title="监控时长" value="Real-time" valueStyle={{ color: '#3f8600' }} />
                  </Card>
                </Col>
              </Row>

              {/* 核心图表 */}
              <Card bordered={false} style={{ background: '#141414', height: '500px' }} bodyStyle={{ height: '100%', padding: '10px' }}>
                <ReactECharts option={getOption()} style={{ height: '100%', width: '100%' }} theme="dark" />
              </Card>
            </>
          ) : (
            <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Empty description="暂无数据，请检查 Kafka/Python 是否在运行" />
            </div>
          )}
        </Content>
      </Layout>
    </Layout>
  );
};

export default App;
