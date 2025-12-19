import React from 'react';
import { Card, Row, Col, Statistic, Typography } from 'antd';
import { ArrowUpOutlined, GlobalOutlined, YoutubeOutlined, VideoCameraOutlined } from '@ant-design/icons';

const { Title } = Typography;

const Dashboard: React.FC = () => {
  return (
    <div style={{ padding: '24px' }}>
      <Title level={2} style={{ color: '#fff' }}>🚀 全网舆情监控总览</Title>

      {/* 核心指标卡 */}
      <Row gutter={24}>
        <Col span={6}>
          <Card bordered={false} style={{ background: '#1f1f1f' }}>
            <Statistic title="总监控直播间" value={12} prefix={<VideoCameraOutlined />} valueStyle={{ color: '#52c41a' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card bordered={false} style={{ background: '#1f1f1f' }}>
            <Statistic title="总分析视频" value={58} prefix={<YoutubeOutlined />} valueStyle={{ color: '#faad14' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card bordered={false} style={{ background: '#1f1f1f' }}>
            <Statistic title="系统运行时间" value="48h 20m" prefix={<GlobalOutlined />} valueStyle={{ color: '#1890ff' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card bordered={false} style={{ background: '#1f1f1f' }}>
            <Statistic
              title="数据吞吐量 (Events/s)"
              value={1280}
              precision={0}
              valueStyle={{ color: '#cf1322' }}
              prefix={<ArrowUpOutlined />}
              suffix="EPS"
            />
          </Card>
        </Col>
      </Row>

      {/* 这里可以放一个巨大的地图或者总热度趋势图，以后再加 */}
      <div style={{ marginTop: 24, height: 400, background: '#141414', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#555' }}>
        [ 此处预留位置：全平台热度聚合大盘图 ]
      </div>
    </div>
  );
};

export default Dashboard;
