import React, { useState } from 'react';
import { Card, Tabs, Input, Button, Form, message, Modal } from 'antd';
import { PlayCircleOutlined, VideoCameraAddOutlined } from '@ant-design/icons';
import { startLiveTask, startVideoTask } from '../api';

const TaskControl: React.FC = () => {
  const [loading, setLoading] = useState(false);

  // 提交直播任务
  const handleLiveSubmit = async (values: { roomId: string }) => {
    setLoading(true);
    try {
      const res = await startLiveTask(values.roomId);
      message.success(`直播监控已启动: ${values.roomId}`);
    } catch (error) {
      message.error('启动失败，请检查后端');
    } finally {
      setLoading(false);
    }
  };

  // 提交视频任务
  const handleVideoSubmit = async (values: { bvId: string }) => {
    setLoading(true);
    try {
      const res = await startVideoTask(values.bvId);
      message.success(`视频分析任务已启动: ${values.bvId}`);
    } catch (error) {
      message.error('启动失败，请检查后端');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card title="任务控制中心" bordered={false} style={{ background: '#1f1f1f', marginBottom: 20 }}>
      <Tabs
        defaultActiveKey="1"
        items={[
          {
            key: '1',
            label: <span><VideoCameraAddOutlined /> 添加直播监控</span>,
            children: (
              <Form layout="inline" onFinish={handleLiveSubmit}>
                <Form.Item name="roomId" rules={[{ required: true, message: '请输入房间号' }]}>
                  <Input placeholder="输入 B站 直播间ID (如 732)" style={{ width: 250 }} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" htmlType="submit" loading={loading} icon={<PlayCircleOutlined />}>
                    启动监控
                  </Button>
                </Form.Item>
              </Form>
            ),
          },
          {
            key: '2',
            label: <span><PlayCircleOutlined /> 视频回放分析</span>,
            children: (
              <Form layout="inline" onFinish={handleVideoSubmit}>
                <Form.Item name="bvId" rules={[{ required: true, message: '请输入BV号' }]}>
                  <Input placeholder="输入视频 BV号 (如 BV1xx...)" style={{ width: 250 }} />
                </Form.Item>
                <Form.Item>
                  <Button type="primary" htmlType="submit" loading={loading} style={{ background: '#faad14', borderColor: '#faad14' }}>
                    开始分析
                  </Button>
                </Form.Item>
              </Form>
            ),
          },
        ]}
      />
    </Card>
  );
};

export default TaskControl;
