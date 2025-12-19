import React, { useState } from 'react';
import { Card, Tabs, Input, Button, Form, message, Modal } from 'antd';
import { PlayCircleOutlined, VideoCameraAddOutlined } from '@ant-design/icons';
import { startLiveTask, startVideoTask } from '../api';

interface Props {
  enableVideo?: boolean;
  platformName: string;
}

const TaskControl: React.FC<Props> = ({ enableVideo = true, platformName }) => {
  const [loading, setLoading] = useState(false);

  // 提交直播任务
  const handleLiveSubmit = async (values: { roomId: string }) => {
    setLoading(true);
    try {
      const res = await startLiveTask(values.roomId, platformName.toLowerCase());
      message.success(`直播监控已启动: ${values.roomId}`);
    } catch (error: any) {
      if (error.response && error.response.data && error.response.data.detail) {
        message.warning(error.response.data.detail)
      } else {
        message.error('启动失败，请检查后端');
      }
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
    } catch (error: any) {
      if (error.response && error.response.data && error.response.data.detail) {
        message.warning(error.response.data.detail)
      } else {
        message.error('启动失败，请检查后端')
      }
    } finally {
      setLoading(false);
    }
  };

  const items = [
    {
      key: "1",
      label: <span><VideoCameraAddOutlined /> 添加{platformName}直播监控</span>,
      children: (
        <Form layout="inline" onFinish={handleLiveSubmit}>
          <Form.Item name="roomId" rules={[{ required: true, message: '请输入房间号' }]}>
            <Input placeholder={`输入${platformName}直播间ID (如 732) `} style={{ width: 250 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={loading} icon={<PlayCircleOutlined />}>
              启动监控
            </Button>
          </Form.Item>
        </Form>
      ),
    }
  ];

  // 只有当enableVideo为true时，才push视频分析的tab
  if (enableVideo) {
    items.push({
      key: "2",
      label: <span><PlayCircleOutlined /> 视频回放分析</span>,
      children: (
        <Form layout="inline" onFinish={handleVideoSubmit}>
          <Form.Item name="bvId" rules={[{ required: true, message: '请输入BV号' }]}>
            <Input placeholder={`输入视频${platformName === 'Bilibili' ? "BV号 (如 BV1xx...)" : "抖音视频id"}`} style={{ width: 250 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={loading} style={{ background: '#faad14', borderColor: '#faad14' }}>
              开始分析
            </Button>
          </Form.Item>
        </Form>
      ),
    });
  }

  return (
    <Card title={`${platformName} 任务控制台`} bordered={false} style={{ background: '#1f1f1f', marginBottom: 20 }}>
      <Tabs defaultActiveKey='1' items={items} />
    </Card>
  );
};

export default TaskControl;

{/* <Card title="任务控制中心" bordered={false} style={{ background: '#1f1f1f', marginBottom: 20 }}> */ }
{/*   <Tabs */ }
{/*     defaultActiveKey="1" */ }
{/*     items={[ */ }
{/*       { */ }
{/*         key: '1', */ }
{/*         label: <span><VideoCameraAddOutlined /> 添加直播监控</span>, */ }
{/*         children: ( */ }
{/*           <Form layout="inline" onFinish={handleLiveSubmit}> */ }
{/*             <Form.Item name="roomId" rules={[{ required: true, message: '请输入房间号' }]}> */ }
{/*               <Input placeholder="输入 B站 直播间ID (如 732)" style={{ width: 250 }} /> */ }
{/*             </Form.Item> */ }
{/*             <Form.Item> */ }
{/*               <Button type="primary" htmlType="submit" loading={loading} icon={<PlayCircleOutlined />}> */ }
{/*                 启动监控 */ }
{/*               </Button> */ }
{/*             </Form.Item> */ }
{/*           </Form> */ }
{/*         ), */ }
{/*       }, */ }
{/*       { */ }
{/*         key: '2', */ }
{/*         label: <span><PlayCircleOutlined /> 视频回放分析</span>, */ }
{/*         children: ( */ }
{/*           <Form layout="inline" onFinish={handleVideoSubmit}> */ }
{/*             <Form.Item name="bvId" rules={[{ required: true, message: '请输入BV号' }]}> */ }
{/*               <Input placeholder="输入视频 BV号 (如 BV1xx...)" style={{ width: 250 }} /> */ }
{/*             </Form.Item> */ }
{/*             <Form.Item> */ }
{/*               <Button type="primary" htmlType="submit" loading={loading} style={{ background: '#faad14', borderColor: '#faad14' }}> */ }
{/*                 开始分析 */ }
{/*               </Button> */ }
{/*             </Form.Item> */ }
{/*           </Form> */ }
{/*         ), */ }
{/*       }, */ }
{/*     ]} */ }
{/*   /> */ }
{/* </Card> */ }
