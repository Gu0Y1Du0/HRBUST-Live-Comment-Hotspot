import React from 'react';
import { Layout, Menu } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  DashboardOutlined,
  YoutubeOutlined,
  TikTokOutlined,
  VideoCameraOutlined,
  CloudServerOutlined
} from '@ant-design/icons';

const { Header, Content } = Layout;

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  // 菜单项配置
  const items = [
    { key: '/', icon: <DashboardOutlined />, label: '全网态势总览' },
    { key: '/bilibili', icon: <YoutubeOutlined />, label: 'Bilibili' },
    { key: '/douyin', icon: <TikTokOutlined />, label: '抖音' },
    { key: '/douyu', icon: <VideoCameraOutlined />, label: '斗鱼' },
  ];

  return (
    <Layout style={{ minHeight: '100vh', background: '#000' }}>
      {/* --- 顶部全局导航 --- */}
      <Header style={{
        position: 'sticky',
        top: 0,
        zIndex: 1,
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        background: '#001529', // 深蓝黑色背景
        borderBottom: '2px solid #1890ff', // 底部加一条亮蓝色的线，增加科技感
        padding: '0 24px'
      }}>
        {/* Logo 区域 */}
        <div style={{
          display: 'flex', alignItems: 'center', marginRight: 40,
          color: '#fff', fontSize: '20px', fontWeight: 'bold', letterSpacing: '1px'
        }}>
          <CloudServerOutlined style={{ fontSize: '28px', color: '#1890ff', marginRight: 10 }} />
          HRBUST <span style={{ color: '#1890ff', marginLeft: 6 }}>MONITOR</span>
        </div>

        {/* 横向菜单 */}
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[location.pathname]}
          items={items}
          onClick={(e) => navigate(e.key)}
          style={{
            flex: 1,
            minWidth: 0,
            background: 'transparent',
            fontSize: '16px'
          }}
        />

        {/* 右侧状态*/}
        <div style={{ color: 'rgba(255,255,255,0.6)', fontSize: '12px' }}>
          <span style={{ marginRight: 15 }}>🟢 System Online</span>
          <span>Server: hadoop01</span>
        </div>
      </Header>

      {/* --- 内容区域 --- */}
      <Content>
        <Outlet />
      </Content>
    </Layout>
  );
};

export default MainLayout;
