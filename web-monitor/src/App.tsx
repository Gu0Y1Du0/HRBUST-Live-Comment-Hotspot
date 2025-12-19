import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import MainLayout from './layouts/MainLayout';
import Dashboard from './pages/Dashboard';
import PlatformMonitor from './pages/PlatformMonitor';

const App: React.FC = () => {
  return (
    <BrowserRouter>
      <Routes>
        {/* 使用 Layout 包裹所有页面 */}
        <Route path="/" element={<MainLayout />}>

          {/* 首页 */}
          <Route index element={<Dashboard />} />

          {/* B站：有直播 + 有视频 */}
          <Route path="bilibili" element={
            <PlatformMonitor platformName="Bilibili" enableVideo={true} />
          } />

          {/* 抖音：有直播 + 有视频 */}
          <Route path="douyin" element={
            <PlatformMonitor platformName="Douyin" enableVideo={true} />
          } />

          {/* 斗鱼：只有直播 (enableVideo=false) */}
          <Route path="douyu" element={
            <PlatformMonitor platformName="Douyu" enableVideo={false} />
          } />

          {/* 404 跳转 */}
          <Route path="*" element={<Navigate to="/" replace />} />

        </Route>
      </Routes>
    </BrowserRouter>
  );
};

export default App;
