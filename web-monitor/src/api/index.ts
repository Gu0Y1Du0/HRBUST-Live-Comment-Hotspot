// src/api/index.ts
import axios from 'axios';

const API_BASE_URL = 'http://hadoop01:8000';

// 创建axios实例
const api = axios.create({
    baseURL: API_BASE_URL,
    timeout: 10000,
});

// 接口定义

// 获取排行榜
export const getRank = () => api.get('/api/live/rank');

// 获取历史数据
export const getHistory = (roomId: string) => api.get(`/api/live/history/${roomId}`);

// 启动直播监控
export const startLiveTask = (roomId: string, platform: string) =>
    api.post('/api/live/monitor/start', { room_id: roomId, platform: platform });

// 启动视频分析
export const startVideoTask = (bvId: string) =>
    api.post('/api/video/analyze', { bv_id: bvId });
