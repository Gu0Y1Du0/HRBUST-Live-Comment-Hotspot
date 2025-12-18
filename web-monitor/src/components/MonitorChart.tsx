import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card, Empty } from 'antd';

interface Props {
  roomId: string | null;
  data: any[];
}

const MonitorChart: React.FC<Props> = ({ roomId, data }) => {
  if (!roomId) {
    return (
      <Card style={{ height: '500px', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#141414' }} bordered={false}>
        <Empty description="请在左侧选择一个房间" />
      </Card>
    );
  }

  const getOption = () => {
    return {
      backgroundColor: 'transparent',
      title: { text: `房间 ${roomId} 实时热度`, left: 'center', textStyle: { color: '#fff' } },
      tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
      grid: { top: '15%', left: '5%', right: '5%', bottom: '15%', containLabel: true },
      xAxis: {
        type: 'time',
        boundaryGap: false,
        axisLabel: { formatter: '{HH}:{mm}:{ss}', color: '#aaa' },
        splitLine: { show: false }
      },
      yAxis: {
        type: 'value',
        scale: true,
        splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.1)' } },
        axisLabel: { color: '#aaa' }
      },
      dataZoom: [
        { type: 'inside', start: 80, end: 100 },
        { type: 'slider', show: true, bottom: 5, height: 20, start: 80, end: 100, textStyle: { color: "#aaa" } }
      ],
      series: [
        {
          name: '热度',
          type: 'line',
          smooth: true,
          symbol: 'none',
          lineStyle: { width: 3, color: '#1890ff', shadowColor: 'rgba(24, 144, 255, 0.5)', shadowBlur: 10 },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [{ offset: 0, color: 'rgba(24, 144, 255, 0.6)' }, { offset: 1, color: 'rgba(24, 144, 255, 0.05)' }]
            }
          },
          data: data.map(item => [item.ts, item.value])
        }
      ]
    };
  };

  return (
    <Card bordered={false} style={{ background: '#141414', height: '500px' }} bodyStyle={{ height: '100%', padding: '10px' }}>
      <ReactECharts option={getOption()} style={{ height: '100%', width: '100%' }} theme="dark" />
    </Card>
  );
};

export default MonitorChart;
