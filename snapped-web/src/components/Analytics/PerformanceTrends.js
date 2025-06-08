import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

const formatNumber = (num) => {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toString();
};

const PerformanceTrends = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Performance Trends</h3>
        <div className="no-data-message">No performance data available</div>
      </div>
    );
  }

  const chartData = [...data.daily_metrics]
    .sort((a, b) => new Date(a.date) - new Date(b.date))
    .map(day => ({
      date: new Date(day.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      views: day.views || 0,
      impressions: day.impressions || 0,
      reach: day.reach || 0,
      swipeUps: day.metrics?.interactions?.swipe_ups || 0,
      shares: day.metrics?.interactions?.shares || 0,
      replies: day.metrics?.interactions?.replies || 0,
      screenshots: day.metrics?.interactions?.screenshots || 0
    }));

  return (
    <div className="chart-container">
      <h3>Performance Trends</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer width="100%" height={400}>
          <LineChart
            data={chartData}
            margin={{ top: 10, right: 20, left: 20, bottom: 80 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis 
              dataKey="date" 
              fontSize={12}
              interval={0}
              angle={-45}
              textAnchor="end"
              height={60}
            />
            <YAxis 
              yAxisId="primary"
              tickFormatter={formatNumber}
              orientation="left"
              fontSize={12}
            />
            <YAxis 
              yAxisId="secondary"
              tickFormatter={formatNumber}
              orientation="right"
              fontSize={12}
            />
            <Tooltip 
              formatter={(value, name) => [formatNumber(value), name]}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend 
              verticalAlign="bottom"
              height={36}
              wrapperStyle={{ paddingTop: '10px', fontSize: '12px' }}
            />
            
            {/* Primary metrics on left axis */}
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="views"
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              name="Views"
            />
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="reach"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={false}
              name="Reach"
            />
            
            {/* Impressions on right axis (larger scale) */}
            <Line
              yAxisId="secondary"
              type="monotone"
              dataKey="impressions"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              name="Impressions"
            />
            
            {/* Engagement metrics on primary axis */}
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="swipeUps"
              stroke="#8b5cf6"
              strokeWidth={2}
              dot={false}
              name="Swipe-Ups"
            />
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="shares"
              stroke="#ef4444"
              strokeWidth={2}
              dot={false}
              name="Shares"
            />
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="replies"
              stroke="#06b6d4"
              strokeWidth={2}
              dot={false}
              name="Replies"
            />
            <Line
              yAxisId="primary"
              type="monotone"
              dataKey="screenshots"
              stroke="#84cc16"
              strokeWidth={2}
              dot={false}
              name="Screenshots"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default PerformanceTrends; 