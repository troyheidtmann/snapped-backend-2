import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Legend
} from 'recharts';
import './ViewsAnalysis.css';

const formatNumber = (num) => {
  if (!num) return '0';
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toString();
};

const COLORS = ['#6366f1', '#10b981', '#f59e0b', '#8b5cf6'];

const ViewsAnalysis = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Interaction Distribution</h3>
        <div className="no-data-message">No interaction data available</div>
      </div>
    );
  }

  const totals = data.daily_metrics.reduce((acc, day) => {
    acc.shares += day.shares || 0;
    acc.replies += day.replies || 0;
    acc.screenshots += day.screenshots || 0;
    acc.swipeUps += day.swipe_ups || 0;
    return acc;
  }, {
    shares: 0,
    replies: 0,
    screenshots: 0,
    swipeUps: 0
  });

  const pieData = [
    { name: 'Shares', value: totals.shares },
    { name: 'Replies', value: totals.replies },
    { name: 'Screenshots', value: totals.screenshots },
    { name: 'Swipe-Ups', value: totals.swipeUps }
  ].filter(item => item.value > 0);

  const totalInteractions = pieData.reduce((sum, item) => sum + item.value, 0);

  if (totalInteractions === 0) {
    return (
      <div className="chart-container">
        <h3>Interaction Distribution</h3>
        <div className="no-data-message">No interactions recorded</div>
      </div>
    );
  }

  return (
    <div className="chart-container">
      <h3>Interaction Distribution</h3>
      <div className="metrics-summary">
        <div className="metric-item">
          <div className="metric-label">Total Interactions</div>
          <div className="metric-value">{formatNumber(totalInteractions)}</div>
        </div>
      </div>
      <div className="chart-wrapper">
        <ResponsiveContainer className="pie-chart">
          <PieChart margin={{ top: 5, right: 5, bottom: 40, left: 5 }}>
            <Pie
              data={pieData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              outerRadius={70}
              label={({ name, value, percent }) => 
                `${name}: ${formatNumber(value)} (${(percent * 100).toFixed(1)}%)`
              }
            >
              {pieData.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={COLORS[index % COLORS.length]}
                />
              ))}
            </Pie>
            <Tooltip 
              formatter={(value) => formatNumber(value)}
            />
            <Legend 
              verticalAlign="bottom"
              height={36}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default ViewsAnalysis; 