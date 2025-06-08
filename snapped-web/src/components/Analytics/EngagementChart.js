/**
 * @fileoverview Line chart component that visualizes daily story view engagement metrics
 * with interactive tooltips and responsive layout.
 */

import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts';

/**
 * Formats large numbers into human-readable strings with K/M suffixes
 * @param {number} num - The number to format
 * @returns {string} Formatted number string (e.g., "1.2K" or "3.4M")
 */
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

/**
 * @typedef {Object} DailyMetric
 * @property {string} date - The date of the metrics
 * @property {number} views - Number of views for that day
 */

/**
 * @typedef {Object} EngagementChartProps
 * @property {Object} data - Analytics data
 * @property {DailyMetric[]} data.daily_metrics - Array of daily metrics
 */

/**
 * Component that displays a line chart of daily story views over time.
 * Features:
 * - Responsive layout that adapts to container size
 * - Interactive tooltips showing exact values
 * - Formatted axis labels for better readability
 * - Grid lines for easier data interpretation
 * - Smooth line transitions
 * 
 * @param {EngagementChartProps} props - Component properties
 * @returns {React.ReactElement} The engagement line chart
 */
const EngagementChart = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Daily Story Views</h3>
        <div className="no-data-message">No data available</div>
      </div>
    );
  }

  const sortedMetrics = [...data.daily_metrics].sort((a, b) => 
    new Date(a.date) - new Date(b.date)
  );

  const chartData = sortedMetrics.map(day => ({
    date: new Date(day.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    views: day.views || 0
  }));

  return (
    <div className="chart-container">
      <h3>Daily Story Views</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer className="line-chart">
          <LineChart 
            data={chartData}
            margin={{ top: 10, right: 20, left: 20, bottom: 40 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
            <XAxis 
              dataKey="date"
              axisLine={{ stroke: 'var(--border-color)' }}
              tickLine={{ stroke: 'var(--border-color)' }}
            />
            <YAxis 
              tickFormatter={formatNumber}
              axisLine={{ stroke: 'var(--border-color)' }}
              tickLine={{ stroke: 'var(--border-color)' }}
            />
            <Tooltip 
              formatter={(value) => formatNumber(value)}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Line
              type="monotone"
              dataKey="views"
              stroke="var(--primary-color)"
              strokeWidth={2}
              dot={false}
              name="Story Views"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default EngagementChart; 