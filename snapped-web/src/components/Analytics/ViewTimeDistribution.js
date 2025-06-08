/**
 * @fileoverview Pie chart component that visualizes the distribution of view time
 * between regular story views and saved story views.
 */

import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

/**
 * Color scheme for different view types
 */
const COLORS = ['#6366f1', '#10b981', '#f59e0b', '#8b5cf6'];

/**
 * Formats time duration in seconds to a human-readable string
 * @param {number} seconds - Time duration in seconds
 * @returns {string} Formatted time string (e.g., "5m" or "2h 30m")
 */
const formatTime = (seconds) => {
  if (!seconds) return '0m';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
};

/**
 * @typedef {Object} DailyMetric
 * @property {number} story_view_time - Regular story view time in seconds
 * @property {number} saved_story_view_time - Saved story view time in seconds
 * @property {number} avg_view_time - Average view time in seconds
 * @property {number} views - Number of views
 */

/**
 * @typedef {Object} ViewTimeDistributionProps
 * @property {Object} data - Analytics data
 * @property {DailyMetric[]} data.daily_metrics - Array of daily metrics
 */

/**
 * Component that displays a pie chart showing view time distribution.
 * Features:
 * - Interactive pie chart with tooltips
 * - Color-coded segments for different view types
 * - Time formatting in hours and minutes
 * - Average metrics display
 * - Responsive layout
 * 
 * @param {ViewTimeDistributionProps} props - Component properties
 * @returns {React.ReactElement} The view time distribution chart
 */
const ViewTimeDistribution = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>View Time Distribution</h3>
        <div className="no-data-message">No view time data available</div>
      </div>
    );
  }

  // Aggregate time metrics across all days
  const totalTimeData = data.daily_metrics.reduce((acc, day) => {
    acc.storyViewTime += day.story_view_time || 0;
    acc.savedStoryViewTime += day.saved_story_view_time || 0;
    acc.avgViewTime = (acc.avgViewTime || 0) + (day.avg_view_time || 0);
    acc.totalViews = (acc.totalViews || 0) + (day.views || 0);
    return acc;
  }, {
    storyViewTime: 0,
    savedStoryViewTime: 0,
    avgViewTime: 0,
    totalViews: 0
  });

  // Calculate average view time across all days
  totalTimeData.avgViewTime = totalTimeData.avgViewTime / data.daily_metrics.length;

  const pieData = [
    { name: 'Story Views', value: totalTimeData.storyViewTime },
    { name: 'Saved Stories', value: totalTimeData.savedStoryViewTime }
  ].filter(item => item.value > 0);

  // Add average metrics below the chart
  const averageMetrics = [
    { label: 'Average View Time', value: formatTime(totalTimeData.avgViewTime) },
    { label: 'Total Views', value: totalTimeData.totalViews.toLocaleString() }
  ];

  return (
    <div className="chart-container">
      <h3>View Time Distribution</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer className="pie-chart-small">
          <PieChart margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
            <Pie
              data={pieData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              outerRadius={60}
              label={({ name, value }) => `${name}: ${formatTime(value)}`}
            >
              {pieData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip formatter={(value) => formatTime(value)} />
            <Legend 
              verticalAlign="bottom"
              height={30}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
      <div className="metrics-summary">
        {averageMetrics.map((metric, index) => (
          <div key={index} className="metric-item">
            <div className="metric-label">{metric.label}</div>
            <div className="metric-value">{metric.value}</div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default ViewTimeDistribution; 