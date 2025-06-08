/**
 * @fileoverview Pie chart component that visualizes content performance metrics
 * including swipe-ups, screenshots, replies, and shares.
 */

import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Legend
} from 'recharts';

/**
 * Formats large numbers into human-readable strings with K/M suffixes
 * @param {number} num - The number to format
 * @returns {string} Formatted number string (e.g., "1.2K" or "3.4M")
 */
const formatNumber = (num) => {
  if (!num) return '0';
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
};

/**
 * Color scheme for different interaction types
 */
const COLORS = {
  swipeUps: '#8b5cf6',
  screenshots: '#f59e0b',
  replies: '#10b981',
  shares: '#6366f1'
};

/**
 * @typedef {Object} DailyMetric
 * @property {Object} metrics - Metrics data
 * @property {Object} metrics.interactions - Interaction counts
 * @property {number} metrics.interactions.swipe_ups - Number of swipe ups
 * @property {number} metrics.interactions.screenshots - Number of screenshots
 * @property {number} metrics.interactions.replies - Number of replies
 * @property {number} metrics.interactions.shares - Number of shares
 */

/**
 * @typedef {Object} ContentPerformanceProps
 * @property {Object} data - Analytics data
 * @property {DailyMetric[]} data.daily_metrics - Array of daily metrics
 */

/**
 * Component that displays a pie chart of content interaction metrics.
 * Features:
 * - Interactive pie chart with hover tooltips
 * - Color-coded segments for different interaction types
 * - Formatted number display
 * - Legend with interaction totals
 * 
 * @param {ContentPerformanceProps} props - Component properties
 * @returns {React.ReactElement} The content performance chart
 */
const ContentPerformance = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Content Overview</h3>
        <div className="no-data-message">No interactions recorded yet</div>
      </div>
    );
  }

  const totals = data.daily_metrics.reduce((acc, day) => {
    const interactions = day.metrics?.interactions || {};
    return {
      swipeUps: acc.swipeUps + (interactions.swipe_ups || 0),
      screenshots: acc.screenshots + (interactions.screenshots || 0),
      replies: acc.replies + (interactions.replies || 0),
      shares: acc.shares + (interactions.shares || 0)
    };
  }, {
    swipeUps: 0,
    screenshots: 0,
    replies: 0,
    shares: 0
  });

  const chartData = [
    { name: 'Swipe-Ups', value: totals.swipeUps, color: COLORS.swipeUps },
    { name: 'Screenshots', value: totals.screenshots, color: COLORS.screenshots },
    { name: 'Replies', value: totals.replies, color: COLORS.replies },
    { name: 'Shares', value: totals.shares, color: COLORS.shares }
  ].filter(item => item.value > 0);

  if (chartData.length === 0) {
    return (
      <div className="chart-container">
        <h3>Content Overview</h3>
        <div className="no-data-message">No interactions recorded yet</div>
      </div>
    );
  }

  return (
    <div className="chart-container">
      <h3>Content Overview</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer className="pie-chart">
          <PieChart margin={{ top: 10, right: 80, left: 10, bottom: 10 }}>
            <Pie
              data={chartData}
              dataKey="value"
              nameKey="name"
              cx="40%"
              cy="50%"
              innerRadius="45%"
              outerRadius="65%"
              paddingAngle={2}
            >
              {chartData.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={entry.color}
                  strokeWidth={0}
                />
              ))}
            </Pie>
            <Tooltip formatter={(value) => formatNumber(value)} />
            <Legend
              verticalAlign="middle"
              align="right"
              layout="vertical"
              formatter={(value, entry) => `${value}: ${formatNumber(entry.payload.value)}`}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default ContentPerformance; 