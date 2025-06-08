/**
 * @fileoverview Complex chart component that combines multiple interaction metrics
 * and view counts in a single visualization using a composed chart layout.
 */

import React from 'react';
import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
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
 * @typedef {Object} Interactions
 * @property {number} shares - Number of shares
 * @property {number} replies - Number of replies
 * @property {number} screenshots - Number of screenshots
 * @property {number} swipe_ups - Number of swipe ups
 */

/**
 * @typedef {Object} DailyMetric
 * @property {string} date - The date of the metrics
 * @property {Object} metrics - Metrics data
 * @property {Interactions} metrics.interactions - Interaction counts
 * @property {number} views - Number of views
 */

/**
 * @typedef {Object} InteractionsChartProps
 * @property {Object} data - Analytics data
 * @property {DailyMetric[]} data.daily_metrics - Array of daily metrics
 */

/**
 * Component that displays a complex chart combining multiple interaction metrics.
 * Features:
 * - Multiple metrics on dual Y-axes
 * - Color-coded lines for different interaction types
 * - Interactive tooltips with formatted values
 * - Responsive layout
 * - Custom legend formatting
 * 
 * @param {InteractionsChartProps} props - Component properties
 * @returns {React.ReactElement} The interactions chart
 */
const InteractionsChart = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Daily Interactions & Views</h3>
        <div className="no-data-message">No interaction data available</div>
      </div>
    );
  }

  const chartData = [...data.daily_metrics]
    .sort((a, b) => new Date(a.date) - new Date(b.date))
    .map(day => {
      const interactions = day.metrics?.interactions || {};
      return {
        date: new Date(day.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        shares: interactions.shares || 0,
        replies: interactions.replies || 0,
        screenshots: interactions.screenshots || 0,
        swipeUps: interactions.swipe_ups || 0,
        views: day.views || 0
      };
    });

  return (
    <div className="chart-container">
      <h3>Daily Interactions & Views</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer className="line-chart">
          <ComposedChart 
            data={chartData}
            margin={{ top: 20, right: 40, left: 40, bottom: 20 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis 
              yAxisId="left"
              tickFormatter={formatNumber}
              width={60}
            />
            <YAxis 
              yAxisId="right"
              orientation="right"
              tickFormatter={formatNumber}
              width={60}
            />
            <Tooltip 
              formatter={(value, name) => {
                return [formatNumber(value), name.replace(/([A-Z])/g, ' $1').trim()];
              }}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend 
              verticalAlign="top"
              height={36}
              formatter={(value) => value.replace(/([A-Z])/g, ' $1').trim()}
            />
            <Line 
              type="monotone"
              dataKey="views"
              name="Story Views"
              stroke="#94a3b8"
              strokeWidth={2}
              yAxisId="right"
              dot={false}
            />
            <Line 
              type="monotone"
              dataKey="swipeUps"
              name="Swipe Ups"
              stroke="#8b5cf6"
              strokeWidth={2}
              yAxisId="left"
              dot={false}
            />
            <Line 
              type="monotone"
              dataKey="screenshots"
              name="Screenshots"
              stroke="#f59e0b"
              strokeWidth={2}
              yAxisId="left"
              dot={false}
            />
            <Line 
              type="monotone"
              dataKey="replies"
              name="Replies"
              stroke="#10b981"
              strokeWidth={2}
              yAxisId="left"
              dot={false}
            />
            <Line 
              type="monotone"
              dataKey="shares"
              name="Shares"
              stroke="#6366f1"
              strokeWidth={2}
              yAxisId="left"
              dot={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default InteractionsChart; 