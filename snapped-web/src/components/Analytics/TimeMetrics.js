/**
 * @fileoverview Component for displaying time-based metrics with proper formatting
 * and handling of various time duration scenarios.
 */

import React from 'react';
import './TimeMetrics.css';

/**
 * Formats time duration in seconds to a human-readable string
 * @param {number} seconds - Time duration in seconds
 * @returns {string} Formatted time string (e.g., "5m 30s" or "2 hours")
 */
const formatTime = (seconds) => {
  // Ensure we have a valid number
  const numSeconds = Number(seconds);
  if (isNaN(numSeconds) || numSeconds < 0) {
    return '0m 0s';
  }
  
  const totalMinutes = Math.floor(numSeconds / 60);
  const remainingSeconds = Math.floor(numSeconds % 60);
  
  // For very large numbers (more than 1 hour), show just hours
  if (totalMinutes >= 60) {
    const hours = Math.floor(totalMinutes / 60);
    return `${hours.toLocaleString()} hours`;
  }
  
  return `${totalMinutes}m ${remainingSeconds}s`;
};

/**
 * @typedef {Object} TimeMetricsData
 * @property {number} [snap_view_time] - Total view time in seconds
 * @property {number} [avg_view_time] - Average view time in seconds
 * @property {number} [completion_rate] - Content completion rate percentage
 * @property {Array<Object>} [daily_metrics] - Daily breakdown of metrics
 */

/**
 * @typedef {Object} TimeMetricsProps
 * @property {TimeMetricsData} data - Time-related metrics data
 */

/**
 * Component that displays various time-based metrics with proper formatting.
 * Features:
 * - Total view time display
 * - Average view time calculation
 * - Completion rate display
 * - Fallback handling for missing data
 * - Support for both direct and daily metric calculations
 * 
 * @param {TimeMetricsProps} props - Component properties
 * @returns {React.ReactElement|null} The time metrics display or null if no data
 */
const TimeMetrics = ({ data }) => {
  if (!data) return null;

  // Use the snap_view_time from the backend response
  let totalViewTime = data.snap_view_time || 0;
  
  // If not at top level, calculate from daily metrics
  if (totalViewTime === 0 && data.daily_metrics && Array.isArray(data.daily_metrics)) {
    totalViewTime = data.daily_metrics.reduce((total, day) => {
      const snapViewTime = day.snap_view_time || day.metrics?.time_metrics?.snap_view_time || 0;
      return total + snapViewTime;
    }, 0);
  }

  return (
    <div className="time-metrics">
      <h3>Time Metrics</h3>
      <div className="metrics-grid">
        <div className="metric-item">
          <div className="time-value">
            {totalViewTime === 0 ? 'No data' : formatTime(totalViewTime)}
          </div>
          <div className="time-label">Total Story View Time</div>
        </div>
        {data.avg_view_time && (
          <div className="metric-item">
            <div className="time-value">{formatTime(data.avg_view_time)}</div>
            <div className="time-label">Average View Time</div>
          </div>
        )}
        {data.completion_rate && (
          <div className="metric-item">
            <div className="time-value">{data.completion_rate}%</div>
            <div className="time-label">Completion Rate</div>
          </div>
        )}
      </div>
    </div>
  );
};

export default TimeMetrics; 