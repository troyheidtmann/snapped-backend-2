/**
 * @fileoverview Summary component that displays key analytics metrics in a card-based layout.
 * Handles formatting of large numbers and time durations for better readability.
 */

import React from 'react';
import './AnalyticsSummary.css';

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
 * Formats time in seconds to a human-readable string
 * @param {number} seconds - Time duration in seconds
 * @returns {string} Formatted time string (e.g., "5m" or "2h")
 */
const formatTime = (seconds) => {
  if (!seconds || seconds === 0) return 'No data';
  
  const totalMinutes = Math.floor(seconds / 60);
  
  // For very large numbers (more than 1 hour), show just hours
  if (totalMinutes >= 60) {
    const hours = Math.floor(totalMinutes / 60);
    return `${hours.toLocaleString()}h`;
  }
  
  return `${totalMinutes}m`;
};

/**
 * @typedef {Object} AnalyticsSummaryProps
 * @property {Object} data - Analytics data to summarize
 * @property {number} data.total_views - Total number of views
 * @property {number} data.impressions - Total number of impressions
 * @property {number} data.reach - Total reach count
 * @property {number} data.snap_view_time - Total view time in seconds
 * @property {Array<Object>} data.daily_metrics - Daily breakdown of metrics
 */

/**
 * Component that displays a summary of key analytics metrics in card format.
 * Shows total views, impressions, reach, and view time with appropriate formatting.
 * 
 * @param {AnalyticsSummaryProps} props - Component properties
 * @returns {React.ReactElement|null} The summary cards or null if no data
 */
const AnalyticsSummary = ({ data }) => {
  if (!data) return null;

  return (
    <div className="analytics-summary">
      <div className="summary-card">
        <h4>Total Views</h4>
        <div className="value">{formatNumber(data.total_views)}</div>
      </div>

      <div className="summary-card">
        <h4>Impressions</h4>
        <div className="value">{formatNumber(data.impressions)}</div>
      </div>

      <div className="summary-card">
        <h4>Reach</h4>
        <div className="value">{formatNumber(data.reach)}</div>
      </div>

      <div className="summary-card">
        <h4>View Time</h4>
        <div className="value">{formatTime(data.snap_view_time)}</div>
      </div>
    </div>
  );
};

export default AnalyticsSummary;