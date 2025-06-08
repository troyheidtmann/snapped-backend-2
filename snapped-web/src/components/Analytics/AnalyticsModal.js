/**
 * @fileoverview Modal component for displaying detailed Snapchat analytics metrics
 * with a focus on view-related statistics and performance indicators.
 */

import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { 
  faEye,
  faClock,
  faChartLine
} from '@fortawesome/free-solid-svg-icons';

/**
 * Formats large numbers into human-readable strings with K/M suffixes
 * @param {number} num - The number to format
 * @returns {string} Formatted number string (e.g., "1.2K" or "3.4M")
 */
const formatNumber = (num) => {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toString();
};

/**
 * @typedef {Object} AnalyticsModalProps
 * @property {Object} data - Analytics data to display
 * @property {number} data.total_views - Total number of views
 * @property {number} data.impressions - Total number of impressions
 * @property {number} data.reach - Total reach count
 * @property {number} data.story_view_time - Total view time in seconds
 * @property {Function} onClose - Callback function to close the modal
 */

/**
 * Modal component that displays detailed analytics metrics with icons and formatting.
 * 
 * @param {AnalyticsModalProps} props - Component properties
 * @returns {React.ReactElement|null} The modal UI or null if no data
 */
const AnalyticsModal = ({ data, onClose }) => {
  if (!data) return null;

  const minutes = Math.floor(data.story_view_time / 60);
  const seconds = data.story_view_time % 60;

  return (
    <div className="media-modal-overlay">
      <div className="media-modal">
        <button className="close-button" onClick={onClose}>×</button>
        
        <div className="analytics-modal-content">
          <h2>Snapchat Analytics</h2>
          <div className="metrics-grid">
            {/* View Metrics */}
            <div className="metric-section">
              <h3>Performance</h3>
              <div className="metric-item">
                <FontAwesomeIcon icon={faEye} />
                <span>Total Views: {formatNumber(data.total_views)}</span>
              </div>
              <div className="metric-item">
                <FontAwesomeIcon icon={faEye} />
                <span>Impressions: {formatNumber(data.impressions)}</span>
              </div>
              <div className="metric-item">
                <FontAwesomeIcon icon={faChartLine} />
                <span>Reach: {formatNumber(data.reach)}</span>
              </div>
              <div className="metric-item">
                <FontAwesomeIcon icon={faClock} />
                <span>View Time: {minutes}m {seconds}s</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnalyticsModal; 