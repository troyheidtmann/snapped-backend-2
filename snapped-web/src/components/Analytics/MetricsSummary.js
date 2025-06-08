import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { 
  faEye, 
  faShare,
  faChartLine,
  faClock
} from '@fortawesome/free-solid-svg-icons';
import './MetricsSummary.css';

const formatNumber = (num) => {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}K`;
  }
  return num.toString();
};

const formatTime = (seconds) => {
  if (!seconds) return '0m';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
};

const MetricsSummary = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) return null;

  const totals = data.daily_metrics.reduce((acc, day) => {
    acc.views += day.views || 0;
    acc.impressions += day.impressions || 0;
    acc.interactions += (day.shares || 0) + (day.replies || 0) + (day.screenshots || 0);
    acc.viewTime += day.story_view_time || 0;
    return acc;
  }, { views: 0, impressions: 0, interactions: 0, viewTime: 0 });

  const metrics = [
    {
      title: 'Total Views',
      value: formatNumber(totals.views),
      icon: faEye,
      color: '#3b82f6'
    },
    {
      title: 'Total Impressions',
      value: formatNumber(totals.impressions),
      icon: faChartLine,
      color: '#10b981'
    },
    {
      title: 'Total Interactions',
      value: formatNumber(totals.interactions),
      icon: faShare,
      color: '#f59e0b'
    },
    {
      title: 'Total View Time',
      value: formatTime(totals.viewTime),
      icon: faClock,
      color: '#8b5cf6'
    }
  ];

  return (
    <div className="metrics-summary">
      {metrics.map((metric, index) => (
        <div key={index} className="metric-card">
          <div className="metric-icon" style={{ backgroundColor: metric.color }}>
            <FontAwesomeIcon icon={metric.icon} />
          </div>
          <div className="metric-info">
            <h3>{metric.title}</h3>
            <p>{metric.value}</p>
          </div>
        </div>
      ))}
    </div>
  );
};

export default MetricsSummary; 