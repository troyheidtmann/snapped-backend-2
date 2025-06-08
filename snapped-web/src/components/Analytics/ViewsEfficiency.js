import React from 'react';
import './ViewsEfficiency.css';

const ViewsEfficiency = ({ data }) => {
  if (!data || !data.daily_metrics || data.daily_metrics.length === 0) {
    return (
      <div className="chart-container">
        <h3>Performance Metrics</h3>
        <div className="no-data-message">No performance data available</div>
      </div>
    );
  }

  // Calculate efficiency metrics
  const totals = data.daily_metrics.reduce((acc, day) => {
    acc.views += day.views || 0;
    acc.impressions += day.impressions || 0;
    acc.reach += day.reach || 0;
    acc.viewTime += day.snap_view_time || 0;
    return acc;
  }, { views: 0, impressions: 0, reach: 0, viewTime: 0 });

  // Calculate metrics
  const viewToImpressionRate = totals.impressions > 0 ? (totals.views / totals.impressions * 100) : 0;
  const reachRate = totals.impressions > 0 ? (totals.reach / totals.impressions * 100) : 0;
  const avgViewTimePerView = totals.views > 0 ? (totals.viewTime / totals.views) : 0;
  const avgViewTimeMinutes = avgViewTimePerView / 60;

  const formatNumber = (num) => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toLocaleString();
  };

  const getEngagementQuality = () => {
    if (viewToImpressionRate > 15) return 'Excellent';
    if (viewToImpressionRate > 10) return 'Good';
    if (viewToImpressionRate > 5) return 'Average';
    return 'Needs Improvement';
  };

  const getTimeInvestment = () => {
    if (avgViewTimeMinutes > 15) return 'High engagement';
    if (avgViewTimeMinutes > 10) return 'Good retention';
    if (avgViewTimeMinutes > 5) return 'Standard viewing';
    return 'Quick views';
  };

  return (
    <div className="chart-container">
      <h3>Performance Metrics</h3>
      
      <div className="performance-metrics">
        <div className="metric-card primary">
          <div className="metric-header">
            <span className="metric-icon">📊</span>
            <span className="metric-title">View Rate</span>
          </div>
          <div className="metric-value">{viewToImpressionRate.toFixed(1)}%</div>
          <div className="metric-description">
            {formatNumber(totals.views)} views from {formatNumber(totals.impressions)} impressions
          </div>
        </div>

        <div className="metric-card secondary">
          <div className="metric-header">
            <span className="metric-icon">👥</span>
            <span className="metric-title">Reach Rate</span>
          </div>
          <div className="metric-value">{reachRate.toFixed(1)}%</div>
          <div className="metric-description">
            {formatNumber(totals.reach)} unique viewers reached
          </div>
        </div>

        <div className="metric-card accent">
          <div className="metric-header">
            <span className="metric-icon">⏱️</span>
            <span className="metric-title">Avg View Time</span>
          </div>
          <div className="metric-value">{avgViewTimeMinutes.toFixed(1)}m</div>
          <div className="metric-description">
            {(totals.viewTime / 3600).toFixed(0)} total hours watched
          </div>
        </div>
      </div>

      <div className="performance-summary">
        <div className="summary-item">
          <strong>Engagement Quality:</strong> {getEngagementQuality()}
        </div>
        <div className="summary-item">
          <strong>Time Investment:</strong> {getTimeInvestment()}
        </div>
      </div>
    </div>
  );
};

export default ViewsEfficiency; 