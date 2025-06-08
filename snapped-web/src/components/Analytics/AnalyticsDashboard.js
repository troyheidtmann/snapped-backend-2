/**
 * @fileoverview Main analytics dashboard component that orchestrates the display of various
 * analytics metrics, charts, and data visualizations for Snapchat performance data.
 */

import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axiosInstance from '../../utils/axiosConfig';
import { API_ENDPOINTS } from '../../config/api';
import ClientSelector from './ClientSelector';
import MetricsSummary from './MetricsSummary';
import EngagementChart from './EngagementChart';
import ContentPerformance from './ContentPerformance';
import PerformanceTrends from './PerformanceTrends';
import AnalyticsSummary from './AnalyticsSummary';
import DateRangePicker from './DateRangePicker';
import RevenueChart from './RevenueChart';
import './AnalyticsDashboard.css';
import { fetchAuthSession } from 'aws-amplify/auth';
import axios from 'axios';

/**
 * Loading spinner component for data fetching states
 * @returns {React.ReactElement} Loading spinner UI
 */
const LoadingSpinner = () => (
  <div className="loading-spinner">
    <div className="spinner-icon">⭯</div>
    <span className="loading-text">Loading analytics data...</span>
  </div>
);

/**
 * Main analytics dashboard component that provides a comprehensive view of Snapchat analytics.
 * Features include:
 * - Client selection
 * - Date range filtering
 * - Vista data synchronization
 * - Real-time analytics data visualization
 * - Multiple performance metric charts
 * 
 * @returns {React.ReactElement} The complete analytics dashboard
 */
const AnalyticsDashboard = () => {
  const [clients, setClients] = useState([]);
  const [selectedClient, setSelectedClient] = useState('');
  const [dateRange, setDateRange] = useState({
    start: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
    end: new Date()
  });
  const [analyticsData, setAnalyticsData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSyncing, setIsSyncing] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    fetchClients();
  }, []);

  useEffect(() => {
    fetchAnalyticsData();
  }, [selectedClient, dateRange]);

  const formatDateForApi = (date) => {
    return date.toLocaleDateString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    }).replace(/\//g, '-');  // Convert 01/06/2025 to 01-06-2025
  };

  const fetchClients = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      console.log('Fetching analytics clients...');
      const response = await axiosInstance.get(
        API_ENDPOINTS.ANALYTICS.GET_CLIENTS,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      console.log('Clients response:', response.data);
      if (response.data.status === 'success' && response.data.data) {
        setClients(response.data.data);
      }
    } catch (error) {
      console.error('Error fetching clients:', error);
    }
  };

  const fetchAnalyticsData = async () => {
    if (!selectedClient) return;
    
    setIsLoading(true);
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      console.log('Fetching analytics data for client:', selectedClient);
      console.log('Date range:', {
        start: formatDateForApi(dateRange.start),
        end: formatDateForApi(dateRange.end)
      });

      const response = await axiosInstance.get(
        API_ENDPOINTS.ANALYTICS.GET_SNAPCHAT,
        {
          params: {
            client_id: selectedClient,
            start_date: formatDateForApi(dateRange.start),
            end_date: formatDateForApi(dateRange.end)
          },
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      console.log('Analytics API response:', response.data);

      if (response.data.status === 'success' && response.data.data) {
        console.log('Setting analytics data:', response.data.data);
        console.log('Sample daily metrics:', response.data.data.daily_metrics?.[0]);
        setAnalyticsData(response.data.data);
      } else {
        console.log('No data in response:', response.data);
      }
    } catch (error) {
      console.error('Error fetching analytics:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleVistaSync = async () => {
    try {
      setIsSyncing(true);
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axios.post(
        API_ENDPOINTS.ANALYTICS.SYNC_VISTA,
        {},  // empty body
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      if (response.data.status === 'success') {
        alert('Vista sync completed successfully');
      }
    } catch (error) {
      console.error('Error syncing Vista:', error);
      alert('Error syncing Vista data');
    } finally {
      setIsSyncing(false);
    }
  };

  return (
    <div className="analytics-dashboard">
      <header className="dashboard-header">
        <h1>Analytics Portal</h1>
        <div className="dashboard-controls">
          <ClientSelector
            clients={clients}
            selectedClient={selectedClient}
            onClientChange={setSelectedClient}
          />
          <DateRangePicker
            startDate={dateRange.start}
            endDate={dateRange.end}
            onChange={setDateRange}
          />
          <button 
            onClick={handleVistaSync}
            disabled={isSyncing}
            className="sync-button"
          >
            {isSyncing ? (
              <>
                <div className="spinner-icon">⭯</div>
                Syncing...
              </>
            ) : 'Sync Vista Data'}
          </button>
        </div>
      </header>

      {isLoading ? (
        <LoadingSpinner />
      ) : (
        <div className="dashboard-content">
          <AnalyticsSummary data={analyticsData} />
          
          <div className="dashboard-grid">
            <div className="dashboard-item">
              <EngagementChart data={analyticsData} />
            </div>
            
            <div className="dashboard-item">
              <ContentPerformance data={analyticsData} />
            </div>
            
            <div className="dashboard-item">
              <PerformanceTrends data={analyticsData} />
            </div>

            <div className="dashboard-item">
              <RevenueChart selectedClient={selectedClient} data={analyticsData} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AnalyticsDashboard; 