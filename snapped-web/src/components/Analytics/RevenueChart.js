import React, { useState, useEffect } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend
} from 'recharts';
import { fetchAuthSession } from 'aws-amplify/auth';
import axiosInstance from '../../utils/axiosConfig';

const formatNumber = (num) => {
  if (num === null || num === undefined) return '0';
  
  const absNum = Math.abs(num);
  
  if (absNum >= 1000000000) {
    return (num / 1000000000).toFixed(1) + 'B';
  }
  if (absNum >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (absNum >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
};

const formatViews = (num) => {
  if (num === null || num === undefined) return '0';
  return formatNumber(num);
};

const formatRevenue = (num) => {
  if (num === null || num === undefined) return '$0';
  return `$${formatNumber(num)}`;
};

const RevenueChart = ({ selectedClient, data }) => {
  const [chartData, setChartData] = useState({
    loading: false,
    monthlyData: []
  });

  useEffect(() => {
    if (selectedClient) {
      fetchChartData();
    }
  }, [selectedClient, data]);

  const fetchChartData = async () => {
    try {
      setChartData(prev => ({ ...prev, loading: true }));
      
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      // Fetch revenue data
      const revenueResponse = await axiosInstance.get('/api/payments/search-payouts', {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      // Process revenue data
      const monthlyRevenue = {};
      if (revenueResponse.data?.payouts && Array.isArray(revenueResponse.data.payouts)) {
        const clientPayout = revenueResponse.data.payouts.find(p => p.client_id === selectedClient);
        
        if (clientPayout && clientPayout.creator_pulls) {
          clientPayout.creator_pulls.forEach(pull => {
            const date = new Date(pull.pull_date);
            const key = `${date.getFullYear()}-${String(date.getMonth()).padStart(2, '0')}`;
            monthlyRevenue[key] = (monthlyRevenue[key] || 0) + pull.pull_amount;
          });
        }
      }

      // Process views data from existing analytics data - same as EngagementChart
      const monthlyViews = {};
      if (data?.daily_metrics && Array.isArray(data.daily_metrics)) {
        data.daily_metrics.forEach(day => {
          const date = new Date(day.date);
          const key = `${date.getFullYear()}-${String(date.getMonth()).padStart(2, '0')}`;
          const views = day.views || 0;  // Same as EngagementChart
          monthlyViews[key] = (monthlyViews[key] || 0) + views;
        });
      }
      
      // Get available months from the data and combine with revenue
      const allMonthKeys = [...new Set([
        ...Object.keys(monthlyRevenue),
        ...Object.keys(monthlyViews)
      ])].sort();
      
      const monthlyData = allMonthKeys.map(key => {
        const [year, month] = key.split('-');
        const monthDate = new Date(parseInt(year), parseInt(month), 1);
        return {
          date: monthDate.toISOString(),
          month: monthDate.toLocaleString('default', { month: 'short' }),
          revenue: monthlyRevenue[key] || 0,
          views: monthlyViews[key] || 0
        };
      }).slice(-3); // Get last 3 months of available data

      setChartData({
        loading: false,
        monthlyData: monthlyData
      });

    } catch (error) {
      console.error('Error fetching chart data:', error);
      setChartData({
        loading: false,
        monthlyData: []
      });
    }
  };

  if (!selectedClient) {
    return (
      <div className="chart-container">
        <h3>Revenue & Views (90 Days)</h3>
        <div className="no-data-message">Select a client to view revenue and views data</div>
      </div>
    );
  }

  if (chartData.loading) {
    return (
      <div className="chart-container">
        <h3>Revenue & Views (90 Days)</h3>
        <div className="loading-spinner">
          <div className="spinner-icon">⭯</div>
          <span className="loading-text">Loading chart data...</span>
        </div>
      </div>
    );
  }

  if (chartData.monthlyData.length === 0) {
    return (
      <div className="chart-container">
        <h3>Revenue & Views (90 Days)</h3>
        <div className="no-data-message">No data available</div>
      </div>
    );
  }

  // Check if we have any views data
  const hasViewsData = chartData.monthlyData.some(month => month.views > 0);
  const chartTitle = hasViewsData ? "Revenue & Views (90 Days)" : "Revenue (90 Days)";

  return (
    <div className="chart-container">
      <h3>{chartTitle}</h3>
      <div className="chart-wrapper">
        <ResponsiveContainer width="100%" height={400}>
          <BarChart
            data={chartData.monthlyData}
            margin={{
              top: 20,
              right: 30,
              left: 20,
              bottom: 60,
            }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" />
            <YAxis 
              yAxisId="revenue"
              orientation="left"
              tickFormatter={formatRevenue}
            />
            <YAxis 
              yAxisId="views"
              orientation="right"
              tickFormatter={formatViews}
            />
            <Tooltip 
              formatter={(value, name, props) => {
                if (name === 'revenue') {
                  return [formatRevenue(value), 'Revenue'];
                } else if (name === 'views') {
                  return [formatViews(value), 'Views'];
                }
                return [value, name];
              }}
            />
            <Legend 
              verticalAlign="bottom"
              height={36}
              wrapperStyle={{ paddingTop: '10px' }}
            />
            <Bar 
              yAxisId="revenue"
              dataKey="revenue" 
              fill="#3b82f6" 
              name="Revenue"
              radius={[2, 2, 0, 0]}
            />
            <Bar 
              yAxisId="views"
              dataKey="views" 
              fill="#10b981" 
              name="Views"
              radius={[2, 2, 0, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default RevenueChart; 