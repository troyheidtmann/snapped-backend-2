/**
 * @fileoverview Employee dashboard component for managing employee data and metrics.
 * Provides comprehensive views of employee performance, timesheets, and invoices.
 */

import React, { useState, useEffect } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { 
  faUsers, 
  faClock, 
  faChartLine, 
  faFileInvoice, 
  faCalendarAlt,
  faDownload,
  faDollarSign,
  faUser,
  faSpinner,
  faExclamationTriangle
} from '@fortawesome/free-solid-svg-icons';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { useAuth } from '../../contexts/AuthContext';
import { fetchAuthSession } from 'aws-amplify/auth';
import axiosInstance from '../../utils/axiosConfig';
import EmployeeSelector from './EmployeeSelector';
import './EmployeeDashboard.css';

/**
 * @typedef {Object} EmployeeMetrics
 * @property {number} total_hours - Total hours worked
 * @property {number} total_earnings - Total earnings
 * @property {number} active_clients - Number of active clients
 * @property {number} avg_hours_per_day - Average hours worked per day
 * @property {number} productivity_score - Employee productivity score
 * @property {string} efficiency_rating - Efficiency rating category
 * @property {string} most_worked_client - ID of the most worked client
 * @property {Array<Object>} client_work_summary - Summary of work per client
 */

/**
 * @typedef {Object} TimeEntry
 * @property {string} date - Date of the time entry
 * @property {string} client_name - Name of the client
 * @property {string} category - Work category
 * @property {number} hours - Hours worked
 * @property {number} minutes - Minutes worked
 * @property {number} earnings - Earnings for the entry
 * @property {string} description - Work description
 */

/**
 * @typedef {Object} Invoice
 * @property {string} qb_id - QuickBooks invoice ID
 * @property {string} status - Invoice status
 * @property {string} start_date - Start date of the invoice period
 * @property {number} total_hours - Total hours billed
 * @property {number} total_earnings - Total amount billed
 */

/**
 * Employee dashboard component for managing employee data and performance metrics.
 * Features:
 * - Overview of key performance metrics
 * - Detailed timesheet tracking
 * - Invoice management and downloads
 * - Client work summaries
 * - Date range filtering
 * - Loading state management
 * - Error handling
 * 
 * @returns {React.ReactElement} The employee dashboard interface
 */
const EmployeeDashboard = () => {
  const { user } = useAuth();
  const [selectedEmployee, setSelectedEmployee] = useState(null);
  const [activeView, setActiveView] = useState('overview');
  const [employeeMetrics, setEmployeeMetrics] = useState(null);
  const [employeeTimesheet, setEmployeeTimesheet] = useState(null);
  const [employeeInvoices, setEmployeeInvoices] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [dateRange, setDateRange] = useState({
    start_date: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
    end_date: new Date().toISOString().split('T')[0]
  });

  useEffect(() => {
    if (selectedEmployee) {
      loadEmployeeData();
    }
  }, [selectedEmployee, dateRange]);

  const loadEmployeeData = async () => {
    if (!selectedEmployee) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      const headers = {
        Authorization: `Bearer ${token}`
      };

      console.log('Loading employee data for:', selectedEmployee.user_id);

      const [metricsResponse, timesheetResponse, invoicesResponse] = await Promise.all([
        axios.get(API_ENDPOINTS.EMPLOYEES.METRICS(selectedEmployee.user_id), {
          params: dateRange,
          headers
        }),
        axios.get(API_ENDPOINTS.EMPLOYEES.TIMESHEET(selectedEmployee.user_id), {
          params: dateRange,
          headers
        }),
        axios.get(API_ENDPOINTS.EMPLOYEES.INVOICES(selectedEmployee.user_id), {
          headers
        })
      ]);

      console.log('Invoices response:', invoicesResponse.data);

      setEmployeeMetrics(metricsResponse.data);
      setEmployeeTimesheet(timesheetResponse.data);
      setEmployeeInvoices(invoicesResponse.data);
    } catch (error) {
      console.error('Error loading employee data:', error);
      console.error('Error response:', error.response?.data);
      setError('Failed to load employee data. ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const handleEmployeeSelect = (employee) => {
    setSelectedEmployee(employee);
    setActiveView('overview');
  };

  const handleDateRangeChange = (field, value) => {
    setDateRange(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleDownloadStatement = async (invoice) => {
    try {
      console.log('Downloading statement for invoice:', invoice);
      console.log('Invoice ID:', invoice.invoice_id);
      console.log('QB ID:', invoice.qb_id);
      console.log('Invoice _id:', invoice._id);

      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      // Get the invoice ID (either QB ID or _id)
      const invoiceId = invoice._id || invoice.qb_id;
      if (!invoiceId) {
        throw new Error('Invalid invoice ID');
      }

      console.log('Using invoice ID:', invoiceId);

      // Make the request to download the statement
      const response = await fetch(
        `${API_ENDPOINTS.EMPLOYEES.DOWNLOAD_INVOICE(selectedEmployee.user_id, invoiceId)}`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to download statement');
      }

      // Get the HTML content and create a blob
      const html = await response.text();
      const blob = new Blob([html], { type: 'text/html' });
      
      // Create a temporary link to download the file
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `statement_${invoiceId}.html`;
      document.body.appendChild(a);
      a.click();
      
      // Cleanup
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      console.error('Error downloading statement:', error);
      alert(error.message || 'Failed to download statement. Please try again.');
    }
  };

  const renderOverview = () => {
    if (!employeeMetrics) return null;

    return (
      <div className="employee-overview">
        <div className="metrics-grid">
          <div className="metric-card">
            <div className="metric-icon">
              <FontAwesomeIcon icon={faClock} />
            </div>
            <div className="metric-content">
              <div className="metric-value">{employeeMetrics.total_hours.toFixed(1)}</div>
              <div className="metric-label">Total Hours</div>
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-icon">
              <FontAwesomeIcon icon={faDollarSign} />
            </div>
            <div className="metric-content">
              <div className="metric-value">${employeeMetrics.total_earnings.toFixed(2)}</div>
              <div className="metric-label">Total Earnings</div>
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-icon">
              <FontAwesomeIcon icon={faUsers} />
            </div>
            <div className="metric-content">
              <div className="metric-value">{employeeMetrics.active_clients}</div>
              <div className="metric-label">Active Clients</div>
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-icon">
              <FontAwesomeIcon icon={faChartLine} />
            </div>
            <div className="metric-content">
              <div className="metric-value">{employeeMetrics.avg_hours_per_day.toFixed(1)}</div>
              <div className="metric-label">Avg Hours/Day</div>
            </div>
          </div>
        </div>

        <div className="performance-section">
          <div className="performance-card">
            <h3>Performance Metrics</h3>
            <div className="performance-details">
              <div className="performance-item">
                <span className="performance-label">Productivity Score:</span>
                <span className="performance-value">{Math.round(employeeMetrics.productivity_score)}%</span>
              </div>
              <div className="performance-item">
                <span className="performance-label">Efficiency Rating:</span>
                <span className={`performance-value rating-${employeeMetrics.efficiency_rating?.toLowerCase().replace(' ', '-')}`}>
                  {employeeMetrics.efficiency_rating}
                </span>
              </div>
              {employeeMetrics.most_worked_client && (
                <div className="performance-item">
                  <span className="performance-label">Top Client:</span>
                  <span className="performance-value">
                    {employeeMetrics.client_work_summary?.find(c => c.client_id === employeeMetrics.most_worked_client)?.client_name || 'Unknown Client'}
                  </span>
                </div>
              )}
              {employeeMetrics.most_worked_category && (
                <div className="performance-item">
                  <span className="performance-label">Top Category:</span>
                  <span className="performance-value">{employeeMetrics.most_worked_category}</span>
                </div>
              )}
            </div>
          </div>

          <div className="client-work-card">
            <h3>Client Work Summary</h3>
            <div className="client-work-list">
              {employeeMetrics.client_work_summary?.map((client, index) => (
                <div key={index} className="client-work-item">
                  <div className="client-info">
                    <div className="client-name">{client.client_name}</div>
                    <div className="client-stats">
                      {client.total_hours.toFixed(1)}h • ${client.total_earnings.toFixed(2)}
                    </div>
                  </div>
                  <div className="client-categories">
                    {Object.entries(client.categories).map(([category, hours]) => (
                      <span key={category} className="category-tag">
                        {category}: {hours.toFixed(1)}h
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  const renderTimesheet = () => {
    if (!employeeTimesheet) return null;

    return (
      <div className="employee-timesheet">
        <div className="timesheet-summary">
          <div className="summary-stats">
            <div className="stat-item">
              <span className="stat-label">Total Hours:</span>
              <span className="stat-value">{employeeTimesheet.total_hours.toFixed(1)}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Total Earnings:</span>
              <span className="stat-value">${employeeTimesheet.total_earnings.toFixed(2)}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Days Worked:</span>
              <span className="stat-value">{employeeTimesheet.days_worked}</span>
            </div>
          </div>
        </div>

        <div className="timesheet-entries">
          <h3>Time Entries</h3>
          <div className="entries-list">
            {employeeTimesheet.entries?.map((entry, index) => (
              <div key={index} className="entry-item">
                <div className="entry-date">{entry.date}</div>
                <div className="entry-client">{entry.client_name}</div>
                <div className="entry-details">
                  <span className="entry-category">{entry.category}</span>
                  <span className="entry-time">{entry.hours}h {entry.minutes}m</span>
                  <span className="entry-earnings">${entry.earnings.toFixed(2)}</span>
                </div>
                <div className="entry-description">{entry.description}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const renderInvoices = () => {
    if (!employeeInvoices) return null;

    console.log('Rendering invoices:', employeeInvoices);

    return (
      <div className="employee-invoices">
        <div className="invoices-summary">
          <div className="summary-cards">
            <div className="summary-card">
              <div className="summary-value">${employeeInvoices.total_draft.toFixed(2)}</div>
              <div className="summary-label">Draft</div>
            </div>
            <div className="summary-card">
              <div className="summary-value">${employeeInvoices.total_unpaid.toFixed(2)}</div>
              <div className="summary-label">Sent</div>
            </div>
          </div>
        </div>

        <div className="invoices-list">
          <h3>Invoice History</h3>
          {console.log('Invoices array:', employeeInvoices.invoices)}
          {employeeInvoices.invoices?.map((invoice, index) => {
            console.log('Processing invoice:', invoice);
            return (
              <div key={index} className="invoice-item">
                <div className="invoice-header">
                  <div className="invoice-id">Invoice #{invoice.qb_id || invoice._id || 'Draft'}</div>
                  <div className={`invoice-status status-${invoice.status}`}>
                    {invoice.status.toUpperCase()}
                  </div>
                </div>
                <div className="invoice-details">
                  <div className="invoice-period">
                    {invoice.start_date ? new Date(invoice.start_date).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: '2-digit',
                      day: '2-digit'
                    }) : new Date(invoice.days?.[0]?.date || Date.now()).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: '2-digit',
                      day: '2-digit'
                    })}
                  </div>
                  <div className="invoice-stats">
                    {invoice.total_hours.toFixed(1)}h • ${invoice.total_earnings.toFixed(2)}
                  </div>
                </div>
                <div className="invoice-actions">
                  <button 
                    className="download-btn"
                    onClick={() => handleDownloadStatement(invoice)}
                  >
                    <FontAwesomeIcon icon={faDownload} />
                    Download
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="lead-tracker-view">
      <div className="employee-dashboard">
        <div className="dashboard-header">
          <h1>Employee Management</h1>
          <div className="header-controls">
            <div className="date-range-controls">
              <label>
                From:
                <input
                  type="date"
                  value={dateRange.start_date}
                  onChange={(e) => handleDateRangeChange('start_date', e.target.value)}
                />
              </label>
              <label>
                To:
                <input
                  type="date"
                  value={dateRange.end_date}
                  onChange={(e) => handleDateRangeChange('end_date', e.target.value)}
                />
              </label>
            </div>
          </div>
        </div>

        <EmployeeSelector
          onEmployeeSelect={handleEmployeeSelect}
          selectedEmployee={selectedEmployee}
        />

        {selectedEmployee && (
          <>
            <div className="dashboard-navigation">
              <button
                className={`nav-btn ${activeView === 'overview' ? 'active' : ''}`}
                onClick={() => setActiveView('overview')}
              >
                <FontAwesomeIcon icon={faChartLine} />
                Overview
              </button>
              <button
                className={`nav-btn ${activeView === 'timesheet' ? 'active' : ''}`}
                onClick={() => setActiveView('timesheet')}
              >
                <FontAwesomeIcon icon={faClock} />
                Timesheet
              </button>
              <button
                className={`nav-btn ${activeView === 'invoices' ? 'active' : ''}`}
                onClick={() => setActiveView('invoices')}
              >
                <FontAwesomeIcon icon={faFileInvoice} />
                Invoices
              </button>
            </div>

            <div className="dashboard-content">
              {loading && (
                <div className="loading-state">
                  <FontAwesomeIcon icon={faSpinner} spin />
                  Loading employee data...
                </div>
              )}

              {error && (
                <div className="error-state">
                  <FontAwesomeIcon icon={faExclamationTriangle} />
                  {error}
                </div>
              )}

              {!loading && !error && (
                <>
                  {activeView === 'overview' && renderOverview()}
                  {activeView === 'timesheet' && renderTimesheet()}
                  {activeView === 'invoices' && renderInvoices()}
                </>
              )}
            </div>
          </>
        )}

        {!selectedEmployee && (
          <div className="no-employee-selected">
            <FontAwesomeIcon icon={faUser} />
            <h3>Select an Employee</h3>
            <p>Choose an employee from the dropdown above to view their dashboard</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default EmployeeDashboard; 