/**
 * @fileoverview TimeSheet component for managing time entries and invoicing.
 * Provides functionality for tracking work hours, managing client assignments,
 * and generating invoices.
 */

import React, { useState, useEffect } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';
import axios from 'axios';
import { fetchAuthSession, signOut } from 'aws-amplify/auth';
import axiosInstance from '../../utils/axiosConfig';
import { useNavigate } from 'react-router-dom';
import './TimeSheet.css';
import '../Employee/EmployeeDashboard.css';
import { format, subDays, startOfToday, endOfToday, parse, isValid } from 'date-fns';

/**
 * @constant {string[]} CATEGORIES - Predefined categories for time entries
 */
const CATEGORIES = [
  'Content Download',
  'Content Upload to Server', 
  'Content Organize',
  'Content Review',
  'Client Admin',
  'General Admin'
];

/**
 * @typedef {Object} DateRangeSelectorProps
 * @property {string} startDate - Start date in YYYY-MM-DD format
 * @property {string} endDate - End date in YYYY-MM-DD format
 * @property {Function} onStartDateChange - Callback when start date changes
 * @property {Function} onEndDateChange - Callback when end date changes
 */

/**
 * Component for selecting date ranges with quick select options.
 * 
 * @param {DateRangeSelectorProps} props - Component properties
 * @returns {React.Element} Rendered date range selector
 */
const DateRangeSelector = ({ startDate, endDate, onStartDateChange, onEndDateChange }) => {
  const handleQuickSelect = (option) => {
    const end = new Date();
    let start = new Date();
    
    switch(option) {
      case 'today':
        // Keep end as today, set start to today
        start = new Date();
        break;
      case 'yesterday':
        // Set both start and end to yesterday
        start = subDays(new Date(), 1);
        end.setTime(start.getTime());
        break;
      case '7days':
        // Set start to 7 days ago, keep end as today
        start = subDays(end, 7);
        break;
      default:
        break;
    }
    
    onStartDateChange(format(start, 'yyyy-MM-dd'));
    onEndDateChange(format(end, 'yyyy-MM-dd'));
  };

  return (
    <div className="header-controls">
      <div className="date-range-controls">
        <label>
          From:
          <div className="date-input-wrapper">
            <input
              type="date"
              value={startDate}
              onChange={(e) => onStartDateChange(e.target.value)}
            />
            <div className="quick-select-buttons">
              <button onClick={() => handleQuickSelect('today')}>Today</button>
              <button onClick={() => handleQuickSelect('yesterday')}>Yesterday</button>
              <button onClick={() => handleQuickSelect('7days')}>7 Days</button>
            </div>
          </div>
        </label>
        <label>
          To:
          <input
            type="date"
            value={endDate}
            onChange={(e) => onEndDateChange(e.target.value)}
          />
        </label>
      </div>
    </div>
  );
};

/**
 * @typedef {Object} TimeEntry
 * @property {string} date - Entry date in YYYY-MM-DD format
 * @property {string} client_id - ID of the client
 * @property {number} hours - Number of hours worked
 * @property {number} minutes - Additional minutes worked
 * @property {string} type - Type of work (e.g., 'item based')
 * @property {string} item - Specific work item identifier
 * @property {string} description - Detailed description of work
 * @property {string} category - Work category from CATEGORIES
 */

/**
 * Main TimeSheet component for managing time entries and invoicing.
 * Provides functionality for:
 * - Creating and editing time entries
 * - Searching and selecting clients
 * - Generating and submitting invoices
 * - Filtering entries by date range and invoice period
 * 
 * @returns {React.Element} Rendered TimeSheet component
 */
const TimeSheet = () => {
  const { user } = useAuth();
  const [entries, setEntries] = useState([]);
  const [clientOptions, setClientOptions] = useState([]);
  const [clientSearch, setClientSearch] = useState('');
  const [selectedClient, setSelectedClient] = useState(null);
  const [newEntry, setNewEntry] = useState({
    date: new Date().toISOString().split('T')[0],
    client_id: '',
    hours: '',
    minutes: '',
    type: 'item based',
    item: '',
    description: '',
    category: ''
  });
  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [invoicePreview, setInvoicePreview] = useState(null);
  const [isQbIdModalOpen, setIsQbIdModalOpen] = useState(false);
  const [qbId, setQbId] = useState('');
  const [invoicePeriod, setInvoicePeriod] = useState('current');
  const [invoicePeriods, setInvoicePeriods] = useState([]);
  const [editingEntry, setEditingEntry] = useState(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const navigate = useNavigate();
  const [startDate, setStartDate] = useState(format(startOfToday(), 'yyyy-MM-dd'));
  const [endDate, setEndDate] = useState(format(endOfToday(), 'yyyy-MM-dd'));

  useEffect(() => {
    document.title = "Time Sheet | Snapped";
    fetchEntries();
    
    // Cleanup - restore original title when component unmounts
    return () => {
      document.title = "SNAPPED";
    };
  }, [invoicePeriod]);

  /**
   * Fetches time entries based on the selected invoice period.
   * Formats and filters entries, and updates invoice periods list.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const fetchEntries = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axiosInstance.get(API_ENDPOINTS.TIMESHEET.ENTRIES, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      // Log the raw response to see the data structure
      console.log('Raw entries response:', response.data);

      // Make sure we have an array of entries
      const entries = Array.isArray(response.data) ? response.data : [];
      
      // Format entries and ensure we have _id
      const formattedEntries = entries.map(entry => ({
        ...entry,
        _id: entry._id || entry.id, // Try both _id and id
        earnings: parseFloat(entry.earnings || 0),
        date: new Date(entry.date),
        created_at: new Date(entry.created_at)
      }));

      console.log('Formatted entries:', formattedEntries);

      // Get unique invoice periods from submitted entries
      const periods = [...new Set(formattedEntries
        .filter(entry => entry.invoice_submitted)
        .map(entry => {
          const date = new Date(entry.date);
          return `${date.toLocaleString('en-US', { month: 'short' })} ${date.getFullYear()}`;
        })
      )].sort();
      console.log('Periods:', periods);
      
      // Always put 'current' at the top
      setInvoicePeriods(['current', ...periods]);

      // Filter entries based on selected period
      const filteredEntries = formattedEntries.filter(entry => {
        if (invoicePeriod === 'current') {
          return !entry.invoice_submitted;
        }
        
        const entryDate = new Date(entry.date);
        const entryPeriod = `${entryDate.toLocaleString('en-US', { month: 'short' })} ${entryDate.getFullYear()}`;
        console.log('Comparing:', {
          entryPeriod,
          selectedPeriod: invoicePeriod,
          isSubmitted: entry.invoice_submitted,
          matches: entry.invoice_submitted && entryPeriod === invoicePeriod
        });
        
        return entry.invoice_submitted && entryPeriod === invoicePeriod;
      });
      console.log('Filtered entries:', filteredEntries);

      setEntries(filteredEntries);
    } catch (error) {
      console.error('Error fetching entries:', error);
      if (error.response) {
        console.error('Error response:', error.response.data);
      }
    }
  };

  /**
   * Searches for clients based on input query.
   * Updates client options list with matching results.
   * 
   * @async
   * @param {string} query - Search query for client lookup
   * @returns {Promise<void>}
   */
  const handleClientSearch = async (query) => {
    if (query.length >= 2) {
      try {
        const { tokens } = await fetchAuthSession();
        const token = tokens.idToken.toString();

        const response = await axiosInstance.get(
          API_ENDPOINTS.TIMESHEET.SEARCH_ASSIGNEES(query),
          {
            headers: {
              'Authorization': `Bearer ${token}`
            }
          }
        );

        if (response.data?.assignees) {
          setClientOptions(response.data.assignees.map(client => ({
            id: client.id,
            name: client.name,
            email: client.email,
            stage_name: client.stage_name
          })));
        }
      } catch (error) {
        console.error('Error searching clients:', error);
        setClientOptions([]);
      }
    } else {
      setClientOptions([]);
    }
  };

  /**
   * Handles client selection from search results.
   * Updates selected client and entry form state.
   * 
   * @param {Object} client - Selected client object
   * @param {string} client.id - Client ID
   * @param {string} client.name - Client name
   * @param {string} client.email - Client email
   * @param {string} client.stage_name - Client stage name
   */
  const handleClientSelect = (client) => {
    setSelectedClient(client);
    setNewEntry(prev => ({
      ...prev,
      client_id: client.id
    }));
    setClientOptions([]);
    setClientSearch('');
  };

  /**
   * Handles submission of new time entry.
   * Validates required fields and creates entry in database.
   * 
   * @async
   * @param {Event} e - Form submission event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    // If already submitting, prevent multiple submissions
    if (isSubmitting) return;
    
    // Validate all required fields
    if (!newEntry.date) {
      alert('Please select a date');
      return;
    }
    
    if (!selectedClient || !newEntry.client_id) {
      alert('Please select a client');
      return;
    }
    
    if (!newEntry.hours && !newEntry.minutes) {
      alert('Please enter either hours or minutes (or both)');
      return;
    }
    
    if (!newEntry.item.trim()) {
      alert('Please enter a work item');
      return;
    }
    
    if (!newEntry.category) {
      alert('Please select a category');
      return;
    }
    
    if (!newEntry.description.trim()) {
      alert('Please enter a description');
      return;
    }
    
    try {
      setIsSubmitting(true);
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axiosInstance.post(
        API_ENDPOINTS.TIMESHEET.ENTRIES,
        {
          ...newEntry,
          type: 'item based',
          hours: parseInt(newEntry.hours) || 0,
          minutes: parseInt(newEntry.minutes) || 0
        },
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      if (response.data.status === 'success') {
        fetchEntries();
        setNewEntry({
          date: new Date().toISOString().split('T')[0],
          client_id: '',
          hours: '',
          minutes: '',
          type: 'item based',
          item: '',
          description: '',
          category: ''
        });
        setSelectedClient(null);
        setClientSearch('');
      }
    } catch (error) {
      console.error('Error submitting entry:', error);
    } finally {
      setIsSubmitting(false);
    }
  };

  /**
   * Generates invoice preview for selected entries.
   * Groups entries by client and calculates totals.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleGenerateInvoice = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axiosInstance.get(
        API_ENDPOINTS.TIMESHEET.PREVIEW_INVOICE,
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      setInvoicePreview(response.data);
      setIsPreviewOpen(true);
    } catch (error) {
      console.error('Error generating invoice preview:', error);
      if (error.response) {
        console.error('Error response:', error.response.data);
      }
      alert('Failed to generate invoice preview. Please try again.');
    }
  };

  /**
   * Handles QuickBooks ID submission for invoice.
   * Updates invoice with QB reference ID.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleQbIdSubmit = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axiosInstance.post(
        `${API_ENDPOINTS.TIMESHEET.SET_QB_ID}?qb_id=${qbId}`,
        {},  // Empty body
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
      if (response.status === 200) {
        setIsQbIdModalOpen(false);
        handleSubmitInvoice();
      }
    } catch (error) {
      console.error('Error setting QB ID:', error);
    }
  };

  /**
   * Submits finalized invoice to the system.
   * Updates entry statuses and closes preview.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleSubmitInvoice = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await axiosInstance.post(
        API_ENDPOINTS.TIMESHEET.SUBMIT_INVOICE,
        {},
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
      if (response.status === 200) {
        setIsPreviewOpen(false);
        setInvoicePeriod('current');  // Switch back to current period
        fetchEntries();  // Refresh entries
      }
    } catch (error) {
      if (error.response?.status === 400 && 
          error.response?.data?.detail?.includes('QuickBooks vendor ID not set')) {
        setIsQbIdModalOpen(true);
      } else {
        console.error('Error submitting invoice:', error);
      }
    }
  };

  /**
   * Handles user sign out.
   * Clears session and redirects to login.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleSignOut = async () => {
    try {
      await signOut();
      navigate('/timesheet-login');
    } catch (error) {
      console.error('Error signing out:', error);
    }
  };

  const handleEditEntry = async (e) => {
    e.preventDefault();
    
    // Prevent multiple submissions
    if (isSubmitting) return;
    
    console.log('Editing entry:', editingEntry);
    try {
      setIsSubmitting(true);
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      // Check if we have a valid item
      if (!editingEntry.item) {
        console.error('No entry item found:', editingEntry);
        return;
      }

      const response = await axiosInstance.put(
        API_ENDPOINTS.TIMESHEET.UPDATE_ENTRY(editingEntry.item),
        {
          date: editingEntry.date,
          client_id: editingEntry.client_id,
          hours: parseInt(editingEntry.hours) || 0,
          minutes: parseInt(editingEntry.minutes) || 0,
          type: editingEntry.type || 'item based',
          item: editingEntry.item,
          description: editingEntry.description,
          category: editingEntry.category
        },
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );

      if (response.data.status === 'success') {
        setIsEditModalOpen(false);
        setEditingEntry(null);
        fetchEntries();
      }
    } catch (error) {
      console.error('Error updating entry:', error);
      console.error('Entry data:', editingEntry);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteEntry = async (entryItem) => {
    if (isDeleting) return; // Prevent multiple deletion requests
    
    // Confirm before deleting
    if (!window.confirm('Are you sure you want to delete this time entry? This action cannot be undone.')) {
      return;
    }
    
    try {
      setIsDeleting(true);
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      
      const response = await axiosInstance.delete(
        API_ENDPOINTS.TIMESHEET.DELETE_ENTRY(entryItem),
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      );
      
      if (response.data.status === 'success') {
        // Update UI - remove the deleted entry
        setEntries(entries.filter(entry => entry.item !== entryItem));
        
        // Refetch entries to ensure data consistency
        fetchEntries();
      }
    } catch (error) {
      console.error('Error deleting entry:', error);
      alert('Failed to delete entry. Please try again.');
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <div className="timesheet-container">
      <button 
        onClick={handleSignOut}
        className="timesheet-logout-button"
        style={{
          position: 'absolute',
          top: '10px',
          right: '10px',
          padding: '6px 14px',
          background: 'none',
          border: '1px solid rgb(26 26 26)',
          borderRadius: '4px',
          color: 'rgb(26 26 26)',
          cursor: 'pointer',
          fontSize: '12px',
          zIndex: 1000
        }}
      >
        Sign Out
      </button>
      <h1 className="timesheet-title">Time Tracking</h1>

      <DateRangeSelector
        startDate={startDate}
        endDate={endDate}
        onStartDateChange={setStartDate}
        onEndDateChange={setEndDate}
      />
      
      <form onSubmit={handleSubmit} className="entry-form">
        <div className="form-grid">
          <div className="form-group-timesheet">
            <label className="form-label">Date</label>
            <input
              type="date"
              value={newEntry.date}
              onChange={(e) => setNewEntry({...newEntry, date: e.target.value})}
              className="form-input"
              required
            />
          </div>
          
          <div className="form-group-timesheet">
            <label className="form-label">Client *</label>
            <div className="client-search-container">
              <input
                type="text"
                value={clientSearch}
                onChange={(e) => {
                  setClientSearch(e.target.value);
                  handleClientSearch(e.target.value);
                }}
                className="form-input"
                placeholder="Search for a client..."
              />
              {clientOptions.length > 0 && (
                <ul className="client-options">
                  {clientOptions.map(client => (
                    <li
                      key={client.id}
                      onClick={() => handleClientSelect(client)}
                      className="client-option"
                    >
                      {client.name}
                    </li>
                  ))}
                </ul>
              )}
              {selectedClient && (
                <div className="selected-client">
                  Selected: {selectedClient.name}
                </div>
              )}
              {!selectedClient && (
                <div className="client-requirement-note">
                  * Please search and select a client from the dropdown
                </div>
              )}
            </div>
          </div>

          <div className="form-group-timesheet">
            <label className="form-label">Hours *</label>
            <input
              type="number"
              value={newEntry.hours}
              onChange={(e) => setNewEntry({...newEntry, hours: e.target.value})}
              className="form-input"
              min="0"
              placeholder="Enter hours"
            />
          </div>

          <div className="form-group-timesheet">
            <label className="form-label">Minutes</label>
            <input
              type="number"
              value={newEntry.minutes}
              onChange={(e) => setNewEntry({...newEntry, minutes: e.target.value})}
              className="form-input"
              min="0"
              max="59"
              placeholder="Enter minutes"
            />
            <small className="field-note">* At least hours or minutes required</small>
          </div>

          <div className="form-group-timesheet">
            <label className="form-label">Item *</label>
            <input
              type="text"
              value={newEntry.item}
              onChange={(e) => setNewEntry({...newEntry, item: e.target.value})}
              className="form-input"
              placeholder="Enter work item"
              required
            />
          </div>

          <div className="form-group-timesheet">
            <label className="form-label">Category *</label>
            <select
              value={newEntry.category}
              onChange={(e) => setNewEntry({...newEntry, category: e.target.value})}
              className="form-select"
              required
            >
              <option value="">Select a category</option>
              {CATEGORIES.map(category => (
                <option key={category} value={category}>
                  {category}
                </option>
              ))}
            </select>
          </div>

          <div className="form-group-timesheet full-width">
            <label className="form-label">Description *</label>
            <textarea
              value={newEntry.description}
              onChange={(e) => setNewEntry({...newEntry, description: e.target.value})}
              className="form-textarea"
              placeholder="Describe the work done..."
              rows="3"
              required
            />
          </div>
        </div>

        <button 
          type="submit" 
          className="submit-button"
          disabled={isSubmitting}
        >
          {isSubmitting ? 'Adding...' : 'Add Entry'}
        </button>
      </form>

      <div className="table-container">
        <div className="table-controls">
          <select 
            value={invoicePeriod}
            onChange={(e) => setInvoicePeriod(e.target.value)}
            className="period-filter"
          >
            {invoicePeriods.map(period => (
              <option key={period} value={period}>
                {period === 'current' ? 'Current Period' : period}
              </option>
            ))}
          </select>
        </div>
        <table className="entries-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Client</th>
              <th>Category</th>
              <th>Time</th>
              <th>Item</th>
              <th>Description</th>
              <th>Earned</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(entry => (
              <tr key={entry._id}>
                <td>{new Date(entry.date).toLocaleDateString()}</td>
                <td>{entry.client_name || entry.client_id}</td>
                <td>{entry.category}</td>
                <td>{`${entry.hours}h ${entry.minutes}m`}</td>
                <td>{entry.item}</td>
                <td>{entry.description}</td>
                <td>${entry.earnings.toFixed(2)}</td>
                <td>
                  <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                    <span className={`status-badge status-${entry.status}`}>
                      {entry.status}
                    </span>
                    {!entry.invoice_submitted && (
                      <>
                        <button
                          onClick={() => {
                            console.log('Entry to edit:', entry);
                            setEditingEntry(entry);
                            setIsEditModalOpen(true);
                          }}
                          className="edit-button"
                        >
                          Edit
                        </button>
                        <button
                          onClick={() => handleDeleteEntry(entry.item)}
                          className="delete-button"
                          disabled={isDeleting}
                        >
                          Delete
                        </button>
                      </>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr>
              <td colSpan="5" className="text-right"><strong>Total Earnings:</strong></td>
              <td colSpan="2">
                <strong>
                  ${entries.reduce((sum, entry) => sum + entry.earnings, 0).toFixed(2)}
                </strong>
              </td>
            </tr>
          </tfoot>
        </table>
      </div>

      {isPreviewOpen && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h2>Invoice Preview</h2>
              <button 
                className="tsheet-close-button"
                onClick={() => setIsPreviewOpen(false)}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              {invoicePreview && (
                <div className="invoice-preview">
                  <div className="preview-header">
                    <h3>Employee: {invoicePreview.employee.name}</h3>
                    <p>Rate: ${invoicePreview.employee.rate}/hour</p>
                    <p>Start Date: {new Date(invoicePreview.start_date).toLocaleDateString()}</p>
                  </div>

                  <div className="preview-entries">
                    {invoicePreview.days.map((day, dayIndex) => (
                      <div key={dayIndex} className="preview-day">
                        <h4>{new Date(day.date).toLocaleDateString()}</h4>
                        <table className="preview-table">
                          <thead>
                            <tr>
                              <th>Client</th>
                              <th>Category</th>
                              <th>Item</th>
                              <th>Description</th>
                              <th>Time</th>
                              <th>Amount</th>
                            </tr>
                          </thead>
                          <tbody>
                            {day.entries.map((entry, entryIndex) => (
                              <tr key={entryIndex}>
                                <td>{entry.client_name}</td>
                                <td>{entry.category}</td>
                                <td>{entry.item}</td>
                                <td>{entry.description}</td>
                                <td>{`${entry.hours}h ${entry.minutes}m`}</td>
                                <td>${entry.earnings.toFixed(2)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ))}
                  </div>

                  <div className="preview-totals">
                    <h4>Invoice Totals</h4>
                    <p>Total Hours: {invoicePreview.totals.hours.toFixed(2)}</p>
                    <p>Total Earnings: ${invoicePreview.totals.earnings.toFixed(2)}</p>
                  </div>
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button 
                className="submit-invoice-button"
                onClick={handleSubmitInvoice}
              >
                Submit Invoice
              </button>
              <button 
                className="cancel-button"
                onClick={() => setIsPreviewOpen(false)}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {isQbIdModalOpen && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h2>QuickBooks Vendor ID Required</h2>
            </div>
            <div className="modal-body">
              <p>Please enter your QuickBooks vendor ID. You can get this from your manager.</p>
              <input
                type="text"
                value={qbId}
                onChange={(e) => setQbId(e.target.value)}
                placeholder="Enter QuickBooks ID"
                className="qb-id-input"
              />
            </div>
            <div className="modal-footer">
              <button 
                className="submit-invoice-button"
                onClick={handleQbIdSubmit}
              >
                Save & Submit
              </button>
              <button 
                className="cancel-button"
                onClick={() => setIsQbIdModalOpen(false)}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {isEditModalOpen && (
        <div className="modal-overlay">
          <div className="modal-content">
            <div className="modal-header">
              <h2>Edit Time Entry</h2>
              <button 
                className="tsheet-close-button"
                onClick={() => {
                  setIsEditModalOpen(false);
                  setEditingEntry(null);
                }}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <form onSubmit={handleEditEntry} className="edit-entry-form">
                <div className="form-grid">
                  <div className="form-group-timesheet">
                    <label className="form-label">Date</label>
                    <input
                      type="date"
                      value={editingEntry ? new Date(editingEntry.date).toISOString().split('T')[0] : ''}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        date: e.target.value
                      })}
                      className="form-input"
                      required
                    />
                  </div>

                  <div className="form-group-timesheet">
                    <label className="form-label">Hours</label>
                    <input
                      type="number"
                      value={editingEntry.hours}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        hours: e.target.value
                      })}
                      className="form-input"
                      min="0"
                      required
                    />
                  </div>

                  <div className="form-group-timesheet">
                    <label className="form-label">Minutes</label>
                    <input
                      type="number"
                      value={editingEntry.minutes}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        minutes: e.target.value
                      })}
                      className="form-input"
                      min="0"
                      max="59"
                    />
                  </div>

                  <div className="form-group-timesheet">
                    <label className="form-label">Category</label>
                    <select
                      value={editingEntry.category}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        category: e.target.value
                      })}
                      className="form-select"
                      required
                    >
                      <option value="">Select a category</option>
                      {CATEGORIES.map(category => (
                        <option key={category} value={category}>
                          {category}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="form-group-timesheet">
                    <label className="form-label">Item</label>
                    <input
                      type="text"
                      value={editingEntry.item}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        item: e.target.value
                      })}
                      className="form-input"
                      required
                    />
                  </div>

                  <div className="form-group-timesheet full-width">
                    <label className="form-label">Description</label>
                    <textarea
                      value={editingEntry.description}
                      onChange={(e) => setEditingEntry({
                        ...editingEntry,
                        description: e.target.value
                      })}
                      className="form-textarea"
                      rows="3"
                      required
                    />
                  </div>
                </div>

                <div className="modal-footer">
                  <button 
                    type="submit" 
                    className="submit-button"
                    disabled={isSubmitting}
                  >
                    {isSubmitting ? 'Saving...' : 'Save Changes'}
                  </button>
                  <button 
                    type="button" 
                    className="cancel-button"
                    onClick={() => {
                      setIsEditModalOpen(false);
                      setEditingEntry(null);
                    }}
                  >
                    Cancel
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      )}

      <button
        onClick={handleGenerateInvoice}
        className="generate-invoice-button"
      >
        Generate Invoice
      </button>
    </div>
  );
};

export default TimeSheet; 