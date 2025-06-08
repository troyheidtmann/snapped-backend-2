/**
 * @fileoverview Date range selection component that allows users to pick start and end dates
 * for analytics data filtering with proper validation and constraints.
 */

import React from 'react';
import './DateRangePicker.css';

/**
 * @typedef {Object} DateRange
 * @property {Date} start - Start date of the range
 * @property {Date} end - End date of the range
 */

/**
 * @typedef {Object} DateRangePickerProps
 * @property {Date} startDate - Currently selected start date
 * @property {Date} endDate - Currently selected end date
 * @property {(range: DateRange) => void} onChange - Callback when dates change
 */

/**
 * Component for selecting a date range with validation and constraints.
 * Features:
 * - Start/end date validation
 * - Max date limited to current date
 * - Min date for end based on start date
 * - ISO date format handling
 * 
 * @param {DateRangePickerProps} props - Component properties
 * @returns {React.ReactElement} The date range picker interface
 */
const DateRangePicker = ({ startDate, endDate, onChange }) => {
  return (
    <div className="date-range-picker">
      <div className="date-picker-wrapper">
        <label>From:</label>
        <input
          type="date"
          value={startDate.toISOString().split('T')[0]}
          onChange={(e) => onChange({ 
            start: new Date(e.target.value), 
            end: endDate 
          })}
          max={new Date().toISOString().split('T')[0]}
        />
      </div>
      <div className="date-picker-wrapper">
        <label>To:</label>
        <input
          type="date"
          value={endDate.toISOString().split('T')[0]}
          onChange={(e) => onChange({ 
            start: startDate, 
            end: new Date(e.target.value) 
          })}
          min={startDate.toISOString().split('T')[0]}
          max={new Date().toISOString().split('T')[0]}
        />
      </div>
    </div>
  );
};

export default DateRangePicker; 