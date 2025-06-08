import React, { useState } from 'react';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import axios from 'axios';

const AnalyticsPage = () => {
  const [fromDate, setFromDate] = useState(new Date());
  const [toDate, setToDate] = useState(new Date());

  const formatDateForApi = (date) => {
    return date.toLocaleDateString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    }).replace(/\//g, '-');  // Convert 01/06/2025 to 01-06-2025
  };

  return (
    <div>
      {/* Update date picker format */}
      <DatePicker
        selected={fromDate}
        onChange={date => setFromDate(date)}
        dateFormat="MM-dd-yyyy"  // This matches MongoDB format
        className="date-picker"
      />
      <DatePicker
        selected={toDate}
        onChange={date => setToDate(date)}
        dateFormat="MM-dd-yyyy"  // This matches MongoDB format
        className="date-picker"
      />
    </div>
  );
};

export default AnalyticsPage; 