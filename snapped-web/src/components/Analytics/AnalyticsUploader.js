/**
 * @fileoverview Component for uploading analytics data files with real-time status updates
 * and error handling. Supports multiple file uploads and displays upload results.
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import './AnalyticsUploader.css';

/**
 * @typedef {Object} UploadStatus
 * @property {string} filename - Name of the uploaded file
 * @property {'success' | 'error'} status - Upload status
 * @property {string} message - Status message or error details
 */

/**
 * Analytics file upload component with status tracking and error handling.
 * Features:
 * - Multiple file upload support
 * - Real-time upload status updates
 * - Error handling and display
 * - CSV file type validation
 * 
 * @returns {React.ReactElement} File upload interface with status display
 */
const AnalyticsUploader = () => {
  const [uploadStatus, setUploadStatus] = useState([]);
  const [isUploading, setIsUploading] = useState(false);

  useEffect(() => {
    document.title = 'Analytics Upload | SNAPPED';
    
    // Cleanup - restore original title when component unmounts
    return () => {
      document.title = 'SNAPPED';
    };
  }, []);

  const handleFileChange = async (event) => {
    const files = Array.from(event.target.files);
    setIsUploading(true);
    const statusUpdates = [];

    for (const file of files) {
      try {
        const formData = new FormData();
        formData.append('files', file);

        const response = await axios.post(API_ENDPOINTS.ANALYTICS.UPLOAD_SNAPCHAT, formData);

        statusUpdates.push({
          filename: file.name,
          status: 'success',
          message: response.data.message
        });
      } catch (error) {
        statusUpdates.push({
          filename: file.name,
          status: 'error',
          message: error.response?.data?.detail || 'Upload failed'
        });
      }
    }

    setUploadStatus(prev => [...prev, ...statusUpdates]);
    setIsUploading(false);
  };

  return (
    <div className="analytics-uploader">
      <div className="upload-container">
        <input
          type="file"
          accept=".csv"
          multiple
          onChange={handleFileChange}
          className="file-input"
        />

        {isUploading && (
          <div className="upload-status">
            <p>Uploading files...</p>
          </div>
        )}

        {uploadStatus.length > 0 && (
          <div className="upload-results">
            <h3>Upload Results:</h3>
            {uploadStatus.map((status, index) => (
              <div 
                key={index} 
                className={`status-item ${status.status}`}
              >
                <p>{status.filename}: {status.message}</p>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default AnalyticsUploader; 