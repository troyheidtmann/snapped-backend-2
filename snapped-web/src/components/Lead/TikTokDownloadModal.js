/**
 * @fileoverview TikTok Download Modal component for downloading TikTok videos.
 * Provides functionality for downloading and tracking progress of TikTok video downloads.
 */

import React, { useState } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSpinner } from '@fortawesome/free-solid-svg-icons';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import './styles/TikTokDownloadModal.css';

/**
 * @typedef {Object} TikTokDownloadModalProps
 * @property {boolean} isOpen - Whether the modal is open
 * @property {Function} onClose - Callback function to close the modal
 * @property {string} clientId - ID of the client
 * @property {Function} onDownload - Callback function called after successful download
 */

/**
 * @typedef {Object} DownloadProgress
 * @property {number} percentage - Download progress percentage
 * @property {number} speed - Download speed in bytes per second
 * @property {number} transferred - Amount of data transferred in bytes
 * @property {number} eta - Estimated time remaining in seconds
 */

/**
 * TikTok Download Modal component for managing video downloads.
 * Features include:
 * - URL input and validation
 * - Download progress tracking
 * - Speed and ETA calculation
 * 
 * @param {TikTokDownloadModalProps} props - Component props
 * @returns {React.ReactElement|null} The rendered modal or null if not open
 */
const TikTokDownloadModal = ({ isOpen, onClose, clientId, onDownload }) => {
  const [tiktokUrl, setTiktokUrl] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [progress, setProgress] = useState({
    current: 0,
    total: 0,
    currentFile: '',
    status: ''
  });

  /**
   * Handles form submission and initiates download
   * @async
   * @param {React.FormEvent} e - Form event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);
    setProgress({ current: 0, total: 0, currentFile: '', status: 'Starting download...' });

    let eventSource;
    try {
      // Set up SSE for progress updates
      eventSource = new EventSource(`/api/tiktok/progress/${clientId}`);
      
      eventSource.onmessage = (event) => {
        try {
          // Remove the "data: " prefix if it exists
          const rawData = event.data.startsWith('data: ') ? event.data.slice(6) : event.data;
          const data = JSON.parse(rawData);
          
          // Log progress updates for debugging
          console.log('Progress update:', data);
          
          setProgress(data);
        } catch (err) {
          console.error('Error parsing progress data:', err, 'Raw data:', event.data);
        }
      };

      eventSource.onerror = (error) => {
        console.error('EventSource error:', error);
        eventSource.close();
      };

      // Start the download
      const response = await onDownload(tiktokUrl);
      
      if (response && response.status === 'success') {
        setProgress(prev => ({ 
          ...prev, 
          status: 'Complete!',
          current: response.total_videos || prev.current,
          total: response.total_videos || prev.total,
          download_progress: 100
        }));
        setTimeout(() => onClose(), 1000);
      } else {
        throw new Error('Download failed');
      }
    } catch (err) {
      console.error('Download error:', err);
      setError(err.message || 'Failed to download TikToks');
    } finally {
      if (eventSource) {
        eventSource.close();
      }
      setIsLoading(false);
    }
  };

  /**
   * Calculates the current download progress percentage
   * @returns {number} Progress percentage
   */
  const getProgressPercentage = () => {
    // Use download_progress if available for individual video progress
    if (progress.download_progress !== undefined) {
      return Math.round(progress.download_progress);
    }
    // Fallback to overall progress based on video count
    if (!progress.total) return 0;
    return Math.round((progress.current / progress.total) * 100);
  };

  /**
   * Formats bytes into human-readable string
   * @param {number} bytes - Number of bytes
   * @returns {string} Formatted size string
   */
  const formatBytes = (bytes) => {
    if (!bytes) return '0 B';
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${sizes[i]}`;
  };

  /**
   * Formats download speed into human-readable string
   * @param {number} bytesPerSec - Bytes per second
   * @returns {string} Formatted speed string
   */
  const formatSpeed = (bytesPerSec) => {
    if (!bytesPerSec) return '';
    return `${formatBytes(bytesPerSec)}/s`;
  };

  /**
   * Formats time in seconds into human-readable string
   * @param {number} seconds - Number of seconds
   * @returns {string} Formatted time string
   */
  const formatTime = (seconds) => {
    if (!seconds) return '';
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.round(seconds % 60);
    return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay">
      <div className="tiktok-download-modal">
        <div className="modal-header">
          <h2>Download TikToks</h2>
          <button className="close-modal" onClick={onClose}>&times;</button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="form-content">
            <div className="input-group">
              <label>TikTok Profile URL</label>
              <input
                type="text"
                value={tiktokUrl}
                onChange={(e) => setTiktokUrl(e.target.value)}
                placeholder="https://tiktok.com/@username"
                required
                disabled={isLoading}
              />
            </div>

            {isLoading && (
              <div className="progress-container">
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ width: `${getProgressPercentage()}%` }}
                  />
                </div>
                <div className="progress-text">
                  <div className="status-text">{progress.status}</div>
                  {progress.currentFile && (
                    <div className="current-file">
                      {progress.currentFile}
                    </div>
                  )}
                  {progress.downloaded_bytes !== undefined && progress.total_bytes > 0 && (
                    <div className="download-stats">
                      {formatBytes(progress.downloaded_bytes)} of {formatBytes(progress.total_bytes)}
                      {progress.speed > 0 && ` • ${formatSpeed(progress.speed)}`}
                      {progress.eta > 0 && ` • ${formatTime(progress.eta)} remaining`}
                    </div>
                  )}
                </div>
              </div>
            )}

            {error && <div className="error-message">{error}</div>}
          </div>

          <div className="modal-footer">
            <button 
              type="button" 
              className="cancel-button" 
              onClick={onClose}
              disabled={isLoading}
            >
              Cancel
            </button>
            <button 
              type="submit" 
              className="download-button-sign"
              disabled={isLoading || !tiktokUrl}
            >
              {isLoading ? (
                <span>
                  <FontAwesomeIcon icon={faSpinner} spin /> Downloading...
                </span>
              ) : (
                'Download'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default TikTokDownloadModal; 