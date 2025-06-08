/**
 * @fileoverview Desktop upload component for handling file uploads to the CDN.
 * Provides a drag-and-drop interface with user and folder selection.
 */

import React, { useState, useCallback, useEffect } from 'react';
import { useDropzone } from 'react-dropzone';
import { useAuth } from '../../contexts/AuthContext';
import axios from 'axios';
import './DesktopUpload.css';
import { API_ENDPOINTS } from '../../config/api';
import { fetchAuthSession } from 'aws-amplify/auth';
import { toast } from 'react-hot-toast';

/**
 * @typedef {Object} FileInfo
 * @property {File} file - The actual file object
 * @property {string} id - Unique identifier for the file
 * @property {string} status - Current status of the file ('pending'|'uploading'|'completed'|'failed')
 * @property {string} [error] - Error message if upload failed
 */

/**
 * @typedef {Object} UploadProgress
 * @property {number} [fileId] - Upload progress percentage for each file
 */

// Add axios interceptor for authentication if not already added
if (!axios.interceptors.request.handlers.some(handler => 
    handler.fulfilled.toString().includes('fetchAuthSession'))) {
  axios.interceptors.request.use(async (config) => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      config.headers.Authorization = `Bearer ${token}`;
    } catch (error) {
      console.error('Error getting auth token:', error);
    }
    return config;
  });
}

/**
 * Component for handling desktop file uploads to the CDN.
 * Features:
 * - Drag and drop file upload
 * - User and folder selection
 * - Multi-file upload support
 * - Upload progress tracking
 * - Error handling and feedback
 * - Stories date selection
 * - File validation
 * 
 * @returns {React.ReactElement} The desktop upload interface
 */
const DesktopUpload = () => {
  const { getAccessToken } = useAuth();
  const [files, setFiles] = useState([]);
  const [uploadProgress, setUploadProgress] = useState({});
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);
  const [showSuccess, setShowSuccess] = useState(false);
  const [users, setUsers] = useState([]);
  const [selectedUser, setSelectedUser] = useState('');
  const [userFolders, setUserFolders] = useState([]);
  const [selectedFolder, setSelectedFolder] = useState('');
  const [isLoadingUsers, setIsLoadingUsers] = useState(false);
  const [isLoadingFolders, setIsLoadingFolders] = useState(false);
  const [permissionError, setPermissionError] = useState(null);
  const [selectedDate, setSelectedDate] = useState('');
  const [availableDates, setAvailableDates] = useState([]);

  // Fetch users on component mount
  useEffect(() => {
    const fetchUsers = async () => {
      try {
        setIsLoadingUsers(true);
        setPermissionError(null);
        
        const response = await axios.get(API_ENDPOINTS.DESKTOP_UPLOAD.USERS);
        
        if (response.data.status === 'success') {
          setUsers(response.data.users);
          if (response.data.users.length === 0) {
            setPermissionError("You don't have access to any users. Please contact an administrator.");
          }
        }
      } catch (error) {
        console.error('Error fetching users:', error);
        if (error.response?.status === 403) {
          setPermissionError("You don't have permission to access this feature.");
        } else {
          setError('Failed to load users');
        }
      } finally {
        setIsLoadingUsers(false);
      }
    };
    fetchUsers();
  }, []);

  // Fetch folders when user is selected
  useEffect(() => {
    const fetchFolders = async () => {
      if (!selectedUser) return;
      
      try {
        setIsLoadingFolders(true);
        setPermissionError(null);
        
        const response = await axios.get(
          API_ENDPOINTS.DESKTOP_UPLOAD.FOLDERS(selectedUser)
        );
        
        if (response.data.status === 'success') {
          setUserFolders(response.data.folders);
          // Select first folder by default if available
          if (response.data.folders.length > 0) {
            setSelectedFolder(response.data.folders[0].path);
          }
        }
      } catch (error) {
        console.error('Error fetching folders:', error);
        if (error.response?.status === 403) {
          setPermissionError(`You don't have permission to access folders for ${selectedUser}`);
          setUserFolders([]);
          setSelectedFolder('');
        } else {
          setError('Failed to load folders');
        }
      } finally {
        setIsLoadingFolders(false);
      }
    };
    fetchFolders();
  }, [selectedUser]);

  // Add function to calculate available dates
  const calculateAvailableDates = useCallback(() => {
    // Get current date in local timezone, without time component
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    
    // Generate dates starting from today
    const dates = [];
    for (let i = 0; i < 6; i++) {
      const date = new Date(today);
      date.setDate(today.getDate() + i);
      // Format as YYYY-MM-DD
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      dates.push(`${year}-${month}-${day}`);
    }
    
    setAvailableDates(dates);
    if (dates.length > 0) {
      setSelectedDate(dates[0]); // Select first available date by default
    }
  }, []);

  // Add useEffect to calculate dates when Stories is selected
  useEffect(() => {
    if (selectedFolder?.includes('/STO/')) {
        calculateAvailableDates();
    } else {
        setSelectedDate('');
    }
  }, [selectedFolder, calculateAvailableDates]);

  const onDrop = useCallback((acceptedFiles) => {
    setError(null);
    setShowSuccess(false);
    setPermissionError(null);

    // Check if adding these files would exceed the limit
    const totalFiles = files.length + acceptedFiles.length;
    if (totalFiles > 200) {
      setError("Maximum 200 files can be uploaded at once");
      return;
    }

    const newFiles = acceptedFiles.map(file => ({
      file,
      id: `file_${Date.now()}_${Math.random().toString(36).substring(7)}`,
      status: 'pending'
    }));
    setFiles(prev => [...prev, ...newFiles]);
  }, [files]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: true,
    maxFiles: 200,
    onDropRejected: (rejectedFiles) => {
      if (rejectedFiles.length > 0) {
        setError("Maximum 200 files can be uploaded at once");
      }
    }
  });

  const FileItem = ({ fileInfo }) => (
    <div 
      key={fileInfo.id} 
      className={`desktop-file-item ${fileInfo.status}`}
    >
      <div className="desktop-file-info">
        <span>
          {fileInfo.file.name} ({(fileInfo.file.size / (1024 * 1024)).toFixed(2)} MB)
          {fileInfo.status === 'failed' && ` - Failed${fileInfo.error ? `: ${fileInfo.error}` : ''}`}
          {fileInfo.status === 'verifying' && ' - Verifying...'}
          {fileInfo.status === 'completed' && ' ✓'}
        </span>
        {(uploading || fileInfo.status === 'verifying') && (
          <span className="desktop-file-progress">
            {uploadProgress[fileInfo.id] || 0}%
            {fileInfo.status === 'verifying' && ' (Verifying)'}
            {fileInfo.status === 'completed' && ' ✓'}
          </span>
        )}
      </div>
      
      {!uploading && fileInfo.status !== 'completed' && fileInfo.status !== 'verifying' && (
        <button 
          className="desktop-remove-button"
          onClick={() => removeFile(fileInfo.id)}
        >
          ✕
        </button>
      )}
    </div>
  );

  const uploadFile = async (fileInfo) => {
    // Don't retry already completed files
    if (fileInfo.status === 'completed') {
      return;
    }

    const formData = new FormData();
    formData.append('files', fileInfo.file);
    formData.append('user_id', selectedUser);
    formData.append('folder_path', selectedFolder);
    
    // Add date for Stories uploads
    if (selectedFolder?.includes('/STO/')) {
        formData.append('date', selectedDate);
    }

    try {
      setFiles(prev => prev.map(f => 
        f.id === fileInfo.id ? { ...f, status: 'uploading' } : f
      ));

      const response = await axios.post(
        API_ENDPOINTS.DESKTOP_UPLOAD.UPLOAD,
        formData,
        {
          headers: {
            'Content-Type': 'multipart/form-data'
          },
          onUploadProgress: (progressEvent) => {
            const percentCompleted = Math.round((progressEvent.loaded * 100) / progressEvent.total);
            setUploadProgress(prev => ({
              ...prev,
              [fileInfo.id]: percentCompleted
            }));
          },
          timeout: 1800000
        }
      );

      // If we get here, the upload was successful (S3 verified it)
      setFiles(prev => prev.map(f => 
        f.id === fileInfo.id ? { ...f, status: 'completed' } : f
      ));
      return { success: true, fileId: fileInfo.id };
      
    } catch (error) {
      if (error.response?.status === 403) {
        // Handle permission error
        setPermissionError(`You don't have permission to upload files for ${selectedUser}`);
        throw error;
      }
      // Set failed status with error message
      setFiles(prev => prev.map(f => 
        f.id === fileInfo.id ? { 
          ...f, 
          status: 'failed', 
          error: error.response?.data?.detail || error.message || 'Upload failed' 
        } : f
      ));
      return { success: false, fileId: fileInfo.id, error };
    }
  };

  const handleUpload = useCallback(async () => {
    if (files.length === 0) return;
    
    // Check if all necessary data is available
    if (!selectedUser || !selectedFolder) {
      setError("Please select both a user and a folder");
      return;
    }
    
    setUploading(true);
    setError(null);
    setShowSuccess(false);
    setPermissionError(null);
    setUploadProgress({});

    try {
      // Only process files that are pending or failed
      const pendingFiles = files.filter(f => f.status === 'pending' || f.status === 'failed');
      const results = {
        success: [],
        failed: []
      };

      // Upload files in this batch concurrently
      const uploadPromises = pendingFiles.map(fileInfo => uploadFile(fileInfo));
      const batchResults = await Promise.allSettled(uploadPromises);
      
      // Process results for this batch
      batchResults.forEach((result, index) => {
        const fileInfo = pendingFiles[index];
        if (result.status === 'fulfilled' && result.value?.success) {
          results.success.push(result.value.fileId);
        } else if (result.status === 'fulfilled' && !result.value?.success) {
          results.failed.push({
            fileId: result.value.fileId,
            error: result.value.error?.message || 'Upload failed'
          });
        } else {
          // Promise rejected
          results.failed.push({
            fileId: fileInfo.id,
            error: result.reason?.message || 'Upload failed'
          });
        }
      });

      // Update files list - remove successful files, keep failed ones
      setFiles(prev => prev.filter(f => !results.success.includes(f.id)));
      setUploadProgress({});

      // Show appropriate messages based on results
      if (results.failed.length > 0) {
        const failedNames = results.failed
          .map(f => files.find(file => file.id === f.fileId)?.file.name)
          .filter(Boolean);
        setError(`Failed to upload: ${failedNames.join(', ')}`);
        toast.error(`${results.failed.length} files failed to upload. You can try uploading them again.`);
      } else {
        setShowSuccess(true);
        toast.success('All files uploaded successfully!');
      }

    } catch (error) {
      console.error('Upload error:', error);
      if (error.response?.status === 403) {
        setPermissionError("You don't have permission to perform this operation");
      } else {
        setError('Upload failed: ' + (error.message || 'Unknown error'));
      }
    } finally {
      setUploading(false);
    }
  }, [files, selectedUser, selectedFolder, permissionError]);

  const removeFile = (fileId) => {
    setFiles(prev => prev.filter(f => f.id !== fileId));
    setError(null);
  };

  // Calculate overall progress
  const totalProgress = files.length 
    ? Math.round(
        Object.values(uploadProgress).reduce((a, b) => a + b, 0) / files.length
      )
    : 0;

  // Get count of failed files
  const failedFilesCount = files.filter(f => f.status === 'failed').length;

  return (
    <div className="desktop-upload-container">
      <div className="desktop-upload-controls">
        <select 
          value={selectedUser} 
          onChange={(e) => setSelectedUser(e.target.value)}
          className="desktop-user-select"
          disabled={uploading || isLoadingUsers}
        >
          <option value="">Select User</option>
          {users.map(user => (
            <option key={user.client_id} value={user.client_id}>
              {user.name || user.client_id}
            </option>
          ))}
        </select>

        {selectedUser && (
          <select
            value={selectedFolder}
            onChange={(e) => setSelectedFolder(e.target.value)}
            className="desktop-folder-select"
            disabled={uploading || isLoadingFolders || userFolders.length === 0}
          >
            <option value="" disabled>Select Folder</option>
            {userFolders.map(folder => (
              <option key={folder.path} value={folder.path}>
                {folder.name}
              </option>
            ))}
          </select>
        )}

        {selectedFolder?.includes('/STO/') && (
          <select
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            className="desktop-date-select"
            disabled={uploading}
          >
            {availableDates.map(date => {
              // Parse date parts directly to avoid timezone issues
              const [year, month, day] = date.split('-');
              const displayDate = new Date(year, month - 1, day);
              return (
                <option key={date} value={date}>
                  {displayDate.toLocaleDateString()}
                </option>
              );
            })}
          </select>
        )}
      </div>

      {/* Permission error message */}
      {permissionError && (
        <div className="desktop-permission-error">
          {permissionError}
        </div>
      )}

      {/* Loading indicators */}
      {isLoadingUsers && <div className="desktop-loading">Loading users...</div>}
      {isLoadingFolders && <div className="desktop-loading">Loading folders...</div>}

      {/* Only show dropzone if user and folder are selected and no permission error */}
      {selectedUser && selectedFolder && !permissionError && (
        <div
          {...getRootProps()}
          className={`desktop-dropzone ${isDragActive ? 'active' : ''}`}
        >
          <input {...getInputProps()} />
          <p>Drag 'n' drop files here, or click to select files</p>
        </div>
      )}

      {files.length > 0 && (
        <div className="desktop-file-list">
          {files.map(fileInfo => (
            <FileItem key={fileInfo.id} fileInfo={fileInfo} />
          ))}
        </div>
      )}

      {error && <div className="desktop-error-message">{error}</div>}
      {showSuccess && (
        <div className="desktop-success-message">
          Files uploaded successfully!
        </div>
      )}

      <button
        className="desktop-upload-button"
        onClick={handleUpload}
        disabled={
          files.length === 0 || 
          uploading || 
          !selectedUser || 
          !selectedFolder ||
          !!permissionError
        }
      >
        {uploading 
          ? `Uploading... ${totalProgress}%` 
          : failedFilesCount > 0 
            ? `Retry Upload (${failedFilesCount} failed files)`
            : `Upload ${files.length} Files`}
      </button>
    </div>
  );
};

export default DesktopUpload;
