/**
 * @fileoverview File operation utilities for the CDN system.
 * Handles file movement, transfer, and selection operations.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';
import { toast } from 'react-toastify';

/**
 * Moves a file from one location to another within the CDN.
 * Features:
 * - Source and destination path validation
 * - Error handling with user feedback
 * - Automatic directory refresh after move
 * 
 * @param {Object} file - File object to move
 * @param {string} file.sourcePath - Current path of the file
 * @param {string} file.name - Name of the file
 * @param {string} destinationPath - Target path for the file
 * @param {Object} refreshCallbacks - Callbacks to refresh directory contents
 * @returns {Promise<void>}
 * @throws {Error} If move operation fails
 */
export const moveFile = async (file, destinationPath, refreshCallbacks) => {
  try {
    const response = await axios.post(API_ENDPOINTS.CDN_MONGO.MOVE_FILE, {
      source_path: file.sourcePath,
      destination_path: destinationPath,
      file_name: file.name
    });

    if (response.data.status === 'success') {
      toast.success('File moved successfully');
      // Refresh both source and destination directories
      if (refreshCallbacks) {
        Object.values(refreshCallbacks).forEach(callback => callback());
      }
    } else {
      throw new Error(response.data.message || 'Failed to move file');
    }
  } catch (error) {
    console.error('Error moving file:', error);
    toast.error(error.message || 'Failed to move file');
    throw error;
  }
};

/**
 * Handles bulk file transfer (dumping) to a target directory.
 * Features:
 * - Multiple file transfer
 * - Error handling
 * - Batch processing
 * 
 * @param {Array<Object>} files - Array of file objects to transfer
 * @param {string} destinationPath - Target directory for the files
 * @returns {Promise<void>}
 * @throws {Error} If dump operation fails
 */
export const dumpFiles = async (files, destinationPath) => {
  try {
    const response = await axios.post(API_ENDPOINTS.CDN_MONGO.DUMP_FILES, {
      files,
      destination: destinationPath
    });

    if (response.data.status !== 'success') {
      throw new Error('Failed to dump files');
    }
  } catch (error) {
    console.error('Error during file dump:', error);
    throw error;
  }
};

/**
 * Toggles file selection for dump mode.
 * Features:
 * - Add/remove files from selection
 * - Path-based file identification
 * 
 * @param {Object} file - File object to toggle
 * @param {string} currentPath - Current directory path
 * @param {Array<string>} selectedFiles - Currently selected file paths
 * @returns {Array<string>} Updated array of selected file paths
 */
export const toggleFileSelection = (file, currentPath, selectedFiles) => {
  const filePath = `${currentPath}/${file.name}`;
  return selectedFiles.includes(filePath)
    ? selectedFiles.filter(f => f !== filePath)
    : [...selectedFiles, filePath];
}; 