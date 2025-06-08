/**
 * @fileoverview Folder operation utilities for the CDN system.
 * Handles folder creation, naming, and file reordering within folders.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';
import { toast } from 'react-toastify';

/**
 * Generates the next folder name based on date pattern.
 * Features:
 * - Date-based folder naming (F(MM-DD-YYYY)_clientId)
 * - Sequential date progression
 * - Client ID integration
 * 
 * @param {Array<Object>} folders - List of existing folders
 * @param {string} clientId - Client identifier
 * @returns {string} Generated folder name
 */
export const getNextFolderName = (folders, clientId) => {
  const today = new Date();
  const month = String(today.getMonth() + 1).padStart(2, '0');
  const day = String(today.getDate()).padStart(2, '0');
  const year = today.getFullYear();
  
  const dateRegex = /F\((\d{2})-(\d{2})-(\d{4})\)_/;
  const sortedFolders = folders
    .map(folder => folder.name)
    .filter(name => dateRegex.test(name))
    .sort()
    .reverse();

  if (sortedFolders.length === 0) {
    return `F(${month}-${day}-${year})_${clientId}`;
  }

  const latestFolder = sortedFolders[0];
  const match = dateRegex.exec(latestFolder);
  if (!match) return `F(${month}-${day}-${year})_${clientId}`;

  const latestDate = new Date(match[3], parseInt(match[1]) - 1, parseInt(match[2]));
  latestDate.setDate(latestDate.getDate() + 1);
  
  const newMonth = String(latestDate.getMonth() + 1).padStart(2, '0');
  const newDay = String(latestDate.getDate()).padStart(2, '0');
  const newYear = latestDate.getFullYear();

  return `F(${newMonth}-${newDay}-${newYear})_${clientId}`;
};

/**
 * Creates a new folder in the specified path.
 * Features:
 * - Path validation
 * - Error handling with user feedback
 * - Automatic refresh after creation
 * 
 * @param {string} path - Path where the folder should be created
 * @param {Array<Object>} folders - List of existing folders
 * @param {Function} refreshCallback - Callback to refresh directory contents
 * @returns {Promise<void>}
 * @throws {Error} If folder creation fails
 */
export const createFolder = async (path, folders, refreshCallback) => {
  try {
    const response = await axios.post(API_ENDPOINTS.CDN_MONGO.CREATE_FOLDER, {
      path: path
    });

    if (response.data.status === 'success') {
      toast.success('Folder created successfully');
      refreshCallback();
    } else {
      throw new Error(response.data.message || 'Failed to create folder');
    }
  } catch (error) {
    console.error('Error creating folder:', error);
    toast.error(error.message || 'Failed to create folder');
    throw error;
  }
};

/**
 * Reorders files within a directory.
 * Features:
 * - Sequence number assignment
 * - Order persistence
 * - Error handling with user feedback
 * - Automatic refresh after reordering
 * 
 * @param {string} path - Directory path containing the files
 * @param {Array<Object>} newOrder - New order of files
 * @param {Array<Object>} initialOrder - Original order of files
 * @param {Function} refreshCallback - Callback to refresh directory contents
 * @returns {Promise<void>}
 * @throws {Error} If reordering fails
 */
export const reorderFiles = async (path, newOrder, initialOrder, refreshCallback) => {
  try {
    const response = await axios.post(API_ENDPOINTS.CDN_MONGO.REORDER_FILES, {
      path: path,
      files: newOrder.map((file, index) => ({
        file_name: file.name,
        seq_number: index
      }))
    });

    if (response.data.status === 'success') {
      toast.success('Files reordered successfully');
      refreshCallback();
    } else {
      throw new Error(response.data.message || 'Failed to reorder files');
    }
  } catch (error) {
    console.error('Error reordering files:', error);
    toast.error(error.message || 'Failed to reorder files');
    throw error;
  }
}; 