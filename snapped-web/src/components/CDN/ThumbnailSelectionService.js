/**
 * @fileoverview Service for managing thumbnail selection in the CDN system.
 * Handles selection, updating, and retrieval of thumbnail preferences.
 */

import axios from 'axios';

/**
 * Service object for managing thumbnail selection operations.
 * Features:
 * - Thumbnail selection persistence
 * - Selection retrieval
 * - Backend synchronization
 */
export const ThumbnailSelectionService = {
  /**
   * Updates thumbnail selections for a session.
   * Features:
   * - Multiple thumbnail support
   * - Backend synchronization
   * - Error handling
   * 
   * @param {string} sessionId - Unique identifier for the session
   * @param {Array<Object>} selectedFiles - Array of selected thumbnail files
   * @returns {Promise<Object>} Response from the backend
   */
  updateThumbnails: async (sessionId, selectedFiles) => {
    console.log('Sending to backend:', {
      session_id: sessionId,
      selected_files: selectedFiles
    });
    
    const response = await axios.post('/api/cdn/update-thumbnails', {
      session_id: sessionId,
      selected_files: selectedFiles
    });
    return response.data;
  },

  /**
   * Retrieves current thumbnail selections for a session.
   * 
   * @param {string} sessionId - Unique identifier for the session
   * @returns {Promise<Array<Object>>} Array of currently selected thumbnail files
   */
  getThumbnails: async (sessionId) => {
    const response = await axios.get(`/api/cdn/get-thumbnails/${sessionId}`);
    return response.data.selected_files;
  }
}; 