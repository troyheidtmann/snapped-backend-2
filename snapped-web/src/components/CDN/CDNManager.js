/**
 * @fileoverview Management component for CDN content.
 * Handles content access control and directory listing.
 */

import React, { useState } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../constants/endpoints';
import './styles/CDNManager.css';

/**
 * Component for managing CDN content access and listing.
 * Features:
 * - Access control validation
 * - Directory content listing
 * - Error handling and display
 * - Loading state management
 * 
 * @returns {React.ReactElement} The CDN manager interface
 */
const CDNManager = () => {
  const [accessError, setAccessError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [contents, setContents] = useState([]);

  /**
   * Fetches contents of a directory with access validation.
   * 
   * @param {string} path - Directory path to fetch contents from
   * @returns {Promise<void>}
   */
  const fetchContents = async (path) => {
    try {
      setLoading(true);
      setAccessError(null);
      const response = await axios.get(`${API_ENDPOINTS.CDN.LIST_CONTENTS}?path=${encodeURIComponent(path)}`);
      setContents(response.data.contents);
    } catch (error) {
      console.error('Error fetching contents:', error);
      if (error.response?.status === 403) {
        setAccessError('You do not have access to this content');
      }
      setContents([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="cdn-manager">
      {accessError && (
        <div className="access-error-banner">
          {accessError}
        </div>
      )}
      {/* ... rest of your component ... */}
    </div>
  );
};

export default CDNManager; 