/**
 * @fileoverview Button component for renumbering files in a directory.
 * Handles file sequence renumbering operations.
 */

import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSort } from '@fortawesome/free-solid-svg-icons';
import axios from 'axios';
import { toast } from 'react-toastify';

/**
 * Button component for renumbering files.
 * Features:
 * - File sequence renumbering
 * - Backend synchronization
 * - Success/error feedback
 * - Content refresh
 * 
 * @param {Object} props - Component properties
 * @param {string} props.path - Directory path containing files to renumber
 * @param {Function} props.onComplete - Callback after successful renumbering
 * @param {Function} props.onContentsUpdate - Callback to update directory contents
 * @returns {React.ReactElement} The renumber button interface
 */
const RenumberButton = ({ path, onComplete, onContentsUpdate }) => {
  /**
   * Handles the renumbering operation.
   * - Sends request to backend
   * - Updates UI with result
   * - Refreshes directory contents
   * 
   * @returns {Promise<void>}
   */
  const handleRenumber = async () => {
    try {
      const response = await axios.post('/api/cdn/renumber-files', {
        path: path
      });

      if (response.data.status === 'success') {
        toast.success(response.data.message);
        if (response.data.contents && onContentsUpdate) {
          onContentsUpdate(response.data.contents);
        }
        onComplete && onComplete();
      } else {
        toast.error('Failed to rename files');
      }
    } catch (error) {
      console.error('Error renumbering files:', error);
      toast.error('Error renumbering files');
    }
  };

  return (
    <button
      className="toolbar-button"
      onClick={handleRenumber}
      title="Renumber Files"
    >
      <FontAwesomeIcon icon={faSort} />
    </button>
  );
};

export default RenumberButton; 