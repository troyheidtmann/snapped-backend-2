/**
 * @fileoverview Custom hook for managing file manager state.
 * Handles directory navigation, file selection, and content management.
 */

import { useState, useCallback, useEffect } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';
import { toast } from 'react-toastify';
import { pathUtils } from '../utils/pathUtils';

// Constants
const ITEMS_PER_PAGE = 60;
const DEFAULT_COLLECTIONS = ['STORIES', 'SPOTLIGHT', 'SAVED'];

/**
 * Hook for managing directory state and navigation.
 * Features:
 * - Directory content management
 * - Pagination
 * - Loading states
 * - Error handling
 * 
 * @returns {Object} Directory state and management functions
 */
const useDirectoryState = () => {
  // Start with showing default collections
  const [contents, setContents] = useState(DEFAULT_COLLECTIONS.map(name => ({
    name,
    type: 'folder',
    path: name
  })));
  const [currentPage, setCurrentPage] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [path, setPath] = useState('');

  /**
   * Fetches directory contents from the backend.
   * 
   * @returns {Promise<void>}
   */
  const fetchContents = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      
      // Always ensure we have at least the default folders
      if (!path) {
        setContents(DEFAULT_COLLECTIONS.map(name => ({
          name,
          type: 'folder',
          path: name
        })));
        return;
      }

      // Try to fetch from backend, but don't fail if we can't connect
      try {
        const response = await axios.get(API_ENDPOINTS.CDN.LIST_CONTENTS, {
          params: { path }
        });
        if (response.data?.contents) {
          setContents(response.data.contents);
        }
      } catch (err) {
        console.error('Failed to fetch contents:', err);
        // Don't clear contents on error - keep showing what we had
      }

    } finally {
      setIsLoading(false);
    }
  }, [path]);

  useEffect(() => {
    fetchContents();
  }, [fetchContents]);

  /**
   * Navigates to a specified directory.
   * 
   * @param {string} to - Target directory path
   */
  const navigate = useCallback((to) => {
    if (to === '..') {
      // Go up one level
      const parts = path.split('/').filter(Boolean);
      parts.pop();
      setPath(parts.join('/'));
    } else {
      // Go into folder
      setPath(path ? `${path}/${to}` : to);
    }
    setCurrentPage(1);
  }, [path]);

  /**
   * Refreshes current directory contents.
   */
  const refresh = useCallback(() => {
    fetchContents();
  }, [fetchContents]);

  /**
   * Gets items for the current page.
   * 
   * @returns {Array<Object>} Items for the current page
   */
  const getCurrentPageItems = useCallback(() => {
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = startIndex + ITEMS_PER_PAGE;
    return contents.slice(startIndex, endIndex);
  }, [contents, currentPage]);

  const totalPages = Math.ceil(contents.length / ITEMS_PER_PAGE);

  return {
    path,
    contents: getCurrentPageItems(),
    isLoading,
    error,
    navigate,
    refresh,
    folders: contents.filter(item => item.type === 'folder'),
    pagination: {
      currentPage,
      totalPages,
      setCurrentPage,
      hasMore: contents.length >= ITEMS_PER_PAGE,
      loadMore: () => setCurrentPage(prev => prev + 1),
      hasPrevPage: currentPage > 1,
      prevPage: () => currentPage > 1 && setCurrentPage(p => p - 1)
    }
  };
};

/**
 * Main hook for file manager state management.
 * Features:
 * - Dual directory navigation
 * - File selection
 * - Reordering mode
 * - Client matching validation
 * 
 * @returns {Object} File manager state and management functions
 */
export const useFileManagerState = () => {
  const leftDir = useDirectoryState();
  const rightDir = useDirectoryState();

  const [selectedLeftFile, setSelectedLeftFile] = useState(null);
  const [selectedRightFile, setSelectedRightFile] = useState(null);
  const [isReorderMode, setIsReorderMode] = useState(false);
  const [reorderedFiles, setReorderedFiles] = useState([]);

  /**
   * Checks if the clients in both directories match.
   * 
   * @returns {boolean} Whether clients match
   */
  const doClientsMatch = useCallback(() => {
    return pathUtils.doClientsMatch(leftDir.path, rightDir.path);
  }, [leftDir.path, rightDir.path]);

  return {
    leftDir,
    rightDir,
    selectedLeftFile,
    setSelectedLeftFile,
    selectedRightFile,
    setSelectedRightFile,
    isReorderMode,
    setIsReorderMode,
    reorderedFiles,
    setReorderedFiles,
    doClientsMatch
  };
};

export default useFileManagerState; 