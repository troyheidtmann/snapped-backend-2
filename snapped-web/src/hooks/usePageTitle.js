/**
 * @fileoverview Custom hook for managing the document title.
 * Provides functionality to update the page title with a consistent format.
 */

import { useEffect } from 'react';

/**
 * Custom hook that updates the document title and restores it on unmount
 * 
 * @function usePageTitle
 * @param {string} title - The title to set for the page
 * @example
 * // In a component:
 * usePageTitle('Dashboard');
 * // Sets document.title to "Dashboard | SNAPPED"
 */
const usePageTitle = (title) => {
  useEffect(() => {
    const previousTitle = document.title;
    document.title = `${title} | SNAPPED`;

    return () => {
      document.title = previousTitle;
    };
  }, [title]);
};

export default usePageTitle; 