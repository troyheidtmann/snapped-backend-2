/**
 * @fileoverview Path utilities for CDN operations.
 * Provides functions for path manipulation and validation in the CDN system.
 */

/**
 * Utility object containing path-related functions for CDN operations.
 * Features:
 * - Path validation
 * - Session path detection
 * - Path manipulation
 */
export const pathUtils = {
  /**
   * Checks if a path represents a session-level directory where media should be displayed.
   * Valid session paths include:
   * - F(MMDDYY)_clientId format
   * - CONTENTDUMP_ prefix
   * 
   * @param {string} path - Path to check
   * @returns {boolean} True if path is a session path
   */
  isSessionPath: (path) => {
    return path?.includes('F(') || path?.includes('CONTENTDUMP_');
  }
}; 