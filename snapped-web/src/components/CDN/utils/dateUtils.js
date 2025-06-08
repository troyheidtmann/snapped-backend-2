/**
 * @fileoverview Date formatting and parsing utilities for CDN operations.
 * Provides functions for handling various date formats used in the CDN system.
 */

/**
 * Formats a date string from MMDDYY format to a more readable format.
 * Example: "012524" becomes "Jan 25, '24"
 * 
 * @param {string} dateStr - Date string in MMDDYY format
 * @returns {string} Formatted date string in "MMM DD, 'YY" format
 */
export const formatDate = (dateStr) => {
  if (!dateStr || dateStr.length !== 6) return dateStr;

  const month = dateStr.substring(0, 2);
  const day = dateStr.substring(2, 4);
  const year = dateStr.substring(4, 6);

  // Convert to Date object for validation
  const date = new Date(2000 + parseInt(year), parseInt(month) - 1, parseInt(day));
  if (isNaN(date.getTime())) return dateStr;

  // Format the date
  const months = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
  ];

  return `${months[date.getMonth()]} ${date.getDate()}, '${year}`;
};

/**
 * Parses a date string from various formats into a Date object.
 * Supports:
 * - MMDDYY format (e.g., "012524")
 * - MM-DD-YY format (e.g., "01-25-24")
 * 
 * @param {string} dateStr - Date string to parse
 * @returns {Date|null} Parsed Date object or null if invalid
 */
export const parseDate = (dateStr) => {
  if (!dateStr) return null;

  // Handle MMDDYY format
  if (dateStr.length === 6) {
    const month = parseInt(dateStr.substring(0, 2)) - 1;
    const day = parseInt(dateStr.substring(2, 4));
    const year = 2000 + parseInt(dateStr.substring(4, 6));
    return new Date(year, month, day);
  }

  // Handle MM-DD-YY format
  const match = dateStr.match(/(\d{2})-(\d{2})-(\d{2})/);
  if (match) {
    const [, month, day, year] = match;
    return new Date(2000 + parseInt(year), parseInt(month) - 1, parseInt(day));
  }

  return null;
};

/**
 * Compare two dates in MMDDYY format
 * @param {string} dateA - First date in MMDDYY format
 * @param {string} dateB - Second date in MMDDYY format
 * @returns {number} -1 if dateA is earlier, 1 if dateA is later, 0 if equal
 */
export const compareDates = (dateA, dateB) => {
  const dateObjA = parseDate(dateA);
  const dateObjB = parseDate(dateB);

  if (!dateObjA || !dateObjB) return 0;
  return dateObjA.getTime() - dateObjB.getTime();
}; 