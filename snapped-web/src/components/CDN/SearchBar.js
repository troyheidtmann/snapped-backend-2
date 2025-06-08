/**
 * @fileoverview Search bar component for filtering CDN content.
 * Handles search input and query submission.
 */

import React, { useState } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch, faSpinner } from '@fortawesome/free-solid-svg-icons';

/**
 * Search bar component with loading state.
 * Features:
 * - Search input handling
 * - Loading state display
 * - Form submission
 * - Visual feedback
 * 
 * @param {Object} props - Component properties
 * @param {Function} props.onSearch - Search handler function
 * @param {boolean} props.loading - Whether search is in progress
 * @returns {React.ReactElement} The search bar interface
 */
const SearchBar = ({ onSearch, loading }) => {
  const [query, setQuery] = useState('');

  /**
   * Handles search form submission.
   * 
   * @param {React.FormEvent} e - Form submission event
   */
  const handleSubmit = (e) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim());
    }
  };

  return (
    <div className="search-bar">
      <form onSubmit={handleSubmit}>
        <div className="search-input-wrapper">
          <input
            type="text"
            className="search-input"
            placeholder="Search files..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <button type="submit" className="search-button" disabled={loading}>
            {loading ? (
              <FontAwesomeIcon icon={faSpinner} spin />
            ) : (
              <FontAwesomeIcon icon={faSearch} />
            )}
          </button>
        </div>
      </form>
    </div>
  );
};

export default SearchBar; 