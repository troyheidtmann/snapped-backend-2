/**
 * @fileoverview Component for collecting YouTube-specific information.
 * Handles username, verification status, and platform metrics.
 */

import React from 'react';

/**
 * @typedef {Object} YouTubeInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} loading - Loading state for YouTube data
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting YouTube platform information.
 * Features:
 * - Username collection
 * - Verification status
 * - Hidden metrics tracking:
 *   - Follower count
 *   - Platform rank
 *   - Views rank
 * 
 * @param {YouTubeInfoProps} props - Component properties
 * @returns {React.ReactElement} The YouTube information form section
 */
export const YouTubeInfo = ({ formData, loading, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">YouTube Information</h3>
      <div className="snapped-call-form__input-group">
        <input
          className="snapped-call-form__input"
          type="text"
          name="YT_Username"
          placeholder="YouTube Username*"
          value={formData.YT_Username}
          onChange={handleChange}
        />
        <div className="snapped-call-form__checkbox-group">
          <input
            className="snapped-call-form__checkbox"
            name="YT_Verified"
            checked={formData.YT_Verified}
            onChange={handleChange}
          />
          <span className="snapped-call-form__checkbox-label">YouTube Verified</span>
        </div>
      </div>
      <input type="hidden" name="YT_Followers" value={formData.YT_Followers} />
      <input type="hidden" name="YT_Rank" value={formData.YT_Rank} />
      <input type="hidden" name="YT_Views_Rank" value={formData.YT_Views_Rank} />
    </div>
  );
}; 