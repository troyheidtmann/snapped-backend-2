/**
 * @fileoverview Component for collecting TikTok-specific information.
 * Handles username, verification status, and platform metrics.
 */

import React from 'react';

/**
 * @typedef {Object} TikTokInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} loading - Loading state for TikTok data
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting TikTok platform information.
 * Features:
 * - Username collection
 * - Verification status
 * - Hidden metrics tracking:
 *   - Follower count
 *   - Platform rank
 *   - Views rank
 * 
 * @param {TikTokInfoProps} props - Component properties
 * @returns {React.ReactElement} The TikTok information form section
 */
export const TikTokInfo = ({ formData, loading, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">TikTok Information</h3>
      <div className="snapped-call-form__input-group">
        <input
          className="snapped-call-form__input"
          type="text"
          name="TT_Username"
          placeholder="TikTok Username*"
          value={formData.TT_Username}
          onChange={handleChange}
        />
        <div className="snapped-call-form__checkbox-group">
          <input
            className="snapped-call-form__checkbox"
            type="checkbox"
            name="TT_Verified"
            checked={formData.TT_Verified}
            onChange={handleChange}
          />
          <span className="snapped-call-form__checkbox-label">TikTok Verified</span>
        </div>
      </div>
      <input type="hidden" name="TT_Followers" value={formData.TT_Followers} />
      <input type="hidden" name="TT_Rank" value={formData.TT_Rank} />
      <input type="hidden" name="TT_Views_Rank" value={formData.TT_Views_Rank} />
    </div>
  );
}; 