/**
 * @fileoverview Component for collecting Instagram-specific information.
 * Handles username, verification status, and platform metrics.
 */

import React from 'react';

/**
 * @typedef {Object} InstagramInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} loading - Loading state for Instagram data
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting Instagram platform information.
 * Features:
 * - Username collection
 * - Verification status
 * - Hidden metrics tracking:
 *   - Follower count
 *   - Platform rank
 *   - Views rank
 *   - Engagement rate
 * 
 * @param {InstagramInfoProps} props - Component properties
 * @returns {React.ReactElement} The Instagram information form section
 */
export const InstagramInfo = ({ formData, loading, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">Instagram Information</h3>
      <div className="snapped-call-form__input-group">
        <input
          className="snapped-call-form__input"
          type="text"
          name="IG_Username"
          placeholder="Instagram Username*"
          value={formData.IG_Username}
          onChange={handleChange}
        />
        <div className="snapped-call-form__checkbox-group">
          <input
            className="snapped-call-form__checkbox"
            name="IG_Verified"
            checked={formData.IG_Verified}
            onChange={handleChange}
          />
          <span className="snapped-call-form__checkbox-label">Instagram Verified</span>
        </div>
      </div>
      <input type="hidden" name="IG_Followers" value={formData.IG_Followers} />
      <input type="hidden" name="IG_Rank" value={formData.IG_Rank} />
      <input type="hidden" name="IG_Views_Rank" value={formData.IG_Views_Rank} />
      <input type="hidden" name="IG_Engagement" value={formData.IG_Engagement} />
    </div>
  );
}; 