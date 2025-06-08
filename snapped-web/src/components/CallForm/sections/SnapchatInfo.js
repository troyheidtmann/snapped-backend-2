/**
 * @fileoverview Component for collecting Snapchat-specific information.
 * Handles username, follower count, and platform status details.
 */

import React from 'react';

/**
 * @typedef {Object} SnapchatInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} errors - Validation errors for Snapchat fields
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting Snapchat platform information.
 * Features:
 * - Username collection
 * - Follower count tracking
 * - Snapchat Star status
 * - Monetization status
 * - Error display
 * 
 * @param {SnapchatInfoProps} props - Component properties
 * @returns {React.ReactElement} The Snapchat information form section
 */
export const SnapchatInfo = ({ formData, errors, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">Snapchat Information</h3>
      
      {/* Snapchat Username */}
      <div className="snapped-call-form__input-group">
        <div className="snapped-call-form__input-container">
          <input
            className={`snapped-call-form__input ${errors.Snap_Username ? 'snapped-call-form__input--error' : ''}`}
            type="text"
            name="Snap_Username"
            placeholder="Snapchat Username*"
            value={formData.Snap_Username}
            onChange={handleChange}
          />
        </div>
        <div className="snapped-call-form__checkbox-group">
          <input
            className="snapped-call-form__checkbox"
            type="checkbox"
            name="Snap_Star"
            checked={formData.Snap_Star}
            onChange={handleChange}
          />
          <span className="snapped-call-form__checkbox-label">Snapchat Star</span>
        </div>
      </div>
      {errors.Snap_Username && <span className="snapped-call-form__error">{errors.Snap_Username}</span>}

      <input
        className={`snapped-call-form__input ${errors.Snap_Followers ? 'snapped-call-form__input--error' : ''}`}
        type="text"
        name="Snap_Followers"
        placeholder="Snapchat Followers"
        value={formData.Snap_Followers}
        onChange={handleChange}
      />
      {errors.Snap_Followers && <span className="snapped-call-form__error">{errors.Snap_Followers}</span>}
      
      <div className="snapped-call-form__checkbox-group">
        <input
          className="snapped-call-form__checkbox"
          type="checkbox"
          name="Snap_Monetized"
          checked={formData.Snap_Monetized}
          onChange={handleChange}
        />
        <span className="snapped-call-form__checkbox-label">Snapchat Monetized</span>
      </div>
    </div>
  );
}; 