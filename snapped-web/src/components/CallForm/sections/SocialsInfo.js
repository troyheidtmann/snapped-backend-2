/**
 * @fileoverview Component for collecting social media platform information.
 * Handles TikTok, Instagram, and YouTube account details.
 */

import React from 'react';

/**
 * @typedef {Object} SocialsInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} loading - Loading states for social media data
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting social media platform information.
 * Features:
 * - TikTok username and verification status
 * - Instagram username and verification status
 * - YouTube username and verification status
 * - Hidden fields for platform metrics
 * 
 * @param {SocialsInfoProps} props - Component properties
 * @returns {React.ReactElement} The social media information form section
 */
export const SocialsInfo = ({ formData, loading, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">Social Media Information</h3>
      
      {/* TikTok */}
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

      {/* Instagram */}
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
            type="checkbox"
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

      {/* YouTube */}
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
            type="checkbox"
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