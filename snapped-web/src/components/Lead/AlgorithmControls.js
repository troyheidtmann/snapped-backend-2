/**
 * @fileoverview Algorithm Controls component for managing scoring thresholds and point allocations
 * for different social media platforms. This component provides an interface to configure
 * engagement metrics, follower thresholds, and performance scoring parameters.
 */

import React, { useState, useEffect } from 'react';
import './styles/AlgorithmControls.css';
import axios from 'axios';

/**
 * @typedef {Object} AlgorithmControlsProps
 * @property {boolean} isOpen - Whether the controls modal is open
 * @property {Function} onClose - Callback function to close the modal
 */

/**
 * Algorithm Controls component that provides an interface for managing scoring thresholds
 * and point allocations across different social media platforms. Handles engagement metrics,
 * follower counts, and performance scoring parameters.
 * 
 * @param {AlgorithmControlsProps} props - Component props
 * @returns {React.ReactElement|null} The rendered component or null if not open
 */
const AlgorithmControls = ({ isOpen, onClose }) => {
  const [settings, setSettings] = useState(null);
  const [isSaving, setIsSaving] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadSettings = async () => {
      try {
        const response = await axios.get('/api/leads/settings');
        setSettings(response.data);
      } catch (error) {
        console.error('Error loading algorithm settings:', error);
      } finally {
        setLoading(false);
      }
    };
    
    if (isOpen) {
      loadSettings();
    }
  }, [isOpen]);

  if (!isOpen || loading || !settings) return null;

  /**
   * Renders input fields for threshold settings of a specific platform and category
   * @param {string} category - The category of thresholds (engagement, followers, etc.)
   * @param {string} platform - The social media platform (instagram, tiktok, etc.)
   * @returns {React.ReactElement} The rendered threshold input group
   */
  const renderThresholdInputs = (category, platform) => {
    const data = settings[category][platform];
    return (
      <div className="algorithm-threshold-group">
        <h4>{platform.charAt(0).toUpperCase() + platform.slice(1)}</h4>
        {Object.entries(data).map(([key, value]) => {
          if (key === 'points') {
            return (
              <div key={key} className="algorithm-points-group">
                <h5>Points</h5>
                {Object.entries(value).map(([level, points]) => (
                  <div key={level} className="algorithm-input-group">
                    <label>{level.charAt(0).toUpperCase() + level.slice(1)}:</label>
                    <input
                      type="number"
                      value={points}
                      onChange={(e) => handleSettingChange(category, platform, key, level, e.target.value)}
                      className="algorithm-input"
                    />
                  </div>
                ))}
              </div>
            );
          }
          return (
            <div key={key} className="algorithm-input-group">
              <label>{key.charAt(0).toUpperCase() + key.slice(1)} Threshold:</label>
              <input
                type="number"
                value={value}
                onChange={(e) => handleSettingChange(category, platform, key, null, e.target.value)}
                className="algorithm-input"
              />
            </div>
          );
        })}
      </div>
    );
  };

  /**
   * Handles changes to individual setting values
   * @param {string} category - The category being modified
   * @param {string} platform - The platform being modified
   * @param {string} type - The type of setting being changed
   * @param {string|null} subtype - Optional subtype for nested settings
   * @param {string|number} value - The new value
   */
  const handleSettingChange = (category, platform, type, subtype, value) => {
    setSettings(prev => {
      const newSettings = { ...prev };
      if (subtype) {
        newSettings[category][platform][type][subtype] = Number(value);
      } else {
        newSettings[category][platform][type] = Number(value);
      }
      return newSettings;
    });
  };

  /**
   * Saves the current settings and triggers score recalculation
   * @async
   * @returns {Promise<void>}
   */
  const handleSave = async () => {
    setIsSaving(true);
    try {
      await axios.post('/api/leads/update-scores');
      onClose();
    } catch (error) {
      console.error('Error updating scores:', error);
      alert('Failed to update scores');
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="algorithm-controls-modal">
      <div className="algorithm-controls-content">
        <button className="close-modal" onClick={onClose}>&times;</button>
        <h2>Algorithm Settings</h2>
        
        {/* Engagement Section */}
        <div className="algorithm-section">
          <h3>Engagement Thresholds</h3>
          <div className="algorithm-grid">
            {Object.keys(settings.engagement).map(platform => 
              renderThresholdInputs('engagement', platform)
            )}
          </div>
        </div>

        {/* Followers Section */}
        <div className="algorithm-section">
          <h3>Follower Thresholds</h3>
          <div className="algorithm-grid">
            {Object.keys(settings.followers).map(platform => 
              renderThresholdInputs('followers', platform)
            )}
          </div>
        </div>

        {/* Recent Performance Section */}
        <div className="algorithm-section">
          <h3>Recent Performance Thresholds</h3>
          <div className="algorithm-grid">
            {Object.keys(settings.recentPerformance).map(platform => 
              renderThresholdInputs('recentPerformance', platform)
            )}
          </div>
        </div>

        {/* Platform Ranks Thresholds */}
        <div className="algorithm-section">
          <h3>Platform Ranks Thresholds</h3>
          <div className="algorithm-grid">
            {/* Instagram */}
            <div className="algorithm-threshold-group">
              <h4>Instagram</h4>
              <div className="algorithm-input-group">
                <label>High Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.instagram.high}
                  onChange={(e) => handleSettingChange('ranks.instagram.high', e.target.value)}
                />
              </div>
              <div className="algorithm-input-group">
                <label>Medium Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.instagram.medium}
                  onChange={(e) => handleSettingChange('ranks.instagram.medium', e.target.value)}
                />
              </div>
              <div className="algorithm-points">
                <h5>Points</h5>
                <div className="algorithm-input-group">
                  <label>High:</label>
                  <input
                    type="number"
                    value={settings.ranks.instagram.points.high}
                    onChange={(e) => handleSettingChange('ranks.instagram.points.high', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Medium:</label>
                  <input
                    type="number"
                    value={settings.ranks.instagram.points.medium}
                    onChange={(e) => handleSettingChange('ranks.instagram.points.medium', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Low:</label>
                  <input
                    type="number"
                    value={settings.ranks.instagram.points.low}
                    onChange={(e) => handleSettingChange('ranks.instagram.points.low', e.target.value)}
                  />
                </div>
              </div>
            </div>

            {/* TikTok */}
            <div className="algorithm-threshold-group">
              <h4>TikTok</h4>
              <div className="algorithm-input-group">
                <label>High Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.tiktok.high}
                  onChange={(e) => handleSettingChange('ranks.tiktok.high', e.target.value)}
                />
              </div>
              <div className="algorithm-input-group">
                <label>Medium Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.tiktok.medium}
                  onChange={(e) => handleSettingChange('ranks.tiktok.medium', e.target.value)}
                />
              </div>
              <div className="algorithm-points">
                <h5>Points</h5>
                <div className="algorithm-input-group">
                  <label>High:</label>
                  <input
                    type="number"
                    value={settings.ranks.tiktok.points.high}
                    onChange={(e) => handleSettingChange('ranks.tiktok.points.high', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Medium:</label>
                  <input
                    type="number"
                    value={settings.ranks.tiktok.points.medium}
                    onChange={(e) => handleSettingChange('ranks.tiktok.points.medium', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Low:</label>
                  <input
                    type="number"
                    value={settings.ranks.tiktok.points.low}
                    onChange={(e) => handleSettingChange('ranks.tiktok.points.low', e.target.value)}
                  />
                </div>
              </div>
            </div>

            {/* YouTube */}
            <div className="algorithm-threshold-group">
              <h4>YouTube</h4>
              <div className="algorithm-input-group">
                <label>High Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.youtube.high}
                  onChange={(e) => handleSettingChange('ranks.youtube.high', e.target.value)}
                />
              </div>
              <div className="algorithm-input-group">
                <label>Medium Threshold:</label>
                <input
                  type="number"
                  value={settings.ranks.youtube.medium}
                  onChange={(e) => handleSettingChange('ranks.youtube.medium', e.target.value)}
                />
              </div>
              <div className="algorithm-points">
                <h5>Points</h5>
                <div className="algorithm-input-group">
                  <label>High:</label>
                  <input
                    type="number"
                    value={settings.ranks.youtube.points.high}
                    onChange={(e) => handleSettingChange('ranks.youtube.points.high', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Medium:</label>
                  <input
                    type="number"
                    value={settings.ranks.youtube.points.medium}
                    onChange={(e) => handleSettingChange('ranks.youtube.points.medium', e.target.value)}
                  />
                </div>
                <div className="algorithm-input-group">
                  <label>Low:</label>
                  <input
                    type="number"
                    value={settings.ranks.youtube.points.low}
                    onChange={(e) => handleSettingChange('ranks.youtube.points.low', e.target.value)}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Verification Points */}
        <div className="algorithm-section">
          <h3>Verification Points</h3>
          <div className="algorithm-grid">
            {Object.entries(settings.verification).map(([platform, points]) => (
              <div key={platform} className="algorithm-input-group">
                <label>{platform.charAt(0).toUpperCase() + platform.slice(1)}:</label>
                <input
                  type="number"
                  value={points}
                  onChange={(e) => handleSettingChange('verification', platform, null, null, e.target.value)}
                  className="algorithm-input"
                />
              </div>
            ))}
          </div>
        </div>

        {/* Cross Platform Bonus */}
        <div className="algorithm-section">
          <h3>Cross Platform Bonus</h3>
          <div className="algorithm-input-group">
            <label>Points:</label>
            <input
              type="number"
              value={settings.crossPlatform}
              onChange={(e) => setSettings(prev => ({ ...prev, crossPlatform: Number(e.target.value) }))}
              className="algorithm-input"
            />
          </div>
        </div>

        <div className="algorithm-actions">
          <button 
            className="algorithm-save-button" 
            onClick={handleSave}
            disabled={isSaving}
          >
            {isSaving ? 'Saving...' : 'Save Changes'}
          </button>
          <button 
            className="algorithm-cancel-button"
            onClick={onClose}
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
};

export default AlgorithmControls; 