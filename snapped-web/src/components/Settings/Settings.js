/**
 * @fileoverview Settings component that provides a dropdown menu for various application settings
 * and configuration options.
 */

import React, { useState, useEffect } from 'react';
import './Settings.css';

/**
 * Settings component that renders a dropdown menu with various configuration options.
 * Features include:
 * - Lead management
 * - Algorithm configuration
 * - Fullscreen toggle
 * - Partner management
 * - Click-outside handling
 * 
 * @returns {React.ReactElement} The rendered settings dropdown
 */
const Settings = () => {
  const [isOpen, setIsOpen] = useState(false);

  /**
   * Sets up click-outside event listener to close the dropdown
   */
  useEffect(() => {
    const handleClick = (e) => {
      const settingsContainer = document.querySelector('.settings-container');
      // If click is outside settings, close it
      if (!settingsContainer?.contains(e.target)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('click', handleClick);
    return () => document.removeEventListener('click', handleClick);
  }, []);

  /**
   * Handles clicks on dropdown items
   * @param {Function} action - The action to execute when clicked
   */
  const handleItemClick = (action) => {
    // Handle the action first
    action();
    // Then close the dropdown
    setIsOpen(false);
  };

  return (
    <div className="settings-container">
      <button 
        className="settings-button"
        onClick={() => setIsOpen(!isOpen)}
      >
        <span className="settings-icon">⚙️</span>
        Settings
      </button>
      
      {isOpen && (
        <div className="settings-dropdown">
          <div className="settings-item" onClick={() => handleItemClick(handleLeadClick)}>
            <span className="item-icon">➕</span>
            Lead
          </div>
          <div className="settings-item" onClick={() => handleItemClick(handleAlgorithmClick)}>
            <span className="item-icon">🔄</span>
            Algorithm
          </div>
          <div className="settings-item" onClick={() => handleItemClick(handleFullscreenClick)}>
            <span className="item-icon">⛶</span>
            Fullscreen
          </div>
          <div className="settings-item" onClick={() => handleItemClick(handlePartnersClick)}>
            <span className="item-icon">👥</span>
            Partners
          </div>
        </div>
      )}
    </div>
  );
};

export default Settings; 