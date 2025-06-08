/**
 * @fileoverview Loading Spinner component for displaying loading states.
 * Provides a centered, full-screen loading indicator.
 */

/**
 * Loading Spinner component that displays a centered loading indicator.
 * Used for indicating loading states across the application.
 * 
 * @returns {React.ReactElement} The rendered loading spinner
 */
import React from 'react';
import './styles/LoadingSpinner.css';

const LoadingSpinner = () => (
  <div className="loading-spinner" />
);

export default LoadingSpinner; 