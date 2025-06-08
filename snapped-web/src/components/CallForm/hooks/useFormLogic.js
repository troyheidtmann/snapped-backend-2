/**
 * @fileoverview Custom hook for managing form state, validation, and submission logic
 * for the creator call form.
 */

import { useState, useCallback } from 'react';
import axios from 'axios';
import debounce from 'lodash/debounce';
import { INITIAL_FORM_STATE } from '../utils/constants';
import { API_ENDPOINTS } from '../../../config/api';

/**
 * @typedef {Object} FormErrors
 * @property {string} [First_Legal_Name] - Error message for first name
 * @property {string} [Last_Legal_Name] - Error message for last name
 * @property {string} [Email_Address] - Error message for email
 * @property {string} [DOB] - Error message for date of birth
 * @property {string} [Phone] - Error message for phone number
 * @property {string} [Timezone] - Error message for timezone
 * @property {string} [Snap_Star] - Error message for Snapchat star status
 * @property {string} [Snap_Monetized] - Error message for Snapchat monetization status
 */

/**
 * @typedef {Object} LoadingState
 * @property {boolean} instagram - Loading state for Instagram data
 * @property {boolean} tiktok - Loading state for TikTok data
 * @property {boolean} youtube - Loading state for YouTube data
 * @property {boolean} snapchat - Loading state for Snapchat data
 */

/**
 * Custom hook for managing form state and logic.
 * Features:
 * - Form state management
 * - Field validation
 * - Error handling
 * - Social media input validation
 * - Form submission to MongoDB
 * 
 * @returns {Object} Form state and handlers
 * @property {Object} formData - Current form state
 * @property {FormErrors} errors - Validation errors
 * @property {LoadingState} loading - Loading states for different operations
 * @property {Object} socialInputWarning - Warning state for social media inputs
 * @property {Function} handleChange - Form field change handler
 * @property {Function} handleSubmit - Form submission handler
 */
export const useFormLogic = () => {
  const [formData, setFormData] = useState(INITIAL_FORM_STATE);
  const [errors, setErrors] = useState({});
  const [socialInputWarning, setSocialInputWarning] = useState({
    show: false,
    field: ''
  });
  const [loading, setLoading] = useState({
    instagram: false,
    tiktok: false,
    youtube: false,
    snapchat: false
  });

  /**
   * Validates all required fields and formats.
   * Checks:
   * - Required field presence
   * - Phone number format
   * - Date of birth format
   * - Email format
   * 
   * @param {Object} data - Form data to validate
   * @returns {boolean} Whether the form data is valid
   */
  const validateForm = (data) => {
    const required = [
      'First_Legal_Name',
      'Last_Legal_Name',
      'Email_Address',
      'DOB',
      'Timezone',
      'Phone',
      'TT_Username',
      'IG_Username',
      'Snap_Star',
      'Snap_Monetized'
    ];

    const newErrors = {};
    
    // Check all required fields
    required.forEach(field => {
      if (field === 'Snap_Star' || field === 'Snap_Monetized') {
        if (data[field] === undefined) {
          newErrors[field] = `${field.replace(/_/g, ' ')} is required`;
        }
      } else if (!data[field] || data[field].trim() === '') {
        newErrors[field] = `${field.replace(/_/g, ' ')} is required`;
      }
    });

    // Additional validation for phone number
    if (data.Phone) {
      const phoneDigits = data.Phone.replace(/\D/g, '');
      if (phoneDigits.length !== 10 && !data.Phone.startsWith('+1')) {
        newErrors.Phone = 'Please enter a valid 10-digit phone number';
      }
    }

    // Additional validation for DOB format
    if (data.DOB && !/^\d{8}$/.test(data.DOB)) {
      newErrors.DOB = 'Date of birth must be in MMDDYYYY format';
    }

    // Additional validation for email format
    if (data.Email_Address && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(data.Email_Address)) {
      newErrors.Email_Address = 'Please enter a valid email address';
    }

    // Set errors (this will also clear any previous errors if validation passes)
    setErrors(newErrors);
    
    // Return true if no errors, false if there are errors
    return Object.keys(newErrors).length === 0;
  };

  /**
   * Handles changes to form fields.
   * Features:
   * - Social media username validation
   * - Client ID generation
   * - Input sanitization
   * 
   * @param {React.ChangeEvent<HTMLInputElement>} e - The change event
   */
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    
    // Add validation for social media usernames
    const isSocialField = ['TT_Username', 'IG_Username', 'YT_Username'].includes(name);
    if (isSocialField && value !== '') {
      // Check for invalid characters
      if (value.includes('@') || value.includes(' ')) {
        setSocialInputWarning({
          show: true,
          field: name
        });
        setTimeout(() => setSocialInputWarning({ show: false, field: '' }), 2000);
        return;
      }
      // Check for valid pattern
      if (!/^[a-zA-Z0-9._]*$/.test(value)) {
        return;
      }
    }

    const newFormData = {
      ...formData,
      [name]: type === 'checkbox' ? checked : type === 'number' ? parseInt(value) || 0 : value
    };

    if ((name === 'First_Legal_Name' || name === 'Last_Legal_Name' || name === 'DOB') &&
        newFormData.First_Legal_Name && 
        newFormData.Last_Legal_Name && 
        newFormData.DOB) {
      const firstInitial = newFormData.First_Legal_Name.charAt(0).toLowerCase();
      const lastInitial = newFormData.Last_Legal_Name.charAt(0).toLowerCase();
      newFormData.client_id = `${firstInitial}${lastInitial}${newFormData.DOB}`;
    }

    setFormData(newFormData);
  };

  /**
   * Handles form submission to MongoDB.
   * Features:
   * - Form validation
   * - Data cleaning
   * - Social media stats initialization
   * - Error handling
   * 
   * @param {React.FormEvent} e - The form event
   * @param {Object} mongoFormData - The form data to submit
   * @returns {Promise<boolean>} Whether the submission was successful
   */
  const handleSubmit = async (e, mongoFormData) => {
    e.preventDefault();
    
    // Use the single validation function
    if (!validateForm(mongoFormData)) {
      return false;
    }

    try {
      // Initialize social stats if they're empty strings
      const submissionData = {
        ...mongoFormData,
        // Instagram stats
        IG_Followers: mongoFormData.IG_Username ? (mongoFormData.IG_Followers || 0) : 0,
        IG_Engagement: mongoFormData.IG_Username ? (mongoFormData.IG_Engagement || 0) : 0,
        IG_Rank: mongoFormData.IG_Username ? (mongoFormData.IG_Rank || 0) : 0,
        IG_Views_Rank: mongoFormData.IG_Username ? (mongoFormData.IG_Views_Rank || 0) : 0,
        
        // TikTok stats
        TT_Followers: mongoFormData.TT_Username ? (mongoFormData.TT_Followers || 0) : 0,
        TT_Engagement: mongoFormData.TT_Username ? (mongoFormData.TT_Engagement || 0) : 0,
        TT_Rank: mongoFormData.TT_Username ? (mongoFormData.TT_Rank || 0) : 0,
        TT_Views_Rank: mongoFormData.TT_Username ? (mongoFormData.TT_Views_Rank || 0) : 0,
        TT_Likes_Last_30_Days: mongoFormData.TT_Username ? (mongoFormData.TT_Likes_Last_30_Days || 0) : 0,
        
        // YouTube stats
        YT_Followers: mongoFormData.YT_Username ? (mongoFormData.YT_Followers || 0) : 0,
        YT_Engagement: mongoFormData.YT_Username ? (mongoFormData.YT_Engagement || 0) : 0,
        YT_Rank: mongoFormData.YT_Username ? (mongoFormData.YT_Rank || 0) : 0,
        YT_Views_Rank: mongoFormData.YT_Username ? (mongoFormData.YT_Views_Rank || 0) : 0,
        YT_Views: mongoFormData.YT_Username ? (mongoFormData.YT_Views || 0) : 0,
        
        // Ensure boolean fields are actually booleans
        IG_Verified: Boolean(mongoFormData.IG_Verified),
        TT_Verified: Boolean(mongoFormData.TT_Verified),
        YT_Verified: Boolean(mongoFormData.YT_Verified),
        Snap_Star: Boolean(mongoFormData.Snap_Star),
        Snap_Monetized: Boolean(mongoFormData.Snap_Monetized)
      };

      console.log('Submitting data to MongoDB:', submissionData);
      const response = await axios.post(API_ENDPOINTS.CALL_FORM, submissionData);
      
      if (response.status === 200) {
        console.log('Form submitted successfully to MongoDB');
        return true;
      }
    } catch (error) {
      console.error('Error submitting form:', error.response?.data || error);
      const errorMessage = error.response?.data?.detail || error.message || 'Please try again';
      throw new Error(errorMessage);
    }
  };

  return {
    formData,
    errors,
    loading,
    socialInputWarning,
    handleChange,
    handleSubmit
  };
}; 