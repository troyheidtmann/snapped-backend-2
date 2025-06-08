/**
 * @fileoverview Component for collecting personal information in the call form.
 * Includes fields for name, contact details, and timezone selection.
 */

import React from 'react';
import { TIMEZONE_OPTIONS } from '../utils/constants';

/**
 * @typedef {Object} PersonalInfoProps
 * @property {Object} formData - Current form state
 * @property {Object} errors - Validation errors for form fields
 * @property {Function} handleChange - Handler for form field changes
 */

/**
 * Component for collecting personal information from creators.
 * Features:
 * - Legal name collection
 * - Email address validation
 * - Phone number formatting
 * - Date of birth validation
 * - Timezone selection
 * - Client ID generation
 * - Error display
 * 
 * @param {PersonalInfoProps} props - Component properties
 * @returns {React.ReactElement} The personal information form section
 */
export const PersonalInfo = ({ formData, errors, handleChange }) => {
  return (
    <div className="snapped-call-form__section">
      <h3 className="snapped-call-form__section-heading">Personal Information</h3>
      <input
        className={`snapped-call-form__input ${errors.First_Legal_Name ? 'snapped-call-form__input--error' : ''}`}
        type="text"
        name="First_Legal_Name"
        placeholder="First Legal Name *"
        value={formData.First_Legal_Name}
        onChange={handleChange}
        required
      />
      {errors.First_Legal_Name && <span className="snapped-call-form__error">{errors.First_Legal_Name}</span>}
      
      <input
        className={`snapped-call-form__input ${errors.Last_Legal_Name ? 'snapped-call-form__input--error' : ''}`}
        type="text"
        name="Last_Legal_Name"
        placeholder="Last Legal Name *"
        value={formData.Last_Legal_Name}
        onChange={handleChange}
        required
      />
      {errors.Last_Legal_Name && <span className="snapped-call-form__error">{errors.Last_Legal_Name}</span>}
      
      <input
        className={`snapped-call-form__input ${errors.Email_Address ? 'snapped-call-form__input--error' : ''}`}
        type="email"
        name="Email_Address"
        placeholder="Email Address *"
        value={formData.Email_Address}
        onChange={handleChange}
        required
      />
      {errors.Email_Address && <span className="snapped-call-form__error">{errors.Email_Address}</span>}
      
      <input
        className={`snapped-call-form__input ${errors.Phone ? 'snapped-call-form__input--error' : ''}`}
        type="tel"
        name="Phone"
        placeholder="Phone Number (Format: +1XXXXXXXXXX) *"
        value={formData.Phone || ''}
        onChange={(e) => {
          // Remove all non-digits and non-plus signs
          let value = e.target.value.replace(/[^\d+]/g, '');
          
          // Handle country code
          if (!value.startsWith('+')) {
            // If input starts with 1, add +
            if (value.startsWith('1')) {
              value = '+' + value;
            } 
            // If input doesn't start with 1, add +1
            else if (value.length > 0) {
              value = '+1' + value;
            }
          }
          
          // Limit to proper E.164 format length (+1 plus 10 digits)
          if (value.length > 12) {
            value = value.slice(0, 12);
          }

          handleChange({
            target: {
              name: 'Phone',
              value: value,
              type: 'tel'
            }
          });
        }}
        maxLength={12}
        required
      />
      {errors.Phone && <span className="snapped-call-form__error">{errors.Phone}</span>}
      <small className="snapped-call-form__helper-text" style={{ color: '#666', marginTop: '-15px', marginBottom: '20px', display: 'block' }}>
        Format: +1XXXXXXXXXX (U.S. numbers only)
      </small>
      
      <input
        className={`snapped-call-form__input ${errors.DOB ? 'snapped-call-form__input--error' : ''}`}
        type="text"
        name="DOB"
        placeholder="Date of Birth (MMDDYYYY) *"
        value={formData.DOB || ''}
        onChange={(e) => {
          const value = e.target.value.replace(/[^\d]/g, '');
          handleChange({
            target: {
              name: 'DOB',
              value: value,
              type: 'text',
              validity: { valid: value.length === 8 }
            }
          });
        }}
        maxLength={8}
        required
      />
      {errors.DOB && <span className="snapped-call-form__error">{errors.DOB}</span>}
      
      <select
        className={`snapped-call-form__input ${errors.Timezone ? 'snapped-call-form__input--error' : ''}`}
        name="Timezone"
        value={formData.Timezone}
        onChange={handleChange}
        required
      >
        {TIMEZONE_OPTIONS.map(tz => (
          <option 
            key={tz.value} 
            value={tz.value}
            disabled={tz.disabled}
            style={tz.disabled ? { 
              backgroundColor: '#f5f5f5',
              fontWeight: 'bold',
              color: '#666'
            } : undefined}
          >
            {tz.label}
          </option>
        ))}
      </select>
      {errors.Timezone && <span className="snapped-call-form__error">{errors.Timezone}</span>}
      
      <input
        type="hidden"
        name="client_id"
        value={formData.client_id}
      />
    </div>
  );
}; 