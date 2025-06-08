/**
 * @fileoverview Employee Onboarding Form component that provides a comprehensive interface
 * for collecting new employee information, including personal details, employment information,
 * and document verification.
 */

import React, { useEffect } from 'react';
import { useOnboardingLogic } from './hooks/useOnboardingLogic';
import './OnboardingForm.css';

/**
 * Employee Onboarding Form component that handles the collection and submission
 * of new employee information. Features include:
 * - Personal information collection
 * - Employment details
 * - Document verification
 * - Form validation
 * - Automatic employee ID generation
 * - Direct deposit information
 * 
 * @returns {React.ReactElement} The rendered onboarding form
 */
const OnboardingForm = () => {
  const { formData, errors, loading, handleChange, handleSubmit } = useOnboardingLogic();
  console.log('Form handlers:', { handleSubmit, handleChange });

  // Set page title on component mount
  useEffect(() => {
    document.title = 'Employee Onboarding | Snapped';
  }, []);

  return (
    <div className="call-form-page">
      <div className="snapped-call-form__container">
        <h1 className="snapped-call-form__title">Employee Onboarding</h1>
        
        <form onSubmit={handleSubmit}>
          {/* Personal Information */}
          <div className="snapped-call-form__section">
            <h3 className="snapped-call-form__section-heading">Personal Information</h3>
            
            <div className="snapped-call-form__input-group">
              <input
                className={`snapped-call-form__input ${errors.first_name ? 'snapped-call-form__input--error' : ''}`}
                type="text"
                name="first_name"
                placeholder="First Name*"
                value={formData.first_name}
                onChange={handleChange}
                required
              />
              {errors.first_name && (
                <span className="snapped-call-form__error">{errors.first_name}</span>
              )}
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className={`snapped-call-form__input ${errors.last_name ? 'snapped-call-form__input--error' : ''}`}
                type="text"
                name="last_name"
                placeholder="Last Name*"
                value={formData.last_name}
                onChange={handleChange}
                required
              />
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="email"
                name="email"
                placeholder="Email Address*"
                value={formData.email}
                onChange={handleChange}
                required
              />
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="tel"
                name="phone"
                placeholder="Phone Number*"
                value={formData.phone}
                onChange={handleChange}
                required
              />
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="date"
                name="date_of_birth"
                placeholder="Date of Birth*"
                value={formData.date_of_birth}
                onChange={handleChange}
                required
              />
            </div>
          </div>

          {/* Employment Information */}
          <div className="snapped-call-form__section">
            <h3 className="snapped-call-form__section-heading">Employment Information</h3>
            
            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="date"
                name="start_date"
                placeholder="Start Date*"
                value={formData.start_date}
                onChange={handleChange}
                required
              />
            </div>

            <div className="snapped-call-form__input-group">
              <select
                className="snapped-call-form__input"
                name="department"
                value={formData.department}
                onChange={handleChange}
                required
              >
                <option value="">Select Department*</option>
                <option value="engineering">Engineering</option>
                <option value="marketing">Marketing</option>
                <option value="sales">Sales</option>
                <option value="hr">Human Resources</option>
                <option value="finance">Finance</option>
              </select>
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="text"
                name="position"
                placeholder="Position/Title*"
                value={formData.position}
                onChange={handleChange}
                required
              />
            </div>

            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="text"
                name="reporting_to"
                placeholder="Reporting To*"
                value={formData.reporting_to}
                onChange={handleChange}
                required
              />
            </div>
          </div>

          {/* Documents Verification */}
          <div className="snapped-call-form__section">
            <h3 className="snapped-call-form__section-heading">Document Verification</h3>
            
            <div className="snapped-call-form__checkbox-group">
              <input
                type="checkbox"
                name="completed_w4"
                checked={formData.completed_w4}
                onChange={handleChange}
                className="snapped-call-form__checkbox"
              />
              <label className="snapped-call-form__checkbox-label">
                I have completed Form W-4
              </label>
            </div>

            <div className="snapped-call-form__checkbox-group">
              <input
                type="checkbox"
                name="completed_i9"
                checked={formData.completed_i9}
                onChange={handleChange}
                className="snapped-call-form__checkbox"
              />
              <label className="snapped-call-form__checkbox-label">
                I have completed Form I-9
              </label>
            </div>

            <div className="snapped-call-form__checkbox-group">
              <input
                type="checkbox"
                name="signed_handbook"
                checked={formData.signed_handbook}
                onChange={handleChange}
                className="snapped-call-form__checkbox"
              />
              <label className="snapped-call-form__checkbox-label">
                I have read and signed the Employee Handbook
              </label>
            </div>

            <div className="snapped-call-form__checkbox-group">
              <input
                type="checkbox"
                name="signed_nda"
                checked={formData.signed_nda}
                onChange={handleChange}
                className="snapped-call-form__checkbox"
              />
              <label className="snapped-call-form__checkbox-label">
                I have signed the Non-Disclosure Agreement
              </label>
            </div>
          </div>

          <button 
            type="submit" 
            className="snapped-call-form__submit-button"
            disabled={loading}
            onClick={(e) => {
              console.log('Button clicked directly');
            }}
          >
            {loading ? 'Submitting...' : 'Complete Onboarding'}
          </button>
        </form>
      </div>
    </div>
  );
};

export default OnboardingForm; 