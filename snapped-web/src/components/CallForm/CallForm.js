/**
 * @fileoverview Main form component for collecting creator information and social media details.
 * Handles user registration, form validation, and data submission.
 */

import React, { useEffect, useState } from 'react';
import { signUp, confirmSignUp } from 'aws-amplify/auth';
import { useFormLogic } from './hooks/useFormLogic';
import { PersonalInfo } from './sections/PersonalInfo';
import './CallForm.css';

/**
 * Main form component for creator registration and data collection.
 * Features:
 * - Personal information collection
 * - Password creation and validation
 * - Social media platform details
 * - AWS Cognito integration for user registration
 * - Form validation and error handling
 * - Confirmation code verification
 * 
 * @returns {React.ReactElement} The complete call form interface
 */
const CallForm = () => {
  const { formData, errors, loading, handleChange, handleSubmit: handleFormSubmit } = useFormLogic();
  const [showNumberWarning, setShowNumberWarning] = React.useState(false);
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [passwordError, setPasswordError] = useState('');
  const [isConfirmation, setIsConfirmation] = useState(false);
  const [confirmationCode, setConfirmationCode] = useState('');
  const [authError, setAuthError] = useState('');
  const [socialInputWarning, setSocialInputWarning] = useState({ show: false, field: null });

  useEffect(() => {
    document.title = 'Call Form | Snapped';
  }, []);

  /**
   * Validates password strength and confirmation match.
   * Requirements:
   * - Minimum 8 characters
   * - At least one number
   * - At least one special character
   * - At least one uppercase letter
   * - At least one lowercase letter
   * - Passwords must match if confirmation is provided
   * 
   * @param {string} pass - The password to validate
   * @param {string} [confirm] - The confirmation password to check against
   * @returns {boolean} Whether the password meets all requirements
   */
  const validatePassword = (pass, confirm) => {
    if (!pass) {
      setPasswordError('');
      return false;
    }

    const requirements = [];
    if (!/\d/.test(pass)) requirements.push('number');
    if (!/[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]/.test(pass)) requirements.push('special character');
    if (!/[A-Z]/.test(pass)) requirements.push('uppercase letter');
    if (!/[a-z]/.test(pass)) requirements.push('lowercase letter');
    if (pass.length < 8) requirements.push('minimum 8 characters');

    if (requirements.length > 0) {
      setPasswordError(`Password needs: ${requirements.join(', ')}`);
      return false;
    }

    if (confirm && pass !== confirm) {
      setPasswordError("Passwords don't match");
      return false;
    }

    setPasswordError('');
    return true;
  };

  /**
   * Handles numeric input validation for follower counts.
   * Shows a warning if non-numeric characters are entered.
   * 
   * @param {React.ChangeEvent<HTMLInputElement>} e - The input change event
   */
  const handleNumberInput = (e) => {
    const value = e.target.value;
    if (value === '' || /^\d+$/.test(value)) {
      handleChange(e);
    } else {
      setShowNumberWarning(true);
      setTimeout(() => setShowNumberWarning(false), 2000);
    }
  };

  /**
   * Handles form submission, including:
   * - Password validation
   * - Data cleaning and formatting
   * - MongoDB submission
   * - AWS Cognito user creation
   * - Error handling
   * 
   * @param {React.FormEvent<HTMLFormElement>} e - The form submission event
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!validatePassword(password, confirmPassword)) {
      return;
    }

    try {
      // Clean up empty social media fields before submission
      const cleanFormData = {
        ...formData,
        // Convert empty strings to null for optional fields
        Snap_Username: formData.Snap_Username || null,
        Snap_Followers: formData.Snap_Followers || null,
        TT_Username: formData.TT_Username || null,
        TT_Followers: formData.TT_Followers || null,
        IG_Username: formData.IG_Username || null,
        IG_Followers: formData.IG_Followers || null,
        YT_Username: formData.YT_Username || null,
        YT_Followers: formData.YT_Followers || null,
        // Ensure boolean fields are properly set
        Snap_Star: formData.Snap_Star || false,
        Snap_Monetized: formData.Snap_Monetized || false,
        TT_Verified: formData.TT_Verified || false,
        IG_Verified: formData.IG_Verified || false,
        YT_Verified: formData.YT_Verified || false,
        // Set rank fields to null if not present
        IG_Rank: formData.IG_Rank || null,
        IG_Views_Rank: formData.IG_Views_Rank || null,
        TT_Rank: formData.TT_Rank || null,
        TT_Views_Rank: formData.TT_Views_Rank || null,
        YT_Rank: formData.YT_Rank || null,
        YT_Views_Rank: formData.YT_Views_Rank || null
      };

      // First submit form to MongoDB with original field names
      const submitSuccess = await handleFormSubmit(e, cleanFormData);
      
      // If validation failed or submission failed, stop here
      if (!submitSuccess) {
        return;
      }

      // Format DOB from MMDDYYYY to YYYY-MM-DD for AWS
      const dob = cleanFormData.DOB;
      const formattedDOB = `${dob.slice(4)}-${dob.slice(0,2)}-${dob.slice(2,4)}`;

      // Ensure phone is in E.164 format for AWS
      let formattedPhone = formData.Phone;
      if (!formattedPhone.startsWith('+')) {
        // Convert from (XXX) XXX-XXXX to +1XXXXXXXXXX
        const digits = formattedPhone.replace(/\D/g, '');
        formattedPhone = `+1${digits}`;
      }

      // Then create AWS account with correctly mapped fields
      await signUp({
        username: cleanFormData.Email_Address,
        password,
        options: {
          userAttributes: {
            email: cleanFormData.Email_Address,
            given_name: cleanFormData.First_Legal_Name,
            family_name: cleanFormData.Last_Legal_Name,
            birthdate: formattedDOB,
            'custom:UserID': cleanFormData.client_id,
            phone_number: formattedPhone
          },
          autoSignIn: true
        }
      });

      setIsConfirmation(true);
    } catch (err) {
      console.error('Auth error:', err);
      // Provide clear error messages for common AWS errors
      if (err.message.includes('phone_number')) {
        setAuthError('Please enter a valid phone number in the format: (555) 555-5555');
      } else {
        setAuthError(err.message);
      }
    }
  };

  /**
   * Handles confirmation code verification after initial signup.
   * On successful confirmation:
   * - Verifies the code with AWS Cognito
   * - Redirects to appropriate onboarding flow
   * 
   * @param {React.FormEvent<HTMLFormElement>} e - The form submission event
   */
  const handleConfirmSignUp = async (e) => {
    e.preventDefault();

    try {
      await confirmSignUp({
        username: formData.Email_Address,
        confirmationCode
      });

      // Check Snapchat followers instead of Instagram
      const snapFollowers = parseInt(formData.Snap_Followers) || 0;
      
      if (snapFollowers >= 50000) {
        window.location.href = 'https://calendly.com/troyheidtmann/20min';
      } else {
        window.location.href = 'https://calendly.com/troyheidtmann/20-minute-meeting-clone';
      }

    } catch (err) {
      console.error('Confirmation error:', err);
      setAuthError(err.message);
    }
  };

  if (isConfirmation) {
    return (
      <div className="call-form-page call-form">
        <div className="snapped-call-form__container">
          <h2>Confirm Your Email</h2>
          <form onSubmit={handleConfirmSignUp}>
            {authError && <div className="snapped-call-form__error-message">{authError}</div>}
            <div className="snapped-call-form__input-group">
              <input
                className="snapped-call-form__input"
                type="text"
                value={confirmationCode}
                onChange={(e) => setConfirmationCode(e.target.value)}
                placeholder="Enter confirmation code"
                required
              />
            </div>
            <button type="submit" className="snapped-call-form__submit-button">
              Confirm Email
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div className="call-form-page call-form">
      <div className="snapped-call-form__container">
        <h1 className="snapped-call-form__title">Snapped Call Form</h1>
        <form onSubmit={handleSubmit}>
          <PersonalInfo 
            formData={formData}
            errors={errors}
            handleChange={handleChange}
            required={true}
          />
          
          {/* Password Section */}
          <div className="snapped-call-form__section">
            <h3 className="snapped-call-form__section-heading">Account Security</h3>
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className="snapped-call-form__input"
                  type="password"
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    validatePassword(e.target.value, confirmPassword);
                  }}
                  placeholder="Password"
                  required
                />
              </div>
            </div>
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className="snapped-call-form__input"
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => {
                    setConfirmPassword(e.target.value);
                    validatePassword(password, e.target.value);
                  }}
                  placeholder="Confirm Password"
                  required
                />
              </div>
            </div>
            {passwordError && (
              <div className="snapped-call-form__error-message">{passwordError}</div>
            )}
            {authError && (
              <div className="snapped-call-form__error-message">{authError}</div>
            )}
          </div>

          {/* Social Media Information */}
          <div className="snapped-call-form__section">
            <h3 className="snapped-call-form__section-heading">Social Media Information</h3>
            
            {/* Snapchat Questions */}
            <div className="snapped-call-form__radio-group">
              <p className="snapped-call-form__label">Are Your Stories Monetized on Snap?*</p>
              <div className="snapped-call-form__radio-options">
                <label>
                  <input
                    type="radio"
                    name="Snap_Monetized"
                    value="true"
                    checked={formData.Snap_Monetized === true}
                    onChange={(e) => handleChange({
                      target: {
                        name: 'Snap_Monetized',
                        value: true,
                        type: 'radio'
                      }
                    })}
                    required
                  />
                  Yes
                </label>
                <label>
                  <input
                    type="radio"
                    name="Snap_Monetized"
                    value="false"
                    checked={formData.Snap_Monetized === false}
                    onChange={(e) => handleChange({
                      target: {
                        name: 'Snap_Monetized',
                        value: false,
                        type: 'radio'
                      }
                    })}
                  />
                  No
                </label>
              </div>
              {errors.Snap_Monetized && (
                <span className="snapped-call-form__error-message">{errors.Snap_Monetized}</span>
              )}
            </div>

            <div className="snapped-call-form__radio-group">
              <p className="snapped-call-form__label">Are You Verified on Snap?*</p>
              <div className="snapped-call-form__radio-options">
                <label>
                  <input
                    type="radio"
                    name="Snap_Star"
                    value="true"
                    checked={formData.Snap_Star === true}
                    onChange={(e) => handleChange({
                      target: {
                        name: 'Snap_Star',
                        value: true,
                        type: 'radio'
                      }
                    })}
                    required
                  />
                  Yes
                </label>
                <label>
                  <input
                    type="radio"
                    name="Snap_Star"
                    value="false"
                    checked={formData.Snap_Star === false}
                    onChange={(e) => handleChange({
                      target: {
                        name: 'Snap_Star',
                        value: false,
                        type: 'radio'
                      }
                    })}
                  />
                  No
                </label>
              </div>
              {errors.Snap_Star && (
                <span className="snapped-call-form__error-message">{errors.Snap_Star}</span>
              )}
            </div>

            {/* Snapchat */}
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className="snapped-call-form__input"
                  type="text"
                  name="Snap_Username"
                  placeholder="Snapchat Username (letters, numbers, dots, underscores only)"
                  value={formData.Snap_Username}
                  onChange={handleChange}
                />
              </div>
            </div>
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className={`snapped-call-form__input ${errors.Snap_Followers ? 'snapped-call-form__input--error' : ''}`}
                  type="text"
                  name="Snap_Followers"
                  placeholder="Snapchat Followers"
                  value={formData.Snap_Followers}
                  onChange={handleNumberInput}
                  inputMode="numeric"
                  pattern="\d*"
                />
                {errors.Snap_Followers && (
                  <span className="snapped-call-form__error-message">{errors.Snap_Followers}</span>
                )}
                {showNumberWarning && (
                  <span className="snapped-call-form__error-message">Only numbers are allowed</span>
                )}
              </div>
            </div>

            {/* TikTok */}
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className={`snapped-call-form__input ${errors.TT_Username ? 'snapped-call-form__input--error' : ''}`}
                  type="text"
                  name="TT_Username"
                  placeholder="TikTok Username* (letters, numbers, dots, underscores only)"
                  value={formData.TT_Username}
                  onChange={handleChange}
                  required
                />
                {errors.TT_Username && (
                  <span className="snapped-call-form__error-message">{errors.TT_Username}</span>
                )}
                {socialInputWarning.show && socialInputWarning.field === 'TT_Username' && (
                  <span className="snapped-call-form__error-message">@ symbols and spaces are not allowed</span>
                )}
              </div>
            </div>

            {/* Instagram */}
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className={`snapped-call-form__input ${errors.IG_Username ? 'snapped-call-form__input--error' : ''}`}
                  type="text"
                  name="IG_Username"
                  placeholder="Instagram Username* (letters, numbers, dots, underscores only)"
                  value={formData.IG_Username}
                  onChange={handleChange}
                  required
                />
                {errors.IG_Username && (
                  <span className="snapped-call-form__error-message">{errors.IG_Username}</span>
                )}
                {socialInputWarning.show && socialInputWarning.field === 'IG_Username' && (
                  <span className="snapped-call-form__error-message">@ symbols and spaces are not allowed</span>
                )}
              </div>
            </div>

            {/* YouTube */}
            <div className="snapped-call-form__input-group">
              <div className="snapped-call-form__input-container">
                <input
                  className="snapped-call-form__input"
                  type="text"
                  name="YT_Username"
                  placeholder="YouTube Username (letters, numbers, dots, underscores only)"
                  value={formData.YT_Username}
                  onChange={handleChange}
                />
                {socialInputWarning.show && socialInputWarning.field === 'YT_Username' && (
                  <span className="snapped-call-form__error-message">@ symbols and spaces are not allowed</span>
                )}
              </div>
            </div>

            {/* Verification Section */}
            <div className="snapped-call-form__verification">
              <p className="snapped-call-form__verification-text">Check all platforms where you are verified</p>
              <div className="snapped-call-form__verification-group">
                <div className="snapped-call-form__checkbox-group">
                  <input
                    className="snapped-call-form__checkbox"
                    type="checkbox"
                    name="TT_Verified"
                    checked={formData.TT_Verified}
                    onChange={handleChange}
                  />
                  <span className="snapped-call-form__checkbox-label">TT</span>
                </div>
                <div className="snapped-call-form__checkbox-group">
                  <input
                    className="snapped-call-form__checkbox"
                    type="checkbox"
                    name="IG_Verified"
                    checked={formData.IG_Verified}
                    onChange={handleChange}
                  />
                  <span className="snapped-call-form__checkbox-label">IG</span>
                </div>
                <div className="snapped-call-form__checkbox-group">
                  <input
                    className="snapped-call-form__checkbox"
                    type="checkbox"
                    name="YT_Verified"
                    checked={formData.YT_Verified}
                    onChange={handleChange}
                  />
                  <span className="snapped-call-form__checkbox-label">YT</span>
                </div>
              </div>
            </div>

            {/* Hidden inputs */}
            <input type="hidden" name="TT_Followers" value={formData.TT_Followers} />
            <input type="hidden" name="TT_Rank" value={formData.TT_Rank} />
            <input type="hidden" name="TT_Views_Rank" value={formData.TT_Views_Rank} />
            <input type="hidden" name="IG_Followers" value={formData.IG_Followers} />
            <input type="hidden" name="IG_Rank" value={formData.IG_Rank} />
            <input type="hidden" name="IG_Views_Rank" value={formData.IG_Views_Rank} />
            <input type="hidden" name="IG_Engagement" value={formData.IG_Engagement} />
            <input type="hidden" name="YT_Followers" value={formData.YT_Followers} />
            <input type="hidden" name="YT_Rank" value={formData.YT_Rank} />
            <input type="hidden" name="YT_Views_Rank" value={formData.YT_Views_Rank} />
            <input type="hidden" name="SC_Rank" value={formData.SC_Rank} />
            <input type="hidden" name="SC_Views_Rank" value={formData.SC_Views_Rank} />
          </div>
          
          <button type="submit" className="snapped-call-form__submit-button">Submit</button>
        </form>
      </div>
    </div>
  );
};

export default CallForm; 