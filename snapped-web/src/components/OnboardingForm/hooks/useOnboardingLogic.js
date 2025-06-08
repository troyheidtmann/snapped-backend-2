/**
 * @fileoverview Custom hook for managing employee onboarding form logic including form state,
 * validation, submission, and employee ID generation.
 */

import { useState } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';

/**
 * @typedef {Object} FormData
 * @property {string} employee_id - Generated employee ID
 * @property {string} first_name - Employee's first name
 * @property {string} last_name - Employee's last name
 * @property {string} email - Employee's email address
 * @property {string} phone - Employee's phone number
 * @property {string} date_of_birth - Employee's date of birth
 * @property {string} start_date - Employee's start date
 * @property {string} department - Employee's department
 * @property {string} position - Employee's position/title
 * @property {string} reporting_to - Employee's supervisor
 * @property {string} emergency_contact_name - Emergency contact name
 * @property {string} emergency_contact_phone - Emergency contact phone
 * @property {string} address - Employee's address
 * @property {string} city - Employee's city
 * @property {string} state - Employee's state
 * @property {string} zip_code - Employee's ZIP code
 * @property {string} social_security - Employee's SSN
 * @property {boolean} direct_deposit - Whether employee uses direct deposit
 * @property {string} bank_name - Employee's bank name
 * @property {string} routing_number - Bank routing number
 * @property {string} account_number - Bank account number
 * @property {boolean} completed_w4 - Whether W-4 is completed
 * @property {boolean} completed_i9 - Whether I-9 is completed
 * @property {boolean} signed_handbook - Whether handbook is signed
 * @property {boolean} signed_nda - Whether NDA is signed
 */

const INITIAL_FORM_STATE = {
  employee_id: '',
  first_name: '',
  last_name: '',
  email: '',
  phone: '',
  date_of_birth: '',
  start_date: '',
  department: '',
  position: '',
  reporting_to: '',
  emergency_contact_name: '',
  emergency_contact_phone: '',
  address: '',
  city: '',
  state: '',
  zip_code: '',
  social_security: '',
  direct_deposit: false,
  bank_name: '',
  routing_number: '',
  account_number: '',
  completed_w4: false,
  completed_i9: false,
  signed_handbook: false,
  signed_nda: false,
};

/**
 * Custom hook for managing employee onboarding form logic.
 * Features include:
 * - Form state management
 * - Form validation
 * - Employee ID generation
 * - Form submission handling
 * - Error handling
 * 
 * @returns {Object} Hook methods and state
 * @property {FormData} formData - Current form data state
 * @property {Object} errors - Form validation errors
 * @property {boolean} loading - Loading state
 * @property {Function} handleChange - Form field change handler
 * @property {Function} handleSubmit - Form submission handler
 */
export const useOnboardingLogic = () => {
  const [formData, setFormData] = useState(INITIAL_FORM_STATE);
  const [errors, setErrors] = useState({});
  const [loading, setLoading] = useState(false);

  /**
   * Validates form data and sets validation errors
   * @param {FormData} data - Form data to validate
   * @returns {boolean} Whether the form is valid
   */
  const validateForm = (data) => {
    const newErrors = {};
    const required = [
      'first_name',
      'last_name',
      'email',
      'phone',
      'date_of_birth',
      'start_date',
      'department',
      'position',
      'reporting_to'
    ];

    required.forEach(field => {
      if (!data[field]) {
        newErrors[field] = 'This field is required';
      }
    });

    // Email validation
    if (data.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(data.email)) {
      newErrors.email = 'Please enter a valid email address';
    }

    // Phone validation
    if (data.phone && !/^\d{10}$/.test(data.phone.replace(/\D/g, ''))) {
      newErrors.phone = 'Please enter a valid 10-digit phone number';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  /**
   * Handles changes to form fields
   * @param {React.ChangeEvent<HTMLInputElement>} e - Change event
   */
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value
    }));
  };

  /**
   * Handles form submission, generates employee ID, and submits data to API
   * @param {React.FormEvent} e - Form submission event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    console.log('Submit button clicked!');
    
    // Generate employee ID using initials + DOB
    const firstInitial = formData.first_name.charAt(0).toUpperCase();
    const lastInitial = formData.last_name.charAt(0).toUpperCase();
    const dob = formData.date_of_birth.replace(/-/g, '');  // Remove dashes from date
    const employeeId = `${firstInitial}${lastInitial}${dob}`;

    const submitData = {
      ...formData,
      employee_id: employeeId
    };
    
    console.log('Submitting data with employee ID:', submitData);
    
    try {
      const response = await axios({
        method: 'post',
        url: API_ENDPOINTS.ONBOARDING,
        data: submitData,
        headers: {
          'Content-Type': 'application/json'
        }
      });
      console.log('Response:', response);
      if (response.status === 200) {
        alert('Employee onboarding completed successfully!');
        setFormData(INITIAL_FORM_STATE);
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Error submitting form: ' + (error.response?.data?.detail || 'Please try again'));
    }
  };

  return {
    formData,
    errors,
    loading,
    handleChange,
    handleSubmit
  };
}; 