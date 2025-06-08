/**
 * @fileoverview Support component for handling customer support requests.
 * Provides a form interface for users to submit support inquiries.
 */

import React, { useState } from 'react';
import './Support.css';  // Change to import local CSS file

/**
 * @typedef {Object} FormData
 * @property {string} name - User's full name
 * @property {string} email - User's email address
 * @property {string} subject - Support request subject
 * @property {string} message - Detailed support request message
 */

/**
 * @typedef {'idle'|'sending'|'success'|'error'} SubmissionStatus
 */

/**
 * Support component that renders a contact form.
 * Features include:
 * - Form validation
 * - Submission status feedback
 * - Error handling
 * - Form field persistence
 * 
 * @returns {React.Element} Rendered Support component
 */
const Support = () => {
  /**
   * Form data state with typed fields.
   * @type {[FormData, React.Dispatch<React.SetStateAction<FormData>>]}
   */
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    subject: '',
    message: ''
  });

  /**
   * Submission status state.
   * @type {[SubmissionStatus, React.Dispatch<React.SetStateAction<SubmissionStatus>>]}
   */
  const [status, setStatus] = useState('idle');

  /**
   * Handles form field changes.
   * Updates formData state with new field values.
   * 
   * @param {React.ChangeEvent<HTMLInputElement|HTMLTextAreaElement>} e - Change event
   */
  const handleChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  /**
   * Handles form submission.
   * Sends support request to API and manages submission status.
   * 
   * @async
   * @param {React.FormEvent<HTMLFormElement>} e - Form submission event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    setStatus('sending');

    try {
      const response = await fetch('/api/support/submit', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData)
      });

      if (response.ok) {
        setStatus('success');
        setFormData({ name: '', email: '', subject: '', message: '' });
      } else {
        setStatus('error');
      }
    } catch (error) {
      setStatus('error');
    }
  };

  return (
    <div className="bm-support-container">
      <h1 className="bm-support-title">BlackMatter Support</h1>
      <div className="bm-support-intro">
        <p>Welcome to BlackMatter Support. We're here to help you with any questions or concerns about our services.</p>
        <p>Our team typically responds within 24-48 business hours.</p>
      </div>
      <div className="bm-support-content">
        <form onSubmit={handleSubmit} className="bm-support-form">
          <div className="bm-form-group">
            <label htmlFor="name">Full Name</label>
            <input
              type="text"
              id="name"
              name="name"
              value={formData.name}
              onChange={handleChange}
              required
              placeholder="Enter your full name"
            />
          </div>
          
          <div className="bm-form-group">
            <label htmlFor="email">Email Address</label>
            <input
              type="email"
              id="email"
              name="email"
              value={formData.email}
              onChange={handleChange}
              required
              placeholder="Enter your email address"
            />
          </div>
          
          <div className="bm-form-group">
            <label htmlFor="subject">Subject</label>
            <input
              type="text"
              id="subject"
              name="subject"
              value={formData.subject}
              onChange={handleChange}
              required
              placeholder="What is your inquiry about?"
            />
          </div>
          
          <div className="bm-form-group">
            <label htmlFor="message">Message</label>
            <textarea
              id="message"
              name="message"
              value={formData.message}
              onChange={handleChange}
              required
              rows="5"
              placeholder="Please provide as much detail as possible..."
            />
          </div>

          <button type="submit" className="bm-support-button" disabled={status === 'sending'}>
            {status === 'sending' ? 'Sending...' : 'Submit Request'}
          </button>

          {status === 'success' && (
            <div className="bm-support-success">
              Thank you for contacting BlackMatter Support! We'll get back to you shortly.
            </div>
          )}
          
          {status === 'error' && (
            <div className="bm-support-error">
              We couldn't send your message at this time. Please try again or email us directly at support@blackmatter.com
            </div>
          )}
        </form>
      </div>
    </div>
  );
};

export default Support;
