/**
 * @fileoverview Survey component for collecting and managing user responses.
 * Provides a multi-section survey interface with validation, progress tracking,
 * and response persistence.
 */

import React, { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';
import { getCurrentUser, fetchUserAttributes } from 'aws-amplify/auth';
import './Survey2.css';

/**
 * @typedef {Object} Question
 * @property {string} id - Unique identifier for the question
 * @property {string} section - Section name the question belongs to
 * @property {number} sectionOrder - Order of the section
 * @property {number} order - Order within the section
 * @property {string} text - Question text
 * @property {string} type - Question type (e.g., 'text', 'radio', 'checkbox')
 * @property {boolean} required - Whether the question requires an answer
 * @property {Array<string>} [options] - Available options for multiple choice questions
 */

/**
 * @typedef {Object} UserInfo
 * @property {string} username - User's username
 * @property {string} email - User's email address
 * @property {string} sub - User's unique identifier
 * @property {string} client_id - Client ID associated with the user
 */

/**
 * Survey component that manages a multi-section questionnaire.
 * Features include:
 * - Section-based navigation
 * - Answer validation
 * - Progress tracking
 * - Local storage persistence
 * - Review mode
 * - Submission handling
 * 
 * @returns {React.Element} Rendered Survey component
 */
function Survey() {
  const navigate = useNavigate();
  const { isAuthenticated, getAccessToken } = useAuth();
  const [questions, setQuestions] = useState([]);
  const [answers, setAnswers] = useState({});
  const [loading, setLoading] = useState(true);
  const [reviewMode, setReviewMode] = useState(false);
  const [currentSection, setCurrentSection] = useState(0);
  const [currentQuestion, setCurrentQuestion] = useState(0);
  const [totalQuestions, setTotalQuestions] = useState(0);
  const [error, setError] = useState(null);
  const [userInfo, setUserInfo] = useState(null);
  const [submitted, setSubmitted] = useState(false);
  const [invalidFields, setInvalidFields] = useState([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showConfirmationModal, setShowConfirmationModal] = useState(false);
  const [showErrorModal, setShowErrorModal] = useState(false);
  const [submissionError, setSubmissionError] = useState(null);
  const questionRefs = useRef({});
  const localStorageKey = 'survey_answers';
  const summaryRef = useRef(null);

  /**
   * Initializes the survey component.
   * Checks authentication, loads user data, and restores saved answers.
   */
  useEffect(() => {
    const init = async () => {
      if (!isAuthenticated) {
        navigate('/survey-login');
        return;
      }
      
      try {
        const { username, userId, signInDetails } = await getCurrentUser();
        const userAttributes = await fetchUserAttributes();
        
        console.log('User attributes:', userAttributes); // Debug log
        
        const userInfoData = {
          username,
          email: signInDetails.loginId,
          sub: userId,
          // Prioritize custom:ClientID over custom:UserID
          client_id: userAttributes['custom:ClientID'] || userAttributes['custom:UserID'] || userId
        };
        
        console.log('User info data:', userInfoData); // Debug log
        
        setUserInfo(userInfoData);
        
        // Create a unique localStorage key based on the user
        const userStorageKey = `${localStorageKey}_${userId}`;
        
        // Load saved answers from localStorage if available
        const savedAnswers = localStorage.getItem(userStorageKey);
        if (savedAnswers) {
          try {
            setAnswers(JSON.parse(savedAnswers));
          } catch (e) {
            console.error('Error parsing saved answers:', e);
            // Clear invalid saved data
            localStorage.removeItem(userStorageKey);
          }
        }
        
        fetchQuestions();
      } catch (error) {
        console.error('Error initializing survey:', error);
        navigate('/survey-login');
      }
    };

    init();
  }, [isAuthenticated, navigate]);

  /**
   * Persists answers to local storage whenever they change.
   * Uses a user-specific storage key to prevent conflicts.
   */
  useEffect(() => {
    if (userInfo?.sub) {
      const userStorageKey = `${localStorageKey}_${userInfo.sub}`;
      localStorage.setItem(userStorageKey, JSON.stringify(answers));
    }
  }, [answers, userInfo]);

  /**
   * Retrieves authentication headers for API requests.
   * Redirects to login if authentication fails.
   * 
   * @async
   * @returns {Promise<Object|null>} Headers object or null if auth fails
   */
  const getAuthHeaders = async () => {
    try {
      const token = await getAccessToken();
      if (!token) {
        throw new Error('No auth token available');
      }
      return {
        headers: {
          Authorization: `Bearer ${token}`
        }
      };
    } catch (error) {
      console.error('Error getting auth headers:', error);
      navigate('/survey-login');
      return null;
    }
  };

  /**
   * Fetches survey questions from the API.
   * Sorts questions by section and order.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const fetchQuestions = async () => {
    try {
      const authHeaders = await getAuthHeaders();
      if (!authHeaders) {
        throw new Error('Failed to get auth headers');
      }
      
      const response = await axios.get(API_ENDPOINTS.SURVEY.QUESTIONS, authHeaders);
      
      // Ensure we have an array of questions
      let questionsData = [];
      if (response.data && Array.isArray(response.data)) {
        questionsData = response.data;
      } else if (response.data && typeof response.data === 'object') {
        questionsData = [response.data];
      }
      
      // Sort questions by section order and question order
      const sortedQuestions = questionsData.sort((a, b) => {
        const sectionCompare = (a.sectionOrder || 0) - (b.sectionOrder || 0);
        if (sectionCompare !== 0) {
          return sectionCompare;
        }
        return (a.order || 0) - (b.order || 0);
      });
      
      console.log('Sorted questions:', sortedQuestions); // Debug log
      setQuestions(sortedQuestions);
      setTotalQuestions(sortedQuestions.length);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching questions:', error);
      setError('Failed to load survey questions');
      setLoading(false);
    }
  };

  /**
   * Groups questions by their section.
   * Creates an object with section names as keys and arrays of questions as values.
   * 
   * @type {Object.<string, Question[]>}
   */
  const questionsBySection = questions.reduce((acc, question) => {
    if (!acc[question.section]) {
      acc[question.section] = [];
    }
    acc[question.section].push(question);
    return acc;
  }, {});

  const sections = Object.keys(questionsBySection);

  /**
   * Handles answer changes for a question.
   * Updates the answers state and removes validation errors.
   * 
   * @param {string} questionId - ID of the answered question
   * @param {*} value - New answer value
   */
  const handleAnswer = (questionId, value) => {
    setAnswers(prev => ({
      ...prev,
      [questionId]: value
    }));
    
    // Remove this question from invalidFields if it was previously marked as invalid
    if (invalidFields.includes(questionId)) {
      setInvalidFields(prev => prev.filter(id => id !== questionId));
    }
  };

  /**
   * Scrolls to the first question with validation errors.
   * Used to guide users to questions requiring attention.
   */
  const scrollToFirstInvalidQuestion = () => {
    if (invalidFields.length > 0) {
      const firstInvalidId = invalidFields[0];
      const element = questionRefs.current[firstInvalidId];
      if (element) {
        element.scrollIntoView({ behavior: 'smooth', block: 'center' });
        element.focus();
      }
    }
  };

  useEffect(() => {
    if (invalidFields.length > 0) {
      scrollToFirstInvalidQuestion();
    }
  }, [invalidFields]);

  /**
   * Validates questions in the current section.
   * Checks for required questions that haven't been answered.
   * 
   * @returns {boolean} True if all required questions are answered
   */
  const validateCurrentSection = () => {
    const currentSectionQuestions = questionsBySection[sections[currentSection]];
    const unansweredRequiredQuestions = currentSectionQuestions
      .filter(q => q.required && !answers[q.id])
      .map(q => q.id);

    if (unansweredRequiredQuestions.length > 0) {
      setInvalidFields(unansweredRequiredQuestions);
      setError('Please answer all required questions marked with *');
      return false;
    }
    return true;
  };

  /**
   * Validates all questions across all sections.
   * Used before final submission.
   * 
   * @returns {boolean} True if all required questions are answered
   */
  const validateAllSections = () => {
    const unansweredRequiredQuestions = questions
      .filter(q => q.required && !answers[q.id])
      .map(q => q.id);

    if (unansweredRequiredQuestions.length > 0) {
      setInvalidFields(unansweredRequiredQuestions);
      setError('Please answer all required questions before submitting');
      
      // Find which section has the first unanswered question
      const firstUnansweredId = unansweredRequiredQuestions[0];
      const firstUnansweredQuestion = questions.find(q => q.id === firstUnansweredId);
      
      if (firstUnansweredQuestion) {
        // Find the section index
        const sectionIndex = sections.findIndex(s => s === firstUnansweredQuestion.section);
        if (sectionIndex !== -1 && sectionIndex !== currentSection) {
          setCurrentSection(sectionIndex);
          
          // Calculate current question position
          let questionPosition = 0;
          for (let i = 0; i < sectionIndex; i++) {
            questionPosition += questionsBySection[sections[i]].length;
          }
          setCurrentQuestion(questionPosition);
        }
      }
      
      setReviewMode(false);
      return false;
    }
    return true;
  };

  /**
   * Handles navigation to the next section.
   * Validates current section before proceeding.
   */
  const handleNext = () => {
    if (!validateCurrentSection()) {
      return;
    }

    setError(null);
    setInvalidFields([]);
    
    if (currentSection < sections.length - 1) {
      setCurrentSection(prev => prev + 1);
      setCurrentQuestion(prev => prev + questionsBySection[sections[currentSection]].length);
      window.scrollTo(0, 0);
    } else {
      // Instead of showing summary in a new page, enable review mode
      setReviewMode(true);
      // Scroll to the summary section
      setTimeout(() => {
        if (summaryRef.current) {
          summaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }, 100);
    }
  };

  /**
   * Handles navigation to the previous section.
   * Updates question counter and scrolls to top.
   */
  const handlePrevious = () => {
    setError(null);
    setInvalidFields([]);
    setReviewMode(false);
    
    if (currentSection > 0) {
      setCurrentSection(prev => prev - 1);
      const previousSectionQuestions = questionsBySection[sections[currentSection - 1]];
      setCurrentQuestion(prev => prev - previousSectionQuestions.length);
      window.scrollTo(0, 0);
    }
  };

  /**
   * Handles final survey submission.
   * Validates all answers and sends data to API.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleSubmit = async () => {
    // Validate one more time before submission
    if (!validateAllSections()) {
      return;
    }
    
    setIsSubmitting(true);
    setError(null);
    setSubmissionError(null);
    
    try {
      const authHeaders = await getAuthHeaders();
      if (!authHeaders) {
        throw new Error('Failed to get auth headers');
      }

      const response = await axios.post(
        API_ENDPOINTS.SURVEY.SUBMIT,
        {
          client_id: userInfo?.client_id,
          user_id: userInfo?.sub,
          username: userInfo?.username,
          email: userInfo?.email,
          responses: answers,
          timestamp: new Date().toISOString()
        },
        authHeaders
      );

      if (response.status === 200) {
        // Clear answers from localStorage on successful submission
        if (userInfo?.sub) {
          localStorage.removeItem(`${localStorageKey}_${userInfo.sub}`);
        }
        setSubmitted(true);
        setShowConfirmationModal(true);
      } else {
        throw new Error('Failed to submit survey');
      }
    } catch (error) {
      console.error('Error submitting survey:', error);
      setSubmissionError(error.response?.data?.detail || 'Failed to submit survey. Please try again.');
      setShowErrorModal(true);
    } finally {
      setIsSubmitting(false);
    }
  };

  /**
   * Handles adding a new section to the survey.
   * 
   * @async
   * @param {string} sectionName - Name of the new section
   * @returns {Promise<void>}
   */
  const handleAddSection = async (sectionName) => {
    try {
      const authHeaders = await getAuthHeaders();
      if (!authHeaders) {
        throw new Error('Failed to get auth headers');
      }
      
      await axios.post(
        `${API_ENDPOINTS.SURVEY.QUESTIONS}/sections/add`,
        { name: sectionName },
        authHeaders
      );
      
      // Refresh sections after adding
      fetchQuestions();
    } catch (error) {
      console.error('Error adding section:', error);
      setError('Failed to add section');
    }
  };

  /**
   * Handles section form submission.
   * Prevents default form behavior.
   * 
   * @param {Event} e - Form submission event
   */
  const handleSectionSubmit = (e) => {
    e.preventDefault();
    const sectionName = e.target.elements.sectionName.value.trim();
    if (sectionName) {
      handleAddSection(sectionName);
      e.target.elements.sectionName.value = '';
    }
  };

  /**
   * Renders a single question based on its type.
   * Supports various question types with appropriate input controls.
   * 
   * @param {Question} question - Question object to render
   * @returns {React.Element} Rendered question component
   */
  function renderQuestion(question) {
    const value = answers[question.id];
    const isInvalid = invalidFields.includes(question.id);
    
    // Define a common class for input fields based on validation state
    const inputClass = `answer-input ${isInvalid ? 'invalid-field' : ''}`;
    
    switch(question.type) {
      case 'text':
      case 'number':
        return (
          <input
            type={question.type}
            value={value || ''}
            onChange={(e) => handleAnswer(question.id, e.target.value)}
            className={inputClass}
            required={question.required}
            placeholder={`Enter ${question.type === 'text' ? 'text' : 'number'}...`}
            ref={el => questionRefs.current[question.id] = el}
          />
        );
      
      case 'select':
        return (
          <select 
            value={value || ''}
            onChange={(e) => handleAnswer(question.id, e.target.value)}
            className={inputClass}
            required={question.required}
            ref={el => questionRefs.current[question.id] = el}
          >
            <option value="">Select an option</option>
            {question.options?.map(option => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        );
      
      case 'checkbox':
        return (
          <div 
            className={`survey-checkbox-group ${isInvalid ? 'invalid-field' : ''}`}
            ref={el => questionRefs.current[question.id] = el}
          >
            {question.options?.map(option => (
              <label key={option} className="survey-checkbox-label">
                <input
                  type="checkbox"
                  checked={value?.includes(option) || false}
                  onChange={(e) => {
                    const currentAnswers = value || [];
                    const newAnswers = e.target.checked
                      ? [...currentAnswers, option]
                      : currentAnswers.filter(item => item !== option);
                    handleAnswer(question.id, newAnswers);
                  }}
                />
                {option}
              </label>
            ))}
          </div>
        );
      
      case 'file':
        return (
          <div 
            className={`file-upload ${isInvalid ? 'invalid-field' : ''}`}
            ref={el => questionRefs.current[question.id] = el}
          >
            <input
              type="file"
              accept={question.fileTypes?.join(',') || '*'}
              onChange={async (e) => {
                const file = e.target.files[0];
                if (!file) return;
                
                try {
                  const formData = new FormData();
                  formData.append('file', file);
                  formData.append('questionId', question.id);
                  
                  const authHeaders = await getAuthHeaders();
                  const response = await axios.post(
                    API_ENDPOINTS.SURVEY.QUESTIONS + '/upload',
                    formData,
                    {
                      ...authHeaders,
                      headers: {
                        ...authHeaders.headers,
                        'Content-Type': 'multipart/form-data'
                      }
                    }
                  );
                  handleAnswer(question.id, response.data.filename);
                } catch (error) {
                  console.error('Error uploading file:', error);
                  setError('Failed to upload file');
                }
              }}
            />
            {value && <div className="file-preview">Uploaded: {value}</div>}
          </div>
        );
      
      default:
        return null;
    }
  }

  if (loading || !userInfo) {
    return <div className="loading">Loading survey...</div>;
  }

  if (!sections.length) {
    return <div className="error-message">No survey questions available.</div>;
  }

  return (
    <div className="survey-page-wrapper">
      <div className="survey-container">
        <div className="progress-container">
          <div className="progress-bar">
            <div 
              className="progress" 
              style={{ width: `${(currentQuestion / totalQuestions) * 100}%` }}
            />
          </div>
          <div className="progress-percentage">
            {Math.round((currentQuestion / totalQuestions) * 100)}%
          </div>
        </div>

        <div className="section-header">
          <h2>{sections[currentSection]}</h2>
        </div>

        {error && <div className="error-message">{error}</div>}

        <div className="section-questions">
          {questionsBySection[sections[currentSection]].map(question => {
            const isInvalid = invalidFields.includes(question.id);
            return (
              <div 
                key={question.id} 
                className={`question-card ${isInvalid ? 'question-invalid' : ''}`}
              >
                <div className="question-header">
                  <h3>
                    {question.question}
                    {question.required && <span className="required-indicator">*</span>}
                  </h3>
                  {isInvalid && (
                    <div className="validation-error">This question requires an answer</div>
                  )}
                </div>
                {renderQuestion(question)}
              </div>
            );
          })}
        </div>

        {/* Navigation buttons for regular mode */}
        {!reviewMode && (
          <div className="button-group">
            {currentSection > 0 && (
              <button onClick={handlePrevious} className="nav-button">
                Previous Section
              </button>
            )}
            <button onClick={handleNext} className="nav-button primary">
              {currentSection === sections.length - 1 ? 'Review Answers' : 'Next Section'}
            </button>
          </div>
        )}

        {/* Review and Submit Section - Only visible in review mode */}
        {reviewMode && (
          <div className="summary-container" ref={summaryRef}>
            <h2 className="summary-title">Review Your Answers</h2>
            
            {sections.map(section => (
              <div key={section} className="summary-section">
                <h3>{section}</h3>
                {questionsBySection[section].map(question => {
                  const isInvalid = invalidFields.includes(question.id);
                  return (
                    <div key={question.id} className={`summary-item ${isInvalid ? 'invalid-field' : ''}`}>
                      <h4>
                        {question.question}
                        {question.required && <span className="required-indicator">*</span>}
                        {isInvalid && <span className="validation-message">Required</span>}
                      </h4>
                      <p>
                        {Array.isArray(answers[question.id])
                          ? answers[question.id].join(', ')
                          : answers[question.id] || 'Not answered'}
                      </p>
                    </div>
                  );
                })}
              </div>
            ))}

            <div className="button-group">
              <button 
                onClick={() => setReviewMode(false)} 
                className="nav-button"
              >
                Continue Editing
              </button>
              <button 
                onClick={handleSubmit} 
                className="nav-button primary"
                disabled={isSubmitting}
              >
                {isSubmitting ? 'Submitting...' : 'Submit Survey'}
              </button>
            </div>
          </div>
        )}

        {/* Error Modal - shown when submission fails */}
        {showErrorModal && (
          <div className="modal-overlay">
            <div className="modal-content error">
              <h2>Submission Failed</h2>
              <p>{submissionError || 'There was an error submitting your survey. Your answers have been saved locally.'}</p>
              <div className="button-group">
                <button
                  onClick={() => {
                    setShowErrorModal(false);
                    setSubmissionError(null);
                  }}
                  className="nav-button"
                >
                  Continue Editing
                </button>
                <button
                  onClick={() => {
                    setShowErrorModal(false);
                    handleSubmit();
                  }}
                  className="nav-button primary"
                >
                  Retry Submission
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Confirmation Modal - only visible when showConfirmationModal is true */}
        {showConfirmationModal && (
          <div className="modal-overlay">
            <div className="modal-content">
              <h2>Survey Submitted Successfully!</h2>
              <p>Thank you for completing the survey.</p>
              <button
                onClick={() => {
                  setShowConfirmationModal(false);
                  // Redirect to FAQ page
                  window.location.href = 'https://faq.snapped.cc';
                }}
                className="nav-button primary"
              >
                Continue to FAQ
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default Survey;
