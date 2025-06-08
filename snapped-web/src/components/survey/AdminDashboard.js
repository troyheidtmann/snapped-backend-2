/**
 * @fileoverview AdminDashboard component for managing survey questions and responses.
 * Provides functionality for creating, organizing, and viewing survey content.
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { DragDropContext, Droppable, Draggable } from '@hello-pangea/dnd';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';
import './AdminDashboard.css';

/**
 * @typedef {Object} Question
 * @property {string} id - Unique identifier for the question
 * @property {string} question - Question text
 * @property {string} type - Question type (text, radio, checkbox, etc.)
 * @property {boolean} required - Whether the question is required
 * @property {Array<string>} options - Options for multiple choice questions
 * @property {string} section - Section the question belongs to
 * @property {number} order - Order within the section
 * @property {Array<string>} fileTypes - Allowed file types for file upload questions
 */

/**
 * @typedef {Object} Response
 * @property {string} user_id - ID of the user who submitted the response
 * @property {string} timestamp - Timestamp of the submission
 * @property {Object.<string, *>} answers - Map of question IDs to answers
 */

/**
 * AdminDashboard component for managing survey content.
 * Features include:
 * - Question creation and editing
 * - Section management
 * - Response viewing
 * - Drag-and-drop organization
 * 
 * @returns {React.Element} Rendered AdminDashboard component
 */
const AdminDashboard = () => {
  const { currentUser, getAccessToken } = useAuth();
  const [activeTab, setActiveTab] = useState('responses'); // responses, addQuestions, organizeQuestions
  const [questions, setQuestions] = useState([]);
  const [detailedResponses, setDetailedResponses] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState('');
  const [selectedResponse, setSelectedResponse] = useState(null);
  const [newQuestion, setNewQuestion] = useState({
    id: '',
    question: '',
    type: 'text',
    required: false,
    options: [],
    section: '',
    order: 0,
    fileTypes: []
  });
  const [editingQuestion, setEditingQuestion] = useState(null);
  const [sections, setSections] = useState([]);
  const [newSection, setNewSection] = useState('');
  const [sectionOrder, setSectionOrder] = useState([]);

  /**
   * Fetches initial data and sets up question ordering.
   */
  useEffect(() => {
    fetchData();
    handleUpdateQuestionOrder();
  }, []);

  /**
   * Updates sections when questions change.
   * Extracts unique sections and maintains order.
   */
  useEffect(() => {
    if (questions.length > 0) {
      const uniqueSections = Array.from(new Set(questions.map(q => q.section)));
      setSections(uniqueSections);
      setSectionOrder(uniqueSections);
    }
  }, [questions]);

  /**
   * Retrieves authentication headers for API requests.
   * 
   * @async
   * @returns {Promise<Object>} Headers object with auth token
   */
  const getAuthHeaders = async () => {
    const token = await getAccessToken();
    return {
      headers: {
        Authorization: `Bearer ${token}`
      }
    };
  };

  /**
   * Fetches questions and responses data from the API.
   * Updates state with fetched data and handles errors.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const fetchData = async () => {
    try {
      const authHeaders = await getAuthHeaders();
      
      const [questionsRes, detailedRes] = await Promise.all([
        axios.get(API_ENDPOINTS.SURVEY.QUESTIONS, authHeaders),
        axios.get(API_ENDPOINTS.SURVEY.RESPONSES.DETAILED, authHeaders)
      ]);

      setQuestions(questionsRes.data);
      setDetailedResponses(detailedRes.data);

      // Update sections
      const uniqueSections = Array.from(new Set(questionsRes.data.map(q => q.section)));
      setSections(uniqueSections);
      setSectionOrder(uniqueSections);
    } catch (error) {
      console.error('Error fetching data:', error);
      if (error.response?.status === 403) {
        alert('You do not have admin access to view this page');
      } else if (error.response?.status === 500) {
        alert('Server error while fetching data. Please try again later.');
      }
    }
  };

  /**
   * Handles user selection for viewing responses.
   * Updates selected response state with user's data.
   * 
   * @async
   * @param {string} userId - ID of the selected user
   * @returns {Promise<void>}
   */
  const handleUserSelect = async (userId) => {
    setSelectedUserId(userId);
    if (!userId) return;

    try {
      const userResponse = detailedResponses.find(response => 
        response.user_id === userId || 
        response.timestamp === userId
      );

      if (userResponse) {
        setSelectedResponse(userResponse);
      } else {
        setSelectedResponse(null);
      }
    } catch (error) {
      console.error('Error selecting user:', error);
      setSelectedResponse(null);
    }
  };

  /**
   * Formats timestamp into localized string.
   * 
   * @param {string} timestamp - ISO timestamp string
   * @returns {string} Formatted date and time string
   */
  const formatTimestamp = (timestamp) => {
    return new Date(timestamp).toLocaleString();
  };

  /**
   * Handles adding a new question to the survey.
   * Creates question with unique ID and updates backend.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleAddQuestion = async () => {
    try {
      // Add an ID if not present
      if (!newQuestion.id) {
        newQuestion.id = `question_${Date.now()}`;
      }
      
      const authHeaders = await getAuthHeaders();
      await axios.post(
        API_ENDPOINTS.SURVEY.ADMIN.ADD_QUESTION,
        newQuestion,
        authHeaders
      );

      // Reset form
      setNewQuestion({
        id: '',
        question: '',
        type: 'text',
        required: false,
        options: [],
        section: '',
        order: 0
      });

      // Refresh questions
      await fetchData();
    } catch (error) {
      console.error('Error adding question:', error);
      alert('Failed to add question');
    }
  };

  /**
   * Updates questions in the backend and state.
   * 
   * @async
   * @param {Array<Question>} updatedQuestions - Array of questions to update
   * @returns {Promise<void>}
   */
  const handleUpdateQuestions = async (updatedQuestions) => {
    try {
      const authHeaders = await getAuthHeaders();
      await axios.post(
        API_ENDPOINTS.SURVEY.ADMIN.UPDATE_QUESTIONS,
        updatedQuestions,
        authHeaders
      );
      setQuestions(updatedQuestions);
    } catch (error) {
      console.error('Error updating questions:', error);
      alert('Failed to update questions');
    }
  };

  /**
   * Retrieves question text by ID.
   * 
   * @param {string} questionId - ID of the question
   * @returns {string} Question text or ID if not found
   */
  const getQuestionText = (questionId) => {
    const question = questions.find(q => q.id === questionId);
    return question ? question.question : questionId;
  };

  /**
   * Handles drag and drop reordering of questions and sections.
   * Updates order in UI and persists changes to backend.
   * 
   * @async
   * @param {Object} result - Drag and drop result object
   * @returns {Promise<void>}
   */
  const handleDragEnd = async (result) => {
    if (!result.destination) return;
    
    const { source, destination, type } = result;
    
    // Create a deep copy of questions to work with
    const updatedQuestions = [...questions];
    
    if (type === 'SECTION') {
      // Handle section reordering
      const newSectionOrder = Array.from(sectionOrder);
      const [movedSection] = newSectionOrder.splice(source.index, 1);
      newSectionOrder.splice(destination.index, 0, movedSection);
      
      // Optimistically update UI
      setSectionOrder(newSectionOrder);
      
      // Update sectionOrder for all questions in the moved sections
      updatedQuestions.forEach(question => {
        question.sectionOrder = newSectionOrder.indexOf(question.section);
      });
    } else {
      // Handle question reordering within a section
      const sourceSection = source.droppableId;
      const destSection = destination.droppableId;
      
      // Get questions for the relevant sections
      const sectionQuestions = updatedQuestions.filter(q => q.section === sourceSection);
      
      // Remove question from source position
      const [movedQuestion] = sectionQuestions.splice(source.index, 1);
      
      if (sourceSection === destSection) {
        // Moving within the same section
        sectionQuestions.splice(destination.index, 0, movedQuestion);
        
        // Update order for all questions in the section
        sectionQuestions.forEach((q, index) => {
          q.order = index;
        });
      } else {
        // Moving between sections
        movedQuestion.section = destSection;
        movedQuestion.sectionOrder = sectionOrder.indexOf(destSection);
        
        // Get questions in destination section
        const destSectionQuestions = updatedQuestions.filter(q => q.section === destSection);
        destSectionQuestions.splice(destination.index, 0, movedQuestion);
        
        // Update order for all questions in both sections
        destSectionQuestions.forEach((q, index) => {
          q.order = index;
        });
      }
    }
    
    // Sort the questions array by section order and question order
    updatedQuestions.sort((a, b) => {
      const sectionCompare = a.sectionOrder - b.sectionOrder;
      if (sectionCompare !== 0) return sectionCompare;
      return a.order - b.order;
    });
    
    // Optimistically update UI
    setQuestions(updatedQuestions);
    
    try {
      // Update the backend
      const authHeaders = await getAuthHeaders();
      await axios.post(
        API_ENDPOINTS.SURVEY.ADMIN.UPDATE_QUESTIONS,
        updatedQuestions,
        authHeaders
      );
    } catch (error) {
      console.error('Error updating questions:', error);
      // Revert optimistic update on error
      fetchData();
      alert('Failed to save question order. Please try again.');
    }
  };

  /**
   * Sets question for editing.
   * 
   * @param {Question} question - Question to edit
   */
  const handleEditQuestion = (question) => {
    setEditingQuestion(question);
  };

  /**
   * Saves edited question to backend.
   * Updates questions array with edited version.
   * 
   * @async
   * @param {Question} editedQuestion - Updated question data
   * @returns {Promise<void>}
   */
  const handleSaveEdit = async (editedQuestion) => {
    try {
      const updatedQuestions = questions.map(q => 
        q.id === editedQuestion.id ? editedQuestion : q
      );
      await handleUpdateQuestions(updatedQuestions);
      setEditingQuestion(null);
    } catch (error) {
      console.error('Error saving question:', error);
      alert('Failed to save question');
    }
  };

  /**
   * Updates selected section in new question form.
   * 
   * @param {string} value - Selected section name
   */
  const handleSectionSelect = (value) => {
    if (value === 'add_new') {
      const newSectionName = prompt('Enter new section name:');
      if (newSectionName?.trim()) {
        setSections([...sections, newSectionName.trim()]);
        setNewQuestion({
          ...newQuestion,
          section: newSectionName.trim()
        });
      }
    } else {
      setNewQuestion({
        ...newQuestion,
        section: value
      });
    }
  };

  /**
   * Adds a new section to the survey.
   * Updates section order and persists changes.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleAddSection = async () => {
    if (newSection.trim()) {
      if (!sections.includes(newSection.trim())) {
        try {
          const authHeaders = await getAuthHeaders();
          await axios.post(
            API_ENDPOINTS.SURVEY.ADMIN.ADD_QUESTION,
            {
              id: `section_${Date.now()}`,
              section: newSection.trim(),
              question: `Welcome to ${newSection.trim()}`,
              type: 'text',
              required: false,
              order: 0,
              sectionOrder: sections.length,
              options: []
            },
            authHeaders
          );
          
          // Refresh data to get updated sections
          await fetchData();
          
          // Clear input
          setNewSection('');
        } catch (error) {
          console.error('Error adding section:', error);
          alert('Failed to add section');
        }
      } else {
        alert('This section already exists!');
      }
    }
  };

  /**
   * Handles drag and drop reordering of sections.
   * Updates section order in UI and backend.
   * 
   * @async
   * @param {Object} result - Drag and drop result object
   * @returns {Promise<void>}
   */
  const handleSectionDragEnd = async (result) => {
    if (!result.destination) return;
    
    const reorderedSections = Array.from(sectionOrder);
    const [movedSection] = reorderedSections.splice(result.source.index, 1);
    reorderedSections.splice(result.destination.index, 0, movedSection);
    
    setSectionOrder(reorderedSections);
    setSections(reorderedSections);
    
    try {
      const updatedQuestions = [...questions].map(question => ({
        ...question,
        sectionOrder: reorderedSections.indexOf(question.section)
      }));
      
      updatedQuestions.sort((a, b) => {
        const sectionCompare = reorderedSections.indexOf(a.section) - reorderedSections.indexOf(b.section);
        if (sectionCompare === 0) {
          return a.order - b.order;
        }
        return sectionCompare;
      });
      
      await handleUpdateQuestions(updatedQuestions);
      
      setQuestions(updatedQuestions);
    } catch (error) {
      console.error('Error updating section order:', error);
      setSectionOrder(sectionOrder);
      setSections(sections);
    }
  };

  /**
   * Deletes a section and its questions.
   * Updates questions array and section order.
   * 
   * @param {string} sectionToDelete - Name of section to delete
   */
  const handleDeleteSection = (sectionToDelete) => {
    const updatedSections = sections.filter(section => section !== sectionToDelete);
    setSections(updatedSections);
    
    const updatedQuestions = questions.filter(question => question.section !== sectionToDelete);
    handleUpdateQuestions(updatedQuestions);
  };

  /**
   * Updates question order based on section order.
   * Ensures questions maintain correct ordering within sections.
   * 
   * @async
   * @returns {Promise<void>}
   */
  const handleUpdateQuestionOrder = async () => {
    try {
      // Get current questions
      const updatedQuestions = [...questions];
      
      // Find the indices of the questions we want to swap
      const firstNameIndex = updatedQuestions.findIndex(q => q.question === 'Legal First Name');
      const lastNameIndex = updatedQuestions.findIndex(q => q.question === 'Legal Last Name');
      
      // Swap the questions
      if (firstNameIndex !== -1 && lastNameIndex !== -1) {
        // Swap the entire question objects but keep their IDs
        [updatedQuestions[firstNameIndex], updatedQuestions[lastNameIndex]] = 
        [updatedQuestions[lastNameIndex], updatedQuestions[firstNameIndex]];
        
        // Update the questions through the API
        await handleUpdateQuestions(updatedQuestions);
        
        // Refresh the questions
        fetchData();
      }
    } catch (error) {
      console.error('Error updating question order:', error);
    }
  };

  /**
   * Renders detailed view of a user's survey response.
   * 
   * @param {Response} response - Response data to render
   * @returns {React.Element} Rendered response details
   */
  const renderResponseDetails = (response) => {
    if (!response) return null;

    return (
      <div className="response-details">
        <h2>Response Details</h2>
        <div className="response-meta">
          <p>User ID: {response.user_id}</p>
          <p>Client ID: {response.client_id}</p>
          <p>Client Name: {response.client_name}</p>
          <p>Timestamp: {formatTimestamp(response.timestamp)}</p>
          {response.groups && (
            <p>User Groups: {response.groups.join(', ')}</p>
          )}
        </div>
        {response.client_info && (
          <div className="client-info">
            <h3>Client Information</h3>
            <p>Email: {response.client_info.email}</p>
            <p>Phone: {response.client_info.phone}</p>
            {/* Add other relevant client info fields */}
          </div>
        )}
        <table className="two-column-table">
          <thead>
            <tr>
              <th>Question</th>
              <th>Response</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(response.responses || {}).map(([key, value]) => (
              <tr key={key}>
                <td>{getQuestionText(key)}</td>
                <td>
                  {Array.isArray(value) ? value.join(', ') : value}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="survey-admin-container" style={{ minHeight: 'unset', background: 'transparent', padding: '0' }}>
      <div className="survey-section-header">
        <h1>Survey Admin Dashboard</h1>
        <div className="tabs">
          <button 
            className={`tab ${activeTab === 'responses' ? 'active' : ''}`}
            onClick={() => setActiveTab('responses')}
          >
            View Responses
          </button>
          <button 
            className={`tab ${activeTab === 'addQuestions' ? 'active' : ''}`}
            onClick={() => setActiveTab('addQuestions')}
          >
            Add Questions
          </button>
          <button 
            className={`tab ${activeTab === 'organizeQuestions' ? 'active' : ''}`}
            onClick={() => setActiveTab('organizeQuestions')}
          >
            Organize Questions
          </button>
        </div>
      </div>

      {activeTab === 'responses' && (
        <div className="survey-question-list">
          <div className="survey-section-header">
            <h2>Survey Responses Dashboard</h2>
            
            <select 
              value={selectedUserId} 
              onChange={(e) => handleUserSelect(e.target.value)}
              className="survey-input"
            >
              <option value="">Select a User</option>
              {detailedResponses.map((response, index) => {
                const name = response.responses?.enter_your_first_name_and_last_initial_follow_the_ || 
                            response.timestamp;
                
                return (
                  <option 
                    key={index} 
                    value={response.timestamp}
                  >
                    {name}
                  </option>
                );
              })}
            </select>
          </div>

          {selectedResponse && renderResponseDetails(selectedResponse)}
        </div>
      )}

      {activeTab === 'addQuestions' && (
        <div className="survey-section-container">
          <h2>Add New Question</h2>
          
          {/* Section Management */}
          <div className="survey-section-list">
            <h3>Manage Sections</h3>
            <DragDropContext onDragEnd={handleSectionDragEnd}>
              <Droppable droppableId="sections-list">
                {(provided) => (
                  <div 
                    {...provided.droppableProps}
                    ref={provided.innerRef}
                    className="survey-section-list"
                  >
                    {sectionOrder.map((section, index) => (
                      <Draggable 
                        key={section} 
                        draggableId={section} 
                        index={index}
                      >
                        {(provided, snapshot) => (
                          <div
                            ref={provided.innerRef}
                            {...provided.draggableProps}
                            className="survey-section-item"
                            style={{
                              ...provided.draggableProps.style,
                              background: snapshot.isDragging ? '#f0f0f0' : '#f8fafc'
                            }}
                          >
                            <div {...provided.dragHandleProps} className="survey-drag-handle">
                              ⋮⋮
                            </div>
                            <span>{section}</span>
                            <button
                              onClick={() => handleDeleteSection(section)}
                              className="survey-delete-button"
                            >
                              ×
                            </button>
                          </div>
                        )}
                      </Draggable>
                    ))}
                    {provided.placeholder}
                  </div>
                )}
              </Droppable>
            </DragDropContext>
            
            <div className="section-input-group">
              <input
                type="text"
                value={newSection}
                onChange={(e) => setNewSection(e.target.value)}
                placeholder="Enter new section name"
                className="survey-section-input"
              />
              <button 
                onClick={handleAddSection}
                type="button"
                className="survey-add-section-button"
              >
                Add Section
              </button>
            </div>
          </div>

          {/* Question Form */}
          <form onSubmit={async (e) => {
            e.preventDefault();
            await handleAddQuestion();
          }}>
            <div className="survey-form-group">
              <label className="survey-label">Section:</label>
              <select
                value={newQuestion.section}
                onChange={(e) => setNewQuestion({...newQuestion, section: e.target.value})}
                className="survey-input"
                required
              >
                <option value="">Select a Section</option>
                {sections.map((section, index) => (
                  <option key={index} value={section}>
                    {section}
                  </option>
                ))}
              </select>
            </div>
            <div className="survey-form-group">
              <label className="survey-label">Question Text:</label>
              <input
                type="text"
                value={newQuestion.question}
                onChange={(e) => setNewQuestion({...newQuestion, question: e.target.value})}
                className="survey-input"
                required
              />
            </div>
            <div className="survey-form-group">
              <label className="survey-label">Question Type:</label>
              <select
                value={newQuestion.type}
                onChange={(e) => setNewQuestion({...newQuestion, type: e.target.value})}
                className="survey-input"
                required
              >
                <option value="text">Text</option>
                <option value="number">Number</option>
                <option value="select">Select</option>
                <option value="checkbox">Checkbox</option>
                <option value="file">File Upload</option>
              </select>
            </div>
            {(newQuestion.type === 'select' || newQuestion.type === 'checkbox') && (
              <div className="survey-form-group">
                <label className="survey-label">Options (comma-separated):</label>
                <input
                  type="text"
                  value={newQuestion.options.join(', ')}
                  onChange={(e) => setNewQuestion({
                    ...newQuestion, 
                    options: e.target.value.split(',').map(opt => opt.trim())
                  })}
                  className="survey-input"
                  required
                />
              </div>
            )}
            <div className="survey-checkbox-group">
              <label className="survey-checkbox-label">
                <input
                  type="checkbox"
                  checked={newQuestion.required}
                  onChange={(e) => setNewQuestion({...newQuestion, required: e.target.checked})}
                />
                Required
              </label>
            </div>
            <button 
              type="submit"
              className="survey-add-section-button"
            >
              Add Question
            </button>
          </form>
        </div>
      )}

      {activeTab === 'organizeQuestions' && (
        <div className="survey-section-container">
          <h2>Organize Questions</h2>
          <DragDropContext onDragEnd={handleDragEnd}>
            <Droppable droppableId="sections" type="SECTION">
              {(provided) => (
                <div
                  {...provided.droppableProps}
                  ref={provided.innerRef}
                  className="sections-list"
                >
                  {sectionOrder.map((section, sectionIndex) => (
                    <Draggable
                      key={section}
                      draggableId={`section-${section}`}
                      index={sectionIndex}
                    >
                      {(provided, snapshot) => (
                        <div
                          ref={provided.innerRef}
                          {...provided.draggableProps}
                          className={`section-container ${snapshot.isDragging ? 'dragging' : ''}`}
                        >
                          <div className="section-header" {...provided.dragHandleProps}>
                            <h3>{section}</h3>
                          </div>
                          <Droppable droppableId={section} type="QUESTION">
                            {(provided, snapshot) => (
                              <div
                                ref={provided.innerRef}
                                {...provided.droppableProps}
                                className={`question-list ${snapshot.isDraggingOver ? 'dragging-over' : ''}`}
                              >
                                {questions
                                  .filter(q => q.section === section)
                                  .sort((a, b) => a.order - b.order)
                                  .map((question, index) => (
                                    <Draggable
                                      key={question.id}
                                      draggableId={question.id}
                                      index={index}
                                    >
                                      {(provided, snapshot) => (
                                        <div
                                          ref={provided.innerRef}
                                          {...provided.draggableProps}
                                          className={`question-item ${snapshot.isDragging ? 'dragging' : ''}`}
                                        >
                                          {editingQuestion?.id === question.id ? (
                                            <div className="question-edit-form">
                                              <div className="form-group">
                                                <label>Question Text:</label>
                                                <input
                                                  type="text"
                                                  value={editingQuestion.question}
                                                  onChange={(e) => setEditingQuestion({
                                                    ...editingQuestion,
                                                    question: e.target.value
                                                  })}
                                                  className="survey-input"
                                                />
                                              </div>
                                              <div className="form-group">
                                                <label>Question Type:</label>
                                                <select
                                                  value={editingQuestion.type}
                                                  onChange={(e) => setEditingQuestion({
                                                    ...editingQuestion,
                                                    type: e.target.value
                                                  })}
                                                  className="survey-input"
                                                >
                                                  <option value="text">Text</option>
                                                  <option value="number">Number</option>
                                                  <option value="select">Select</option>
                                                  <option value="checkbox">Checkbox</option>
                                                  <option value="file">File Upload</option>
                                                </select>
                                              </div>
                                              {(editingQuestion.type === 'select' || editingQuestion.type === 'checkbox') && (
                                                <div className="form-group">
                                                  <label>Options (comma-separated):</label>
                                                  <input
                                                    type="text"
                                                    value={editingQuestion.options?.join(', ') || ''}
                                                    onChange={(e) => setEditingQuestion({
                                                      ...editingQuestion,
                                                      options: e.target.value.split(',').map(opt => opt.trim())
                                                    })}
                                                    className="survey-input"
                                                  />
                                                </div>
                                              )}
                                              <div className="form-group">
                                                <label>Section:</label>
                                                <select
                                                  value={editingQuestion.section}
                                                  onChange={(e) => setEditingQuestion({
                                                    ...editingQuestion,
                                                    section: e.target.value
                                                  })}
                                                  className="survey-input"
                                                >
                                                  {sections.map((s) => (
                                                    <option key={s} value={s}>{s}</option>
                                                  ))}
                                                </select>
                                              </div>
                                              <div className="survey-checkbox-group">
                                                <label>
                                                  <input
                                                    type="checkbox"
                                                    checked={editingQuestion.required}
                                                    onChange={(e) => setEditingQuestion({
                                                      ...editingQuestion,
                                                      required: e.target.checked
                                                    })}
                                                  />
                                                  Required
                                                </label>
                                              </div>
                                              <div className="question-actions">
                                                <button
                                                  onClick={() => handleSaveEdit(editingQuestion)}
                                                  className="edit-button"
                                                >
                                                  Save
                                                </button>
                                                <button
                                                  onClick={() => setEditingQuestion(null)}
                                                  className="delete-button"
                                                >
                                                  Cancel
                                                </button>
                                              </div>
                                            </div>
                                          ) : (
                                            <>
                                              <div {...provided.dragHandleProps} className="drag-handle">
                                                ⋮⋮
                                              </div>
                                              <div className="question-content">
                                                <h4>{question.question}</h4>
                                                <p>Type: {question.type}</p>
                                                {question.options && question.options.length > 0 && (
                                                  <p>Options: {question.options.join(', ')}</p>
                                                )}
                                                {question.required && (
                                                  <span className="required-badge">Required</span>
                                                )}
                                              </div>
                                              <div className="question-actions">
                                                <button
                                                  onClick={() => handleEditQuestion(question)}
                                                  className="edit-button"
                                                >
                                                  Edit
                                                </button>
                                                <button
                                                  onClick={() => {
                                                    const newQuestions = questions.filter(q => q.id !== question.id);
                                                    handleUpdateQuestions(newQuestions);
                                                  }}
                                                  className="delete-button"
                                                >
                                                  Delete
                                                </button>
                                              </div>
                                            </>
                                          )}
                                        </div>
                                      )}
                                    </Draggable>
                                  ))}
                                {provided.placeholder}
                              </div>
                            )}
                          </Droppable>
                        </div>
                      )}
                    </Draggable>
                  ))}
                  {provided.placeholder}
                </div>
              )}
            </Droppable>
          </DragDropContext>
        </div>
      )}
    </div>
  );
};

export default AdminDashboard; 