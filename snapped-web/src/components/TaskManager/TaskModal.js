/**
 * @fileoverview TaskModal component for creating and editing tasks.
 * Provides a modal interface for task management with form controls.
 */

import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import './styles/TaskModal.css';
import { fetchAuthSession } from 'aws-amplify/auth';

/**
 * @typedef {Object} TaskModalProps
 * @property {boolean} show - Whether the modal is visible
 * @property {Function} onClose - Callback function to close the modal
 * @property {Object} task - Task data to edit or create
 * @property {Function} onSubmit - Callback function for form submission
 * @property {boolean} editMode - Whether the modal is in edit mode
 */

/**
 * TaskModal component for creating and editing tasks.
 * Provides form controls for task properties and handles task submission.
 * 
 * @component
 * @param {TaskModalProps} props - Component props
 * @returns {React.ReactElement} The rendered TaskModal component
 */
const TaskModal = ({ 
  show, 
  onClose, 
  task, 
  onSubmit, 
  editMode 
}) => {
  const [formData, setFormData] = useState({
    title: '',
    description: '',
    priority: 'medium',
    status: 'pending',
    due_date: '',
    assignees: [],
    visible_to: []
  });
  const [assigneeOptions, setAssigneeOptions] = useState([]);
  const [userGroups, setUserGroups] = useState([]);

  /**
   * Fetches user groups on component mount.
   * Sets initial visibility based on user groups.
   */
  useEffect(() => {
    const fetchUserGroups = async () => {
      try {
        const { tokens } = await fetchAuthSession();
        const decodedToken = JSON.parse(atob(tokens.idToken.toString().split('.')[1]));
        const groups = decodedToken['cognito:groups'] || [];
        setUserGroups(groups);
        
        if (!editMode) {
          setFormData(prev => ({
            ...prev,
            visible_to: groups
          }));
        }
      } catch (error) {
        console.error('Error fetching user groups:', error);
      }
    };
    
    fetchUserGroups();
  }, [editMode]);

  /**
   * Updates form data when task prop changes.
   */
  useEffect(() => {
    if (task) {
      setFormData({
        title: task.title || '',
        description: task.description || '',
        priority: task.priority || 'medium',
        status: task.status || 'pending',
        due_date: task.due_date || '',
        assignees: task.assignees || [],
        visible_to: task.visible_to || userGroups
      });
    }
  }, [task, userGroups]);

  /**
   * Resets form when modal closes.
   */
  useEffect(() => {
    if (!show) {
      setFormData({
        title: '',
        description: '',
        priority: 'medium',
        status: 'pending',
        due_date: '',
        assignees: [],
        visible_to: userGroups
      });
      setAssigneeOptions([]);
    }
  }, [show, userGroups]);

  /**
   * Cleanup effect when component unmounts.
   */
  useEffect(() => {
    return () => {
      setFormData({
        title: '',
        description: '',
        priority: 'medium',
        status: 'pending',
        due_date: '',
        assignees: [],
        visible_to: []
      });
      setAssigneeOptions([]);
    };
  }, []);

  /**
   * Handles closing the modal.
   * 
   * @function handleClose
   */
  const handleClose = useCallback(() => {
    if (typeof onClose === 'function') {
      onClose();
    }
  }, [onClose]);

  /**
   * Handles form submission.
   * 
   * @async
   * @function handleSubmit
   * @param {Event} e - Form submit event
   * @returns {Promise<void>}
   */
  const handleSubmit = useCallback(async (e) => {
    e.preventDefault();
    try {
      const submitData = {
        ...formData,
        _id: editMode ? task?._id : undefined
      };
      await onSubmit(submitData);
      handleClose();
    } catch (error) {
      console.error('Error submitting task:', error);
      const errorMessage = error.response?.data?.detail || error.message;
      alert('Error: ' + errorMessage);
    }
  }, [formData, editMode, task, onSubmit, handleClose]);

  /**
   * Handles form field changes.
   * 
   * @function handleChange
   * @param {Event} e - Input change event
   */
  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  /**
   * Handles assignee search.
   * 
   * @async
   * @function handleAssigneeSearch
   * @param {Event} e - Input change event
   * @returns {Promise<void>}
   */
  const handleAssigneeSearch = async (e) => {
    const query = e.target.value;
    if (query.length >= 2) {
      try {
        setAssigneeOptions([]); // Clear previous options while loading
        const response = await axios.get(`/api/search_assignees?query=${query}`);
        console.log('Search results:', response.data);
        
        if (response.data?.assignees) {
          const newOptions = response.data.assignees.filter(
            option => !formData.assignees.some(
              existing => existing.id === option.id
            )
          );
          setAssigneeOptions(newOptions);
        }
      } catch (error) {
        console.error('Error searching assignees:', error);
        setAssigneeOptions([]);
      }
    } else {
      setAssigneeOptions([]);
    }
  };

  /**
   * Handles selecting an assignee from search results.
   * 
   * @function handleAssigneeSelect
   * @param {Object} assignee - Selected assignee
   */
  const handleAssigneeSelect = (assignee) => {
    setFormData(prev => ({
      ...prev,
      assignees: [
        ...prev.assignees.filter(a => a.id !== assignee.id),
        {
          id: assignee.id,
          name: assignee.name,
          type: assignee.type,
          client_id: assignee.type === 'client' ? assignee.id : null,
          employee_id: assignee.type === 'employee' ? assignee.id : null
        }
      ]
    }));
    setAssigneeOptions([]);
  };

  /**
   * Handles removing an assignee.
   * 
   * @function handleRemoveAssignee
   * @param {string} assigneeId - ID of assignee to remove
   */
  const handleRemoveAssignee = (assigneeId) => {
    setFormData(prev => ({
      ...prev,
      assignees: prev.assignees.filter(a => a.id !== assigneeId)
    }));
  };

  /**
   * Handles changing group visibility.
   * 
   * @function handleGroupVisibilityChange
   * @param {string} group - Group to toggle visibility for
   */
  const handleGroupVisibilityChange = (group) => {
    setFormData(prev => ({
      ...prev,
      visible_to: prev.visible_to.includes(group)
        ? prev.visible_to.filter(g => g !== group)
        : [...prev.visible_to, group]
    }));
  };

  if (!show) return null;

  return (
    <div className="task-modal-overlay">
      <div className="task-modal">
        <div className="task-modal__header">
          <h2>{editMode ? 'Edit Task' : 'Add New Task'}</h2>
          <button 
            className="task-modal__close" 
            onClick={handleClose}
            type="button"
          >
            &times;
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="task-modal__body">
            <div className="task-modal__field">
              <label>Title</label>
              <input 
                type="text" 
                name="title"
                value={formData.title}
                onChange={handleChange}
                required
                placeholder="Enter task title"
              />
            </div>

            <div className="task-modal__field">
              <label>Description</label>
              <textarea 
                name="description"
                value={formData.description}
                onChange={handleChange}
                required
                placeholder="Enter task description"
              />
            </div>

            <div className="task-modal__field">
              <label>Priority</label>
              <select 
                name="priority"
                value={formData.priority}
                onChange={handleChange}
                required
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
              </select>
            </div>

            <div className="task-modal__field">
              <label>Status</label>
              <select 
                name="status"
                value={formData.status}
                onChange={handleChange}
                required
              >
                <option value="active">ACTIVE</option>
                <option value="complete">COMPLETE</option>
                <option value="hold">HOLD</option>
              </select>
            </div>

            <div className="task-modal__field">
              <label>Due Date</label>
              <input 
                type="date"
                name="due_date"
                value={formData.due_date}
                onChange={handleChange}
                required
              />
            </div>

            <div className="task-modal__field">
              <label>Assignees</label>
              <input 
                type="text"
                placeholder="Search assignees (minimum 2 characters)"
                onChange={handleAssigneeSearch}
              />
              {assigneeOptions.length > 0 && (
                <ul className="assignee-options">
                  {assigneeOptions.map(option => (
                    <li 
                      key={option.id} 
                      onClick={() => handleAssigneeSelect(option)}
                      className="assignee-option"
                    >
                      {option.name} ({option.type})
                    </li>
                  ))}
                </ul>
              )}
              <div className="selected-assignees">
                {formData.assignees.map(assignee => (
                  <span key={assignee.id} className="assignee-tag">
                    {assignee.name} ({assignee.type})
                    <button 
                      type="button" 
                      className="remove-assignee"
                      onClick={() => handleRemoveAssignee(assignee.id)}
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
            </div>

            {userGroups.includes('admin') && (
              <div className="task-modal__field">
                <label>Visible to Groups</label>
                <div className="group-visibility-options">
                  {userGroups.map(group => (
                    <label key={group} className="group-checkbox">
                      <input
                        type="checkbox"
                        checked={formData.visible_to.includes(group)}
                        onChange={() => handleGroupVisibilityChange(group)}
                      />
                      {group}
                    </label>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="task-modal__footer">
            <button 
              type="button" 
              onClick={handleClose}
              className="task-modal__button--secondary"
            >
              Cancel
            </button>
            <button type="submit" className="task-modal__button--primary">
              {editMode ? 'Update Task' : 'Create Task'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default TaskModal; 