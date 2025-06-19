/**
 * @fileoverview TaskModal component for creating and editing tasks.
 * Provides a modal interface for task management with form controls.
 */

import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import './styles/TaskModal.css';
import { fetchAuthSession } from 'aws-amplify/auth';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';

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
  const [isEditing, setIsEditing] = useState(false);
  const [isCompleting, setIsCompleting] = useState(false);
  const [completionData, setCompletionData] = useState({
    hours: 0,
    minutes: 0,
    notes: ''
  });
  const [formData, setFormData] = useState({
    title: '',
    description: '',
    priority: 'medium',
    status: 'pending',
    due_date: '',
    assignees: [],
    visible_to: [],
    estimated_hours: 0,
    actual_hours: 0
  });
  const [assigneeOptions, setAssigneeOptions] = useState([]);
  const [userGroups, setUserGroups] = useState([]);

  const { getAccessToken } = useAuth();

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
        console.error('Error fetching user groups:', error.message);
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
        visible_to: task.visible_to || userGroups,
        estimated_hours: task.estimated_hours || 0,
        actual_hours: task.actual_hours || 0
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
        visible_to: userGroups,
        estimated_hours: 0,
        actual_hours: 0
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
        visible_to: [],
        estimated_hours: 0,
        actual_hours: 0
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
      console.log('Task updated successfully');
      handleClose();
    } catch (error) {
      console.error('Error updating task:', error.message);
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
        const token = await getAccessToken();
        const response = await axios.get(`${API_ENDPOINTS.TASKS}/search_assignees?query=${query}`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        
        if (response.data?.assignees) {
          const newOptions = response.data.assignees.filter(
            option => !formData.assignees.some(
              existing => existing.id === option.id
            )
          );
          setAssigneeOptions(newOptions);
        }
      } catch (error) {
        console.error('Error searching assignees:', error.message);
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

  const handleComplete = async () => {
    try {
      // Convert float hours to integer hours and minutes
      const totalHours = parseFloat(completionData.hours) || 0;
      const hours = Math.floor(totalHours);
      const minutesFromHours = Math.round((totalHours - hours) * 60);
      const totalMinutes = minutesFromHours + (parseInt(completionData.minutes) || 0);

      const token = await getAccessToken();

      // Create timesheet entry
      const timesheetEntry = {
        date: new Date().toISOString().split('T')[0], // Current date in YYYY-MM-DD format
        client_id: task.assignees?.find(a => a.type === 'client')?.client_id || '',
        hours: hours,
        minutes: totalMinutes,
        type: 'task',
        item: task._id, // Use task ID as the item identifier
        description: `Completed task: ${task.title}`,
        category: 'Task Work'
      };

      // Create timesheet entry
      await axios.post(
        `${API_ENDPOINTS.TIMESHEET.ENTRIES}`,
        timesheetEntry,
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );

      // Update task status to complete
      await axios.put(
        `${API_ENDPOINTS.TASKS}/${task._id}`,
        {
          ...task,
          status: 'complete',
          completion_notes: completionData.notes
        },
        {
          headers: { Authorization: `Bearer ${token}` }
        }
      );
      
      console.log('Task completed and timesheet entry created');
      
      onSubmit({
        ...task,
        status: 'complete'
      });
      onClose();
    } catch (error) {
      console.error('Error completing task:', error.message);
    }
  };

  if (!show) return null;

  return (
    <div className="task-modal-overlay">
      <div className="task-modal">
        <div className="task-modal__header">
          <h2>{isCompleting ? 'Complete Task' : editMode ? 'Edit Task' : 'Task Details'}</h2>
          <button className="task-modal__close" onClick={handleClose}>&times;</button>
        </div>
        
        <div className="task-modal__body">
          {isCompleting ? (
            <div className="task-modal__completion-form">
              <h3>{task?.title}</h3>
              
              <div className="task-modal__field">
                <label>Hours Spent *</label>
                <input
                  type="number"
                  min={0}
                  step={0.5}
                  value={completionData.hours}
                  onChange={(e) =>
                    setCompletionData({ ...completionData, hours: parseFloat(e.target.value) || 0 })
                  }
                  required
                  className="task-modal__input"
                />
              </div>
              
              <div className="task-modal__field">
                <label>Additional Minutes *</label>
                <input
                  type="number"
                  min={0}
                  max={59}
                  step={1}
                  value={completionData.minutes}
                  onChange={(e) =>
                    setCompletionData({ ...completionData, minutes: parseInt(e.target.value) || 0 })
                  }
                  required
                  className="task-modal__input"
                />
              </div>
              
              <div className="task-modal__field">
                <label>Completion Notes</label>
                <textarea
                  value={completionData.notes}
                  onChange={(e) =>
                    setCompletionData({ ...completionData, notes: e.target.value })
                  }
                  placeholder="Add any notes about task completion..."
                  className="task-modal__textarea"
                />
              </div>
            </div>
          ) : (
            <form onSubmit={handleSubmit}>
              <div className="task-modal__field">
                <label>Title *</label>
                <input 
                  type="text"
                  name="title"
                  value={formData.title}
                  onChange={handleChange}
                  required
                  placeholder="Enter task title"
                  className="task-modal__input"
                />
              </div>

              <div className="task-modal__field">
                <label>Description *</label>
                <textarea 
                  name="description"
                  value={formData.description}
                  onChange={handleChange}
                  required
                  placeholder="Enter task description"
                  className="task-modal__textarea"
                />
              </div>

              <div className="task-modal__field">
                <label>Priority *</label>
                <select 
                  name="priority"
                  value={formData.priority}
                  onChange={handleChange}
                  required
                  className="task-modal__select"
                >
                  <option value="low">Low</option>
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                </select>
              </div>

              <div className="task-modal__field">
                <label>Status *</label>
                <select 
                  name="status"
                  value={formData.status}
                  onChange={handleChange}
                  required
                  className="task-modal__select"
                >
                  <option value="active">ACTIVE</option>
                  <option value="complete">COMPLETE</option>
                  <option value="hold">HOLD</option>
                </select>
              </div>

              <div className="task-modal__field">
                <label>Due Date *</label>
                <input 
                  type="date"
                  name="due_date"
                  value={formData.due_date}
                  onChange={handleChange}
                  required
                  className="task-modal__input"
                />
              </div>

              <div className="task-modal__field">
                <label>Estimated Hours</label>
                <input
                  type="number"
                  name="estimated_hours"
                  value={formData.estimated_hours}
                  onChange={handleChange}
                  min={0}
                  step={0.5}
                  className="task-modal__input"
                />
              </div>

              <div className="task-modal__field">
                <label>Actual Hours</label>
                <input
                  type="number"
                  name="actual_hours"
                  value={formData.actual_hours}
                  onChange={handleChange}
                  min={0}
                  step={0.5}
                  className="task-modal__input"
                />
              </div>

              <div className="task-modal__field">
                <label>Assignees</label>
                <input 
                  type="text"
                  placeholder="Search assignees (minimum 2 characters)"
                  onChange={handleAssigneeSearch}
                  className="task-modal__input"
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
            </form>
          )}
        </div>
        
        <div className="task-modal__footer">
          {isCompleting ? (
            <>
              <button 
                type="button" 
                className="task-modal__button task-modal__button--secondary" 
                onClick={() => setIsCompleting(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="task-modal__button task-modal__button--primary"
                onClick={handleComplete}
                disabled={completionData.hours === 0 && completionData.minutes === 0}
              >
                Complete Task
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                className="task-modal__button task-modal__button--success"
                onClick={() => setIsCompleting(true)}
                disabled={task?.status === 'complete'}
              >
                Mark Complete
              </button>
              <button 
                type="button" 
                className="task-modal__button task-modal__button--secondary" 
                onClick={handleClose}
              >
                Cancel
              </button>
              <button 
                type="submit" 
                className="task-modal__button task-modal__button--primary"
                onClick={handleSubmit}
              >
                {editMode ? 'Update Task' : 'Create Task'}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default TaskModal; 