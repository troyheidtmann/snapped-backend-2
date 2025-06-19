/**
 * @fileoverview Custom hook for managing task logic and state.
 * Provides functionality for task CRUD operations and state management.
 */

import { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';

/**
 * @typedef {Object} Task
 * @property {string} id - Unique identifier for the task
 * @property {string} title - Task title
 * @property {string} description - Task description
 * @property {string} status - Task status (active, complete, hold)
 * @property {string} priority - Task priority level
 * @property {string} due_date - Task due date
 * @property {Array<Object>} assignees - Task assignees
 * @property {string} created_by - Task creator ID
 */

/**
 * Initial state for a new task.
 * 
 * @constant
 * @type {Task}
 */
const INITIAL_TASK_STATE = {
  title: '',
  description: '',
  status: 'active',
  priority: 'medium',
  due_date: '',
  assignees: [],
  created_by: '' // Will be set when creating task
};

/**
 * Custom hook for managing task logic and state.
 * Provides functionality for task management including CRUD operations,
 * filtering, and state management.
 * 
 * @returns {Object} Task management functions and state
 */
export const useTaskLogic = () => {
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('all');
  const [showTaskModal, setShowTaskModal] = useState(false);
  const [currentTask, setCurrentTask] = useState(INITIAL_TASK_STATE);
  const [editMode, setEditMode] = useState(false);

  /**
   * Fetches tasks based on active tab filter.
   * 
   * @async
   * @function fetchTasks
   * @returns {Promise<void>}
   */
  const fetchTasks = useCallback(async () => {
    setLoading(true);
    try {
      let url = API_ENDPOINTS.TASKS;
      
      // Only add filter if not "all" tab
      if (activeTab !== 'all') {
        // Map frontend tab names to backend filter types
        const filterMap = {
          'priority': 'priority',
          'completed': 'completed'
        };
        
        if (filterMap[activeTab]) {
          url += `?filter_type=${filterMap[activeTab]}`;
        }
      }
      
      const response = await axios.get(url);
      if (response.data && Array.isArray(response.data.tasks)) {
        // Process tasks to ensure consistent status values
        const processedTasks = response.data.tasks.map(task => ({
          ...task,
          // Normalize status values for frontend display
          status: task.status?.toLowerCase() === 'completed' ? 'complete' : task.status?.toLowerCase() || 'active',
          // Normalize priority values for frontend display
          priority: task.priority?.toLowerCase() || 'medium'
        }));

        // Filter tasks based on active tab
        let filteredTasks = processedTasks;
        if (activeTab === 'all' || activeTab === 'priority') {
          // For all and priority tabs, exclude completed tasks
          filteredTasks = processedTasks.filter(task => 
            task.status.toLowerCase() !== 'complete' && 
            task.status.toLowerCase() !== 'completed'
          );
        } else if (activeTab === 'completed') {
          // For completed tab, only show completed tasks
          filteredTasks = processedTasks.filter(task => 
            task.status.toLowerCase() === 'complete' || 
            task.status.toLowerCase() === 'completed'
          );
        }

        setTasks(filteredTasks);
      } else {
        console.error('Invalid response format:', response.data);
        setTasks([]);
      }
    } catch (error) {
      console.error('Error fetching tasks:', error);
      const errorMessage = error.response?.data?.detail || error.message;
      alert('Failed to fetch tasks: ' + errorMessage);
      setTasks([]);
    } finally {
      setLoading(false);
    }
  }, [activeTab]);

  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  /**
   * Handles adding a new task.
   * 
   * @function handleAddTask
   */
  const handleAddTask = () => {
    setEditMode(false);
    setCurrentTask(INITIAL_TASK_STATE);
    setShowTaskModal(true);
  };

  /**
   * Handles editing an existing task.
   * 
   * @function handleEditTask
   * @param {Task} task - Task to edit
   */
  const handleEditTask = (task) => {
    setEditMode(true);
    setCurrentTask({
      ...task,
      _id: task._id
    });
    setShowTaskModal(true);
  };

  /**
   * Handles deleting a task.
   * 
   * @async
   * @function handleDeleteTask
   * @param {string} taskId - ID of task to delete
   * @returns {Promise<void>}
   */
  const handleDeleteTask = async (taskId) => {
    if (!window.confirm('Are you sure you want to delete this task?')) return;

    try {
      const response = await axios.delete(`${API_ENDPOINTS.TASKS}/${taskId}`);
      if (response.data?.status === 'success') {
        setTasks(prevTasks => prevTasks.filter(task => task._id !== taskId));
        
        if (currentTask?._id === taskId) {
          setShowTaskModal(false);
          setEditMode(false);
          setCurrentTask(INITIAL_TASK_STATE);
        }
        
        alert('Task deleted successfully');
      }
    } catch (error) {
      console.error('Error deleting task:', error);
      const errorMessage = error.response?.data?.detail || 
                          error.response?.data?.message || 
                          error.message || 
                          'Failed to delete task';
      alert(errorMessage);
    }
  };

  /**
   * Handles submitting a task (create or update).
   * 
   * @async
   * @function handleSubmitTask
   * @param {Task} taskData - Task data to submit
   * @returns {Promise<boolean>} Success status
   */
  const handleSubmitTask = useCallback(async (taskData) => {
    try {
      const cleanTaskData = {
        title: taskData.title,
        description: taskData.description,
        status: taskData.status,
        priority: taskData.priority,
        due_date: taskData.due_date,
        assignees: taskData.assignees.map(assignee => ({
          id: assignee.id,
          name: assignee.name,
          type: assignee.type,
          client_id: assignee.type === 'client' ? assignee.id : null,
          employee_id: assignee.type === 'employee' ? assignee.id : null
        })),
        visible_to: taskData.visible_to || []
      };

      if (editMode && taskData._id) {
        await axios.put(`/api/tasks/${taskData._id}`, cleanTaskData);
      } else {
        await axios.post('/api/tasks', cleanTaskData);
      }
      
      await fetchTasks();
      return true; // Return success
    } catch (error) {
      console.error('Error submitting task:', error);
      const errorMessage = error.response?.data?.detail || error.message;
      alert('Error saving task: ' + errorMessage);
      return false; // Return failure
    }
  }, [editMode, fetchTasks]);

  /**
   * Handles changing the active tab filter.
   * 
   * @function handleTabChange
   * @param {string} tab - Tab to switch to
   */
  const handleTabChange = (tab) => {
    setActiveTab(tab);
  };

  /**
   * Handles updating task assignees.
   * 
   * @function handleAssigneeChange
   * @param {Array<Object>} assignees - Updated assignees list
   */
  const handleAssigneeChange = (assignees) => {
    setCurrentTask(prev => ({
      ...prev,
      assignees
    }));
  };

  return {
    tasks,
    loading,
    activeTab,
    showTaskModal,
    currentTask,
    editMode,
    handleAddTask,
    handleEditTask,
    handleDeleteTask,
    handleSubmitTask,
    handleTabChange,
    handleAssigneeChange,
    setShowTaskModal,
    setEditMode
  };
}; 