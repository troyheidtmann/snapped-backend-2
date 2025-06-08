/**
 * @fileoverview TaskGrid component for displaying and managing tasks in a grid layout.
 * Provides functionality for viewing, editing, and managing task items.
 */

import React, { useState, useEffect, useCallback } from 'react';
import { useTaskLogic } from './hooks/useTaskLogic';
import TaskModal from './TaskModal';
import './styles/TaskGrid.css';
// Import Material UI icons
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import RadioButtonUncheckedIcon from '@mui/icons-material/RadioButtonUnchecked';
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutline';

/**
 * TaskGrid component for displaying tasks in a grid layout.
 * Provides functionality for task management including adding, editing, and deleting tasks.
 * 
 * @component
 * @returns {React.ReactElement} The rendered TaskGrid component
 */
const TaskGrid = () => {
  const { 
    tasks,
    loading,
    activeTab,
    handleAddTask,
    handleEditTask,
    handleDeleteTask,
    handleTabChange,
    showTaskModal,
    setShowTaskModal,
    currentTask,
    handleSubmitTask,
    editMode,
    setEditMode
  } = useTaskLogic();

  /**
   * Handles closing the task modal.
   * 
   * @function handleCloseModal
   */
  const handleCloseModal = useCallback(() => {
    setShowTaskModal(false);
    setEditMode(false);
  }, [setShowTaskModal, setEditMode]);

  /**
   * Handles deleting a task.
   * 
   * @async
   * @function handleDelete
   * @param {string} taskId - ID of the task to delete
   * @returns {Promise<void>}
   */
  const handleDelete = useCallback(async (taskId) => {
    await handleDeleteTask(taskId);
  }, [handleDeleteTask]);

  /**
   * Handles editing a task.
   * 
   * @function handleEdit
   * @param {Object} task - Task to edit
   */
  const handleEdit = (task) => {
    console.log('Edit clicked, task:', task);
    handleEditTask(task);
    setShowTaskModal(true);
  };

  /**
   * Handles adding a new task.
   * 
   * @function handleAdd
   */
  const handleAdd = () => {
    handleAddTask();
    setShowTaskModal(true);
  };

  /**
   * Gets the appropriate status icon based on task status.
   * 
   * @function getStatusIcon
   * @param {string} status - Task status
   * @returns {React.ReactElement} Status icon component
   */
  const getStatusIcon = (status) => {
    switch (status) {
      case 'complete':
        return <CheckCircleIcon className="status-icon completed" />;
      case 'hold':
        return <RadioButtonUncheckedIcon className="status-icon hold" />;
      default:
        return <RadioButtonUncheckedIcon className="status-icon active" />;
    }
  };

  /**
   * Gets the display text for a task status.
   * 
   * @function getStatusText
   * @param {string} status - Task status
   * @returns {string} Status display text
   */
  const getStatusText = (status) => {
    switch (status) {
      case 'complete':
        return 'COMPLETE';
      case 'hold':
        return 'HOLD';
      default:
        return 'ACTIVE';
    }
  };

  return (
    <div className="task-manager">
      <div className="task-manager__header">
        <h1>Task Manager</h1>
        <button 
          className="task-manager__add-button"
          onClick={handleAdd}
        >
          Add Task
        </button>
      </div>

      <div className="task-manager__tabs">
        <button 
          className={`tab ${activeTab === 'all' ? 'active' : ''}`}
          onClick={() => handleTabChange('all')}
        >
          All Tasks
        </button>
        <button 
          className={`tab ${activeTab === 'priority' ? 'active' : ''}`}
          onClick={() => handleTabChange('priority')}
        >
          Priority Tasks
        </button>
        <button 
          className={`tab ${activeTab === 'completed' ? 'active' : ''}`}
          onClick={() => handleTabChange('completed')}
        >
          Completed Tasks
        </button>
      </div>

      <div className="task-grid">
        {loading ? (
          <div>Loading...</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Title</th>
                <th>Description</th>
                <th>Priority</th>
                <th>Due Date</th>
                <th>Assignees</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map(task => (
                <tr key={task._id}>
                  <td>{task.title}</td>
                  <td>{task.description}</td>
                  <td>
                    <span className={`priority-badge ${task.priority}`}>
                      {task.priority}
                    </span>
                  </td>
                  <td>{task.due_date}</td>
                  <td>
                    {task.assignees.map(assignee => (
                      <span key={assignee.id} className="assignee-tag">
                        {assignee.name}
                      </span>
                    ))}
                  </td>
                  <td>
                    <div className="status-wrapper">
                      {getStatusIcon(task.status)}
                      <span className="status-text">{getStatusText(task.status)}</span>
                    </div>
                  </td>
                  <td>
                    <button 
                      onClick={() => handleEdit(task)} 
                      className="icon-button edit"
                      title="Edit"
                    >
                      <EditIcon />
                    </button>
                    <button 
                      onClick={() => handleDelete(task._id)}
                      className="icon-button delete"
                      title="Delete"
                    >
                      <DeleteIcon />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {showTaskModal && (
        <TaskModal
          show={showTaskModal}
          onClose={handleCloseModal}
          task={currentTask}
          onSubmit={handleSubmitTask}
          editMode={editMode}
        />
      )}
    </div>
  );
};

export default TaskGrid; 