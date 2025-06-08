/**
 * @fileoverview Notes Modal component for managing client notes and tasks.
 * Provides functionality for adding, viewing, and managing notes and tasks
 * associated with a client.
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { format } from 'date-fns';
import './styles/NotesModal.css';
import { Box, Button } from '@mui/material';
import TaskModal from '../TaskManager/TaskModal';
import { API_ENDPOINTS } from '../../config/api';

/**
 * @typedef {Object} NotesModalProps
 * @property {boolean} isOpen - Whether the modal is open
 * @property {Function} onClose - Callback function to close the modal
 * @property {string} clientId - ID of the client
 * @property {string} clientName - Name of the client
 */

/**
 * @typedef {Object} Note
 * @property {string} content - Content of the note
 * @property {string} timestamp - When the note was created
 * @property {string} author - Author of the note
 */

/**
 * @typedef {Object} Task
 * @property {string} _id - Task ID
 * @property {string} title - Task title
 * @property {string} description - Task description
 * @property {string} status - Current status of the task
 * @property {string} due_date - Due date of the task
 */

/**
 * Notes Modal component for managing client notes and tasks.
 * Features include:
 * - Adding and deleting notes
 * - Viewing note history
 * - Managing tasks and their status
 * - Task creation and updates
 * 
 * @param {NotesModalProps} props - Component props
 * @returns {React.ReactElement|null} The rendered modal or null if not open
 */
const NotesModal = ({ isOpen, onClose, clientId, clientName }) => {
  const [notes, setNotes] = useState({
    conversation: [],
    status: [],
    tasks: []
  });
  const [newNote, setNewNote] = useState('');
  const [activeTab, setActiveTab] = useState('conversation');
  const [loading, setLoading] = useState(true);
  const [isTaskModalOpen, setIsTaskModalOpen] = useState(false);

  useEffect(() => {
    console.log('NotesModal props:', { isOpen, clientId, clientName });
  }, [isOpen, clientId, clientName]);

  useEffect(() => {
    if (isOpen && clientId) {
      fetchNotes();
      if (activeTab === 'tasks') {
        fetchTasks();
      }
    }
  }, [isOpen, clientId, activeTab]);

  /**
   * Fetches notes for the current client
   * @async
   * @returns {Promise<void>}
   */
  const fetchNotes = async () => {
    try {
      const response = await axios.get(API_ENDPOINTS.LEADS.NOTES.GET(clientId));
      if (response.data.status === 'success') {
        const sortedConversation = response.data.data.conversation?.sort((a, b) => 
          new Date(b.timestamp) - new Date(a.timestamp)
        ) || [];
        
        const sortedStatus = response.data.data.status?.sort((a, b) => 
          new Date(b.timestamp) - new Date(a.timestamp)
        ) || [];

        setNotes(prev => ({
          ...prev,
          conversation: sortedConversation,
          status: sortedStatus,
          tasks: prev.tasks
        }));
      }
      setLoading(false);
    } catch (error) {
      console.error('Error fetching notes:', error);
      setLoading(false);
    }
  };

  /**
   * Fetches tasks for the current client
   * @async
   * @returns {Promise<void>}
   */
  const fetchTasks = async () => {
    try {
      console.log('Fetching tasks for client:', clientId);
      const response = await axios.get(`${API_ENDPOINTS.TASKS}/client/${clientId}`);
      console.log('Tasks response:', response.data);
      
      if (response.data.status === 'success' && Array.isArray(response.data.tasks)) {
        const sortedTasks = response.data.tasks.sort((a, b) => 
          new Date(b.created_at) - new Date(a.created_at)
        );

        setNotes(prev => ({
          ...prev,
          tasks: sortedTasks
        }));
      } else {
        setNotes(prev => ({
          ...prev,
          tasks: []
        }));
      }
      setLoading(false);
    } catch (error) {
      console.error('Error fetching tasks:', error);
      setNotes(prev => ({
        ...prev,
        tasks: []
      }));
      setLoading(false);
    }
  };

  /**
   * Adds a new note for the client
   * @async
   * @returns {Promise<void>}
   */
  const handleAddNote = async () => {
    if (!newNote.trim()) return;
    
    if (!clientId) {
      console.error('No client ID provided');
      return;
    }

    try {
      console.log('Sending note:', {
        clientId,
        type: activeTab,
        note: newNote
      });

      const response = await axios.post(API_ENDPOINTS.LEADS.NOTES.CREATE(clientId, activeTab), {
        text: newNote,
        author: 'Current User'
      });

      if (response.data.status === 'success') {
        setNotes(prev => ({
          ...prev,
          [activeTab]: [response.data.data, ...prev[activeTab]]
        }));
        setNewNote('');
      }
    } catch (error) {
      console.error('Error adding note:', error.response?.data || error);
    }
  };

  /**
   * Deletes a note by its timestamp
   * @async
   * @param {string} timestamp - Timestamp of the note to delete
   * @returns {Promise<void>}
   */
  const handleDeleteNote = async (timestamp) => {
    try {
      const response = await axios.delete(API_ENDPOINTS.LEADS.NOTES.DELETE(clientId, activeTab, timestamp));
      
      if (response.data.status === 'success') {
        setNotes(prev => ({
          ...prev,
          [activeTab]: prev[activeTab].filter(note => note.timestamp !== timestamp)
        }));
      }
    } catch (error) {
      console.error('Error deleting note:', error);
    }
  };

  /**
   * Updates the status of a task
   * @async
   * @param {string} taskId - ID of the task
   * @param {string} newStatus - New status to set
   * @returns {Promise<void>}
   */
  const handleStatusChange = async (taskId, newStatus) => {
    try {
      // Get the existing task data
      const taskToUpdate = notes.tasks.find(task => task._id === taskId);
      if (!taskToUpdate) return;

      // Prepare the complete task data required by the endpoint
      const updatedTaskData = {
        ...taskToUpdate,
        status: newStatus,
        // Include all required fields from TaskModal
        title: taskToUpdate.title,
        description: taskToUpdate.description,
        priority: taskToUpdate.priority,
        due_date: taskToUpdate.due_date,
        assignees: taskToUpdate.assignees || [],
        created_by: taskToUpdate.created_by
      };

      const response = await axios.put(`${API_ENDPOINTS.TASKS}/${taskId}`, updatedTaskData);
      
      if (response.data.status === 'success') {
        // Update local state
        setNotes(prev => ({
          ...prev,
          tasks: prev.tasks.map(task => 
            task._id === taskId ? { ...task, status: newStatus } : task
          )
        }));
      }
    } catch (error) {
      console.error('Error updating task status:', error);
    }
  };

  /**
   * Renders the list of tasks with their current status
   * @returns {React.ReactElement} The rendered task list
   */
  const renderTaskList = () => {
    if (!notes.tasks) return null;
    
    return notes.tasks.map((task) => (
      <div key={task._id} className="note-item task-item">
        <div className="task-header">
          <h3>{task.title}</h3>
          <div className="task-actions">
            <select
              className="status-select"
              value={task.status}
              onChange={(e) => handleStatusChange(task._id, e.target.value)}
            >
              <option value="active">ACTIVE</option>
              <option value="complete">COMPLETE</option>
              <option value="hold">HOLD</option>
            </select>
            <span className={`task-priority ${task.priority}`}>{task.priority}</span>
          </div>
        </div>
        <div className="note-content">{task.description}</div>
        <div className="note-metadata">
          <span>Due: {new Date(task.due_date).toLocaleDateString()}</span>
        </div>
      </div>
    ));
  };

  /**
   * Handles submission of a new task
   * @async
   * @param {Object} taskData - Data for the new task
   * @returns {Promise<void>}
   */
  const handleTaskSubmit = async (taskData) => {
    try {
      const taskWithClient = {
        ...taskData,
        client_id: clientId,
        client_name: clientName,
        assignees: [{
          id: clientId,
          name: clientName,
          type: 'client',
          client_id: clientId
        }]
      };
      
      const response = await axios.post('/api/tasks', taskWithClient);
      
      if (response.data.status === 'success') {
        setIsTaskModalOpen(false);
        fetchTasks();
      }
    } catch (error) {
      console.error('Error creating task:', error);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="notes-modal-overlay">
      <div className="notes-modal">
        <div className="notes-modal-header">
          <h2>Client Tracking {clientName}</h2>
          <button className="close-button" onClick={onClose}>×</button>
        </div>

        <div className="notes-tabs">
          <button
            className={`tab-button ${activeTab === 'conversation' ? 'active' : ''}`}
            onClick={() => setActiveTab('conversation')}
          >
            Conversation Track
          </button>
          <button
            className={`tab-button ${activeTab === 'status' ? 'active' : ''}`}
            onClick={() => setActiveTab('status')}
          >
            Creative Ideas
          </button>
          <button
            className={`tab-button ${activeTab === 'tasks' ? 'active' : ''}`}
            onClick={() => setActiveTab('tasks')}
          >
            Tasks
          </button>
        </div>

        {activeTab !== 'tasks' ? (
          <>
            <div className="notes-input-section">
              <textarea
                value={newNote}
                onChange={(e) => setNewNote(e.target.value)}
                placeholder={`Add new ${activeTab} note...`}
                rows="3"
              />
              <button className="add-note-button" onClick={handleAddNote}>
                Add Note
              </button>
            </div>
            <div className="notes-list">
              {loading ? (
                <div className="loading">Loading notes...</div>
              ) : (
                notes[activeTab].map((note, index) => (
                  <div key={index} className="note-item">
                    <div className="note-content">{note.text}</div>
                    <div className="note-metadata">
                      <span className="note-author">{note.author}</span>
                      <span className="note-timestamp">
                        {format(new Date(note.timestamp), 'MMM d, yyyy h:mm a')}
                      </span>
                      <button 
                        className="delete-note-button"
                        onClick={() => handleDeleteNote(note.timestamp)}
                        title="Delete note"
                      >
                        ×
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </>
        ) : (
          <div className="notes-list tasks-list">
            <Box sx={{ mb: 2, display: 'flex', justifyContent: 'flex-end' }}>
              <Button 
                variant="contained"
                color="primary"
                size="small"
                onClick={() => setIsTaskModalOpen(true)}
                sx={{ mb: 2 }}
              >
                Add Task
              </Button>
            </Box>
            
            {loading ? (
              <div className="loading">Loading tasks...</div>
            ) : (
              renderTaskList()
            )}
          </div>
        )}
      </div>
      <TaskModal 
        show={isTaskModalOpen}
        onClose={() => setIsTaskModalOpen(false)}
        onSubmit={handleTaskSubmit}
        editMode={false}
        currentUser="system"
      />
    </div>
  );
};

export default NotesModal; 