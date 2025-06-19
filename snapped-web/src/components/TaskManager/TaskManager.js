import React, { useState } from 'react';
import TaskGrid from './TaskGrid';
import TaskTemplate from './TaskTemplate';
import './styles/TaskManager.css';

const TaskManager = () => {
  const [activeTab, setActiveTab] = useState('all');

  return (
    <div className="task-manager">
      <div className="task-manager__header">
        <h1>Task Manager</h1>
        <button 
          className="task-manager__add-button"
        >
          Add Task
        </button>
      </div>

      <div className="task-manager__tabs">
        <button 
          className={`tab ${activeTab === 'all' ? 'active' : ''}`}
          onClick={() => setActiveTab('all')}
        >
          All Tasks
        </button>
        <button 
          className={`tab ${activeTab === 'priority' ? 'active' : ''}`}
          onClick={() => setActiveTab('priority')}
        >
          Priority Tasks
        </button>
        <button 
          className={`tab ${activeTab === 'completed' ? 'active' : ''}`}
          onClick={() => setActiveTab('completed')}
        >
          Completed Tasks
        </button>
        <button 
          className={`tab ${activeTab === 'templates' ? 'active' : ''}`}
          onClick={() => setActiveTab('templates')}
        >
          Templates
        </button>
      </div>

      {activeTab === 'templates' ? (
        <TaskTemplate />
      ) : (
        <TaskGrid activeTab={activeTab} />
      )}
    </div>
  );
};

export default TaskManager; 