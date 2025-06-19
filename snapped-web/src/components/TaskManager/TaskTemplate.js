import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';
import './styles/TaskTemplate.css';

const TaskTemplate = () => {
  const [templates, setTemplates] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const [formData, setFormData] = useState({
    title: '',
    description: '',
    frequency: 'daily',
    priority: 'medium',
    job_type: '',
    assignees: [],
    is_active: true
  });
  const [searchResults, setSearchResults] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  
  const { getAccessToken } = useAuth();
  
  useEffect(() => {
    fetchTemplates();
  }, []);
  
  const fetchTemplates = async () => {
    try {
      const token = await getAccessToken();
      const response = await axios.get(API_ENDPOINTS.TASK_TEMPLATES, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setTemplates(response.data);
    } catch (error) {
      console.error('Error fetching templates:', error);
    }
  };
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const token = await getAccessToken();
      await axios.post(API_ENDPOINTS.TASK_TEMPLATES, formData, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setIsOpen(false);
      fetchTemplates();
      setFormData({
        title: '',
        description: '',
        frequency: 'daily',
        priority: 'medium',
        job_type: '',
        assignees: [],
        is_active: true
      });
    } catch (error) {
      console.error('Error creating template:', error);
    }
  };
  
  const handleToggleActive = async (templateId, currentStatus) => {
    try {
      const token = await getAccessToken();
      const template = templates.find(t => t._id === templateId);
      await axios.put(`${API_ENDPOINTS.TASK_TEMPLATES}/${templateId}`, {
        ...template,
        is_active: !currentStatus
      }, {
        headers: { Authorization: `Bearer ${token}` }
      });
      fetchTemplates();
    } catch (error) {
      console.error('Error updating template:', error);
    }
  };
  
  const searchAssignees = async (query) => {
    if (!query) {
      setSearchResults([]);
      return;
    }
    try {
      const token = await getAccessToken();
      const response = await axios.get(`${API_ENDPOINTS.TASKS}/search_assignees?query=${query}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setSearchResults(response.data.assignees || []);
    } catch (error) {
      console.error('Error searching assignees:', error);
    }
  };
  
  const handleSelectAssignee = (assignee) => {
    if (!formData.assignees.find(a => a.id === assignee.id)) {
      setFormData({
        ...formData,
        assignees: [...formData.assignees, assignee]
      });
    }
    setSearchQuery('');
    setSearchResults([]);
  };

  const handleRemoveAssignee = (assigneeId) => {
    setFormData({
      ...formData,
      assignees: formData.assignees.filter(a => a.id !== assigneeId)
    });
  };
  
  return (
    <div className="task-template">
      <button 
        className="task-template__create-button"
        onClick={() => setIsOpen(true)}
      >
        Create Task Template
      </button>
      
      <table className="task-template__table">
        <thead>
          <tr>
            <th>Title</th>
            <th>Job Type</th>
            <th>Frequency</th>
            <th>Priority</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {templates.map((template) => (
            <tr key={template._id}>
              <td>{template.title}</td>
              <td>{template.job_type}</td>
              <td>
                <span className={`badge ${template.frequency === 'daily' ? 'badge--green' : 'badge--blue'}`}>
                  {template.frequency}
                </span>
              </td>
              <td>
                <span className={`badge badge--${template.priority}`}>
                  {template.priority}
                </span>
              </td>
              <td>
                <span className={`badge ${template.is_active ? 'badge--green' : 'badge--gray'}`}>
                  {template.is_active ? 'Active' : 'Inactive'}
                </span>
              </td>
              <td>
                <label className="switch">
                  <input
                    type="checkbox"
                    checked={template.is_active}
                    onChange={() => handleToggleActive(template._id, template.is_active)}
                  />
                  <span className="slider"></span>
                </label>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      
      {isOpen && (
        <div className="task-modal-overlay">
          <div className="task-modal">
            <div className="task-modal__header">
              <h2>Create Task Template</h2>
              <button 
                className="task-modal__close"
                onClick={() => setIsOpen(false)}
              >
                ×
              </button>
            </div>
            <div className="task-modal__body">
              <form onSubmit={handleSubmit}>
                <div className="task-modal__field">
                  <label>Title *</label>
                  <input
                    type="text"
                    value={formData.title}
                    onChange={(e) => setFormData({ ...formData, title: e.target.value })}
                    required
                    className="task-modal__input"
                  />
                </div>
                
                <div className="task-modal__field">
                  <label>Description *</label>
                  <textarea
                    value={formData.description}
                    onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                    required
                    className="task-modal__input"
                  />
                </div>
                
                <div className="task-modal__field">
                  <label>Job Type *</label>
                  <input
                    type="text"
                    value={formData.job_type}
                    onChange={(e) => setFormData({ ...formData, job_type: e.target.value })}
                    placeholder="e.g. Content Team, Talent Team"
                    required
                    className="task-modal__input"
                  />
                </div>
                
                <div className="task-modal__field">
                  <label>Frequency *</label>
                  <select
                    value={formData.frequency}
                    onChange={(e) => setFormData({ ...formData, frequency: e.target.value })}
                    required
                    className="task-modal__input"
                  >
                    <option value="daily">Daily</option>
                    <option value="weekly">Weekly</option>
                  </select>
                </div>
                
                <div className="task-modal__field">
                  <label>Priority *</label>
                  <select
                    value={formData.priority}
                    onChange={(e) => setFormData({ ...formData, priority: e.target.value })}
                    required
                    className="task-modal__input"
                  >
                    <option value="low">Low</option>
                    <option value="medium">Medium</option>
                    <option value="high">High</option>
                  </select>
                </div>
                
                <div className="task-modal__field">
                  <label>Assignees</label>
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => {
                      setSearchQuery(e.target.value);
                      if (e.target.value.length >= 2) {
                        searchAssignees(e.target.value);
                      } else {
                        setSearchResults([]);
                      }
                    }}
                    placeholder="Search for assignees..."
                    className="task-modal__input"
                  />
                  {searchResults.length > 0 && (
                    <div className="task-modal__search-results">
                      {searchResults.map(assignee => (
                        <div
                          key={assignee.id}
                          className="task-modal__search-result"
                          onClick={() => handleSelectAssignee(assignee)}
                        >
                          {assignee.name} ({assignee.type})
                        </div>
                      ))}
                    </div>
                  )}
                  <div className="task-modal__assignees">
                    {formData.assignees.map(assignee => (
                      <div key={assignee.id} className="task-modal__assignee">
                        {assignee.name}
                        <button
                          type="button"
                          onClick={() => handleRemoveAssignee(assignee.id)}
                          className="task-modal__remove-assignee"
                        >
                          ×
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              </form>
            </div>
            
            <div className="task-modal__footer">
              <button 
                type="button" 
                className="task-modal__button task-modal__button--secondary"
                onClick={() => setIsOpen(false)}
              >
                Cancel
              </button>
              <button 
                type="submit" 
                className="task-modal__button task-modal__button--primary"
                onClick={handleSubmit}
              >
                Create Template
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default TaskTemplate; 