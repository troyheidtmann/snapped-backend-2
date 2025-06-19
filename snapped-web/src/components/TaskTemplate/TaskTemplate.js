import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { API_ENDPOINTS } from '../../config/api';
import './TaskTemplate.css';
import { fetchAuthSession } from 'aws-amplify/auth';
import { toast } from 'react-hot-toast';

const TaskTemplate = () => {
  const [templates, setTemplates] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [selectedAssignees, setSelectedAssignees] = useState([]);
  const [editingTemplate, setEditingTemplate] = useState(null);
  const [expandedGroups, setExpandedGroups] = useState({});
  const [creators, setCreators] = useState({});
  const [isAdmin, setIsAdmin] = useState(false);
  const [formData, setFormData] = useState({
    title: '',
    description: '',
    frequency: 'daily',
    priority: 'medium',
    job_type: '',
    assignees: [],
    is_active: true
  });
  
  const { getAccessToken } = useAuth();
  
  // Add new state for bulk copy modal
  const [isBulkCopyModalOpen, setBulkCopyModalOpen] = useState(false);
  const [selectedClientTemplates, setSelectedClientTemplates] = useState(null);
  const [newClientSearchQuery, setNewClientSearchQuery] = useState('');
  const [newClientSearchResults, setNewClientSearchResults] = useState([]);
  
  useEffect(() => {
    fetchTemplates();
    fetchCreators();
    checkAdminStatus();
  }, []);

  const fetchCreators = async () => {
    try {
      const token = await getAccessToken();
      const response = await axios.get(API_ENDPOINTS.EMPLOYEES.LIST, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const creatorMap = {};
      response.data.forEach(employee => {
        creatorMap[employee.user_id] = `${employee.first_name} ${employee.last_name}`;
      });
      setCreators(creatorMap);
    } catch (error) {
      console.error('Error fetching creators:', error);
    }
  };
  
  const fetchTemplates = async () => {
    try {
      const token = await getAccessToken();
      const response = await axios.get(API_ENDPOINTS.TASK_TEMPLATES, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setTemplates(response.data);
      
      // Initialize any new groups as collapsed, but preserve existing state
      const newGroups = {};
      response.data.forEach(template => {
        const clientAssignee = template.assignees?.find(a => a.type === 'client');
        const groupKey = clientAssignee ? clientAssignee.name : 'unassigned';
        if (expandedGroups[groupKey] === undefined) {
          newGroups[groupKey] = false; // New groups start collapsed
        }
      });
      
      setExpandedGroups(prev => ({
        ...prev,
        ...newGroups
      }));
    } catch (error) {
      console.error('Error fetching templates:', error);
    }
  };

  const toggleGroup = (groupKey) => {
    setExpandedGroups(prev => ({
      ...prev,
      [groupKey]: !prev[groupKey]
    }));
  };

  // Group templates by client
  const groupedTemplates = templates.reduce((groups, template) => {
    const clientAssignee = template.assignees?.find(a => a.type === 'client');
    const groupKey = clientAssignee ? clientAssignee.name : 'unassigned';
    
    if (!groups[groupKey]) {
      groups[groupKey] = [];
    }
    groups[groupKey].push(template);
    return groups;
  }, {});

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
  
  const handleSearchChange = (e) => {
    const query = e.target.value;
    setSearchQuery(query);
    searchAssignees(query);
  };
  
  const handleSelectAssignee = (assignee) => {
    if (!selectedAssignees.find(a => a.id === assignee.id)) {
      const newAssignees = [...selectedAssignees, assignee];
      setSelectedAssignees(newAssignees);
      setFormData({ ...formData, assignees: newAssignees });
    }
    setSearchQuery('');
    setSearchResults([]);
  };
  
  const handleRemoveAssignee = (assigneeId) => {
    const newAssignees = selectedAssignees.filter(a => a.id !== assigneeId);
    setSelectedAssignees(newAssignees);
    setFormData({ ...formData, assignees: newAssignees });
  };
  
  const resetForm = () => {
    setFormData({
      title: '',
      description: '',
      frequency: 'daily',
      priority: 'medium',
      job_type: '',
      assignees: [],
      is_active: true
    });
    setSelectedAssignees([]);
    setEditingTemplate(null);
  };

  const handleEdit = (template) => {
    setEditingTemplate(template);
    setFormData({
      title: template.title,
      description: template.description,
      frequency: template.frequency,
      priority: template.priority,
      job_type: template.job_type,
      assignees: template.assignees,
      is_active: template.is_active
    });
    setSelectedAssignees(template.assignees || []);
    setIsOpen(true);
  };
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!isAdmin) {
      toast.error('You do not have permission to create or edit templates');
      return;
    }
    
    try {
      const token = await getAccessToken();
      if (editingTemplate) {
        const updateData = {
          ...editingTemplate,
          title: formData.title,
          description: formData.description,
          frequency: formData.frequency,
          priority: formData.priority,
          job_type: formData.job_type,
          assignees: formData.assignees,
          is_active: formData.is_active
        };
        await axios.put(`${API_ENDPOINTS.TASK_TEMPLATES}/${editingTemplate._id}`, updateData, {
          headers: { Authorization: `Bearer ${token}` }
        });
      } else {
        await axios.post(API_ENDPOINTS.TASK_TEMPLATES, formData, {
          headers: { Authorization: `Bearer ${token}` }
        });
      }
      setIsOpen(false);
      fetchTemplates();
      resetForm();
    } catch (error) {
      console.error('Error saving template:', error);
      if (error.response?.status === 403) {
        toast.error('You do not have permission to perform this action');
      }
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
  
  const handleDuplicate = async (template) => {
    try {
      const token = await getAccessToken();
      await axios.post(`${API_ENDPOINTS.TASK_TEMPLATES}/${template._id}/duplicate`, {}, {
        headers: { Authorization: `Bearer ${token}` }
      });
      fetchTemplates();
    } catch (error) {
      console.error('Error duplicating template:', error);
    }
  };
  
  // Update handleBulkCopy with debugging and fixes
  const handleBulkCopy = async (newClient) => {
    try {
      const token = await getAccessToken();
      
      console.log('Selected templates to copy:', selectedClientTemplates);
      console.log('New client to assign:', newClient);
      
      // Create copies of all templates with the new client but keep other assignees
      const copyPromises = selectedClientTemplates.map(template => {
        console.log('Original template assignees:', template.assignees);
        
        // Keep all employee assignees
        const employeeAssignees = template.assignees?.filter(a => a.type === 'employee') || [];
        console.log('Filtered employee assignees:', employeeAssignees);
        
        // Create new template object without _id
        const { _id, ...templateWithoutId } = template;
        
        const newTemplate = {
          ...templateWithoutId,
          assignees: [
            ...employeeAssignees,
            { 
              id: newClient.id,
              name: newClient.name,
              type: 'client'
            }
          ]
        };
        
        console.log('New template to create:', newTemplate);
        
        return axios.post(API_ENDPOINTS.TASK_TEMPLATES, newTemplate, {
          headers: { Authorization: `Bearer ${token}` }
        });
      });
      
      await Promise.all(copyPromises);
      setBulkCopyModalOpen(false);
      setSelectedClientTemplates(null);
      setNewClientSearchQuery('');
      setNewClientSearchResults([]);
      fetchTemplates();
    } catch (error) {
      console.error('Error bulk copying templates:', error);
      console.error('Error details:', error.response?.data);
    }
  };
  
  // Update searchClients to use the task assignee search endpoint
  const searchClients = async (query) => {
    if (!query) {
      setNewClientSearchResults([]);
      return;
    }
    try {
      const token = await getAccessToken();
      const response = await axios.get(`${API_ENDPOINTS.TASKS}/search_assignees?query=${query}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      
      // Filter to only show clients from the results
      const clientResults = (response.data.assignees || [])
        .filter(assignee => assignee.type === 'client');
      
      setNewClientSearchResults(clientResults);
    } catch (error) {
      console.error('Error searching clients:', error);
      setNewClientSearchResults([]);
    }
  };
  
  // Update handleDelete function with correct endpoint
  const handleDelete = async (templateId) => {
    if (!window.confirm('Are you sure you want to delete this template?')) {
      return;
    }
    
    try {
      const token = await getAccessToken();
      await axios.delete(`${API_ENDPOINTS.TASK_TEMPLATES}/${templateId}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      fetchTemplates();
    } catch (error) {
      console.error('Error deleting template:', error);
      if (error.response) {
        console.error('Error details:', error.response.data);
      }
    }
  };
  
  // Update handleDeleteGroup function with correct endpoint
  const handleDeleteGroup = async (templates) => {
    if (!window.confirm('Are you sure you want to delete all templates for this client?')) {
      return;
    }
    
    try {
      const token = await getAccessToken();
      const deletePromises = templates.map(template => 
        axios.delete(`${API_ENDPOINTS.TASK_TEMPLATES}/${template._id}`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      );
      await Promise.all(deletePromises);
      fetchTemplates();
    } catch (error) {
      console.error('Error deleting templates:', error);
      if (error.response) {
        console.error('Error details:', error.response.data);
      }
    }
  };
  
  // Add admin check
  const checkAdminStatus = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const groups = tokens.accessToken.payload['cognito:groups'] || [];
      setIsAdmin(groups.includes('ADMIN'));
    } catch (error) {
      console.error('Error checking admin status:', error);
      setIsAdmin(false);
    }
  };
  
  return (
    <div className="task-template">
      <div className="task-template__header">
        <h1>Task Templates</h1>
        <button 
          className="task-template__create-button"
          onClick={() => {
            resetForm();
            setIsOpen(true);
          }}
        >
          Create Template
        </button>
      </div>
      
      <div className="task-template__groups">
        {Object.entries(groupedTemplates).map(([clientName, clientTemplates]) => (
          <div key={clientName} className="template-group">
            <div 
              className="template-group__header"
              onClick={() => toggleGroup(clientName)}
            >
              <div className="template-group__header-content">
                <h2>{clientName === 'unassigned' ? 'Unassigned Templates' : clientName}</h2>
                <span className="template-group__count">
                  {clientTemplates.length} templates
                </span>
              </div>
              {clientName !== 'unassigned' && (
                <div className="template-group__actions">
                  <button
                    className="task-template__copy-all-button"
                    onClick={(e) => {
                      e.stopPropagation();
                      console.log('Templates to copy for client:', clientName, clientTemplates);
                      setSelectedClientTemplates([...clientTemplates]);
                      setBulkCopyModalOpen(true);
                    }}
                  >
                    <i className="far fa-copy"></i>
                  </button>
                  <button
                    className="task-template__delete-button"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteGroup(clientTemplates);
                    }}
                  >
                    <i className="fas fa-trash-alt"></i>
                  </button>
                </div>
              )}
              <button className="template-group__toggle">
                {expandedGroups[clientName] ? '−' : '+'}
              </button>
            </div>
            
            {expandedGroups[clientName] && (
              <div className="template-group__content">
                <table className="task-template__table">
                  <thead>
                    <tr>
                      <th>Title</th>
                      <th>Job Type</th>
                      <th>Assignees</th>
                      <th>Frequency</th>
                      <th>Priority</th>
                      <th>Status</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {clientTemplates.map((template) => (
                      <tr key={template._id}>
                        <td>{template.title}</td>
                        <td>{template.job_type}</td>
                        <td>
                          <div className="assignee-tags">
                            {template.assignees?.filter(a => a.type === 'client').map(assignee => (
                              <span key={assignee.id} className="assignee-tag">
                                {assignee.name}
                              </span>
                            ))}
                          </div>
                        </td>
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
                        <td className="task-template__actions">
                          <button
                            className="task-template__edit-button"
                            onClick={() => handleEdit(template)}
                          >
                            <i className="fas fa-pencil-alt"></i>
                          </button>
                          <button
                            className="task-template__copy-button"
                            onClick={() => handleDuplicate(template)}
                          >
                            <i className="far fa-copy"></i>
                          </button>
                          <button
                            className="task-template__delete-button"
                            onClick={() => handleDelete(template._id)}
                          >
                            <i className="fas fa-trash-alt"></i>
                          </button>
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
              </div>
            )}
          </div>
        ))}
      </div>
      
      {isOpen && (
        <div className="task-modal-overlay">
          <div className="task-modal">
            <div className="task-modal__header">
              <h2>{editingTemplate ? 'Edit Task Template' : 'Create Task Template'}</h2>
              <button 
                className="task-modal__close"
                onClick={() => {
                  setIsOpen(false);
                  resetForm();
                }}
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
                  <div className="assignee-search">
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={handleSearchChange}
                      placeholder="Search employees or clients..."
                      className="task-modal__input"
                    />
                    {searchResults.length > 0 && (
                      <div className="assignee-search__results">
                        {searchResults.map(assignee => (
                          <div
                            key={assignee.id}
                            className="assignee-search__result"
                            onClick={() => handleSelectAssignee(assignee)}
                          >
                            <span>{assignee.name}</span>
                            <span className="assignee-type">{assignee.type}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="selected-assignees">
                    {selectedAssignees.map(assignee => (
                      <div key={assignee.id} className="selected-assignee">
                        <span>{assignee.name}</span>
                        <span className="assignee-type">{assignee.type}</span>
                        <button
                          type="button"
                          onClick={() => handleRemoveAssignee(assignee.id)}
                          className="remove-assignee"
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
                onClick={() => {
                  setIsOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </button>
              <button 
                type="submit" 
                className="task-modal__button task-modal__button--primary"
                onClick={handleSubmit}
              >
                {editingTemplate ? 'Save Changes' : 'Create Template'}
              </button>
            </div>
          </div>
        </div>
      )}
      
      {/* Add bulk copy modal */}
      {isBulkCopyModalOpen && (
        <div className="task-modal-overlay">
          <div className="task-modal">
            <div className="task-modal__header">
              <h2>Copy All Templates</h2>
              <button 
                className="task-modal__close"
                onClick={() => {
                  setBulkCopyModalOpen(false);
                  setSelectedClientTemplates(null);
                  setNewClientSearchQuery('');
                  setNewClientSearchResults([]);
                }}
              >
                ×
              </button>
            </div>
            <div className="task-modal__body">
              <div className="task-modal__field">
                <label>Select New Client</label>
                <div className="assignee-search">
                  <input
                    type="text"
                    value={newClientSearchQuery}
                    onChange={(e) => {
                      const value = e.target.value;
                      setNewClientSearchQuery(value);
                      if (value.length >= 2) {
                        searchClients(value);
                      } else {
                        setNewClientSearchResults([]);
                      }
                    }}
                    placeholder="Search for a client..."
                    className="task-modal__input"
                  />
                  {newClientSearchResults.length > 0 && (
                    <div className="assignee-search__results">
                      {newClientSearchResults.map(result => (
                        <div
                          key={result.id}
                          className="assignee-search__result"
                          onClick={() => handleBulkCopy(result)}
                        >
                          <span>{result.name}</span>
                          <span className="assignee-type">{result.type}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default TaskTemplate; 