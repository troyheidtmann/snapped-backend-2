/**
 * @fileoverview Edit Modal component for managing lead/client information.
 * Provides functionality for editing client details, managing employee assignments,
 * handling payouts, and syncing analytics.
 */

/**
 * @typedef {Object} EditModalProps
 * @property {boolean} isOpen - Whether the modal is open
 * @property {Function} onClose - Callback function to close the modal
 * @property {Object} data - Client data to edit
 * @property {Function} onSave - Callback function called after successful save
 * @property {Array<Object>} partners - List of available partners
 */

/**
 * @typedef {Object} Employee
 * @property {string} _id - Employee ID
 * @property {string} name - Employee name
 * @property {string} email - Employee email
 * @property {string} role - Employee role
 */

/**
 * @typedef {Object} PayoutInfo
 * @property {string} payee_id - Payee ID
 * @property {number} percentage - Payout percentage
 * @property {string} name - Payee name
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { 
  faUser, 
  faProjectDiagram,
  faShareNodes,
  faFileContract,
  faMoneyBill,
  faChartLine,
  faCheckCircle,
  faCamera,
  faVideo,
  faPlay,
  faGhost,
  faArrowRight
} from '@fortawesome/free-solid-svg-icons';
import { toast } from 'react-hot-toast';
import './styles/EditLeadModal.css';
import { useAuth } from '../../contexts/AuthContext';
import { fetchAuthSession } from 'aws-amplify/auth';

/**
 * Edit Modal component for managing client information and settings.
 * Features include:
 * - Editing client personal and social media information
 * - Managing employee assignments
 * - Handling payout configurations
 * - Syncing analytics and social media stats
 * 
 * @param {EditModalProps} props - Component props
 * @returns {React.ReactElement} The rendered modal
 */
const EditModal = ({ isOpen, onClose, data, onSave, partners }) => {
  const { user } = useAuth();
  
  // Add debugging logs
  console.log('Auth user:', user);
  
  const [isAdmin, setIsAdmin] = useState(false);

  // Add this useEffect to check admin status
  useEffect(() => {
    const checkAdminStatus = async () => {
      try {
        const { tokens } = await fetchAuthSession();
        const groups = tokens.accessToken.payload['cognito:groups'] || [];
        console.log('Token groups:', groups);
        setIsAdmin(groups.includes('ADMIN'));
      } catch (error) {
        console.error('Error checking admin status:', error);
        setIsAdmin(false);
      }
    };
    
    if (isOpen) {
      checkAdminStatus();
    }
  }, [isOpen]);

  const [formData, setFormData] = useState({
    First_Legal_Name: '',
    Last_Legal_Name: '',
    Email_Address: '',
    Stage_Name: '',
    DOB: '',
    IG_Username: '',
    IG_Followers: 0,
    IG_Verified: false,
    IG_Engagement: 0,
    TT_Username: '',
    TT_Followers: 0,
    TT_Verified: false,
    YT_Username: '',
    YT_Followers: 0,
    YT_Verified: false,
    Snap_Username: '',
    Snap_Followers: 0,
    Snap_Star: false,
    Snap_Monetized: false,
    is_contractout: false,
    is_signed: false,
    is_groupchat: false,
    is_dead: false,
    client_id: '',
    snap_id: '',
    assigned_employees: [],
    payout_email: '',
  });
  const [employeeSearch, setEmployeeSearch] = useState('');
  const [matchingEmployees, setMatchingEmployees] = useState([]);
  const [loading, setLoading] = useState(false);
  const [assignedEmployees, setAssignedEmployees] = useState([]);
  const [profileName, setProfileName] = useState('');
  const [syncStatus, setSyncStatus] = useState('');
  const [payoutInfo, setPayoutInfo] = useState(null);
  const [isSyncing, setIsSyncing] = useState(false);
  const [activeSection, setActiveSection] = useState('personal');
  const [isSaving, setIsSaving] = useState(false);
  const [showSplitModal, setShowSplitModal] = useState(false);
  const [selectedPayee, setSelectedPayee] = useState(null);
  const [splitPercentage, setSplitPercentage] = useState('');
  const [splits, setSplits] = useState([]);
  const [payees, setPayees] = useState([]);

  const sections = [
    { id: 'personal', label: 'Personal Information', icon: faUser },
    { id: 'socials', label: 'Social Media', icon: faShareNodes },
    { id: 'contract', label: 'Contract Status', icon: faFileContract },
    { id: 'payout', label: 'Payout Information', icon: faMoneyBill },
    { id: 'backend', label: 'Backend', icon: faProjectDiagram }
  ];

  const socialPlatforms = [
    {
      id: 'instagram',
      name: 'Instagram',
      fields: [
        { name: 'IG_Username', label: 'Username', type: 'text' },
        { name: 'IG_Followers', label: 'Followers', type: 'number' },
        { name: 'IG_Engagement', label: 'Engagement Rate', type: 'number', step: '0.01' },
        { name: 'IG_Rank', label: 'Rank', type: 'number' },
        { name: 'IG_Views_Rank', label: 'Views Rank', type: 'number' },
        { name: 'IG_Verified', label: 'Verified', type: 'checkbox' }
      ],
      fetchStats: (username) => fetchSocialStats('instagram', username)
    },
    {
      id: 'tiktok',
      name: 'TikTok',
      fields: [
        { name: 'TT_Username', label: 'Username', type: 'text' },
        { name: 'TT_Followers', label: 'Followers', type: 'number' },
        { name: 'TT_Rank', label: 'Rank', type: 'number' },
        { name: 'TT_Views_Rank', label: 'Views Rank', type: 'number' },
        { name: 'TT_Verified', label: 'Verified', type: 'checkbox' }
      ],
      fetchStats: (username) => fetchSocialStats('tiktok', username)
    },
    {
      id: 'youtube',
      name: 'YouTube',
      fields: [
        { name: 'YT_Username', label: 'Username', type: 'text' },
        { name: 'YT_Followers', label: 'Followers', type: 'number' },
        { name: 'YT_Rank', label: 'Rank', type: 'number' },
        { name: 'YT_Views_Rank', label: 'Views Rank', type: 'number' },
        { name: 'YT_Verified', label: 'Verified', type: 'checkbox' }
      ],
      fetchStats: (username) => fetchSocialStats('youtube', username)
    },
    {
      id: 'snapchat',
      name: 'Snapchat',
      fields: [
        { name: 'Snap_Username', label: 'Username', type: 'text' },
        { name: 'Snap_Followers', label: 'Followers', type: 'number' },
        { name: 'Snap_Star', label: 'Verified', type: 'checkbox' }
      ]
    }
  ];

  /**
   * Loads the list of employees assigned to the client
   * @async
   * @returns {Promise<void>}
   */
  const loadAssignedEmployees = async () => {
    if (formData.assigned_employees?.length) {
      try {
        const { tokens } = await fetchAuthSession();
        const response = await axios.get(API_ENDPOINTS.EMPLOYEES.GET_ASSIGNED, {
          params: {
            user_ids: formData.assigned_employees.join(',')
          },
          headers: {
            'Authorization': `Bearer ${tokens.accessToken.toString()}`
          }
        });
        setAssignedEmployees(response.data);
      } catch (error) {
        console.error('Error loading assigned employees:', error);
      }
    }
  };

  // Load existing assigned employees on modal open
  useEffect(() => {
    if (formData.assigned_employees?.length) {
      loadAssignedEmployees();
    }
  }, [formData.assigned_employees]);

  /**
   * Assigns an employee to the client
   * @param {Employee} employee - Employee to assign
   * @returns {void}
   */
  const handleAssignEmployee = (employee) => {
    setAssignedEmployees(prev => [...prev, employee]);
    setFormData(prev => ({
      ...prev,
      assigned_employees: [...(prev.assigned_employees || []), employee.user_id]
    }));
    setEmployeeSearch('');
    setMatchingEmployees([]);
  };

  /**
   * Removes an employee from the client's assignments
   * @param {string} userId - ID of the employee to remove
   * @returns {void}
   */
  const handleRemoveEmployee = (userId) => {
    setAssignedEmployees(prev => prev.filter(emp => emp.user_id !== userId));
    setFormData(prev => ({
      ...prev,
      assigned_employees: prev.assigned_employees.filter(id => id !== userId)
    }));
  };

  useEffect(() => {
    if (isOpen && data) {
      console.log('Setting form data:', data);
      setFormData(prevData => ({
        ...prevData,
        ...data,
        IG_Followers: data.IG_Followers || 0,
        IG_Verified: Boolean(data.IG_Verified),
        IG_Engagement: data.IG_Engagement || 0,
        TT_Followers: data.TT_Followers || 0,
        TT_Verified: Boolean(data.TT_Verified),
        YT_Followers: data.YT_Followers || 0,
        YT_Verified: Boolean(data.YT_Verified),
        Snap_Followers: data.Snap_Followers || 0,
        Snap_Star: Boolean(data.Snap_Star),
        Snap_Monetized: Boolean(data.Snap_Monetized),
        is_contractout: Boolean(data.is_contractout),
        is_signed: Boolean(data.is_signed),
        is_groupchat: Boolean(data.is_groupchat),
        is_dead: Boolean(data.is_dead),
        assigned_employees: data.assigned_employees || [],
      }));
      
      if (data.assigned_employees?.length) {
        loadAssignedEmployees();
      }
      
      if (data.client_id) {
        loadPayoutInfo();
        fetchApprovalStatus(data.client_id);
      }
    }
  }, [isOpen, data]);

  /**
   * Loads payout information for the client
   * @async
   * @returns {Promise<void>}
   */
  const loadPayoutInfo = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const response = await axios.get(API_ENDPOINTS.LEADS.PAYOUT_INFO(data.client_id), {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`
        }
      });
      const info = response.data;
      setPayoutInfo(info);
      if (info.payout_email) {
        setFormData(prev => ({ ...prev, payout_email: info.payout_email }));
      }
    } catch (error) {
      console.error('Error loading payout info:', error);
    }
  };

  /**
   * Syncs payout information with external systems
   * @async
   * @returns {Promise<void>}
   */
  const handleSyncPayout = async () => {
    try {
      setIsSyncing(true);
      const { tokens } = await fetchAuthSession();
      const response = await axios.post(API_ENDPOINTS.LEADS.SYNC_PAYOUT, {
        client_id: data.client_id,
        payout_email: formData.payout_email
      }, {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`
        }
      });
      
      if (response.data.status === 'success') {
        await loadPayoutInfo();
        toast.success('Payout email synced successfully');
      } else {
        toast.error(response.data.detail || 'Failed to sync payout email');
      }
    } catch (error) {
      console.error('Error syncing payout:', error);
      toast.error('Failed to sync payout email');
    } finally {
      setIsSyncing(false);
    }
  };

  /**
   * Handles changes to form fields
   * @param {React.ChangeEvent<HTMLInputElement>} e - Change event
   * @returns {void}
   */
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    
    if (type === 'checkbox') {
      setFormData(prev => ({
        ...prev,
        [name]: checked
      }));
    } else {
      setFormData(prev => ({
        ...prev,
        [name]: type === 'number' ? Number(value) : value
      }));
    }
  };

  /**
   * Handles form submission
   * @async
   * @param {React.FormEvent} e - Form event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    const updatedData = { ...formData };
    
    try {
      setIsSaving(true);
      const { tokens } = await fetchAuthSession();
      
      if (updatedData.client_id) {
        const uploadApprovalResponse = await axios.post(
          API_ENDPOINTS.LEADS.UPDATE_APPROVAL_STATUS,
          { 
            client_id: updatedData.client_id,
            approved: updatedData.approved
          },
          {
            headers: {
              'Authorization': `Bearer ${tokens.accessToken.toString()}`
            }
          }
        );
        console.log('Content approval update response:', uploadApprovalResponse.data);
      }

      await onSave(updatedData);
      
      // Show success toast
      toast.success('Information Updated', {
        duration: 3000,
        position: 'top-center',
      });
      
    } catch (error) {
      console.error('Error saving lead:', error);
      toast.error('Failed to save changes');
    } finally {
      setIsSaving(false);
    }
  };

  /**
   * Fetches social media statistics for a platform
   * @async
   * @param {string} platform - Social media platform
   * @param {string} username - Username on the platform
   * @returns {Promise<Object>} Platform statistics
   */
  const fetchSocialStats = async (platform, username) => {
    if (!username || username.length < 3) return;
    
    try {
      const { tokens } = await fetchAuthSession();
      const response = await axios.get(API_ENDPOINTS.SOCIAL_STATS(platform, username), {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`
        }
      });
      console.log(`${platform} response:`, response.data);
      
      if (response.data) {
        console.log(`${platform} statistics:`, response.data.statistics);
        console.log(`${platform} misc:`, response.data.misc);
        console.log(`${platform} ranks:`, response.data.ranks);
        
        switch(platform) {
          case 'instagram':
            const updatedData = {
              IG_Followers: response.data.statistics?.followers || 0,
              IG_Verified: response.data.misc?.sb_verified || false,
              IG_Engagement: response.data.statistics?.engagement_rate || 0,
              IG_Rank: response.data.ranks?.sbrank || 0,
              IG_Views_Rank: response.data.ranks?.following || 0
            };
            console.log('Updating Instagram data:', updatedData);
            setFormData(prev => ({
              ...prev,
              ...updatedData
            }));
            break;
          case 'tiktok':
            setFormData(prev => ({
              ...prev,
              TT_Followers: response.data.statistics?.followers || 0,
              TT_Verified: response.data.misc?.sb_verified || false,
              TT_Rank: response.data.ranks?.sbrank || 0,
              TT_Views_Rank: response.data.ranks?.following || 0
            }));
            break;
          case 'youtube':
            setFormData(prev => ({
              ...prev,
              YT_Followers: response.data.statistics?.subscribers || 0,
              YT_Verified: response.data.misc?.sb_verified || false,
              YT_Rank: response.data.ranks?.sbrank || 0,
              YT_Views_Rank: response.data.ranks?.views || 0
            }));
            break;
        }
      }
    } catch (error) {
      console.error(`Error fetching ${platform} stats:`, error);
      if (error.response?.status === 404) {
        toast.error(`${platform.charAt(0).toUpperCase() + platform.slice(1)} user "${username}" not found. Please check the username and try again.`);
      } else if (error.response?.data?.detail) {
        toast.error(`Error: ${error.response.data.detail}`);
      } else {
        toast.error(`Failed to fetch ${platform} stats. Please try again later.`);
      }
    }
  };

  /**
   * Searches for employees by name or email
   * @async
   * @param {string} searchTerm - Search term
   * @returns {Promise<Array<Employee>>} Matching employees
   */
  const searchEmployees = async (searchTerm) => {
    if (!formData.referred_by && !formData.monetized_by) return;
    
    setLoading(true);
    try {
      const { tokens } = await fetchAuthSession();
      const partnerIds = [...new Set([formData.referred_by, formData.monetized_by].filter(Boolean))];
      
      const searchPromises = partnerIds.map(partnerId =>
        axios.get(API_ENDPOINTS.EMPLOYEES.SEARCH, {
          params: {
            partner_id: partnerId,
            search: searchTerm
          },
          headers: {
            'Authorization': `Bearer ${tokens.accessToken.toString()}`
          }
        })
      );
      
      const responses = await Promise.all(searchPromises);
      
      const combinedResults = responses.reduce((acc, response) => {
        const newEmployees = response.data.filter(newEmp => 
          !acc.some(existingEmp => existingEmp.user_id === newEmp.user_id)
        );
        return [...acc, ...newEmployees];
      }, []);
      
      setMatchingEmployees(combinedResults);
    } catch (error) {
      console.error('Error searching employees:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const timer = setTimeout(() => {
      if (employeeSearch.length >= 2) {
        searchEmployees(employeeSearch);
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [employeeSearch, formData.referred_by, formData.monetized_by]);

  /**
   * Syncs analytics data with external systems
   * @async
   * @returns {Promise<void>}
   */
  const handleSyncAnalytics = async () => {
    try {
      setSyncStatus('Syncing...');
      
      const { tokens } = await fetchAuthSession();
      const response = await fetch('https://track.snapped.cc/api/analytics/sync/profile', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          profile_name: profileName,
          snap_id: formData.snap_id,
          user_id: formData.client_id
        })
      });

      if (!response.ok) {
        throw new Error('Sync failed');
      }

      setSyncStatus('Sync successful!');
      setTimeout(() => setSyncStatus(''), 3000);

    } catch (error) {
      console.error('Sync error:', error);
      setSyncStatus('Sync failed');
      setTimeout(() => setSyncStatus(''), 3000);
    }
  };

  /**
   * Fetches approval status for a client
   * @async
   * @param {string} clientId - Client ID
   * @returns {Promise<Object>} Approval status
   */
  const fetchApprovalStatus = async (clientId) => {
    try {
      const { tokens } = await fetchAuthSession();
      const response = await axios.get(API_ENDPOINTS.LEADS.APPROVAL_STATUS(clientId), {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`
        }
      });
      if (response.data && response.data.status === 'success') {
        setFormData(prev => ({
          ...prev, 
          approved: response.data.approved
        }));
      }
    } catch (error) {
      console.error('Error fetching approval status:', error);
    }
  };

  /**
   * Renders social platform input fields
   * @param {string} platform - Social media platform
   * @returns {React.ReactElement} Rendered platform fields
   */
  const renderSocialPlatform = (platform) => {
    return (
      <div key={platform.id} className="-editmodel-social-section">
        <div className="-editmodel-platform-header">
          <FontAwesomeIcon icon={platform.icon} />
          {platform.name}
        </div>

        <div className="-editmodel-platform-content">
          <div className="-editmodel-username-group">
            <input
              type="text"
              name={platform.fields.find(f => f.name.endsWith('Username')).name}
              value={formData[platform.fields.find(f => f.name.endsWith('Username')).name] || ''}
              onChange={handleChange}
              placeholder="Username"
            />
            {platform.fetchStats && (
              <button 
                type="button"
                onClick={() => platform.fetchStats(formData[platform.fields.find(f => f.name.endsWith('Username')).name])}
                className="-editmodel-fetch-button"
                disabled={!formData[platform.fields.find(f => f.name.endsWith('Username')).name]}
              >
                Fetch
              </button>
            )}
          </div>

          {platform.fields.filter(f => f.type === 'number' && !f.name.endsWith('Username')).map(field => (
            <input
              key={field.name}
              type="number"
              name={field.name}
              value={formData[field.name] || ''}
              onChange={handleChange}
              className="-editmodel-stat-input"
              placeholder={field.label}
              step={field.step || '1'}
            />
          ))}
        </div>

        <div className="-editmodel-checkbox-group">
          {platform.fields.filter(f => f.type === 'checkbox').map(field => (
            <div key={field.name} className="-editmodel-checkbox-item">
              <input
                type="checkbox"
                id={field.name}
                name={field.name}
                checked={formData[field.name] || false}
                onChange={handleChange}
              />
              <label htmlFor={field.name}>{field.label}</label>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const handleAddSplit = async () => {
    if (!selectedPayee || !splitPercentage || !data.client_id) {
      toast.error('Please select a payee and enter a percentage');
      return;
    }

    const percentage = parseInt(splitPercentage);
    if (isNaN(percentage) || percentage <= 0 || percentage > 100) {
      toast.error('Please enter a valid percentage between 1 and 100');
      return;
    }

    try {
      const { tokens } = await fetchAuthSession();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.SAVE_SPLIT_PROFILE, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          clientId: data.client_id,
          splits: [...splits, {
            payeeId: selectedPayee._id,
            payeeName: selectedPayee.name,
            companyName: selectedPayee.company_name || '',
            percentage
          }]
        })
      });

      if (!response.ok) {
        throw new Error('Failed to add split');
      }

      await loadSplits();
      setShowSplitModal(false);
      setSelectedPayee(null);
      setSplitPercentage('');
      toast.success('Split added successfully');
    } catch (error) {
      console.error('Error adding split:', error);
      toast.error('Failed to add split');
    }
  };

  const handleRemoveSplit = async (payeeId) => {
    try {
      const { tokens } = await fetchAuthSession();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.DELETE_SPLIT_PROFILE(data.client_id, payeeId), {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error('Failed to remove split');
      }

      await loadSplits();
      toast.success('Split removed successfully');
    } catch (error) {
      console.error('Error removing split:', error);
      toast.error('Failed to remove split');
    }
  };

  const loadPayees = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.SEARCH_PAYEES, {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error('Failed to load payees');
      }

      const data = await response.json();
      setPayees(data.payees || []);
    } catch (error) {
      console.error('Error loading payees:', error);
      toast.error('Failed to load payees');
    }
  };

  const loadSplits = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.GET_SPLIT_PROFILE(data.client_id), {
        headers: {
          'Authorization': `Bearer ${tokens.accessToken.toString()}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error('Failed to load splits');
      }

      const splitData = await response.json();
      setSplits(splitData.splits || []);
    } catch (error) {
      console.error('Error loading splits:', error);
      toast.error('Failed to load splits');
    }
  };

  useEffect(() => {
    if (isOpen && data?.client_id) {
      loadPayees();
      loadSplits();
    }
  }, [isOpen, data?.client_id]);

  if (!isOpen) return null;

  return (
    <div className="-editmodel-overlay">
      <div className="-editmodel-container">
        <div className="-editmodel-sidebar">
          {sections.map(section => (
            <div
              key={section.id}
              className={`-editmodel-nav-item ${activeSection === section.id ? 'active' : ''}`}
              onClick={() => setActiveSection(section.id)}
            >
              <FontAwesomeIcon icon={section.icon} />
              <span>{section.label}</span>
            </div>
          ))}
        </div>

        <div className="-editmodel-main">
          <div className="-editmodel-header">
            <h2>Edit Lead: {formData.First_Legal_Name} {formData.Last_Legal_Name}</h2>
            <button className="-editmodel-close" onClick={onClose}>&times;</button>
          </div>

          <form onSubmit={handleSubmit}>
            <div className="-editmodel-content">
              {/* Personal Information Section */}
              {activeSection === 'personal' && (
                <div className="-editmodel-section">
                  <div className="-editmodel-section-header">
                    <h3>Personal Information</h3>
                  </div>
                  <div className="-editmodel-grid">
                    <div className="-editmodel-field">
                      <label>First Name</label>
                      <input
                        name="First_Legal_Name"
                        value={formData.First_Legal_Name || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Last Name</label>
                      <input
                        name="Last_Legal_Name"
                        value={formData.Last_Legal_Name || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Stage Name</label>
                      <input
                        name="Stage_Name"
                        value={formData.Stage_Name || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Email</label>
                      <input
                        type="email"
                        name="Email_Address"
                        value={formData.Email_Address || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Date of Birth</label>
                      <input
                        name="DOB"
                        placeholder="MMDDYYYY"
                        value={formData.DOB || ''}
                        onChange={(e) => {
                          const value = e.target.value.replace(/\D/g, '').slice(0, 8);
                          handleChange({
                            target: {
                              name: 'DOB',
                              value
                            }
                          });
                        }}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Timezone</label>
                      <input
                        name="Timezone"
                        value={formData.Timezone || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Client ID</label>
                      <input
                        name="client_id"
                        value={formData.client_id || ''}
                        onChange={handleChange}
                      />
                    </div>
                    <div className="-editmodel-field">
                      <label>Snap ID</label>
                      <input
                        name="snap_id"
                        value={formData.snap_id || ''}
                        onChange={handleChange}
                      />
                    </div>
                  </div>
                </div>
              )}

              {/* Backend Section */}
              {activeSection === 'backend' && (
                <div className="-editmodel-content">
                  {/* Project Management Module */}
                  <div className="pm-module">
                    <div className="pm-module-header">
                      <h3>Project Management</h3>
                    </div>
                    <div className="pm-company-section">
                      <div className="pm-company-label">Partner Company</div>
                      <input
                        type="text"
                        value={formData.referred_by_name || ''}
                        disabled
                        className="pm-company-input"
                      />
                    </div>
                    {formData.referred_by && (
                      <div className="pm-manager-section">
                        <div className="pm-manager-label">Project Manager Assignment</div>
                        <div className="pm-search-container">
                          <input
                            type="text"
                            value={employeeSearch}
                            onChange={(e) => setEmployeeSearch(e.target.value)}
                            placeholder="Type to search for project managers..."
                            className="pm-search-input"
                          />
                          {employeeSearch.length >= 2 && matchingEmployees.length > 0 && (
                            <div className="pm-search-results">
                              {matchingEmployees.map(emp => (
                                <div 
                                  key={emp.user_id}
                                  className="pm-search-result"
                                  onClick={() => handleAssignEmployee(emp)}
                                >
                                  {emp.first_name} {emp.last_name}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                        <div className="pm-tags-container">
                          {assignedEmployees.map(emp => (
                            <div key={emp.user_id} className="pm-tag">
                              <span className="pm-tag-name">
                                {emp.first_name} {emp.last_name}
                              </span>
                              <button 
                                onClick={() => handleRemoveEmployee(emp.user_id)}
                                className="pm-tag-remove"
                                title="Remove project manager"
                              >
                                ×
                              </button>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Analytics Module */}
                  <div className="analytics-module">
                    <div className="analytics-header">
                      <h3>Analytics</h3>
                    </div>
                    <div className="analytics-field">
                      <div className="analytics-label">Profile Name</div>
                      <div className="analytics-input-group">
                        <input
                          type="text"
                          value={profileName}
                          onChange={(e) => setProfileName(e.target.value)}
                          placeholder="Enter Snapchat profile name"
                          className="analytics-input"
                        />
                        <button
                          type="button"
                          onClick={handleSyncAnalytics}
                          className="analytics-sync-button"
                        >
                          Sync Analytics
                        </button>
                      </div>
                      {syncStatus && (
                        <div className={`analytics-sync-status ${
                          syncStatus === 'Syncing...' ? 'syncing' :
                          syncStatus === 'Sync successful!' ? 'success' :
                          'error'
                        }`}>
                          {syncStatus}
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Content Approval Module */}
                  <div className="ca-module">
                    <div className="ca-header">
                      <h3>Content Approval</h3>
                    </div>
                    <div className="ca-approval-row">
                      <input
                        type="checkbox"
                        id="content-approval"
                        name="approved"
                        checked={formData.approved}
                        onChange={handleChange}
                        className="ca-checkbox"
                      />
                      <label htmlFor="content-approval" className="ca-label">
                        Approve Client Content
                      </label>
                    </div>
                    <div className="ca-description">
                      Approving client content will mark their uploads as approved in the system.
                    </div>
                  </div>
                </div>
              )}

              {/* Social Media Section */}
              {activeSection === 'socials' && (
                <div className="-editmodel-section">
                  <div className="-editmodel-section-header">
                    <h3>Social Media Profiles</h3>
                  </div>
                  <div className="-editmodel-social-table">
                    <div className="-editmodel-table-header">
                      <div className="-editmodel-header-cell">Platform</div>
                      <div className="-editmodel-header-cell">Username</div>
                      <div className="-editmodel-header-cell">Verified</div>
                      <div className="-editmodel-header-cell">Followers</div>
                      <div className="-editmodel-header-cell">Rank</div>
                      <div className="-editmodel-header-cell">Views Rank</div>
                    </div>
                    {socialPlatforms.map(platform => {
                      const usernameField = platform.fields.find(f => f.name.endsWith('Username'));
                      const followersField = platform.fields.find(f => f.name.includes('Followers'));
                      const engagementField = platform.fields.find(f => f.name.includes('Engagement'));
                      const rankField = platform.fields.find(f => f.name.includes('Rank') && !f.name.includes('Views'));
                      const viewsRankField = platform.fields.find(f => f.name.includes('Views_Rank'));
                      const verifiedField = platform.fields.find(f => 
                        f.name.includes('Verified') || f.name === 'Snap_Star'
                      );

                      return (
                        <div key={platform.id} className="-editmodel-platform-row">
                          <div className="-editmodel-platform-name">
                            {platform.name}
                          </div>
                          <div className="-editmodel-username-cell">
                            <div className="-editmodel-username-group">
                              <input
                                type="text"
                                name={usernameField.name}
                                value={formData[usernameField.name] || ''}
                                onChange={handleChange}
                                placeholder="Username"
                              />
                              {platform.fetchStats && (
                                <button 
                                  type="button"
                                  onClick={() => platform.fetchStats(formData[usernameField.name])}
                                  className="-editmodel-fetch-button"
                                  disabled={!formData[usernameField.name]}
                                >
                                  Fetch
                                </button>
                              )}
                            </div>
                          </div>
                          <div className="-editmodel-verified-cell">
                            {verifiedField && (
                              <input
                                type="checkbox"
                                name={verifiedField.name}
                                checked={formData[verifiedField.name] || false}
                                onChange={handleChange}
                                className="-editmodel-verified-checkbox"
                              />
                            )}
                          </div>
                          <div className="-editmodel-stat-cell">
                            {followersField && (
                              <input
                                type="number"
                                name={followersField.name}
                                value={formData[followersField.name] || ''}
                                onChange={handleChange}
                                placeholder="Followers"
                                className="-editmodel-stat-input"
                              />
                            )}
                          </div>
                          <div className="-editmodel-stat-cell">
                            {(engagementField || rankField) && (
                              <input
                                type="number"
                                name={(engagementField || rankField).name}
                                value={formData[(engagementField || rankField).name] || ''}
                                onChange={handleChange}
                                placeholder={engagementField ? 'Engagement Rate' : 'Rank'}
                                step={engagementField ? '0.01' : '1'}
                                className="-editmodel-stat-input"
                              />
                            )}
                          </div>
                          <div className="-editmodel-stat-cell">
                            {viewsRankField && (
                              <input
                                type="number"
                                name={viewsRankField.name}
                                value={formData[viewsRankField.name] || ''}
                                onChange={handleChange}
                                placeholder="Views Rank"
                                className="-editmodel-stat-input"
                              />
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Contract Status Section */}
              {activeSection === 'contract' && (
                <div className="-editmodel-section">
                  <div className="contract-status-header">
                    <h3>Contract Status</h3>
                  </div>
                  <div className="contract-status-grid">
                    <div className="contract-status-cell">
                      <input
                        type="checkbox"
                        id="is_contractout"
                        name="is_contractout"
                        className="contract-status-checkbox"
                        checked={formData.is_contractout || false}
                        onChange={handleChange}
                      />
                      <label htmlFor="is_contractout">Contract Out</label>
                    </div>
                    <div className="contract-status-cell">
                      <input
                        type="checkbox"
                        id="is_signed"
                        name="is_signed"
                        className="contract-status-checkbox"
                        checked={formData.is_signed || false}
                        onChange={handleChange}
                      />
                      <label htmlFor="is_signed">Signed</label>
                    </div>
                    <div className="contract-status-cell">
                      <input
                        type="checkbox"
                        id="is_groupchat"
                        name="is_groupchat"
                        className="contract-status-checkbox"
                        checked={formData.is_groupchat || false}
                        onChange={handleChange}
                      />
                      <label htmlFor="is_groupchat">In Groupchat</label>
                    </div>
                    <div className="contract-status-cell">
                      <input
                        type="checkbox"
                        id="is_dead"
                        name="is_dead"
                        className="contract-status-checkbox"
                        checked={formData.is_dead || false}
                        onChange={handleChange}
                      />
                      <label htmlFor="is_dead">Dead</label>
                    </div>
                  </div>
                </div>
              )}

              {/* Payout Information Section */}
              {activeSection === 'payout' && (
                <>
                  <div className="-editmodel-section">
                    <div className="-editmodel-section-header">
                      <h3>Payout Information</h3>
                    </div>
                    <div className="-editmodel-grid">
                      <div className="-editmodel-field">
                        <label>Payout Email</label>
                        <div className="payout-email-group">
                          <input
                            type="email"
                            className="payout-email-input"
                            value={formData.payout_email || ''}
                            onChange={(e) => handleChange({
                              target: {
                                name: 'payout_email',
                                value: e.target.value
                              }
                            })}
                            placeholder="Enter payout email"
                          />
                          <button 
                            className="payout-sync-button"
                            onClick={handleSyncPayout}
                            disabled={isSyncing || !formData.payout_email}
                          >
                            {isSyncing ? 'Syncing...' : 'Sync'}
                          </button>
                        </div>
                      </div>

                      {payoutInfo && (
                        <div className="payout-info">
                          <div className="payout-stat">
                            <label>Total Paid to Date:</label>
                            <span>${payoutInfo.total_paid?.toLocaleString()}</span>
                          </div>
                          <div className="payout-stat">
                            <label>Year to Date:</label>
                            <span>${payoutInfo.year_to_date?.toLocaleString()}</span>
                          </div>
                          <div className="payout-stat">
                            <label>Quarter to Date:</label>
                            <span>${payoutInfo.quarter_to_date?.toLocaleString()}</span>
                          </div>
                          <div className="payout-sync-status">
                            Last synced: {payoutInfo.last_sync ? 
                              new Date(payoutInfo.last_sync).toLocaleString() : 
                              'Never'}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Payment Splits Section - Admin Only */}
                  {isAdmin && (
                    <div className="-editmodel-section">
                      <div className="-editmodel-section-header">
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
                          <h3>Payment Splits</h3>
                          <button 
                            className="-editmodel-fetch-button"
                            onClick={() => setShowSplitModal(true)}
                            style={{ marginLeft: 'auto' }}
                          >
                            Add Split
                          </button>
                        </div>
                      </div>
                      
                      {splits.length > 0 ? (
                        <div className="splits-table">
                          <div className="splits-table-header">
                            <div>Payee</div>
                            <div>Company</div>
                            <div>Percentage</div>
                            <div>Actions</div>
                          </div>
                          {splits.map((split) => (
                            <div key={split.payeeId} className="splits-table-row">
                              <div>{split.payeeName}</div>
                              <div>{split.companyName || 'N/A'}</div>
                              <div>{split.percentage}%</div>
                              <div>
                                <button
                                  className="split-remove-button"
                                  onClick={() => handleRemoveSplit(split.payeeId)}
                                >
                                  Remove
                                </button>
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="no-splits-message">
                          No payment splits configured
                        </div>
                      )}
                    </div>
                  )}

                  <div className="payout-history">
                    <div className="payout-history-header">
                      <h4>Payout History</h4>
                    </div>
                    <div className="payout-history-list">
                      {payoutInfo?.creator_pulls?.length > 0 ? (
                        payoutInfo.creator_pulls.map((pull, index) => (
                          <div key={index} className="payout-item">
                            <div className="payout-date">
                              {new Date(pull.pull_date).toLocaleDateString('en-US', {
                                year: 'numeric',
                                month: 'short',
                                day: 'numeric'
                              })}
                            </div>
                            <div className="payout-amount">
                              ${pull.pull_amount.toLocaleString(undefined, {
                                minimumFractionDigits: 2,
                                maximumFractionDigits: 2
                              })}
                            </div>
                            <div className="payout-status">
                              Completed
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="no-payouts-message">
                          No payout history available
                        </div>
                      )}
                    </div>
                  </div>
                </>
              )}
            </div>

            <div className="-editmodel-actions">
              <button 
                type="button" 
                className="-editmodel-button -editmodel-button-secondary" 
                onClick={onClose}
                disabled={isSaving}
              >
                Cancel
              </button>
              <button 
                type="submit" 
                className="-editmodel-button -editmodel-button-primary"
                disabled={isSaving}
              >
                {isSaving ? 'Saving...' : 'Save Changes'}
              </button>
            </div>
          </form>
        </div>
      </div>

      {/* Split Modal */}
      {showSplitModal && (
        <div className="split-modal-overlay">
          <div className="split-modal">
            <div className="split-modal-header">
              <h3>Add Payment Split</h3>
              <button onClick={() => setShowSplitModal(false)}>&times;</button>
            </div>
            <div className="split-modal-content">
              <div className="split-modal-field">
                <label>Payee</label>
                <select 
                  value={selectedPayee?._id || ''} 
                  onChange={(e) => {
                    const payee = payees.find(p => p._id === e.target.value);
                    setSelectedPayee(payee);
                  }}
                >
                  <option value="">Select a Payee</option>
                  {payees.map(payee => (
                    <option key={payee._id} value={payee._id}>
                      {payee.name} {payee.company_name ? `(${payee.company_name})` : ''}
                    </option>
                  ))}
                </select>
              </div>
              <div className="split-modal-field">
                <label>Percentage</label>
                <input
                  type="number"
                  min="1"
                  max="100"
                  value={splitPercentage}
                  onChange={(e) => setSplitPercentage(e.target.value)}
                  placeholder="Enter percentage (1-100)"
                />
              </div>
            </div>
            <div className="split-modal-actions">
              <button 
                className="split-modal-cancel"
                onClick={() => setShowSplitModal(false)}
              >
                Cancel
              </button>
              <button 
                className="split-modal-add"
                onClick={handleAddSplit}
                disabled={!selectedPayee || !splitPercentage}
              >
                Add Split
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default EditModal; 