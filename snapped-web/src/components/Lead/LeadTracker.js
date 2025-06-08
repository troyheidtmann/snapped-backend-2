import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import { getCurrentUser, fetchAuthSession } from 'aws-amplify/auth';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faPencilAlt, faTrashAlt, faSync, faCompress, faExpand, faNoteSticky, faShare, faComments, faCog, faChartLine, faFileSignature, faPlus, faDownload, faUsers, faFileAlt, faClock, faDollarSign, faUser, faSpinner, faExclamationTriangle, faCaretDown, faBook, faClipboardList, faUserTie } from '@fortawesome/free-solid-svg-icons';
import { API_ENDPOINTS } from '../../config/api';
import axios from 'axios';
import './styles/LeadTracker.css';
import './styles/Contracting.css';
import { 
  ClientSideRowModelModule,
  ModuleRegistry
} from 'ag-grid-community';
import NotesModal from './NotesModal';
import EditModal from './EditModal';
import AlgorithmControls from './AlgorithmControls';
import { Link, useNavigate } from 'react-router-dom';
import Dashboard from './Dashboard';
import ContractModal from './ContractModal';
import TaskGrid from '../TaskManager/TaskGrid';
import UploadTracker from '../UploadTracker/UploadTracker';
import TikTokDownloadModal from './TikTokDownloadModal';
import PartnersModal from './PartnersModal';
import { signOut } from 'aws-amplify/auth';
import AnalyticsDashboard from '../Analytics/AnalyticsDashboard';
import AdminSurvey from '../survey/AdminDashboard';
import EmployeeDashboard from '../Employee/EmployeeDashboard';
import { toast } from 'react-hot-toast';
import { useAuth } from '../../contexts/AuthContext';

// Define base URL directly to ensure it's available
const API_BASE_URL = 'https://track.snapped.cc';

// Register the required modules
ModuleRegistry.registerModules([
  ClientSideRowModelModule
]);

// Add this after your imports, before the EditModal component
axios.interceptors.request.use(async (config) => {
  try {
    const { tokens } = await fetchAuthSession();
    const token = tokens.idToken.toString();
    config.headers.Authorization = `Bearer ${token}`;
  } catch (error) {
    console.error('Error getting auth token:', error);
  }
  return config;
});

const SocialLinksModal = ({ isOpen, onClose, data }) => {
  if (!isOpen) return null;

  console.log('Modal Data:', data); // Debug log
  
  const socialLinks = [
    { 
      platform: 'Instagram', 
      username: data?.IG_Username,
      baseUrl: 'https://www.instagram.com/'
    },
    { 
      platform: 'TikTok', 
      username: data?.TT_Username,
      baseUrl: 'https://www.tiktok.com/@'
    },
    { 
      platform: 'YouTube', 
      username: data?.YT_Username,
      baseUrl: 'https://www.youtube.com/'
    },
    { 
      platform: 'Snapchat', 
      username: data?.Snap_Username,
      baseUrl: 'https://snapchat.com/add/'
    }
  ].filter(link => link.username);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="social-links-modal" onClick={e => e.stopPropagation()}>
        <h3>Social Links</h3>
        <div className="social-links-list">
          {socialLinks.map(({ platform, username, baseUrl }) => (
            <a 
              key={platform}
              href={`${baseUrl}${username}`}
              target="_blank"
              rel="noopener noreferrer"
              className="social-link"
            >
              {platform}
            </a>
          ))}
        </div>
        <button onClick={onClose} className="close-button">Close</button>
      </div>
    </div>
  );
};

const EmployeesSheet = () => {
  return (
    <div className="google-sheet-container">
      <iframe
        src="https://docs.google.com/spreadsheets/d/1HRG1QJxk8QW9KtDHoAGDHmXeagnjcR-GzBs8Aq4hWGw/edit?usp=sharing"
        style={{ width: '100%', height: '100vh', border: 'none' }}
        allowFullScreen={true}
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"
      />
    </div>
  );
};

const PreLeadsSheet = () => {
  return (
    <div className="google-sheet-container">
      <iframe
        src="https://docs.google.com/spreadsheets/d/15bWel78i-yKT0kHU7xO0ZeImBWBd7UD30wr09lyXSXI/edit?usp=sharing"
        style={{ width: '100%', height: '100vh', border: 'none' }}
        allowFullScreen={true}
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"
      />
    </div>
  );
};

const OnboardingSheet = () => {
  return (
    <div className="google-sheet-container">
      <iframe
        src="https://docs.google.com/spreadsheets/d/1y-KBq1Nh6nZfeTs5-8HPDorQrJH2aNrbfu-VbeYrN4o/edit?usp=sharing"
        style={{ width: '100%', height: '100vh', border: 'none' }}
        allowFullScreen={true}
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"
      />
    </div>
  );
};

const SystemsSheet = () => {
  return (
    <div className="google-sheet-container">
      <iframe
        src="https://docs.google.com/spreadsheets/d/1WPYxKaj98fz1pvXvmtTQ_g0thFjGI8fXo4j9tZnFv0U/edit?usp=sharing"
        style={{ width: '100%', height: '100vh', border: 'none' }}
        allowFullScreen={true}
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"
      />
    </div>
  );
};

const TalentPayoutsSheet = () => {
  return (
    <div className="google-sheet-container">
      <iframe
        src="https://docs.google.com/spreadsheets/d/1qE9rDC9M7_bQqN6MwMS8HWQEksA8sdWc4N7yVcnag1g/edit?usp=sharing"
        style={{ width: '100%', height: '100vh', border: 'none' }}
        allowFullScreen={true}
        allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
        loading="lazy"
      />
    </div>
  );
};

// Update the tab filters mapping at the top level
const TAB_FILTERS = {
  'All Leads': null,
  'New': 'is_groupchat',
  'Signed': 'is_signed',
  'Verified': 'Snap_Star',
  'Monetized': 'Snap_Monetized',
  'Dead': 'is_dead'
};

const LeadTracker = () => {
  const { user } = useAuth();
  const [isAdmin, setIsAdmin] = useState(false);
  const [rowData, setRowData] = useState([]);
  const [gridApi, setGridApi] = useState(null);
  const [columnApi, setColumnApi] = useState(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [activeTab, setActiveTab] = useState('All Leads');
  const [searchTerm, setSearchTerm] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedLead, setSelectedLead] = useState(null);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [notesModalOpen, setNotesModalOpen] = useState(false);
  const [selectedClient, setSelectedClient] = useState(null);
  const [socialLinksModalOpen, setSocialLinksModalOpen] = useState(false);
  const [selectedSocialData, setSelectedSocialData] = useState(null);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [leadToDelete, setLeadToDelete] = useState(null);
  const [showAlgorithmControls, setShowAlgorithmControls] = useState(false);
  const [currentView, setCurrentView] = useState('DASHBOARD');
  const [contractModalOpen, setContractModalOpen] = useState(false);
  const [selectedContractClient, setSelectedContractClient] = useState(null);
  const [tiktokModalOpen, setTiktokModalOpen] = useState(false);
  const [selectedTiktokClient, setSelectedTiktokClient] = useState(null);
  const [partners, setPartners] = useState([]);
  const [showPartnersModal, setShowPartnersModal] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [totalViews, setTotalViews] = useState({
    all: 0,
    filtered: 0,
    loading: false,
    sevenDay: 0,
    sevenDayFiltered: 0
  });
  const [rankData, setRankData] = useState({});
  const [partnersLoading, setPartnersLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isSheetDropdownOpen, setIsSheetDropdownOpen] = useState(false);
  const navigate = useNavigate();
  const [lastFetchTime, setLastFetchTime] = useState(0);
  const FETCH_COOLDOWN = 5000; // 5 seconds cooldown
  const [moreMenuOpen, setMoreMenuOpen] = useState(false);
  const [docsMenuOpen, setDocsMenuOpen] = useState(false);
  const [settingsDropdownOpen, setSettingsDropdownOpen] = useState(false);
  const moreMenuRef = useRef(null);
  const docsMenuRef = useRef(null);
  const settingsDropdownRef = useRef(null);

  // Define handleTabClick once at the top level
  const handleTabClick = useCallback((tab) => {
    console.log('Tab changed to:', tab);
    setActiveTab(tab);
    // Reset search when changing tabs
    setSearchTerm('');
  }, []);

  // Memoized filtered data calculation
  const filteredData = useMemo(() => {
    console.log('Starting filter calculation:', {
      rowDataLength: rowData?.length || 0,
      activeTab,
      searchTerm,
      tabFilter: TAB_FILTERS[activeTab]
    });

    if (!rowData || !Array.isArray(rowData)) {
      console.log('No valid row data');
      return [];
    }

    let filtered = [...rowData];
    console.log('Initial data count:', filtered.length);

    // Apply tab filters first
    if (activeTab === 'Dead') {
      filtered = filtered.filter(row => row.is_dead === true);
      console.log('After dead leads filter:', filtered.length);
    } else {
      // For all other tabs, exclude dead leads
      filtered = filtered.filter(row => !row.is_dead);
      console.log('After excluding dead leads:', filtered.length);

      // Apply specific tab filters
      if (activeTab === 'Needs $') {
        filtered = filtered.filter(row => !row.Snap_Monetized);
        console.log('After needs monetization filter:', filtered.length);
      } else if (TAB_FILTERS[activeTab]) {
        filtered = filtered.filter(row => row[TAB_FILTERS[activeTab]] === true);
        console.log(`After ${activeTab} filter:`, filtered.length);
      }
    }

    // Apply search filter if exists
    if (searchTerm) {
      const searchLower = searchTerm.toLowerCase();
      console.log('Applying search filter:', searchLower);
      filtered = filtered.filter(row => {
        const searchFields = [
          'First_Legal_Name',
          'Last_Legal_Name',
          'Email_Address',
          'Stage_Name',
          'IG_Username',
          'TT_Username',
          'YT_Username',
          'Snap_Username'
        ];

        return searchFields.some(field => {
          const value = row[field];
          const matches = value && value.toString().toLowerCase().includes(searchLower);
          if (matches) {
            console.log('Found match in field:', field, 'value:', value);
          }
          return matches;
        });
      });
      console.log('After search filter:', filtered.length);
    }

    console.log('Final filtered data:', {
      originalCount: rowData.length,
      filteredCount: filtered.length,
      activeTab,
      hasSearch: !!searchTerm
    });

    return filtered;
  }, [rowData, activeTab, searchTerm]);

  // Update onGridReady handler
  const onGridReady = useCallback((params) => {
    setGridApi(params.api);
    setColumnApi(params.columnApi);
    
    if (params.api) {
      params.api.sizeColumnsToFit();
    }
  }, []);

  // Update effect to handle filtered data changes
  useEffect(() => {
    if (gridApi && filteredData) {
      gridApi.setGridOption('rowData', filteredData);
    }
  }, [gridApi, filteredData]);

  // Remove the duplicate useEffect for filtering data
  useEffect(() => {
    console.log('Filtering data:', { activeTab, rowDataLength: rowData?.length });
    
    if (!rowData || !Array.isArray(rowData)) {
        console.log('No row data available or invalid format');
        return;
    }
  }, [activeTab, rowData]);

  // Fix the useEffect to correctly access token payload
  useEffect(() => {
    const checkAdminStatus = async () => {
      try {
        const { tokens } = await fetchAuthSession();
        const groups = tokens.accessToken.payload['cognito:groups'] || [];
        console.log('Cognito groups:', groups); // Debug log
        setIsAdmin(groups.includes('ADMIN'));
      } catch (error) {
        console.error('Error checking admin status:', error);
        setIsAdmin(false);
      }
    };
    
    if (user) {
      checkAdminStatus();
    }
  }, [user]);

  // Add debug log to help troubleshoot
  useEffect(() => {
    console.log('Admin status:', isAdmin);
  }, [isAdmin]);

  // Handle status changes (checkboxes)
  const handleStatusChange = async (leadId, field, value) => {
    try {
      const updateData = {
        [field]: value
      };
      
      console.log('Sending status update:', {
        leadId,
        field,
        value,
        updateData
      });
      
      // Update UI optimistically
      setRowData(prevData => 
        prevData.map(row => 
          row.client_id === leadId ? { ...row, [field]: value } : row
        )
      );

      // Use the correct API endpoint with client_id
      const response = await axios.put(`${API_BASE_URL}/api/leads/${leadId}`, updateData, {
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      if (response.data.status === 'success') {
        console.log('Update successful');
        // Fetch fresh data in the background
        fetchLeads();
        // Also refresh ranks data
        fetchRanks();
      } else {
        // Revert on failure
        fetchLeads();
      }

    } catch (error) {
      console.error('Error updating status:', error);
      console.error('Error details:', {
        message: error.message,
        response: error.response?.data
      });
      // Revert the optimistic update on error
      fetchLeads();
    }
  };

  const gridOptions = useMemo(() => ({
    defaultColDef: {
      sortable: true,
      filter: true,
      resizable: true,
      suppressSizeToFit: false,
      cellStyle: { 
        display: 'flex',
        alignItems: 'center'
      }
    },
    rowClass: params => params.node.rowIndex % 2 === 0 ? 'ag-row-even' : 'ag-row-odd',
    theme: 'ag-theme-alpine',
    suppressColumnVirtualisation: true,
    suppressCellFocus: false,
    rowHeight: 48,
    headerHeight: 48,
    rowSelection: 'single',
    animateRows: true,
    domLayout: 'normal',
    onFirstDataRendered: (params) => {
      params.api.sizeColumnsToFit();
    }
  }), []);

  const columns = [
    {
      headerName: 'CLIENT INFO',
      headerClass: 'centered-header',
      children: [
        {
          headerName: 'ACTIONS',
          field: 'actions',
          width: 300,
          sortable: false,
          filter: false,
          pinned: 'left',
          cellRenderer: params => {
            return (
              <div className="action-buttons">
                <button className="edit-button" title="Edit" onClick={() => handleEdit(params.data)}>
                  <FontAwesomeIcon icon={faPencilAlt} />
                </button>
                <button title="Contract" onClick={(e) => handleContractClick(params.data)}>
                  <FontAwesomeIcon icon={faFileSignature} />
                </button>
                <button className="note-button1" title="Notes" onClick={(e) => {
                  e.stopPropagation();
                  setSelectedClient({
                    client_id: params.data.client_id,
                    name: `${params.data.First_Legal_Name || ''} ${params.data.Last_Legal_Name || ''}`.trim()
                  });
                  setNotesModalOpen(true);
                }}>
                  <FontAwesomeIcon icon={faNoteSticky} />
                </button>
                <button className="social-links-button" title="Social Links" onClick={(e) => {
                  e.stopPropagation();
                  setSelectedSocialData(params.data);
                  setSocialLinksModalOpen(true);
                }}>
                  <FontAwesomeIcon icon={faShare} />
                </button>
                <button 
                  className="tiktok-download-button"
                  title="Download TikToks"
                  onClick={(e) => {
                    e.stopPropagation();
                    setSelectedTiktokClient(params.data);
                    setTiktokModalOpen(true);
                  }}
                >
                  <FontAwesomeIcon icon={faDownload} />
                </button>
                <button className="delete-button" title="Delete" onClick={(e) => {
                  e.stopPropagation();
                  setLeadToDelete(params.data);
                  setShowDeleteConfirm(true);
                }}>
                  <FontAwesomeIcon icon={faTrashAlt} />
                </button>
              </div>
            );
          }
        }
      ]
    },
    {
      headerName: 'Rank',
      headerClass: 'centered-header',
      children: [
        {
          headerName: 'SCORE',
          field: 'SCORE',
          width: 100,
          pinned: 'left',
          sort: 'desc',
          sortable: true,
          valueGetter: (params) => {
            const id = params.data?._id || params.data?.id;  // Try both _id and id
            const clientId = params.data?.client_id;
            return rankData[id] || rankData[clientId] || 0;  // Try both IDs for score lookup
          },
          cellRenderer: params => {
            const score = params.value;
            let backgroundColor;
            
            if (score <= 30) backgroundColor = '#ffcdd2';
            else if (score <= 50) backgroundColor = '#ffe6e6';
            else if (score <= 60) backgroundColor = '#e6ffe6';
            else if (score <= 80) backgroundColor = '#ccffcc';
            else backgroundColor = '#99ff99';

            return (
              <div 
                style={{
                  width: '100%',
                  height: '100%',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  backgroundColor,
                  color: '#000000'
                }}
              >
                {score || 'N/A'}
              </div>
            );
          }
        }
      ]
    },
    // Basic Info Group
    {
      headerName: 'Basic Info',
      headerClass: 'centered-header',
      children: [
        {
          headerName: 'LEGAL NAME',
          valueGetter: params => {
            const firstName = params.data?.First_Legal_Name || '';
            const lastName = params.data?.Last_Legal_Name || '';
            return `${firstName} ${lastName}`.trim();
          },
          width: 200,
          pinned: 'left'
        },
        {
          headerName: 'G / S / V / M / D',
          width: 180,
          headerClass: 'centered-header',
          cellStyle: { display: 'flex', justifyContent: 'center' },
          cellRenderer: params => (
            <div className="checkbox-group" style={{ justifyContent: 'center', width: '100%' }}>
              <input 
                type="checkbox" 
                checked={params.data.is_groupchat || false}
                onChange={e => handleStatusChange(params.data.client_id, 'is_groupchat', e.target.checked)}
              />
              <span className="checkbox-separator">/</span>
              <input 
                type="checkbox" 
                checked={params.data.is_signed || false}
                onChange={e => handleStatusChange(params.data.client_id, 'is_signed', e.target.checked)}
              />
              <span className="checkbox-separator">/</span>
              <input 
                type="checkbox" 
                checked={params.data.Snap_Star || false}
                onChange={e => handleStatusChange(params.data.client_id, 'Snap_Star', e.target.checked)}
              />
              <span className="checkbox-separator">/</span>
              <input 
                type="checkbox" 
                checked={params.data.Snap_Monetized || false}
                onChange={e => handleStatusChange(params.data.client_id, 'Snap_Monetized', e.target.checked)}
              />
              <span className="checkbox-separator">/</span>
              <input 
                type="checkbox" 
                checked={params.data.is_dead || false}
                onChange={e => handleStatusChange(params.data.client_id, 'is_dead', e.target.checked)}
              />
            </div>
          ),
          headerComponent: () => (
            <div className="checkbox-header" style={{ justifyContent: 'center', width: '100%', display: 'flex' }}>
              G <span>/</span> S <span>/</span> V <span>/</span> M <span>/</span> D
            </div>
          )
        },
        {
          headerName: 'Referred By',
          field: 'referred_by_name',
          width: 175,
          cellRenderer: (params) => partnerCellRenderer(params, 'referred')
        },
        {
          headerName: 'Monetized By',
          field: 'monetized_by_name',
          flex: 1,
          minWidth: 175,
          cellRenderer: (params) => partnerCellRenderer(params, 'monetized')
        }
      ]
    },
    // Info Group
    {
      headerName: 'Info',
      children: [
        {
          headerName: 'Email',
          field: 'Email_Address',
          width: 250,
          hide: false
        },
        {
          headerName: 'Client ID',
          field: 'client_id',
          width: 150,
        },
        {
          headerName: 'Snap ID',
          field: 'snap_id',
          width: 150,
        }
      ]
    },
    // Add this new group after the Info group
    {
      headerName: 'Social',
      children: [
        {
          headerName: 'Snapchat',
          field: 'Snap_Username',
          width: 150,
          hide: true
        }
      ]
    },
    // Created column - hidden
    {
      headerName: 'Created',
      field: 'created_at',
      hide: true
    }
  ];

  const fetchLeads = useCallback(async () => {
    setIsLoading(true);
    try {
      console.log('Starting to fetch leads...');
      
      // Get leads with essential data in a single call
      const leadsResponse = await axios.get(API_ENDPOINTS.LEADS.GRID);
      const leads = leadsResponse.data;
      
      // Set initial data immediately
      setRowData(leads);
      
      // Start fetching ranks data in the background
      const fetchRanksAndAnalytics = async () => {
        try {
          const ranksResponse = await axios.get(API_ENDPOINTS.LEADS.RANKS);
          const ranks = {};
          ranksResponse.data.forEach(item => {
            if (item.id) {
              ranks[item.id] = item.score;
              if (item.client_id) {
                ranks[item.client_id] = item.score;
              }
            }
          });
          
          // Update ranks data
          setRankData(ranks);
          
          // Get client IDs for analytics
          const clientIds = leads
            .map(lead => lead.client_id)
            .filter(Boolean);
            
          if (clientIds.length > 0) {
            const analyticsResponse = await axios.get(API_ENDPOINTS.ANALYTICS.GET_MOBILE, {
              params: { user_ids: clientIds.join(',') }
            });
            
            if (analyticsResponse.data?.status === 'success') {
              setTotalViews({
                all: analyticsResponse.data.total_views || 0,
                filtered: analyticsResponse.data.total_views || 0,
                loading: false
              });
            }
          }
        } catch (error) {
          console.error('Error fetching additional data:', error);
        }
      };
      
      // Start background data fetch
      fetchRanksAndAnalytics();
      
    } catch (error) {
      console.error('Error fetching leads:', error);
      toast.error('Failed to load leads data');
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Update useEffect to avoid redundant calls
  useEffect(() => {
    fetchLeads();
    const refreshInterval = setInterval(fetchLeads, 5 * 60 * 1000);
    return () => clearInterval(refreshInterval);
  }, [fetchLeads]);

  // Add title effect
  useEffect(() => {
    document.title = 'Leads | Snapped';
  }, []);

  const toggleFullscreen = () => {
    const element = document.querySelector('.lead-tracker-view');
    
    if (!document.fullscreenElement) {
      // Enter fullscreen
      if (element.requestFullscreen) {
        element.requestFullscreen();
      } else if (element.webkitRequestFullscreen) {
        element.webkitRequestFullscreen();
      } else if (element.msRequestFullscreen) {
        element.msRequestFullscreen();
      }
      setIsFullscreen(true);
    } else {
      // Exit fullscreen
      if (document.exitFullscreen) {
        document.exitFullscreen();
      } else if (document.webkitExitFullscreen) {
        document.webkitExitFullscreen();
      } else if (document.msExitFullscreen) {
        document.msExitFullscreen();
      }
      setIsFullscreen(false);
    }
  };

  // Add fullscreen change event listener
  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };

    document.addEventListener('fullscreenchange', handleFullscreenChange);
    document.addEventListener('webkitfullscreenchange', handleFullscreenChange);
    document.addEventListener('mozfullscreenchange', handleFullscreenChange);
    document.addEventListener('MSFullscreenChange', handleFullscreenChange);

    return () => {
      document.removeEventListener('fullscreenchange', handleFullscreenChange);
      document.removeEventListener('webkitfullscreenchange', handleFullscreenChange);
      document.removeEventListener('mozfullscreenchange', handleFullscreenChange);
      document.removeEventListener('MSFullscreenChange', handleFullscreenChange);
    };
  }, []);

  const openSocialLinks = (data) => {
    setSelectedLead(data);
    setModalOpen(true);
  };

  const handleEdit = (data) => {
    console.log('Edit button clicked, data:', data);
    console.log('Current view before:', currentView);
    setCurrentView('VIEW_LEADS');
    setSelectedLead(data);
    console.log('Selected lead set to:', data);
    setEditModalOpen(true);
    console.log('Edit modal should be open now');
  };

  const handleSave = async (updatedData) => {
    try {
        console.log('Starting update with data:', updatedData);
        
        // Get the auth token using fetchAuthSession
        let authToken;
        try {
            const { tokens } = await fetchAuthSession();
            authToken = tokens.accessToken.toString();
        } catch (error) {
            console.error('Error getting auth token:', error);
            toast.error('Authentication error');
            return;
        }
        
        // Use client_id for the update
        const clientId = updatedData.client_id;
        if (!clientId) {
            console.error('No client_id found in updatedData:', updatedData);
            toast.error('Missing client ID');
            return;
        }

        // Log the request details
        const updateUrl = API_ENDPOINTS.LEADS.UPDATE(clientId);
        console.log('Making request to:', updateUrl);
        console.log('With data:', JSON.stringify(updatedData, null, 2));
        console.log('Auth token present:', !!authToken);
        
        const response = await axios.put(
            updateUrl,
            updatedData,
            {
                headers: {
                    'Authorization': `Bearer ${authToken}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        console.log('Received response:', response.data);
        
        if (response.data && response.data.status === "success") {
            // Update the row data in place
            setRowData(prevData => 
                prevData.map(row => 
                    row.client_id === clientId ? updatedData : row
                )
            );
            
            // Update grid if it exists
            if (gridApi) {
                const transaction = {
                    update: [updatedData]
                };
                gridApi.applyTransaction(transaction);
            }

            // Show success message
            toast.success('Lead updated successfully');
        } else {
            console.error('Update failed:', response.data);
            toast.error(response.data?.message || 'Failed to update lead');
        }
    } catch (error) {
        console.error('Error updating lead:', error);
        console.error('Error response:', error.response?.data);
        console.error('Error status:', error.response?.status);
        console.error('Error details:', {
            message: error.message,
            response: error.response?.data,
            status: error.response?.status,
            config: error.config
        });
        toast.error(error.response?.data?.detail || 'Error updating lead');
    }
};

  const handleDelete = async (leadId) => {
    try {
      await axios.delete(`/api/leads/${leadId}`);
      setRowData(prevData => prevData.filter(row => row.id !== leadId));
      setShowDeleteConfirm(false);
      setLeadToDelete(null);
    } catch (error) {
      console.error('Error deleting lead:', error);
    }
  };

  useEffect(() => {
    if (gridApi) {
      console.log('Grid API initialized');
      console.log('Grid classes:', document.querySelector('.ag-theme-alpine').classList);
      
      // Force refresh of the grid
      gridApi.refreshCells({
        force: true,
        columns: ['Performance_Score']
      });
    }
  }, [gridApi]);

  // Add this function to handle opening the add lead modal
  const handleAddLead = () => {
    setCurrentView('VIEW_LEADS');
    setSelectedLead({}); // Empty data for new lead
    setEditModalOpen(true);
  };

  const handleNavClick = (view) => {
    setCurrentView(view);
    if (view === 'ADD_LEAD') {
      handleAddLead();
    }
  };

  const handleContractClick = (clientData) => {
    console.log('Opening contract for client:', clientData); // Debug log
    setSelectedContractClient({
      ...clientData,
      _id: clientData._id || clientData.id // Make sure we have the ID
    });
    setContractModalOpen(true);
  };

  const handleTikTokDownload = async (tiktokUrl) => {
    try {
      const response = await axios.post(API_ENDPOINTS.TIKTOK.DOWNLOAD, {
        client_id: selectedTiktokClient.client_id,
        tiktok_url: tiktokUrl
      });
      
      if (response.data.status === 'success') {
        console.log('TikToks downloaded successfully');
      }
      return response.data;  // Return the response data
    } catch (error) {
      console.error('Error downloading TikToks:', error);
      throw new Error('Failed to download TikToks');
    }
  };

  // Add state for download progress
  const [downloadProgress, setDownloadProgress] = useState(null);

  const fetchPartners = async () => {
    try {
      setPartnersLoading(true);
      const response = await axios.get(API_ENDPOINTS.PARTNERS.LIST);
      console.log('Partners response:', response.data);
      
      // Ensure we handle both array and object responses
      const partnersData = Array.isArray(response.data) ? response.data : [];
      
      setPartners(partnersData);
    } catch (error) {
      console.error('Error fetching partners:', error);
      setError('Failed to load partners');
    } finally {
      setPartnersLoading(false);
    }
  };

  useEffect(() => {
    fetchPartners();
  }, []);

  const partnerCellRenderer = (params, type) => {
    const partnerId = params.data?.[`${type}_by`] || '';
    const endpoint = type === 'referred' ? API_ENDPOINTS.PARTNERS.REFERRED : API_ENDPOINTS.PARTNERS.MONETIZED;
    
    return (
      <select
        value={partnerId}
        onChange={async (e) => {
          const newPartnerId = e.target.value;
          try {
            const clientId = params.data.client_id;
            
            // Get the partner name before making the API call
            const partner = partners.find(p => p.id === newPartnerId);
            const partnerName = partner ? partner.name : '';
            
            // Create updated row data
            const updatedData = { ...params.data };
            updatedData[`${type}_by`] = newPartnerId;
            updatedData[`${type}_by_name`] = partnerName;
            
            // Use AG-Grid transaction update to prevent scroll jumping
            const transaction = {
              update: [updatedData]
            };
            params.api.applyTransaction(transaction);
            
            // Update memory state without triggering refresh
            setRowData(prevData => 
              prevData.map(row => 
                row.client_id === clientId ? updatedData : row
              )
            );
            
            // Make the API call
            await axios.post(endpoint, {
              client_id: clientId,
              partner_id: newPartnerId
            });
            
          } catch (error) {
            console.error(`Error updating ${type} by:`, error);
            // Revert changes on error
            const transaction = {
              update: [params.data]
            };
            params.api.applyTransaction(transaction);
            
            setRowData(prevData => 
              prevData.map(row => 
                row.client_id === params.data.client_id ? params.data : row
              )
            );
          }
        }}
      >
        <option value="">Select Partner</option>
        {partners.map(partner => (
          <option 
            key={partner.id} 
            value={partner.id}
          >
            {partner.name}
          </option>
        ))}
      </select>
    );
  };

  const fetchRanks = async () => {
    try {
      const response = await axios.get(API_ENDPOINTS.LEADS.RANKS);
      const ranks = {};
      response.data.forEach(item => {
        if (item.id) {
          ranks[item.id] = item.score;  // Map using the MongoDB _id
          if (item.client_id) {
            ranks[item.client_id] = item.score;  // Also map using client_id as fallback
          }
        }
      });
      setRankData(ranks);
      
      // Force grid refresh if gridApi exists
      if (gridApi) {
        gridApi.refreshCells({
          force: true,
          columns: ['SCORE']
        });
      }
      
      return ranks;
    } catch (error) {
      console.error('Error fetching ranks:', error);
      return {};
    }
  };

  // Update fetchTotalViews
  const fetchTotalViews = useCallback(async (clientIds) => {
    // Implement cooldown to prevent rapid refetching
    const now = Date.now();
    if (now - lastFetchTime < FETCH_COOLDOWN) {
      console.log('Skipping analytics fetch - cooldown period');
      return;
    }
    setLastFetchTime(now);

    try {
      setTotalViews(prev => ({ ...prev, loading: true }));
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      // If no client IDs, return zero views
      if (!clientIds || clientIds.length === 0) {
        setTotalViews({
          all: 0,
          filtered: 0,
          sevenDay: 0,
          sevenDayFiltered: 0,
          loading: false
        });
        return;
      }

      // Filter out any empty or invalid client IDs
      const validClientIds = clientIds.filter(id => id && id.trim() !== '');
      
      // Get analytics for filtered clients
      const response = await axios.get(`${API_BASE_URL}/api/analytics/mobile`, {
        params: { 
          user_ids: validClientIds.join(','),
          days: 7  // Request 7-day view count
        },
        headers: { Authorization: `Bearer ${token}` }
      });

      if (response.data?.status === 'success') {
        setTotalViews({
          all: response.data.total_views || 0,
          filtered: response.data.total_views || 0,
          sevenDay: response.data.seven_day_views || 0,
          sevenDayFiltered: response.data.seven_day_views || 0,
          loading: false
        });
      } else {
        setTotalViews({
          all: 0,
          filtered: 0,
          sevenDay: 0,
          sevenDayFiltered: 0,
          loading: false
        });
      }

    } catch (error) {
      console.error('Error fetching total views:', error);
      setTotalViews(prev => ({ ...prev, loading: false }));
    }
  }, [lastFetchTime]);

  // Update the fetchTotalViews effect to use memoized filtered data
  useEffect(() => {
    if (!filteredData.length) return;
    
    const now = Date.now();
    if (now - lastFetchTime < FETCH_COOLDOWN) {
      console.log('Skipping analytics fetch - cooldown period');
      return;
    }
    
    const clientIds = filteredData
      .map(row => row.client_id)
      .filter(id => id && id.trim() !== '');
      
    if (clientIds.length) {
      fetchTotalViews(clientIds);
    } else {
      setTotalViews({
        all: 0,
        filtered: 0,
        loading: false
      });
    }
  }, [filteredData, lastFetchTime, FETCH_COOLDOWN, fetchTotalViews]);

  // Update loadData to use the same fetchTotalViews
  const loadData = async () => {
    try {
      setIsLoading(true);
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      
      const response = await axios.get(`${API_BASE_URL}/api/leads`, {
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      if (response.data) {
        setRowData(response.data);
      }
    } catch (error) {
      console.error('Error loading data:', error);
      toast.error('Failed to load leads data');
      setTotalViews({ all: 0, filtered: 0, loading: false });
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    const loadData = async () => {
      try {
        // Load both leads and ranks together
        const [leadsResponse, ranksResponse] = await Promise.all([
          fetchLeads(),
          fetchRanks()
        ]);
        
        console.log('Ranks loaded:', ranksResponse); // Debug log
      } catch (error) {
        console.error('Error loading data:', error);
      }
    };
    
    loadData();
  }, []); // Empty dependency array for initial load

  const handleSignOut = async () => {
    try {
      await signOut();
      navigate('/login');
    } catch (error) {
      console.error('Error signing out:', error);
    }
  };

  // Modify the refresh button handler to show loading state
  const handleManualRefresh = async () => {
    try {
      setIsLoading(true); // Add loading indicator
      await fetchLeads();
    } finally {
      setIsLoading(false);
    }
  };

  // Add scroll handler to prevent back/forward navigation
  useEffect(() => {
    const preventNavigation = (e) => {
      if (e.target.closest('.google-sheet-container')) {
        e.preventDefault();
      }
    };

    window.addEventListener('wheel', preventNavigation, { passive: false });
    return () => window.removeEventListener('wheel', preventNavigation);
  }, []);

  // Render the search section
  const renderSearchSection = () => (
    <div className="search-container">
      <input
        type="text"
        className="quick-search-input"
        placeholder="Search across all fields..."
        value={searchTerm}
        onChange={(e) => setSearchTerm(e.target.value)}
      />
      {searchTerm && (
        <div className="search-results">
          Found {filteredData.length} results
          {activeTab !== 'All Leads' && ` in ${activeTab}`}
        </div>
      )}
    </div>
  );

  // Render the action buttons
  const renderActionButtons = () => (
    <div style={{ display: 'flex', gap: '8px' }}>
      <button onClick={handleManualRefresh} className="action-button" disabled={isLoading}>
        <FontAwesomeIcon icon={faSync} spin={isLoading} />
      </button>
      <div className="settings-dropdown" ref={settingsDropdownRef}>
        <button 
          className="action-button"
          onClick={(e) => {
            e.stopPropagation();
            setSettingsDropdownOpen(!settingsDropdownOpen);
          }}
        >
          <FontAwesomeIcon icon={faCog} />
        </button>
        {settingsDropdownOpen && (
          <div className="settings-dropdown-content">
            <button onClick={() => {
              setShowAlgorithmControls(true);
              setSettingsDropdownOpen(false);
            }}>
              <FontAwesomeIcon icon={faCog} /> Algorithm
            </button>
            <button onClick={() => {
              toggleFullscreen();
              setSettingsDropdownOpen(false);
            }}>
              <FontAwesomeIcon icon={isFullscreen ? faCompress : faExpand} />
              {isFullscreen ? " Exit Fullscreen" : " Fullscreen"}
            </button>
            <button onClick={() => {
              setShowPartnersModal(true);
              setSettingsDropdownOpen(false);
            }}>
              <FontAwesomeIcon icon={faUsers} /> Partners
            </button>
          </div>
        )}
      </div>
    </div>
  );

  const renderCurrentView = () => {
    switch (currentView) {
      case 'DASHBOARD':
        return (
          <Dashboard 
            filteredData={filteredData} 
            isLoading={isLoading}
            totalViews={totalViews || { all: 0, filtered: 0, loading: false }}
            handleTabClick={handleTabClick}
            setActiveTab={setActiveTab}
          />
        );
      case 'VIEW_LEADS':
        return (
          <div className="lead-tracker-content">
            <div className="lead-tracker-controls">
              <div className="lead-tracker-tabs">
                {Object.keys(TAB_FILTERS).map(tab => (
                  <button
                    key={tab}
                    className={`tab-button ${activeTab === tab ? 'active' : ''}`}
                    onClick={() => handleTabClick(tab)}
                  >
                    {tab}
                  </button>
                ))}
              </div>
              <div className="controls-right">
                <input
                  type="text"
                  className="quick-search-input"
                  placeholder="Search across all fields..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
                <button onClick={handleManualRefresh} className="action-button" disabled={isLoading}>
                  <FontAwesomeIcon icon={faSync} spin={isLoading} />
                </button>
                <div className="settings-dropdown" ref={settingsDropdownRef}>
                  <button 
                    className="action-button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setSettingsDropdownOpen(!settingsDropdownOpen);
                    }}
                  >
                    <FontAwesomeIcon icon={faCog} />
                  </button>
                  {settingsDropdownOpen && (
                    <div className="settings-dropdown-content">
                      <button onClick={() => {
                        setShowAlgorithmControls(true);
                        setSettingsDropdownOpen(false);
                      }}>
                        <FontAwesomeIcon icon={faCog} /> Algorithm
                      </button>
                      <button onClick={() => {
                        toggleFullscreen();
                        setSettingsDropdownOpen(false);
                      }}>
                        <FontAwesomeIcon icon={isFullscreen ? faCompress : faExpand} />
                        {isFullscreen ? " Exit Fullscreen" : " Fullscreen"}
                      </button>
                      <button onClick={() => {
                        setShowPartnersModal(true);
                        setSettingsDropdownOpen(false);
                      }}>
                        <FontAwesomeIcon icon={faUsers} /> Partners
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </div>
            <div 
              className={`ag-theme-alpine lead-tracker-grid ${isFullscreen ? 'fullscreen' : ''}`}
              style={{ 
                height: '80vh',
                width: '100%',
                overflow: 'hidden',
                display: 'flex',
                flexDirection: 'column',
                border: '1px solid #ddd',
                borderRadius: '4px',
                backgroundColor: 'white',
                marginTop: '16px'
              }}
            >
              <AgGridReact
                modules={[ClientSideRowModelModule]}
                columnDefs={columns}
                rowData={filteredData}
                {...gridOptions}
                onGridReady={onGridReady}
                pagination={true}
                paginationPageSize={100}
                context={{ openSocialLinks, handleStatusChange }}
              />
            </div>
          </div>
        );
      case 'TASKS':
        return <TaskGrid />;
      case 'UPLOADS':
        return <UploadTracker />;
      case 'ANALYTICS':
        return <AnalyticsDashboard />;
      case 'EMPLOYEES':
        return isAdmin ? <EmployeesSheet /> : null;
      case 'PRE_LEADS':
        return isAdmin ? <PreLeadsSheet /> : null;
      case 'ONBOARDING':
        return isAdmin ? <OnboardingSheet /> : null;
      case 'SYSTEMS':
        return isAdmin ? <SystemsSheet /> : null;
      case 'TALENT_PAYOUTS':
        return isAdmin ? <TalentPayoutsSheet /> : null;
      case 'ADMIN_SURVEY':
        return <AdminSurvey />;
      case 'EMPLOYEE_DASHBOARD':
        return <EmployeeDashboard />;
      default:
        return null;
    }
  };

  // Grid configuration
  const defaultColDef = useMemo(() => ({
    sortable: true,
    filter: true,
    resizable: true,
    minWidth: 100,
    flex: 1
  }), []);

  // Add window resize handler
  useEffect(() => {
    const handleResize = () => {
      if (gridApi) {
        gridApi.sizeColumnsToFit();
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [gridApi]);

  // Close menus when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (moreMenuRef.current && !moreMenuRef.current.contains(event.target)) {
        setMoreMenuOpen(false);
        setDocsMenuOpen(false);
      }
      if (settingsDropdownRef.current && !settingsDropdownRef.current.contains(event.target)) {
        setSettingsDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div className="lead-tracker-view">
      <div className="lead-tracker-navigation">
        <div className="nav-links">
          <button 
            className={`nav-link ${currentView === 'DASHBOARD' ? 'active' : ''}`}
            onClick={() => handleNavClick('DASHBOARD')}
          >
            DASHBOARD
          </button>
          <button 
            className={`nav-link ${currentView === 'VIEW_LEADS' ? 'active' : ''}`}
            onClick={() => handleNavClick('VIEW_LEADS')}
          >
            CLIENTS
          </button>
          <button 
            className={`nav-link ${currentView === 'UPLOADS' ? 'active' : ''}`}
            onClick={() => handleNavClick('UPLOADS')}
          >
            UPLOADS
          </button>
          <button 
            className={`nav-link ${currentView === 'TASKS' ? 'active' : ''}`}
            onClick={() => handleNavClick('TASKS')}
          >
            TASKS
          </button>
          <button 
            className={`nav-link ${currentView === 'EMPLOYEE_DASHBOARD' ? 'active' : ''}`}
            onClick={() => handleNavClick('EMPLOYEE_DASHBOARD')}
          >
            EMPLOYEES
          </button>
          <button 
            className="nav-link"
            onClick={() => setDocsMenuOpen(true)}
          >
            MORE
          </button>
        </div>
      </div>

      {/* Add the docs modal */}
      {docsMenuOpen && (
        <div className="docs-modal-overlay" onClick={() => setDocsMenuOpen(false)}>
          <div className="docs-modal" onClick={e => e.stopPropagation()}>
            <div className="docs-modal-header">
              <h2>More Options</h2>
              <button className="close-button" onClick={() => setDocsMenuOpen(false)}>
                ×
              </button>
            </div>
            <div className="docs-modal-content">
              <div className="docs-grid">
                <div className="docs-grid-item" onClick={() => {
                  handleNavClick('ANALYTICS');
                  setDocsMenuOpen(false);
                }}>
                  <span>ANALYTICS</span>
                </div>
                <div className="docs-grid-item" onClick={() => {
                  handleNavClick('ADMIN_SURVEY');
                  setDocsMenuOpen(false);
                }}>
                  <span>SURVEY</span>
                </div>
                {isAdmin && (
                  <>
                    <div className="docs-grid-item" onClick={() => {
                      handleNavClick('EMPLOYEES');
                      setDocsMenuOpen(false);
                    }}>
                      <span>EMPLOYEES</span>
                    </div>
                    <div className="docs-grid-item" onClick={() => {
                      handleNavClick('PRE_LEADS');
                      setDocsMenuOpen(false);
                    }}>
                      <span>PRE-LEADS</span>
                    </div>
                    <div className="docs-grid-item" onClick={() => {
                      handleNavClick('ONBOARDING');
                      setDocsMenuOpen(false);
                    }}>
                      <span>ONBOARDING</span>
                    </div>
                    <div className="docs-grid-item" onClick={() => {
                      handleNavClick('SYSTEMS');
                      setDocsMenuOpen(false);
                    }}>
                      <span>SYSTEMS</span>
                    </div>
                    <div className="docs-grid-item" onClick={() => {
                      handleNavClick('TALENT_PAYOUTS');
                      setDocsMenuOpen(false);
                    }}>
                      <span>TALENT PAYOUTS</span>
                    </div>
                  </>
                )}
                <div className="docs-grid-item sign-out" onClick={() => {
                  handleSignOut();
                  setDocsMenuOpen(false);
                }}>
                  <span>SIGN OUT</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="lead-tracker-content">
        {renderCurrentView()}
      </div>

      {/* Modals */}
      <SocialLinksModal 
        isOpen={socialLinksModalOpen}
        onClose={() => setSocialLinksModalOpen(false)}
        data={selectedSocialData}
      />
      <EditModal
        isOpen={editModalOpen}
        onClose={() => setEditModalOpen(false)}
        data={selectedLead}
        onSave={handleSave}
      />
      <NotesModal
        isOpen={notesModalOpen}
        onClose={() => setNotesModalOpen(false)}
        clientId={selectedClient?.client_id}
      />
      <ContractModal
        isOpen={contractModalOpen}
        onClose={() => setContractModalOpen(false)}
        clientData={selectedContractClient}
      />
      <TikTokDownloadModal
        isOpen={tiktokModalOpen}
        onClose={() => setTiktokModalOpen(false)}
        clientData={selectedTiktokClient}
        onDownload={handleTikTokDownload}
      />
      <PartnersModal
        isOpen={showPartnersModal}
        onClose={() => setShowPartnersModal(false)}
        partners={partners}
      />
      {downloadProgress && (
        <div className="fixed inset-0 flex items-center justify-center bg-black bg-opacity-50 z-50">
          <div className="bg-white p-6 rounded-lg shadow-xl max-w-md w-full">
            <h3 className="text-lg font-semibold mb-4">Downloading TikTok Videos</h3>
            <div className="mb-4">
              <div className="text-sm text-gray-600 mb-2">
                {downloadProgress.currentFile}
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2.5">
                <div
                  className="bg-blue-600 h-2.5 rounded-full"
                  style={{
                    width: `${(downloadProgress.current / downloadProgress.total) * 100}%`
                  }}
                ></div>
              </div>
              <div className="text-sm text-gray-600 mt-2">
                {downloadProgress.status}
              </div>
            </div>
          </div>
        </div>
      )}
      <AlgorithmControls 
        isOpen={showAlgorithmControls}
        onClose={() => setShowAlgorithmControls(false)}
      />
    </div>
  );
};

export default LeadTracker; 