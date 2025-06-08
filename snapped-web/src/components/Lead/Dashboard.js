/**
 * @fileoverview Dashboard component for displaying and managing lead analytics, metrics,
 * and performance data. Provides comprehensive views of client statistics, social media
 * metrics, and engagement data.
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Switch,
  FormControlLabel,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
} from '@mui/material';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
} from 'recharts';
import SocialLinksModal from './SocialLinksModal';
import './styles/Dashboard.css';
import TaskModal from '../TaskManager/TaskModal';
import NotesModal from './NotesModal';
import ContractModal from './ContractModal';
import { API_ENDPOINTS } from '../../config/api';
import { toast } from 'react-hot-toast';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];

/**
 * @typedef {Object} ViewsData
 * @property {boolean} loading - Whether views data is currently loading
 * @property {string} timeframe - Current timeframe selection ('7d' or '30d')
 * @property {Object} total - Total views data across all clients
 * @property {Object} clientViews - Individual client views data
 */

/**
 * @typedef {Object} EarningsData
 * @property {boolean} loading - Whether earnings data is currently loading
 * @property {number} total - Total earnings across all clients
 * @property {Array<Object>} dailyData - Daily earnings breakdown
 * @property {Object} clientEarnings - Individual client earnings data
 */

/**
 * @typedef {Object} DashboardProps
 * @property {Array<Object>} filteredData - Filtered list of leads/clients
 * @property {boolean} isLoading - Whether the dashboard is loading
 * @property {Object} totalViews - Total views statistics
 * @property {Function} setActiveTab - Function to set the active tab
 * @property {Function} handleTabClick - Function to handle tab clicks
 */

/**
 * Dashboard component for displaying comprehensive analytics and metrics.
 * Features include:
 * - Client statistics and performance metrics
 * - Social media platform analytics
 * - Engagement and monetization tracking
 * - Revenue analytics and reporting
 * 
 * @param {DashboardProps} props - Component props
 * @returns {React.ReactElement} The rendered dashboard
 */
const Dashboard = ({ 
  filteredData, 
  isLoading, 
  totalViews = { all: 0, filtered: 0, loading: false, sevenDay: 0, sevenDayFiltered: 0 },
  setActiveTab,
  handleTabClick
}) => {
  const { getAccessToken } = useAuth();
  // State declarations
  const [searchResults, setSearchResults] = useState([]);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);
  const [selectedUser, setSelectedUser] = useState(null);
  const [partners, setPartners] = useState([]);
  const [ranks, setRanks] = useState({});
  const [pendingRequests, setPendingRequests] = useState(new Set());
  const [searchParams, setSearchParams] = useState({
    query: '',
    platform: '',
    is_monetized: null,
    is_signed: null,
    min_followers: '',
    max_followers: '',
  });
  const [searchSuggestions, setSearchSuggestions] = useState([]);
  const [socialModalOpen, setSocialModalOpen] = useState(false);
  const [selectedSocialLead, setSelectedSocialLead] = useState(null);
  const [isTaskModalOpen, setIsTaskModalOpen] = useState(false);
  const [isNotesModalOpen, setIsNotesModalOpen] = useState(false);
  const [isContractModalOpen, setIsContractModalOpen] = useState(false);
  const [searchValue, setSearchValue] = useState('');
  const [debouncedSearchValue] = useState(() => {
    let timeoutId = null;
    return (value, callback) => {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      timeoutId = setTimeout(() => {
        callback(value);
      }, 300); // 300ms delay
    };
  });
  const [viewsData, setViewsData] = useState({
    loading: false,
    timeframe: '7d',
    total: {
      '7d': {
        views: 0,
        impressions: 0,
        reach: 0,
        storyViewTime: 0
      },
      '30d': {
        views: 0,
        impressions: 0,
        reach: 0,
        storyViewTime: 0
      }
    },
    clientViews: {}
  });

  const [earningsData, setEarningsData] = useState({
    loading: false,
    total: 0,
    dailyData: [],
    clientEarnings: {}
  });

  // Add a debug effect to log state changes
  useEffect(() => {
    console.log('viewsData state updated:', viewsData);
  }, [viewsData]);

  /**
   * Tracks an asynchronous request and manages its pending state
   * @template T
   * @param {Promise<T>} requestPromise - The promise to track
   * @param {string} requestId - Unique identifier for the request
   * @returns {Promise<T>} The result of the request
   */
  const trackRequest = async (requestPromise, requestId) => {
    setPendingRequests(prev => new Set(prev).add(requestId));
    try {
      const result = await requestPromise;
      return result;
    } finally {
      setPendingRequests(prev => {
        const newSet = new Set(prev);
        newSet.delete(requestId);
        return newSet;
      });
    }
  };

  useEffect(() => {
    const initializeDashboard = async () => {
      try {
        // Track all initial data fetches
        const [
          statsResponse,
          partnersResponse,
          ranksResponse,
          // ... other requests ...
        ] = await Promise.all([
          trackRequest(fetch(API_ENDPOINTS.LEADS.RAW), 'stats'),
          trackRequest(fetch(API_ENDPOINTS.PARTNERS.LIST), 'partners'),
          trackRequest(fetch(API_ENDPOINTS.LEADS.RANKS), 'ranks'),
          // ... add other initial requests ...
        ]);

        // Process responses
        const [statsData, partnersData, ranksData] = await Promise.all([
          statsResponse.json(),
          partnersResponse.json(),
          ranksResponse.json(),
        ]);

        // Update state with fetched data
        setStats(statsData);
        setPartners(partnersData);
        setRanks(ranksData);
        // ... update other state ...

      } catch (err) {
        console.error('Dashboard initialization error:', err);
      }
    };

    initializeDashboard();
  }, []);

  // Single effect to handle data population
  useEffect(() => {
    // Keep loading true until we have data and stats are calculated
    if (filteredData && filteredData.length > 0) {
      const calculatedStats = calculateStats(filteredData);
      setStats(calculatedStats);
      setSearchResults(filteredData);
    }
  }, [filteredData]);

  const handleSearchChange = (e) => {
    const value = e.target.value;
    setSearchValue(value);
    
    debouncedSearchValue(value, (searchTerm) => {
      const filtered = filteredData?.filter(lead => {
        return (
          lead.First_Legal_Name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
          lead.Last_Legal_Name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
          lead.Stage_Name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
          lead.Email_Address?.toLowerCase().includes(searchTerm.toLowerCase())
        );
      });

      setSearchResults(filtered || []);
      setSearchParams(prev => ({
        ...prev,
        query: searchTerm
      }));
    });
  };

  /**
   * Calculates statistics based on the provided lead data
   * @param {Array<Object>} data - Array of lead/client data
   * @returns {Object} Calculated statistics including monetization, verification, and platform metrics
   */
  const calculateStats = (data) => {
    // If we have a selected user, only use that user's data
    const leadsToProcess = selectedUser ? [selectedUser] : (searchResults || data || []);
    
    const stats = {
      total_leads: leadsToProcess.length,
      monetized_leads: 0,
      signed_leads: 0,
      signed_and_monetized: 0,
      signed_and_snapstar: 0,
      platform_stats: {
        instagram: {
          total_followers: 0,
          monetized_followers: 0,
          signed_followers: 0,
          verified_count: 0
        },
        tiktok: {
          total_followers: 0,
          monetized_followers: 0,
          signed_followers: 0,
          verified_count: 0
        },
        youtube: {
          total_followers: 0,
          monetized_followers: 0,
          signed_followers: 0,
          verified_count: 0
        },
        snapchat: {
          total_followers: 0,
          monetized_followers: 0,
          signed_followers: 0,
          star_count: 0
        }
      }
    };

    leadsToProcess.forEach(lead => {
      if (lead.Snap_Monetized) stats.monetized_leads++;
      if (lead.is_signed) stats.signed_leads++;
      if (lead.is_signed && lead.Snap_Monetized) stats.signed_and_monetized++;
      if (lead.is_signed && lead.Snap_Star) stats.signed_and_snapstar++;

      // Instagram
      const igFollowers = parseInt(lead.IG_Followers || 0);
      stats.platform_stats.instagram.total_followers += igFollowers;
      if (lead.Snap_Monetized) stats.platform_stats.instagram.monetized_followers += igFollowers;
      if (lead.is_signed) stats.platform_stats.instagram.signed_followers += igFollowers;
      if (lead.IG_Verified) stats.platform_stats.instagram.verified_count++;

      // TikTok
      const ttFollowers = parseInt(lead.TT_Followers || 0);
      stats.platform_stats.tiktok.total_followers += ttFollowers;
      if (lead.Snap_Monetized) stats.platform_stats.tiktok.monetized_followers += ttFollowers;
      if (lead.is_signed) stats.platform_stats.tiktok.signed_followers += ttFollowers;
      if (lead.TT_Verified) stats.platform_stats.tiktok.verified_count++;

      // YouTube
      const ytFollowers = parseInt(lead.YT_Followers || 0);
      stats.platform_stats.youtube.total_followers += ytFollowers;
      if (lead.Snap_Monetized) stats.platform_stats.youtube.monetized_followers += ytFollowers;
      if (lead.is_signed) stats.platform_stats.youtube.signed_followers += ytFollowers;
      if (lead.YT_Verified) stats.platform_stats.youtube.verified_count++;

      // Snapchat
      const snapFollowers = lead.Snap_Followers === "na" ? 0 : parseInt(lead.Snap_Followers || 0);
      stats.platform_stats.snapchat.total_followers += snapFollowers;
      if (lead.Snap_Monetized) stats.platform_stats.snapchat.monetized_followers += snapFollowers;
      if (lead.is_signed) stats.platform_stats.snapchat.signed_followers += snapFollowers;
      if (lead.Snap_Star) stats.platform_stats.snapchat.star_count++;
    });

    return stats;
  };

  // Calculate current stats whenever filtered data changes
  const currentStats = calculateStats(filteredData);

  // Prepare data for charts based on current stats
  const getPlatformData = () => {
    if (selectedUser) {
      return [
        { name: 'Instagram', followers: parseInt(selectedUser.IG_Followers || 0) },
        { name: 'TikTok', followers: parseInt(selectedUser.TT_Followers || 0) },
        { name: 'YouTube', followers: parseInt(selectedUser.YT_Followers || 0) },
        { name: 'Snapchat', followers: selectedUser.Snap_Followers === "na" ? 0 : parseInt(selectedUser.Snap_Followers || 0) },
      ];
    }
    return [
      { name: 'Instagram', followers: currentStats.platform_stats.instagram.total_followers },
      { name: 'TikTok', followers: currentStats.platform_stats.tiktok.total_followers },
      { name: 'YouTube', followers: currentStats.platform_stats.youtube.total_followers },
      { name: 'Snapchat', followers: currentStats.platform_stats.snapchat.total_followers },
    ];
  };

  const getMonetizationData = () => {
    if (selectedUser) {
      return [
        { name: 'Monetized', value: selectedUser.Snap_Monetized ? 1 : 0 },
        { name: 'Not Monetized', value: selectedUser.Snap_Monetized ? 0 : 1 },
      ];
    }
    const totalClients = searchResults?.length || 0;
    return [
      { name: 'Monetized', value: currentStats.monetized_leads },
      { name: 'Not Monetized', value: totalClients - currentStats.monetized_leads },
    ];
  };

  // Add lead selection handler
  const handleLeadSelect = (lead) => {
    const individualStats = {
      total_followers: calculateTotalFollowers(lead),
      platform_stats: {
        instagram: {
          followers: parseInt(lead.IG_Followers || 0),
          verified: lead.IG_Verified || false,
          engagement: parseFloat(lead.IG_Engagement || 0),
          rank: parseInt(lead.IG_Rank || 0),
          views_rank: parseInt(lead.IG_Views_Rank || 0)
        },
        tiktok: {
          followers: parseInt(lead.TT_Followers || 0),
          verified: lead.TT_Verified || false,
          rank: parseInt(lead.TT_Rank || 0),
          views_rank: parseInt(lead.TT_Views_Rank || 0)
        },
        youtube: {
          followers: parseInt(lead.YT_Followers || 0),
          verified: lead.YT_Verified || false,
          rank: parseInt(lead.YT_Rank || 0),
          views_rank: parseInt(lead.YT_Views_Rank || 0)
        },
        snapchat: {
          followers: lead.Snap_Followers === "na" ? 0 : parseInt(lead.Snap_Followers || 0),
          star: lead.Snap_Star || false,
          monetized: lead.Snap_Monetized || false
        }
      },
      status: {
        is_signed: lead.is_signed || false,
        is_contractout: lead.is_contractout || false,
        is_groupchat: lead.is_groupchat || false,
        is_note: lead.is_note || false
      }
    };
    setStats(prevStats => ({
      ...prevStats,
      selected: individualStats
    }));
  };

  // Add handler for opening social links
  const handleSocialClick = (lead) => {
    setSelectedSocialLead(lead);
    setSocialModalOpen(true);
  };

  /**
   * Formats axis values for charts
   * @param {number} value - Value to format
   * @returns {string} Formatted axis label
   */
  const formatYAxis = (value) => {
    if (value === 0) return '0';
    if (value >= 1000000) {
      return `${(value / 1000000).toFixed(1)}M`;
    }
    if (value >= 1000) {
      return `${(value / 1000).toFixed(1)}K`;
    }
    return value.toString();
  };

  // Update the table row click handler
  const handleRowClick = (lead) => {
    setSelectedUser(lead);
  };

  // Add a clear selection function
  const clearSelection = () => {
    setSelectedUser(null);
  };

  const handleTaskSubmit = async (taskData) => {
    try {
      if (!selectedUser) {
        toast.error('No client selected');
        return;
      }

      // Add the client information to the task
      const taskWithClient = {
        ...taskData,
        client_id: selectedUser.client_id || `jm${selectedUser.DOB?.replace(/-/g, "")}`,
        client_name: `${selectedUser.First_Legal_Name} ${selectedUser.Last_Legal_Name}`
      };
      
      const response = await axios.post('/api/tasks', taskWithClient);
      
      if (response.data.status === 'success') {
        toast.success('Task created successfully');
        setIsTaskModalOpen(false);
      } else {
        throw new Error(response.data.message || 'Failed to create task');
      }
    } catch (error) {
      console.error('Error creating task:', error);
      toast.error(error.message || 'Failed to create task');
    }
  };

  // Add this function to calculate total views from leads
  const calculateTotalViews = (leads) => {
    return leads.reduce((total, lead) => {
      return total + (lead.seven_day_story_views || 0);
    }, 0);
  };

  // Helper function to format time duration
  const formatViewTime = (minutes) => {
    if (!minutes || minutes === 0) return '0m';
    if (minutes < 60) return `${Math.round(minutes)}m`;
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = Math.round(minutes % 60);
    return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
  };

  // Add toggle function for timeframe
  const toggleTimeframe = () => {
    setViewsData(prev => ({
      ...prev,
      timeframe: prev.timeframe === '7d' ? '30d' : '7d'
    }));
  };

  /**
   * Fetches views data for specified client IDs
   * @async
   * @param {Array<string>} clientIds - Array of client IDs
   * @returns {Promise<void>}
   */
  const fetchViewsData = async (clientIds) => {
    try {
      setViewsData(prev => ({ ...prev, loading: true }));
      
      // Get views from mobile endpoint for both timeframes
      const [sevenDayResponse, thirtyDayResponse] = await Promise.all([
        axios.get('/api/analytics/mobile', {
          params: { 
            user_ids: clientIds.join(','),
            days: 7
          }
        }),
        axios.get('/api/analytics/mobile', {
          params: { 
            user_ids: clientIds.join(','),
            days: 30
          }
        })
      ]);

      // Get metrics for both timeframes
      const today = new Date();
      const sevenDaysAgo = new Date(today);
      const thirtyDaysAgo = new Date(today);
      sevenDaysAgo.setDate(today.getDate() - 7);
      thirtyDaysAgo.setDate(today.getDate() - 30);

      // Format dates as MM-DD-YYYY
      const formatDate = (date) => {
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const year = date.getFullYear();
        return `${month}-${day}-${year}`;
      };

      const endDate = formatDate(today);
      const sevenDayStartDate = formatDate(sevenDaysAgo);
      const thirtyDayStartDate = formatDate(thirtyDaysAgo);

      // Get metrics for both timeframes
      const [sevenDayMetrics, thirtyDayMetrics] = await Promise.all([
        axios.get('/api/analytics/snapchat', {
          params: { 
            client_id: clientIds[0],
            start_date: sevenDayStartDate,
            end_date: endDate
          }
        }),
        axios.get('/api/analytics/snapchat', {
          params: { 
            client_id: clientIds[0],
            start_date: thirtyDayStartDate,
            end_date: endDate
          }
        })
      ]);

      if (sevenDayResponse.data?.status === 'success' && thirtyDayResponse.data?.status === 'success') {
        setViewsData(prev => ({
          ...prev,
          total: {
            '7d': {
              views: sevenDayResponse.data.total_views || 0,
              impressions: sevenDayMetrics.data.data?.impressions || 0,
              reach: sevenDayMetrics.data.data?.reach || 0,
              storyViewTime: sevenDayMetrics.data.data?.story_view_time || 0
            },
            '30d': {
              views: thirtyDayResponse.data.total_views || 0,
              impressions: thirtyDayMetrics.data.data?.impressions || 0,
              reach: thirtyDayMetrics.data.data?.reach || 0,
              storyViewTime: thirtyDayMetrics.data.data?.story_view_time || 0
            }
          },
          loading: false
        }));

        // Log the data for debugging
        console.log('Updated viewsData:', {
          '7d': {
            views: sevenDayResponse.data.total_views || 0,
            impressions: sevenDayMetrics.data.data?.impressions || 0,
            reach: sevenDayMetrics.data.data?.reach || 0,
            storyViewTime: sevenDayMetrics.data.data?.story_view_time || 0
          },
          '30d': {
            views: thirtyDayResponse.data.total_views || 0,
            impressions: thirtyDayMetrics.data.data?.impressions || 0,
            reach: thirtyDayMetrics.data.data?.reach || 0,
            storyViewTime: thirtyDayMetrics.data.data?.story_view_time || 0
          }
        });
      }
    } catch (error) {
      console.error('Error fetching metrics:', error);
      setViewsData(prev => ({ ...prev, loading: false }));
    }
  };

  // Update the fetchClientViews function to handle both timeframes
  const fetchClientViews = async (clientId) => {
    try {
      setViewsData(prev => ({ ...prev, loading: true }));
      
      // Get views from mobile endpoint for both timeframes
      const [sevenDayResponse, thirtyDayResponse] = await Promise.all([
        axios.get('/api/analytics/mobile', {
          params: { 
            user_id: clientId,
            days: 7
          }
        }),
        axios.get('/api/analytics/mobile', {
          params: { 
            user_id: clientId,
            days: 30
          }
        })
      ]);

      // Get metrics for both timeframes
      const today = new Date();
      const sevenDaysAgo = new Date(today);
      const thirtyDaysAgo = new Date(today);
      sevenDaysAgo.setDate(today.getDate() - 7);
      thirtyDaysAgo.setDate(today.getDate() - 30);

      // Format dates as MM-DD-YYYY
      const formatDate = (date) => {
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const year = date.getFullYear();
        return `${month}-${day}-${year}`;
      };

      const endDate = formatDate(today);
      const sevenDayStartDate = formatDate(sevenDaysAgo);
      const thirtyDayStartDate = formatDate(thirtyDaysAgo);

      // Get metrics for both timeframes
      const [sevenDayMetrics, thirtyDayMetrics] = await Promise.all([
        axios.get('/api/analytics/snapchat', {
          params: { 
            client_id: clientId,
            start_date: sevenDayStartDate,
            end_date: endDate
          }
        }),
        axios.get('/api/analytics/snapchat', {
          params: { 
            client_id: clientId,
            start_date: thirtyDayStartDate,
            end_date: endDate
          }
        })
      ]);

      if (sevenDayResponse.data?.status === 'success' && thirtyDayResponse.data?.status === 'success') {
        setViewsData(prev => ({
          ...prev,
          clientViews: {
            ...prev.clientViews,
            [clientId]: {
              '7d': {
                views: sevenDayResponse.data.total_views || 0,
                impressions: sevenDayMetrics.data.data?.impressions || 0,
                reach: sevenDayMetrics.data.data?.reach || 0,
                storyViewTime: sevenDayMetrics.data.data?.story_view_time || 0
              },
              '30d': {
                views: thirtyDayResponse.data.total_views || 0,
                impressions: thirtyDayMetrics.data.data?.impressions || 0,
                reach: thirtyDayMetrics.data.data?.reach || 0,
                storyViewTime: thirtyDayMetrics.data.data?.story_view_time || 0
              }
            }
          },
          loading: false
        }));

        // Log the data for debugging
        console.log('Updated client viewsData:', {
          [clientId]: {
            '7d': {
              views: sevenDayResponse.data.total_views || 0,
              impressions: sevenDayMetrics.data.data?.impressions || 0,
              reach: sevenDayMetrics.data.data?.reach || 0,
              storyViewTime: sevenDayMetrics.data.data?.story_view_time || 0
            },
            '30d': {
              views: thirtyDayResponse.data.total_views || 0,
              impressions: thirtyDayMetrics.data.data?.impressions || 0,
              reach: thirtyDayMetrics.data.data?.reach || 0,
              storyViewTime: thirtyDayMetrics.data.data?.story_view_time || 0
            }
          }
        });
      }
    } catch (error) {
      console.error('Error fetching client metrics:', error);
      setViewsData(prev => ({ ...prev, loading: false }));
    }
  };

  // Update useEffect to fetch views when filteredData changes
  useEffect(() => {
    if (!filteredData?.length) return;
    
    const clientIds = filteredData
      .map(lead => lead.client_id)
      .filter(id => id && id.trim() !== '');
      
    if (clientIds.length) {
      fetchViewsData(clientIds);
    } else {
      setViewsData(prev => ({
        ...prev,
        total: {
          '7d': {
            views: 0,
            impressions: 0,
            reach: 0,
            storyViewTime: 0
          },
          '30d': {
            views: 0,
            impressions: 0,
            reach: 0,
            storyViewTime: 0
          }
        },
        loading: false
      }));
    }
  }, [filteredData]);

  // Update useEffect to fetch views when selectedUser changes
  useEffect(() => {
    if (selectedUser?.client_id) {
      fetchClientViews(selectedUser.client_id);
    } else {
      setViewsData(prev => ({
        ...prev,
        clientViews: {},
        loading: false
      }));
    }
  }, [selectedUser]);

  // Update handleStatCardClick
  const handleStatCardClick = (filterType) => {
    let filteredResults = [];
    
    switch(filterType) {
      case 'monetized':
        filteredResults = filteredData.filter(lead => lead.Snap_Monetized);
        break;
      case 'verified':
        filteredResults = filteredData.filter(lead => lead.Snap_Star);
        break;
      case 'views':
        // We'll show all leads since we're getting views from analytics
        filteredResults = filteredData;
        break;
      case 'all':
        filteredResults = filteredData;
        break;
      default:
        filteredResults = filteredData;
    }
    
    setSearchResults(filteredResults);
    setSearchValue('');
    setSearchParams(prev => ({
      ...prev,
      query: '',
      is_monetized: null,
      is_signed: null
    }));
  };

  // Add back the handleSearchParamChange function
  const handleSearchParamChange = (param, value) => {
    setSearchParams(prev => {
      // For switches, null means show all
      const newParams = { 
        ...prev, 
        [param]: value === false ? null : value 
      };
      
      // Apply all current filters at once
      const filtered = filteredData?.filter(lead => {
        const matchesQuery = !newParams.query || 
          lead.First_Legal_Name?.toLowerCase().includes(newParams.query.toLowerCase()) ||
          lead.Last_Legal_Name?.toLowerCase().includes(newParams.query.toLowerCase()) ||
          lead.Stage_Name?.toLowerCase().includes(newParams.query.toLowerCase()) ||
          lead.Email_Address?.toLowerCase().includes(newParams.query.toLowerCase());
          
        // Only apply monetized filter if it's not null
        const matchesMonetized = newParams.is_monetized === null || 
          Boolean(lead.Snap_Monetized) === Boolean(newParams.is_monetized);
          
        // Only apply signed filter if it's not null
        const matchesSigned = newParams.is_signed === null || 
          Boolean(lead.is_signed) === Boolean(newParams.is_signed);
          
        return matchesQuery && matchesMonetized && matchesSigned;
      }) || [];
      
      // Update search results and recalculate stats
      setSearchResults(filtered);
      
      // Update currentStats by calling calculateStats with filtered data
      const newStats = calculateStats(filtered);
      setStats(prevStats => ({
        ...prevStats,
        ...newStats
      }));
      
      return newParams;
    });
  };

  // Add back calculateTotalFollowers
  const calculateTotalFollowers = (lead) => {
    return (
      parseInt(lead.IG_Followers || 0) +
      parseInt(lead.TT_Followers || 0) +
      parseInt(lead.YT_Followers || 0) +
      (lead.Snap_Followers === "na" ? 0 : parseInt(lead.Snap_Followers || 0))
    );
  };

  /**
   * Handles clicks on social media links
   * @param {string} platform - Social media platform
   * @param {string} username - Username on the platform
   * @param {React.MouseEvent} e - Click event
   */
  const handleSocialLinkClick = (platform, username, e) => {
    e.stopPropagation();
    let url;
    switch (platform) {
      case 'instagram':
        url = `https://www.instagram.com/${username}/`;
        break;
      case 'tiktok':
        const tiktokUsername = username.startsWith('@') ? username.slice(1) : username;
        url = `https://www.tiktok.com/@${tiktokUsername}`;
        break;
      case 'youtube':
        url = `https://www.youtube.com/@${username}`;
        break;
      case 'snapchat':
        url = `https://www.snapchat.com/add/${username}`;
        break;
      default:
        return;
    }
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  // Update the Clear All Filters button click handler
  const handleClearAllFilters = () => {
    setSearchValue('');
    setSearchParams({
      query: '',
      platform: '',
      is_monetized: null,
      is_signed: null,
      min_followers: '',
      max_followers: ''
    });
    setSelectedUser(null);
    setSearchResults(rowData || []);
    handleTabClick('All Leads');
  };

  // Update earnings fetching function
  /**
   * Fetches earnings data from the API
   * @async
   * @returns {Promise<void>}
   */
  const fetchEarningsData = async () => {
    try {
      setEarningsData(prev => ({ ...prev, loading: true }));
      const token = await getAccessToken();
      
      const response = await fetch('/api/payments/search-payouts', {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      if (response.ok) {
        const data = await response.json();
        if (data.payouts && Array.isArray(data.payouts)) {
          // Get the relevant payouts based on whether a user is selected
          const relevantPayouts = selectedUser 
            ? [data.payouts.find(p => p.client_id === selectedUser.client_id)].filter(Boolean)
            : data.payouts;

          // Simple monthly totals
          const monthlyTotals = {};

          // Sum up the pulls by month
          relevantPayouts.forEach(payout => {
            if (payout.creator_pulls) {
              payout.creator_pulls.forEach(pull => {
                const date = new Date(pull.pull_date);
                const key = `${date.getFullYear()}-${date.getMonth() - 1}`;
                monthlyTotals[key] = (monthlyTotals[key] || 0) + pull.pull_amount;
              });
            }
          });

          // Get last 3 complete months
          const monthlyData = [];
          const today = new Date();
          // Start from 2 months ago (to show last 3 complete months)
          const startMonth = today.getMonth() - 2;
          
          for (let i = 2; i >= 0; i--) {
            const monthDate = new Date(today.getFullYear(), startMonth - i, 1);
            const key = `${monthDate.getFullYear()}-${monthDate.getMonth()}`;
            monthlyData.push({
              date: monthDate.toISOString(),
              earnings: monthlyTotals[key] || 0
            });
          }

          setEarningsData(prev => ({
            ...prev,
            loading: false,
            dailyData: monthlyData,
            total: Object.values(monthlyTotals).reduce((sum, val) => sum + val, 0)
          }));
        } else {
          console.error('Invalid earnings response:', data);
          setEarningsData(prev => ({ ...prev, loading: false, total: 0, dailyData: [] }));
        }
      } else {
        console.error('Failed to fetch earnings data');
        setEarningsData(prev => ({ ...prev, loading: false, total: 0, dailyData: [] }));
      }
    } catch (error) {
      console.error('Error fetching earnings data:', error);
      setEarningsData(prev => ({ ...prev, loading: false, total: 0, dailyData: [] }));
    }
  };

  // Update single client earnings fetch
  const fetchClientEarnings = async (clientId) => {
    try {
      const token = await getAccessToken();
      
      const response = await fetch(`/api/payments/client-monthly-earnings/${clientId}`, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      if (response.ok) {
        const data = await response.json();
        if (data.status === 'success') {
          setEarningsData(prev => ({
            ...prev,
            clientEarnings: {
              ...prev.clientEarnings,
              [clientId]: data.total_earnings || 0
            }
          }));
        } else {
          console.error('Invalid client earnings response:', data);
        }
      } else {
        console.error('Failed to fetch client earnings');
      }
    } catch (error) {
      console.error('Error fetching client earnings:', error);
    }
  };

  // Update useEffect to fetch earnings when filteredData or selectedUser changes
  useEffect(() => {
    if (!filteredData?.length) return;
    
    const clientIds = filteredData
      .map(lead => lead.client_id)
      .filter(id => id && id.trim() !== '');
      
    if (clientIds.length) {
      fetchViewsData(clientIds);
      fetchEarningsData();
    } else {
      setViewsData(prev => ({
        ...prev,
        total: {
          '7d': {
            views: 0,
            impressions: 0,
            reach: 0,
            storyViewTime: 0
          },
          '30d': {
            views: 0,
            impressions: 0,
            reach: 0,
            storyViewTime: 0
          }
        },
        loading: false
      }));
      setEarningsData({
        loading: false,
        total: 0,
        dailyData: [],
        clientEarnings: {}
      });
    }
  }, [filteredData, selectedUser]);

  // Add this utility function near the top of the component
  /**
   * Formats large numbers into human-readable strings with appropriate suffixes
   * @param {number} num - Number to format
   * @returns {string} Formatted number string (e.g., "1.2M", "500K")
   */
  const formatNumber = (num) => {
    if (num === null || num === undefined) return '0';
    
    const absNum = Math.abs(num);
    
    if (absNum >= 1000000000) {
      return (num / 1000000000).toFixed(1) + 'B';
    }
    if (absNum >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (absNum >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  // Add this helper function near the top of the component
  const getTimeframeData = (data, timeframe, field) => {
    if (!data) return 0;
    return data[timeframe]?.[field] || 0;
  };

  // Add this function before the return statement
  const getYAxisTicks = () => {
    const data = getPlatformData();
    const maxFollowers = Math.max(...data.map(item => item.followers));
    const maxWithBuffer = maxFollowers * 1.1; // Add 10%
    
    // Create 5 evenly spaced ticks from 0 to maxWithBuffer
    const tickCount = 5;
    const ticks = [];
    for (let i = 0; i <= tickCount; i++) {
      ticks.push(Math.round(maxWithBuffer * (i / tickCount)));
    }
    return ticks;
  };

  if (isLoading) {
    return (
      <div style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        backgroundColor: 'rgba(255, 255, 255, 0.8)',
        zIndex: 9999
      }}>
        <CircularProgress size={60} />
      </div>
    );
  }

  return (
    <div className="lead-tracker-view">
      {/* Selected user info */}
      {selectedUser && (
        <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">
            Viewing data for: {selectedUser.First_Legal_Name} {selectedUser.Last_Legal_Name}
          </Typography>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <Button 
              variant="contained"
              color="primary"
              size="small"
              onClick={() => setIsNotesModalOpen(true)}
            >
              Notes
            </Button>
            <Button 
              variant="contained"
              color="primary"
              size="small"
              onClick={() => setIsContractModalOpen(true)}
            >
              Contract
            </Button>
            <Button 
              variant="contained"
              color="primary"
              size="small"
              onClick={() => setIsTaskModalOpen(true)}
            >
              Add Task
            </Button>
          </Box>
        </Box>
      )}

      {/* Search Section */}
      <Card className="search-card">
        <CardContent>
          <Box className="search-container">
            <Grid 
              container
              className="search-grid"
            >
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Search"
                  value={searchValue}
                  onChange={handleSearchChange}
                  onKeyDown={(e) => {
                    if (e.key === 'Escape') {
                      setSearchValue('');
                      setSearchParams(prev => ({ ...prev, query: '' }));
                      setSearchResults(filteredData || []);
                    }
                  }}
                  sx={{
                    '& .MuiInputBase-input': {
                      textAlign: 'left !important'
                    },
                    '& .MuiInputLabel-root': {
                      textAlign: 'left !important',
                      width: 'auto'
                    }
                  }}
                  className="dashboard-search-field"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={searchParams.is_monetized || false}
                      onChange={(e) => handleSearchParamChange('is_monetized', e.target.checked)}
                    />
                  }
                  label="Monetized"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={searchParams.is_signed || false}
                      onChange={(e) => handleSearchParamChange('is_signed', e.target.checked)}
                    />
                  }
                  label="Signed"
                />
              </Grid>
              <Grid item xs={12} md={2}>
                <Button
                  variant="outlined"
                  onClick={toggleTimeframe}
                  sx={{
                    minWidth: '120px',
                    height: '40px'
                  }}
                >
                  {viewsData.timeframe === '7d' ? '7 Days' : '30 Days'}
                </Button>
              </Grid>
            </Grid>
          </Box>
        </CardContent>
      </Card>

      {/* Metrics Overview */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" component="div" gutterBottom>
                Engagement ({viewsData.timeframe === '7d' ? '7 Days' : '30 Days'})
              </Typography>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={[
                      {
                        name: 'Story Views',
                        value: selectedUser 
                          ? (viewsData.clientViews[selectedUser.client_id]?.[viewsData.timeframe]?.views || 0)
                          : (viewsData.total[viewsData.timeframe]?.views || 0),
                        color: '#609cf5'
                      },
                      {
                        name: 'Impressions',
                        value: selectedUser 
                          ? (viewsData.clientViews[selectedUser.client_id]?.[viewsData.timeframe]?.impressions || 0)
                          : (viewsData.total[viewsData.timeframe]?.impressions || 0),
                        color: '#3b82f6'
                      },
                      {
                        name: 'Unique Reach',
                        value: selectedUser 
                          ? (viewsData.clientViews[selectedUser.client_id]?.[viewsData.timeframe]?.reach || 0)
                          : (viewsData.total[viewsData.timeframe]?.reach || 0),
                        color: '#1668f3'
                      }
                    ]}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={113}
                    labelLine={false}
                    label={false}
                  >
                    {
                      [
                        { color: '#609cf5' }, // Lighter blue
                        { color: '#3b82f6' }, // Base blue
                        { color: '#1668f3' }  // Darker blue
                      ].map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.color} />
                      ))
                    }
                  </Pie>
                  <Tooltip 
                    formatter={(value) => formatNumber(value)}
                    contentStyle={{ 
                      backgroundColor: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      boxShadow: '0 2px 5px rgba(0,0,0,0.1)'
                    }}
                  />
                  <Legend 
                    verticalAlign="bottom" 
                    height={36}
                    formatter={(value, entry) => `${value}: ${formatNumber(entry.payload.value)}`}
                  />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" component="div" gutterBottom>
                Revenue (90 Days)
              </Typography>
              {earningsData.loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 300 }}>
                  <CircularProgress />
                </Box>
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart
                    data={earningsData.dailyData}
                    margin={{
                      top: 5,
                      right: 30,
                      left: 20,
                      bottom: 5,
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="date" 
                      tickFormatter={(date) => {
                        // Ensure we're using the correct month
                        const d = new Date(date);
                        d.setMonth(d.getMonth() + 1); // Shift forward one month to correct alignment
                        return d.toLocaleString('default', { month: 'short' });
                      }}
                    />
                    <YAxis 
                      tickFormatter={(value) => `$${formatNumber(value)}`}
                    />
                    <Tooltip 
                      formatter={(value) => [`$${formatNumber(value)}`, "Revenue"]}
                      labelFormatter={(date) => {
                        // Ensure tooltip shows correct month
                        const d = new Date(date);
                        d.setMonth(d.getMonth() + 1); // Shift forward one month to correct alignment
                        return d.toLocaleDateString('default', { month: 'long', year: 'numeric' });
                      }}
                      contentStyle={{
                        backgroundColor: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        boxShadow: '0 2px 5px rgba(0,0,0,0.1)'
                      }}
                    />
                    <Bar
                      dataKey="earnings"
                      fill="#3b82f6"
                    >
                      {
                        earningsData.dailyData.map((entry, index) => {
                          // Use different shades based on the month index
                          const colors = ['#609cf5', '#3b82f6', '#1668f3'];
                          return <Cell key={`cell-${index}`} fill={colors[index % 3]} />;
                        })
                      }
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Search Results Table */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h6">Clients</Typography>
            <Button 
              variant="outlined"
              size="small"
              onClick={handleClearAllFilters}
            >
              Clear All Filters
            </Button>
          </Box>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Email</TableCell>
                  <TableCell>Instagram</TableCell>
                  <TableCell>TikTok</TableCell>
                  <TableCell>YouTube</TableCell>
                  <TableCell>Snapchat</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {(searchResults || []).map((lead) => (
                  <TableRow 
                    key={lead.id || lead._id}
                    onClick={() => handleRowClick(lead)}
                    sx={{ 
                      cursor: 'pointer', 
                      '&:hover': { bgcolor: 'action.hover' },
                      bgcolor: selectedUser?.id === lead.id ? 'action.selected' : 'inherit'
                    }}
                  >
                    <TableCell>{`${lead.First_Legal_Name} ${lead.Last_Legal_Name}`}</TableCell>
                    <TableCell>{lead.Email_Address}</TableCell>
                    <TableCell 
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSocialLinkClick('instagram', lead.IG_Username, e);
                      }}
                      sx={{ 
                        cursor: 'pointer',
                        '&:hover': { 
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {lead.IG_Username} 
                      {lead.IG_Verified && '✓'} 
                      {parseInt(lead.IG_Followers) > 0 && (
                        <>
                          <br />
                          <Typography variant="caption" color="textSecondary">
                            {formatNumber(parseInt(lead.IG_Followers))}
                          </Typography>
                        </>
                      )}
                    </TableCell>
                    <TableCell
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSocialLinkClick('tiktok', lead.TT_Username, e);
                      }}
                      sx={{ 
                        cursor: 'pointer',
                        '&:hover': { 
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {lead.TT_Username}
                      {lead.TT_Verified && '✓'}
                      {parseInt(lead.TT_Followers) > 0 && (
                        <>
                          <br />
                          <Typography variant="caption" color="textSecondary">
                            {formatNumber(parseInt(lead.TT_Followers))}
                          </Typography>
                        </>
                      )}
                    </TableCell>
                    <TableCell
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSocialLinkClick('youtube', lead.YT_Username, e);
                      }}
                      sx={{ 
                        cursor: 'pointer',
                        '&:hover': { 
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {lead.YT_Username}
                      {lead.YT_Verified && '✓'}
                      {parseInt(lead.YT_Followers) > 0 && (
                        <>
                          <br />
                          <Typography variant="caption" color="textSecondary">
                            {formatNumber(parseInt(lead.YT_Followers))}
                          </Typography>
                        </>
                      )}
                    </TableCell>
                    <TableCell
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSocialLinkClick('snapchat', lead.Snap_Username, e);
                      }}
                      sx={{ 
                        cursor: 'pointer',
                        '&:hover': { 
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {lead.Snap_Username}
                      {lead.Snap_Star && '⭐'}
                      {lead.Snap_Followers !== "na" && parseInt(lead.Snap_Followers) > 0 && (
                        <>
                          <br />
                          <Typography variant="caption" color="textSecondary">
                            {formatNumber(parseInt(lead.Snap_Followers))}
                          </Typography>
                        </>
                      )}
                    </TableCell>
                    <TableCell>
                      {lead.Snap_Monetized && '💰'}
                      {lead.is_signed && '📝'}
                      {lead.is_groupchat && '👥'}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* Stats Overview */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card 
            sx={{ cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
            onClick={() => handleStatCardClick('all')}
          >
            <CardContent>
              <Typography variant="h6">Total Clients</Typography>
              <Typography variant="h4">
                {searchResults?.length || 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card 
            sx={{ cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
            onClick={() => handleStatCardClick('monetized')}
          >
            <CardContent>
              <Typography variant="h6">Monetized Clients</Typography>
              <Typography variant="h4">
                {currentStats.monetized_leads}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card 
            sx={{ cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
            onClick={() => handleStatCardClick('verified')}
          >
            <CardContent>
              <Typography variant="h6">Snap Star Clients</Typography>
              <Typography variant="h4">
                {currentStats.platform_stats.snapchat.star_count}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography variant="h6">Total Followers</Typography>
              <Typography variant="h4">
                {formatNumber(getPlatformData()?.reduce((acc, curr) => acc + curr.followers, 0))}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Charts */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>Followers by Platform</Typography>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={getPlatformData()}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis 
                    tickFormatter={formatYAxis}
                    ticks={getYAxisTicks()}
                    domain={[0, 'auto']}
                  />
                  <Tooltip 
                    formatter={(value) => [
                      `${(value).toLocaleString()} followers`,
                      "Followers"
                    ]}
                  />
                  <Legend />
                  <Bar dataKey="followers" fill="#3b82f6" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography variant="h6" sx={{ mb: 2 }}>Monetization Status</Typography>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={getMonetizationData()}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label
                  >
                    {getMonetizationData().map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={['#609cf5', '#1668f3'][index % 2]} />
                    ))}
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Modals */}
      <SocialLinksModal
        open={socialModalOpen}
        onClose={() => setSocialModalOpen(false)}
        lead={selectedSocialLead}
      />

      <TaskModal 
        show={isTaskModalOpen}
        onClose={() => setIsTaskModalOpen(false)}
        onSubmit={handleTaskSubmit}
        editMode={false}
      />

      <NotesModal 
        isOpen={isNotesModalOpen}
        onClose={() => setIsNotesModalOpen(false)}
        clientId={selectedUser ? (selectedUser.client_id || `jm${selectedUser.DOB?.replace(/-/g, "")}`) : ''}
        clientName={selectedUser ? `${selectedUser.First_Legal_Name} ${selectedUser.Last_Legal_Name}` : ''}
      />

      <ContractModal 
        isOpen={isContractModalOpen}
        onClose={() => setIsContractModalOpen(false)}
        client={selectedUser}
        onSave={() => {
          // Handle contract save if needed
          setIsContractModalOpen(false);
        }}
      />
    </div>
  );
};

export default Dashboard;