/**
 * @fileoverview UploadTracker component for monitoring and managing media uploads.
 * Provides functionality for tracking uploads, viewing media details, and managing content.
 */

import { useEffect, useState, useRef, useMemo } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import './UploadTracker.css';
import MediaModal from './MediaModal';
import { fetchAuthSession } from 'aws-amplify/auth';
import MediaModalData from './MediaModal-Data';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { 
  faVideo, 
  faImage, 
  faFolder, 
  faDatabase,
  faFileAlt,
  faComments,
  faCamera,
  faChartBar,
  faExclamationTriangle,
  faTriangleExclamation
} from '@fortawesome/free-solid-svg-icons';
import axiosInstance from '../../utils/axiosConfig';
import { toast } from 'react-hot-toast';

/**
 * @typedef {Object} MediaItem
 * @property {string} id - Unique identifier for the media item
 * @property {string} type - Type of media (video, image, etc.)
 * @property {string} url - URL to access the media
 * @property {string} client_id - Associated client ID
 * @property {Date} upload_date - Date when the media was uploaded
 */

/**
 * @typedef {Object} AnalyticsData
 * @property {number} views - Number of views
 * @property {number} likes - Number of likes
 * @property {number} comments - Number of comments
 * @property {number} shares - Number of shares
 */

// Add auth interceptor for all requests
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

/**
 * UploadTracker component for monitoring and managing media uploads.
 * Provides a grid view of uploads by client and date, with detailed analytics
 * and management capabilities.
 * 
 * @component
 * @returns {React.ReactElement} The rendered UploadTracker component
 */
const UploadTracker = () => {
  const [clientData, setClientData] = useState([]);
  const [dumpData, setDumpData] = useState([]);
  const [dates, setDates] = useState([]);
  const [selectedMedia, setSelectedMedia] = useState(null);
  const [selectedDump, setSelectedDump] = useState(null);
  const containerRef = useRef(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedPost, setSelectedPost] = useState(null);
  const [postData, setPostData] = useState([]);

  // Add new state variables for each section's search
  const [uploadSearchQuery, setUploadSearchQuery] = useState('');
  const [dumpSearchQuery, setDumpSearchQuery] = useState('');
  const [postSearchQuery, setPostSearchQuery] = useState('');

  // Add new state for spotlights
  const [spotlightData, setSpotlightData] = useState([]);
  const [spotlightSearchQuery, setSpotlightSearchQuery] = useState('');

  // Add new state for analytics data
  const [analyticsData, setAnalyticsData] = useState({});

  // Add new state for saved data
  const [savedData, setSavedData] = useState([]);
  const [savedSearchQuery, setSavedSearchQuery] = useState('');

  // Add new state for view toggle
  const [activeView, setActiveView] = useState('uploads'); // 'uploads', 'dumps', 'spotlights', or 'saved'

  // Add new state for analytics modal
  const [selectedAnalytics, setSelectedAnalytics] = useState(null);

  // Add new state for content flags
  const [contentFlags, setContentFlags] = useState({});

  // Add filter functions for each section
  const filteredUploadData = useMemo(() => {
    const currentDate = new Date().toISOString().split('T')[0];
    
    // First apply the search filter
    let filtered = [...clientData]; // Create a copy to avoid mutating original data
    if (uploadSearchQuery.trim()) {
      filtered = clientData.filter(client => {
        const searchTerm = uploadSearchQuery.toLowerCase();
        return (
          (client.clientName && client.clientName.toLowerCase().includes(searchTerm)) ||
          (client.client_ID && client.client_ID.toLowerCase().includes(searchTerm))
        );
      });
    }
    
    // Then sort the filtered results
    return filtered.sort((a, b) => {
      // Check for uploads on current date
      const aHasCurrentUploads = a.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      const bHasCurrentUploads = b.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      
      // First sort by current date uploads
      if (aHasCurrentUploads && !bHasCurrentUploads) return -1;
      if (!aHasCurrentUploads && bHasCurrentUploads) return 1;
      
      // Then sort by any uploads
      const aHasUploads = a.uploads?.some(u => u.stats?.hasContent) || false;
      const bHasUploads = b.uploads?.some(u => u.stats?.hasContent) || false;
      if (aHasUploads && !bHasUploads) return -1;
      if (!aHasUploads && bHasUploads) return 1;
      
      // Finally sort by name
      return (a.clientName || '').localeCompare(b.clientName || '');
    });
  }, [clientData, uploadSearchQuery]);

  const filteredDumpData = useMemo(() => {
    const currentDate = new Date().toISOString().split('T')[0];
    
    // First apply the search filter
    let filtered = [...dumpData];
    if (dumpSearchQuery.trim()) {
      filtered = dumpData.filter(client => {
        const searchTerm = dumpSearchQuery.toLowerCase();
        return (
          (client.clientName && client.clientName.toLowerCase().includes(searchTerm)) ||
          (client.client_ID && client.client_ID.toLowerCase().includes(searchTerm))
        );
      });
    }
    
    // Then sort the filtered results
    return filtered.sort((a, b) => {
      // Check for dumps on current date
      const aHasCurrentDumps = a.dumps?.some(d => d.date === currentDate && d.stats?.totalFiles > 0) || false;
      const bHasCurrentDumps = b.dumps?.some(d => d.date === currentDate && d.stats?.totalFiles > 0) || false;
      
      // First sort by current date dumps
      if (aHasCurrentDumps && !bHasCurrentDumps) return -1;
      if (!aHasCurrentDumps && bHasCurrentDumps) return 1;
      
      // Then sort by any dumps
      const aHasDumps = (a.dumps?.length > 0) || false;
      const bHasDumps = (b.dumps?.length > 0) || false;
      if (aHasDumps && !bHasDumps) return -1;
      if (!aHasDumps && bHasDumps) return 1;
      
      return (a.clientName || '').localeCompare(b.clientName || '');
    });
  }, [dumpData, dumpSearchQuery]);

  // Add this new filter function here
  const filteredPostData = useMemo(() => {
    let filtered = postData;
    if (postSearchQuery.trim()) {
      filtered = postData.filter(client => 
        client.clientName.toLowerCase().includes(postSearchQuery.toLowerCase())
      );
    }
    
    // Sort clients - those with posts come first
    return filtered.sort((a, b) => {
      const aHasPosts = a.posts.some(p => p.stats?.hasActivity);
      const bHasPosts = b.posts.some(p => p.stats?.hasActivity);
      if (aHasPosts && !bHasPosts) return -1;
      if (!aHasPosts && bHasPosts) return 1;
      return a.clientName.localeCompare(b.clientName);
    });
  }, [postData, postSearchQuery]);

  // Add filter function for saved content
  const filteredSavedData = useMemo(() => {
    const currentDate = new Date().toISOString().split('T')[0];
    
    // First apply the search filter
    let filtered = [...savedData];
    if (savedSearchQuery.trim()) {
      filtered = savedData.filter(client => {
        const searchTerm = savedSearchQuery.toLowerCase();
        return (
          (client.clientName && client.clientName.toLowerCase().includes(searchTerm)) ||
          (client.client_ID && client.client_ID.toLowerCase().includes(searchTerm))
        );
      });
    }
    
    // Then sort the filtered results
    return filtered.sort((a, b) => {
      // Check for saved content on current date
      const aHasCurrentSaved = a.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      const bHasCurrentSaved = b.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      
      // First sort by current date saved content
      if (aHasCurrentSaved && !bHasCurrentSaved) return -1;
      if (!aHasCurrentSaved && bHasCurrentSaved) return 1;
      
      // Then sort by any saved content
      const aHasSaved = a.uploads?.some(u => u.stats?.hasContent) || false;
      const bHasSaved = b.uploads?.some(u => u.stats?.hasContent) || false;
      if (aHasSaved && !bHasSaved) return -1;
      if (!aHasSaved && bHasSaved) return 1;
      
      return (a.clientName || '').localeCompare(b.clientName || '');
    });
  }, [savedData, savedSearchQuery]);

  // Add title effect
  useEffect(() => {
    document.title = 'Content | Snapped';
  }, []);

  // Debug log to see client data structure
  useEffect(() => {
    console.log('Client data:', clientData.map(client => ({
      _id: client._id,
      client_ID: client.client_ID,
      clientName: client.clientName
    })));
  }, [clientData]);

  /**
   * Fetches upload data and analytics for the tracker.
   * Retrieves media items, client information, and associated analytics.
   * 
   * @async
   * @function fetchData
   * @returns {Promise<void>}
   */
  useEffect(() => {
    const fetchData = async () => {
      try {
        // First get the uploads data
        const uploadsResponse = await axios.get(API_ENDPOINTS.UPLOAD_ACTIVITY);
        console.log('Upload activity response:', uploadsResponse.data);
        
        const clients = uploadsResponse?.data?.data || [];
        setClientData(clients);

        // Then fetch flags for each client
        const flagsMap = {};
        await Promise.all(
          clients.map(async (client) => {
            try {
              const clientId = client.client_ID || client._id;
              if (!clientId) return;

              const flagsResponse = await axios.get(`${API_ENDPOINTS.CONTENT_FLAGS}/${clientId}`);
              console.log(`Flags for client ${client.clientName} (${clientId}):`, flagsResponse.data);
              
              if (flagsResponse?.data?.data) {
                flagsResponse.data.data.forEach(flag => {
                  if (!flagsMap[clientId]) {
                    flagsMap[clientId] = {};
                  }
                  if (!flagsMap[clientId][flag.folder_id]) {
                    flagsMap[clientId][flag.folder_id] = [];
                  }
                  flagsMap[clientId][flag.folder_id].push(flag);
                  console.log(`Added flag for folder ${flag.folder_id}:`, flag);
                });
              }
            } catch (error) {
              console.error(`Error fetching flags for client ${client._id}:`, error);
            }
          })
        );
        console.log('Final flags map:', flagsMap);
        setContentFlags(flagsMap);

        // Continue with other data fetching...
        const [dumpsResponse, savedResponse] = await Promise.all([
          axios.get(API_ENDPOINTS.CONTENT_DUMPS),
          axios.get(API_ENDPOINTS.SAVED_ACTIVITY)
        ]);

        setDumpData(dumpsResponse?.data?.data || []);
        setSavedData(savedResponse?.data?.data || []);

        // Try post request separately so it won't affect other data if it fails
        try {
          const postsResponse = await axios.get(API_ENDPOINTS.POST_ACTIVITY);
          setPostData(postsResponse?.data?.data || []);
        } catch (postErr) {
          console.error('Error fetching post data:', postErr);
          setPostData([]);
        }
        
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        
        // Generate dates with future dates to the left and past dates to the right
        const dateList = Array.from({length: 38}, (_, i) => {
          const date = new Date(today);
          // Start from 5 days in future and go backwards
          date.setDate(today.getDate() + (5 - i));
          
          const year = date.getFullYear();
          const month = String(date.getMonth() + 1).padStart(2, '0');
          const day = String(date.getDate()).padStart(2, '0');
          return `${year}-${month}-${day}`;
        });

        setDates(dateList);
      } catch (err) {
        console.error('Error fetching data:', err);
        setClientData([]);
        setDumpData([]);
        setPostData([]);
      }
    };

    fetchData();
  }, []);

  // Modify the scrolling effect to handle both tables
  useEffect(() => {
    if (containerRef.current && dates.length > 0) {
      const indexOfToday = 5; // Today is at index 5 (after 5 future dates)
      const columnWidth = 100; // Width of date columns
      const clientColumnWidth = 200; // Width of client column

      // Scroll both tables to position today's date next to client column
      const tables = document.querySelectorAll('.upload-table');
      tables.forEach(table => {
        table.scrollLeft = indexOfToday * columnWidth;
      });
    }
  }, [dates]);

  /**
   * Handles clicking on a cell in the upload grid.
   * Opens the media modal for the selected client and date.
   * 
   * @async
   * @function handleCellClick
   * @param {string} client - Client identifier
   * @param {string} date - Selected date
   * @returns {Promise<void>}
   */
  const handleCellClick = async (client, date) => {
    if (activeView === 'uploads') {
      try {
        console.log('Client data in click:', client);
        const clientId = client.client_ID || client.clientId || client._id;
        if (!clientId) {
          console.error('No client ID found in:', client);
          return;
        }

        setSelectedMedia({
          files: [],
          isLoading: true,
          client_ID: clientId
        });

        // Format date as MM-DD-YYYY for folder ID
        const [year, month, day] = date.split('-');
        const folderID = `F(${month}-${day}-${year})_${clientId}`;

        console.log('Making request with:', {
          clientId,
          folderID,
          date
        });

        const response = await axios.get(
          `${API_ENDPOINTS.UPLOAD_MEDIA_DETAILS}/${clientId}/${folderID}`
        );

        console.log('Got response:', {
          data: response.data,
          files: response.data?.files,
          fileCount: response.data?.files?.length
        });
        
        if (response.data?.files?.length > 0) {
          const mediaData = {
            ...response.data,
            isLoading: false,
            client_ID: clientId,
            clientName: client.clientName,
            date: date,
            folder_id: folderID,
            type: 'upload'
          };
          console.log('Setting selectedMedia:', mediaData);
          setSelectedMedia(mediaData);
        } else {
          console.log('No files found in response');
          setSelectedMedia(null);
          toast.error('No files found for this date');
        }
      } catch (err) {
        console.error('Error handling cell click:', err);
        setSelectedMedia(null);
        toast.error('Failed to load media details');
      }
    } else if (activeView === 'spotlights') {
      try {
        // Get the correct client ID format
        const clientId = client.client_ID || client.clientId || client._id;
        if (!clientId) {
          console.error('No client ID found in:', client);
          toast.error('Invalid client data');
          return;
        }

        setSelectedMedia({
          files: [],
          isLoading: true,
          client_ID: clientId
        });

        // Format date for API - should be in YYYY-MM-DD format
        const response = await axios.get(
          `${API_ENDPOINTS.SPOTLIGHT_DETAILS}/${clientId}/${date}`
        );

        console.log('Spotlight details response:', response.data);
        
        if (response.data?.files?.length > 0) {
          const views = analyticsData[clientId]?.[date] || 0;
          
          const mediaData = {
            clientName: client.clientName,
            date: date,
            files: response.data.files,
            client_ID: clientId,
            type: 'spotlight',
            stats: {
              story_views: views,
              impressions: 0,
              follower_change: 0,
              reach: 0,
              story_view_time: 0
            }
          };
          
          console.log('Setting selectedMedia for spotlight:', mediaData);
          setSelectedMedia(mediaData);
        } else {
          console.log('No spotlight files found');
          setSelectedMedia(null);
          toast.error('No spotlight content found for this date');
        }
      } catch (err) {
        console.error('Error handling spotlight click:', err);
        setSelectedMedia(null);
        toast.error('Failed to load spotlight content');
      }
    } else if (activeView === 'saved') {
      try {
        // Get the correct client ID format - try all possible fields
        const clientId = client.client_ID || client.clientId || client._id;
        
        if (!clientId) {
          console.error('No valid client ID found in:', client);
          toast.error('Invalid client data');
          return;
        }

        // Set loading state
        setSelectedMedia({
          files: [],
          isLoading: true,
          client_ID: clientId,
          clientName: client.clientName
        });

        // Format date from YYYY-MM-DD to MM-DD-YYYY for folder ID
        const [year, month, day] = date.split('-');
        const folderID = `F(${month}-${day}-${year})_${clientId}`;

        console.log('Making saved content request with:', {
          clientId,
          folderID,
          date,
          client
        });

        const response = await axios.get(
          `${API_ENDPOINTS.SAVED_MEDIA_DETAILS}/${clientId}/${folderID}`
        );
        
        console.log('Saved content response:', response.data);
        
        if (response.data?.files?.length > 0) {
          const mediaData = {
            clientName: client.clientName,
            date: date,
            files: response.data.files.map(file => ({
              ...file,
              CDN_link: file.CDN_link,
              file_name: file.file_name,
              file_type: file.file_type,
              seq_number: file.seq_number || 0
            })),
            folder_id: folderID,
            client_ID: clientId,
            type: 'saved',
            stats: {
              story_views: analyticsData[clientId]?.[date] || 0,
              impressions: 0,
              follower_change: 0,
              reach: 0,
              story_view_time: 0
            }
          };
          
          console.log('Setting selectedMedia for saved content:', mediaData);
          setSelectedMedia(mediaData);
        } else {
          console.log('No files found in response');
          setSelectedMedia(null);
          toast.error('No saved content found for this date');
        }
      } catch (err) {
        console.error('Error fetching saved content:', err);
        setSelectedMedia(null);
        toast.error('Failed to load saved content');
      }
    }
  };

  /**
   * Handles clicking on the content dump button.
   * Initiates content dump process for selected client and date.
   * 
   * @async
   * @function handleDumpClick
   * @param {string} client - Client identifier
   * @param {string} date - Selected date
   * @returns {Promise<void>}
   */
  const handleDumpClick = async (client, date) => {
    try {
      // Get the correct client ID - try client_ID first, then _id, then client_id
      const clientId = client.client_ID || client._id || client.client_id;
      
      if (!clientId) {
        console.error('No valid client ID found in:', client);
        toast.error('Invalid client data');
        return;
      }
      
      // For dumps, we use the CONTENTDUMP_clientId format
      const dumpSessionId = `CONTENTDUMP_${clientId}`;
      
      console.log('Making dump request with:', {
        clientId,
        dumpSessionId,
        client
      });
      
      const response = await axios.get(`${API_ENDPOINTS.UPLOAD_MEDIA_DETAILS}/${clientId}/${dumpSessionId}`);
      console.log('Dump details response:', response.data);
      
      if (response.data?.files?.length > 0) {
        setSelectedDump({
          clientName: client.clientName,
          date: date,
          isLoading: false,
          files: response.data.files,
          client_ID: clientId,
          type: 'dump'
        });
      } else {
        console.log('No dump files found');
        setSelectedDump(null);
        toast.error('No dump content found for this date');
      }
    } catch (err) {
      console.error('Error fetching dump files:', err);
      setSelectedDump(null);
      toast.error('Failed to load dump content');
    }
  };

  /**
   * Handles clicking on the post button.
   * Manages post creation and scheduling for selected content.
   * 
   * @async
   * @function handlePostClick
   * @param {string} client - Client identifier
   * @param {string} date - Selected date
   * @returns {Promise<void>}
   */
  const handlePostClick = async (client, date) => {
    const postStats = client.posts.find(p => p.date === date)?.stats;
    if (postStats?.hasActivity) {
      try {
        // Get the correct client ID - try client_ID first, then _id
        const clientId = client.client_ID || client._id;
        
        console.log('Making post activity details request with:', {
          clientId,
          date,
          client
        });

        // Get the queue data for this date
        const response = await axios.get(`${API_ENDPOINTS.POST_ACTIVITY_DETAILS}/${clientId}/${date}`);
        
        console.log('Post activity details response:', response.data);
        
        // Get the stories from the client's queue
        const stories = response.data?.client_queues?.[clientId]?.stories || [];
        
        console.log('Found stories:', stories);
        
        setSelectedPost({
          clientName: client.clientName,
          date: date,
          isLoading: false,
          stats: postStats,
          type: 'post',
          files: stories.map((story, index) => ({
            file_name: story.file_name,
            file_type: story.file_type,
            CDN_link: story.cdn_url,
            seq_number: index,
            scheduled_time: story.scheduled_time
          })),
          client_ID: clientId
        });
      } catch (err) {
        console.error('Error fetching post files:', err);
        toast.error('Failed to load post details');
        setSelectedPost({
          clientName: client.clientName,
          date: date,
          isLoading: false,
          stats: postStats,
          type: 'post',
          files: [],
          client_ID: client.client_ID || client._id
        });
      }
    }
  };

  /**
   * Fetches spotlight data for featured content.
   * Retrieves highlighted media items and their performance metrics.
   * 
   * @async
   * @function fetchSpotlightData
   * @returns {Promise<void>}
   */
  useEffect(() => {
    const fetchSpotlightData = async () => {
      try {
        const response = await axios.get(API_ENDPOINTS.SPOTLIGHTS);
        if (response.data?.data) {
          setSpotlightData(response.data.data);
        }
      } catch (error) {
        console.error('Error fetching spotlight data:', error);
      }
    };

    fetchSpotlightData();
  }, []);

  // Add filter function for spotlights
  const filteredSpotlightData = useMemo(() => {
    const currentDate = new Date().toISOString().split('T')[0];
    
    // First apply the search filter
    let filtered = [...spotlightData];
    if (spotlightSearchQuery.trim()) {
      filtered = spotlightData.filter(client => {
        const searchTerm = spotlightSearchQuery.toLowerCase();
        return (
          (client.clientName && client.clientName.toLowerCase().includes(searchTerm)) ||
          (client.client_ID && client.client_ID.toLowerCase().includes(searchTerm))
        );
      });
    }
    
    // Then sort the filtered results
    return filtered.sort((a, b) => {
      // Check for spotlights on current date
      const aHasCurrentSpotlights = a.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      const bHasCurrentSpotlights = b.uploads?.some(u => 
        u.date === currentDate && u.stats?.hasContent
      ) || false;
      
      // First sort by current date spotlights
      if (aHasCurrentSpotlights && !bHasCurrentSpotlights) return -1;
      if (!aHasCurrentSpotlights && bHasCurrentSpotlights) return 1;
      
      // Then sort by any spotlights
      const aHasSpotlights = a.uploads?.some(u => u.stats?.hasContent) || false;
      const bHasSpotlights = b.uploads?.some(u => u.stats?.hasContent) || false;
      if (aHasSpotlights && !bHasSpotlights) return -1;
      if (!aHasSpotlights && bHasSpotlights) return 1;
      
      return (a.clientName || '').localeCompare(b.clientName || '');
    });
  }, [spotlightData, spotlightSearchQuery]);

  /**
   * Fetches analytics data for all tracked media.
   * Retrieves comprehensive performance metrics across all content.
   * 
   * @async
   * @function fetchAllAnalytics
   * @returns {Promise<void>}
   */
  useEffect(() => {
    const fetchAllAnalytics = async () => {
      try {
        const response = await axios.get(API_ENDPOINTS.STORY_METRICS);
        
        // Convert the response into the same format we were using before
        const analyticsMap = {};
        
        response.data.data.forEach(client => {
          analyticsMap[client._id] = {};  // Now using the user_id as the key
          client.uploads.forEach(upload => {
            analyticsMap[client._id][upload.date] = upload.views;
          });
        });

        setAnalyticsData(analyticsMap);

      } catch (error) {
        console.error('Error fetching analytics:', error);
      }
    };

    fetchAllAnalytics();
  }, []);

  /**
   * Formats numbers for display with appropriate suffixes (K, M, etc.).
   * 
   * @function formatNumber
   * @param {number} num - Number to format
   * @returns {string} Formatted number string
   */
  const formatNumber = (num) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num;
  };

  /**
   * Handles clicking on analytics data.
   * Opens detailed analytics view for selected content.
   * 
   * @async
   * @function handleAnalyticsClick
   * @param {Event} e - Click event
   * @param {string} client - Client identifier
   * @param {string} date - Selected date
   * @param {Object} dayData - Analytics data for the day
   * @returns {Promise<void>}
   */
  const handleAnalyticsClick = async (e, client, date, dayData) => {
    e.stopPropagation();
    
    try {
      const [year, month, day] = date.split('-');
      const folderID = `F(${month}-${day}-${year})_${client._id}`;
      const views = analyticsData[client._id]?.[date] || 0;

      const mediaResponse = await axios.get(
        `${API_ENDPOINTS.UPLOAD_MEDIA_DETAILS}/${client._id}/${folderID}`
      );

      setSelectedAnalytics({
        clientName: client.clientName,
        date: date,
        stats: {
          postCount: dayData.videoCount + dayData.imageCount,
          views: views,  // Use the views from our analytics data
          hasContent: dayData.hasContent,
          videoCount: dayData.videoCount,
          imageCount: dayData.imageCount
        },
        type: 'analytics',
        client_ID: client._id,
        files: mediaResponse.data.files || []
      });

    } catch (err) {
      console.error('Error fetching data:', err);
      setSelectedAnalytics(null);
      toast.error('Failed to load analytics data');
    }
  };

  /**
   * Refreshes upload data and analytics.
   * Updates the tracker with latest content and metrics.
   * 
   * @async
   * @function refreshUploadData
   * @returns {Promise<void>}
   */
  const refreshUploadData = async () => {
    try {
      const uploadsResponse = await axios.get(API_ENDPOINTS.UPLOAD_ACTIVITY);
      console.log('Refreshing upload activity data:', uploadsResponse.data);
      const clients = uploadsResponse?.data?.data || [];
      setClientData(clients);

      // Refresh flags for each client
      const flagsMap = {};
      await Promise.all(
        clients.map(async (client) => {
          try {
            const clientId = client.client_ID || client._id;
            if (!clientId) return;

            const flagsResponse = await axios.get(`${API_ENDPOINTS.CONTENT_FLAGS}/${clientId}`);
            if (flagsResponse?.data?.data) {
              flagsResponse.data.data.forEach(flag => {
                if (!flagsMap[clientId]) {
                  flagsMap[clientId] = {};
                }
                if (!flagsMap[clientId][flag.folder_id]) {
                  flagsMap[clientId][flag.folder_id] = [];
                }
                flagsMap[clientId][flag.folder_id].push(flag);
              });
            }
          } catch (error) {
            console.error(`Error fetching flags for client ${client._id}:`, error);
          }
        })
      );
      setContentFlags(flagsMap);
    } catch (error) {
      console.error('Error refreshing upload data:', error);
      toast.error('Failed to refresh data');
    }
  };

  return (
    <div className="upload-tracker">
      <div className="upload-section">
        <div className="section-header-track">
          <h1>UPLOAD MANAGER</h1>
          <div className="controls-row">
            <div className="left-controls">
              <div className="view-toggle">
                <button 
                  className={`toggle-button ${activeView === 'uploads' ? 'active' : ''}`}
                  onClick={() => setActiveView('uploads')}
                >
                  STORIES
                </button>
                <button 
                  className={`toggle-button ${activeView === 'dumps' ? 'active' : ''}`}
                  onClick={() => setActiveView('dumps')}
                >
                  DUMPS
                </button>
                <button 
                  className={`toggle-button ${activeView === 'spotlights' ? 'active' : ''}`}
                  onClick={() => setActiveView('spotlights')}
                >
                  SPOTLIGHTS
                </button>
                {/* Temporarily hidden SAVED button
                <button 
                  className={`toggle-button ${activeView === 'saved' ? 'active' : ''}`}
                  onClick={() => setActiveView('saved')}
                >
                  SAVED
                </button>
                */}
              </div>
            </div>
            <div className="upload-tracker-search-container">
              <input
                type="text"
                placeholder={`Search ${activeView}...`}
                value={
                  activeView === 'uploads' ? uploadSearchQuery :
                  activeView === 'dumps' ? dumpSearchQuery :
                  activeView === 'spotlights' ? spotlightSearchQuery :
                  savedSearchQuery
                }
                onChange={(e) => {
                  const value = e.target.value;
                  console.log('Search value changed:', value);
                  switch(activeView) {
                    case 'uploads':
                      setUploadSearchQuery(value);
                      break;
                    case 'dumps':
                      setDumpSearchQuery(value);
                      break;
                    case 'spotlights':
                      setSpotlightSearchQuery(value);
                      break;
                    /* Temporarily hidden SAVED case
                    case 'saved':
                      setSavedSearchQuery(value);
                      break;
                    */
                  }
                }}
                className="upload-tracker-search-input"
              />
            </div>
          </div>
        </div>

        <div className="upload-table" ref={containerRef}>
          <div className="table-header">
            <div className="header-cell client-column">Client</div>
            {dates.map(date => {
              const displayDate = new Date(date);
              displayDate.setDate(displayDate.getDate() + 1);
              
              return (
                <div key={date} className="header-cell date-column">
                  {displayDate.toLocaleDateString('en-US', {
                    month: 'short',
                    day: 'numeric'
                  })}
                </div>
              );
            })}
          </div>

          <div className="table-body">
            {(activeView === 'uploads' ? filteredUploadData : 
              activeView === 'dumps' ? filteredDumpData :
              /* Temporarily modified to remove saved option
              activeView === 'spotlights' ? filteredSpotlightData :
              filteredSavedData */
              filteredSpotlightData
            ).map(client => (
              <div key={client.client_ID} className="table-row">
                <div className="cell client-column">{client.clientName}</div>
                {dates.map(date => {
                  if (activeView === 'uploads') {
                    const dayData = client.uploads.find(u => u.date === date);
                    const hasUploads = dayData?.stats && (
                      dayData.stats.videoCount > 0 || dayData.stats.imageCount > 0
                    );
                    const views = analyticsData[client._id]?.[date] || 0;

                    return (
                      <div 
                        key={date}
                        className={`cell date-column 
                          ${hasUploads ? 'has-uploads' : 'no-content'} 
                          ${views > 0 ? 'has-analytics' : ''}
                          ${(() => {
                            // Format date as MM-DD-YYYY for folder ID
                            const [year, month, day] = date.split('-');
                            const folderID = `F(${month}-${day}-${year})_${client._id}`;
                            
                            // Check if there are any flags for this folder that aren't removed
                            const folderFlags = contentFlags[client._id]?.[folderID] || [];
                            const hasFlags = folderFlags.some(flag => 
                              !flag.content_matches_status || flag.content_matches_status !== 'removed'
                            );
                            
                            console.log(`Checking flags for client ${client.clientName}, date ${date}:`, {
                              folderID,
                              flags: folderFlags,
                              hasFlags,
                              icon: faExclamationTriangle || faTriangleExclamation
                            });
                            
                            return hasFlags ? 'has-flags' : '';
                          })()} 
                          ${dayData?.sessions?.some(session => session.approved === true) ? 'approved' : ''}`}
                        onClick={() => hasUploads && handleCellClick(client, date)}
                      >
                        {hasUploads && (
                          <>
                            <div 
                              className="time clickable"
                              onClick={() => handleCellClick(client, date)}
                            >
                              {dayData.stats.videoMinutes}
                            </div>
                            <div className="stats">
                              <span>
                                <FontAwesomeIcon icon={faVideo} className="fa-icon" /> 
                                {dayData.stats.videoCount}
                              </span>
                              <span>
                                <FontAwesomeIcon icon={faImage} className="fa-icon" /> 
                                {dayData.stats.imageCount}
                              </span>
                              {views > 0 && (
                                <span 
                                  className="analytics-trigger"
                                  onClick={(e) => handleAnalyticsClick(e, client, date, dayData)}
                                >
                                  <FontAwesomeIcon icon={faChartBar} className="fa-icon" />
                                  <span className="stat-number">
                                    {formatNumber(views)}
                                  </span>
                                </span>
                              )}
                            </div>
                            {(() => {
                              const [year, month, day] = date.split('-');
                              const folderID = `F(${month}-${day}-${year})_${client._id}`;
                              const folderFlags = contentFlags[client._id]?.[folderID] || [];
                              const hasFlags = folderFlags.some(flag => 
                                !flag.content_matches_status || flag.content_matches_status !== 'removed'
                              );
                              
                              return hasFlags ? (
                                <div className="flag-indicator">
                                  <FontAwesomeIcon 
                                    icon={faExclamationTriangle || faTriangleExclamation}
                                    className="fa-icon warning"
                                    style={{ display: 'block', fontSize: '20px' }}
                                  />
                                </div>
                              ) : null;
                            })()}
                          </>
                        )}
                      </div>
                    );
                  } else if (activeView === 'dumps') {
                    const dumpData = client.dumps.find(d => d.date === date)?.stats;
                    const hasDump = dumpData && (dumpData.totalFiles > 0);

                    return (
                      <div 
                        key={date}
                        className={`cell date-column ${hasDump ? 'has-uploads' : 'no-content'}`}
                        onClick={() => hasDump && handleDumpClick(client, date)}
                      >
                        {hasDump && (
                          <div className="stats">
                            <span>
                              <FontAwesomeIcon icon={faCamera} className="fa-icon" /> 
                              <span className="stat-number">{dumpData.totalFiles}</span>
                            </span>
                            <span>
                              <span className="stat-number">{dumpData.totalSize}</span>
                            </span>
                          </div>
                        )}
                      </div>
                    );
                  } else if (activeView === 'spotlights') {
                    const spotlightData = client.uploads.find(u => u.date === date)?.stats;
                    const hasSpotlight = spotlightData && (
                      spotlightData.videoCount > 0 || spotlightData.imageCount > 0
                    );

                    return (
                      <div 
                        key={date}
                        className={`cell date-column ${hasSpotlight ? 'has-uploads' : 'no-content'}`}
                        onClick={() => hasSpotlight && handleCellClick(client, date)}
                      >
                        {hasSpotlight && (
                          <>
                            <div 
                              className="time clickable"
                              onClick={() => handleCellClick(client, date)}
                            >
                              {spotlightData.videoMinutes}
                            </div>
                            <div className="stats">
                              <span>
                                <FontAwesomeIcon icon={faVideo} className="fa-icon" /> 
                                {spotlightData.videoCount}
                              </span>
                              <span>
                                <FontAwesomeIcon icon={faImage} className="fa-icon" /> 
                                {spotlightData.imageCount}
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    );
                  } 
                  /* Temporarily hidden SAVED logic
                  else {
                    // Saved content logic
                    const savedData = client.uploads.find(u => u.date === date)?.stats;
                    const hasSaved = savedData && (
                      savedData.videoCount > 0 || savedData.imageCount > 0
                    );

                    return (
                      <div 
                        key={date}
                        className={`cell date-column ${hasSaved ? 'has-uploads' : 'no-content'}`}
                        onClick={() => hasSaved && handleCellClick(client, date)}
                      >
                        {hasSaved && (
                          <>
                            <div 
                              className="time clickable"
                              onClick={() => handleCellClick(client, date)}
                            >
                              {savedData.videoMinutes}
                            </div>
                            <div className="stats">
                              <span>
                                <FontAwesomeIcon icon={faVideo} className="fa-icon" /> 
                                {savedData.videoCount}
                              </span>
                              <span>
                                <FontAwesomeIcon icon={faImage} className="fa-icon" /> 
                                {savedData.imageCount}
                              </span>
                            </div>
                          </>
                        )}
                      </div>
                    );
                  }
                  */
                })}
              </div>
            ))}
            <div className="upload-table-fade" />
          </div>
        </div>
      </div>

      {/* POST TRACKER SECTION - COMMENTED OUT
      <div className="posts-section">
        <h1>POST TRACKER</h1>
        <div className="upload-tracker-search-container">
          <input
            type="text"
            placeholder="Search posts..."
            value={postSearchQuery}
            onChange={(e) => setPostSearchQuery(e.target.value)}
            className="upload-tracker-search-input"
          />
        </div>
        <div className="upload-table">
          <div className="table-header">
            <div className="header-cell client-column">Client</div>
            {dates.map(date => {
              const displayDate = new Date(date);
              displayDate.setDate(displayDate.getDate() + 1);
              
              return (
                <div key={date} className="header-cell date-column">
                  {displayDate.toLocaleDateString('en-US', {
                    month: 'short',
                    day: 'numeric'
                  })}
                </div>
              );
            })}
          </div>

          <div className="table-body">
            {filteredPostData.map(client => (
              <div key={client.client_ID} className="table-row">
                <div className="cell client-column">{client.clientName}</div>
                {dates.map(date => {
                  const postStats = client.posts.find(p => p.date === date)?.stats;
                  const hasActivity = postStats?.hasActivity;
                  const views = analyticsData[client._id]?.[date] || 0;

                  return (
                    <div 
                      key={date}
                      className={`cell date-column ${hasActivity ? 'has-uploads' : 'no-content'}`}
                      onClick={() => hasActivity && handlePostClick(client, date)}
                    >
                      {hasActivity && (
                        <div className="stats">
                          <span>
                            <FontAwesomeIcon icon={faCamera} className="fa-icon" />
                            <span className="stat-number">{postStats.postCount}</span>
                          </span>
                          <span>
                            <FontAwesomeIcon icon={faChartBar} className="fa-icon" />
                            <span className="stat-number">
                              {formatNumber(views)}
                            </span>
                          </span>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ))}
            <div className="upload-table-fade" />
          </div>
        </div>
      </div>
      */}

      {selectedMedia && (
        <MediaModal 
          media={selectedMedia} 
          onClose={() => {
            setSelectedMedia(null);
            refreshUploadData();
          }}
          onContentDeleted={refreshUploadData}
        />
      )}
      
      {selectedDump && (
        <MediaModal 
          media={selectedDump}
          onClose={() => setSelectedDump(null)}
          isLoading={selectedDump.isLoading}
        />
      )}

      {/* POST MODAL - COMMENTED OUT
      {selectedPost && (
        <MediaModalData 
          media={selectedPost}
          onClose={() => setSelectedPost(null)}
          isLoading={selectedPost.isLoading}
          type="post"
        />
      )}
      */}

      {selectedAnalytics && (
        <MediaModalData
          media={selectedAnalytics}
          onClose={() => setSelectedAnalytics(null)}
          type="analytics"
        />
      )}
    </div>
  );
};

export default UploadTracker; 