/**
 * @fileoverview MediaModal-Data component for displaying and managing media data analytics.
 * Provides functionality for viewing and analyzing media performance metrics.
 */

import React, { useState, useEffect, useRef } from 'react';
import './MediaModal.css';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { toast } from 'react-hot-toast';
import { fetchAuthSession } from '@aws-amplify/auth';

/**
 * @typedef {Object} MediaData
 * @property {string} id - Unique identifier for the media
 * @property {Object} analytics - Analytics data for the media
 * @property {Object} metadata - Additional metadata about the media
 * @property {Array<Object>} files - Associated media files
 */

/**
 * @typedef {Object} MediaModalDataProps
 * @property {MediaData} media - Media data to display
 * @property {Function} onClose - Callback function to close the modal
 */

/**
 * MediaModal-Data component for displaying media analytics and data.
 * 
 * @component
 * @param {MediaModalDataProps} props - Component props
 * @returns {React.ReactElement} The rendered MediaModal-Data component
 */
const MediaModal = ({ media, onClose }) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [currentIndex, setCurrentIndex] = useState(0);
  const videoRef = useRef(null);
  const [showNoteModal, setShowNoteModal] = useState(false);
  const [noteText, setNoteText] = useState('');
  const [showSessionNoteModal, setShowSessionNoteModal] = useState(false);
  const [sessionNoteText, setSessionNoteText] = useState('');
  const [currentNote, setCurrentNote] = useState(null);
  const [showNotesListModal, setShowNotesListModal] = useState(false);
  const [sessionNotes, setSessionNotes] = useState([]);
  const [mediaUrl, setMediaUrl] = useState('');
  const [isLoadingDetails, setIsLoadingDetails] = useState(false);
  const [selectedPost, setSelectedPost] = useState(null);
  const [analyticsData, setAnalyticsData] = useState({
    story_views: 0,
    impressions: 0,
    follower_change: 0,
    reach: 0,
    story_view_time: 0
  });
  const [videoSummary, setVideoSummary] = useState(null);

  /**
   * Gets authenticated URL for accessing media files.
   * 
   * @async
   * @function getAuthenticatedUrl
   * @param {Object} file - File to get URL for
   * @returns {Promise<string>} Authenticated URL for the file
   */
  const getAuthenticatedUrl = async (file) => {
    // Log the file to see its structure
    console.log('Getting authenticated URL for file:', file);
    
    if (!file?.CDN_link) {
      console.log('No CDN link found for file:', file);
      return;
    }
    
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      const url = file.CDN_link;
      const separator = url.includes('?') ? '&' : '?';
      const authenticatedUrl = `${url}${separator}token=${token}`;
      
      setMediaUrl(authenticatedUrl);
    } catch (error) {
      console.error('Error getting authenticated URL:', error);
    }
  };

  /**
   * Extracts files from media data.
   * 
   * @function extractFiles
   * @param {MediaData} mediaData - Media data to extract files from
   * @returns {Array<Object>} Array of extracted files
   */
  const extractFiles = (mediaData) => {
    if (!mediaData?.client_queues) return [];
    
    const allFiles = [];
    Object.entries(mediaData.client_queues).forEach(([clientId, data]) => {
      if (data.stories) {
        data.stories.forEach((story, index) => {
          allFiles.push({
            ...story,
            CDN_link: story.cdn_url,
            seq_number: index,
            client_id: clientId
          });
        });
      }
    });
    return allFiles;
  };

  /**
   * Converts ID to user ID format.
   * 
   * @function convertToUserId
   * @param {string} id - ID to convert
   * @returns {string} Converted user ID
   */
  const convertToUserId = (id) => {
    // Convert formats like 'jm07161995' to proper user_id format
    if (!id) return null;
    
    // Extract just the numbers
    const numbers = id.match(/\d+/)?.[0];
    if (!numbers) return null;
    
    // Format: first 2 letters + numbers
    const prefix = id.slice(0, 2).toLowerCase();
    return `${prefix}${numbers}`;
  };

  // Sort files by sequence number and set initial selected file
  useEffect(() => {
    if (media?.files?.length > 0) {
      const sortedFiles = [...media.files].sort((a, b) => 
        (a.seq_number || 0) - (b.seq_number || 0)
      );
      // Set the first file as selected immediately
      setSelectedFile(sortedFiles[0]);
      setCurrentIndex(0);
      
      // Also set the media URL for the first file
      if (sortedFiles[0]?.CDN_link) {
        getAuthenticatedUrl(sortedFiles[0]);
      }
    }
  }, [media]);

  // Update media URL when selected file changes
  useEffect(() => {
    if (selectedFile) {
      getAuthenticatedUrl(selectedFile);
    }
  }, [selectedFile]);

  // Update keyboard navigation
  useEffect(() => {
    const handleKeyPress = (e) => {
      let files = [];
      
      if (media?.files?.length > 0) {
        files = media.files;
      } else if (media?.client_queues) {
        Object.entries(media.client_queues).forEach(([clientId, data]) => {
          if (data.stories) {
            data.stories.forEach((story, index) => {
              files.push({
                ...story,
                CDN_link: story.cdn_url,
                seq_number: index,
                client_id: clientId
              });
            });
          }
        });
      }
      
      if (!files.length) return;
      
      const sortedFiles = files.sort((a, b) => 
        (a.seq_number || 0) - (b.seq_number || 0)
      );
      
      if (e.key === 'ArrowUp' || e.key === 'ArrowLeft') {
        const newIndex = Math.max(0, currentIndex - 1);
        setCurrentIndex(newIndex);
        setSelectedFile(sortedFiles[newIndex]);
      } else if (e.key === 'ArrowDown' || e.key === 'ArrowRight') {
        const newIndex = Math.min(sortedFiles.length - 1, currentIndex + 1);
        setCurrentIndex(newIndex);
        setSelectedFile(sortedFiles[newIndex]);
      }
    };

    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [media, currentIndex]);

  // Single useEffect for handling media changes and analytics
  useEffect(() => {
    const loadAnalytics = async () => {
      const userId = media?.client_ID;
      const formattedDate = media.date;

      try {
        const { tokens } = await fetchAuthSession();
        const token = tokens.idToken.toString();
        
        // Get all metrics from /metrics endpoint
        const response = await axios.get(
          `https://track.snapped.cc/api/analytics/snapchat/metrics`,
          {
            params: {
              client_id: userId,
              date: formattedDate
            },
            headers: {
              'Authorization': `Bearer ${token}`
            }
          }
        );

        console.log('Metrics response:', response.data);

        // Update the stats with all metrics
        if (media.files) {
          media.files.forEach(file => {
            if (!file.stats) file.stats = {};
            file.stats.views = response.data.story_views || 0;
          });
        }

        // Update all analytics data
        setAnalyticsData({
          story_views: response.data.story_views || 0,
          impressions: response.data.impressions || 0,
          follower_change: response.data.follower_change || 0,
          reach: response.data.reach || 0,
          story_view_time: response.data.story_view_time || 0
        });

      } catch (err) {
        console.error('Error fetching metrics:', err);
        setAnalyticsData({
          story_views: 0,
          impressions: 0,
          follower_change: 0,
          reach: 0,
          story_view_time: 0
        });
      }
    };

    if (media) {
      loadAnalytics();
    }
  }, [media]);

  useEffect(() => {
    if (!selectedFile) return;
    
    // Check if the file has a video summary directly
    if (selectedFile.video_summary) {
      console.log('Found video summary in file:', selectedFile.video_summary);
      setVideoSummary(selectedFile.video_summary);
      return;
    }
    
    // If no direct summary, try to fetch it
    const fetchVideoSummary = async () => {
      try {
        const sessionId = getSessionFolder();
        console.log('Fetching video summary for:', {
          clientId: media.client_ID,
          sessionId,
          fileName: selectedFile.file_name
        });
        
        const response = await axios.get(
          API_ENDPOINTS.UPLOAD.VIDEO_SUMMARY(
            media.client_ID,
            sessionId,
            selectedFile.file_name
          )
        );
        
        if (response.data?.video_summary) {
          console.log('Video summary received:', response.data.video_summary);
          setVideoSummary(response.data.video_summary);
        } else {
          console.log('No video summary found');
          setVideoSummary(null);
        }
      } catch (err) {
        console.error('Error fetching video summary:', err);
        setVideoSummary(null);
      }
    };

    fetchVideoSummary();
  }, [selectedFile, media]);

  if (!media) {
    return null;
  }

  /**
   * Gets sorted list of files.
   * 
   * @function getSortedFiles
   * @returns {Array<Object>} Sorted array of files
   */
  const getSortedFiles = () => {
    let files = [];
    
    if (media?.files?.length > 0) {
      files = media.files;
    } else if (media?.client_queues) {
      Object.entries(media.client_queues).forEach(([clientId, data]) => {
        if (data.stories) {
          data.stories.forEach((story, index) => {
            files.push({
              ...story,
              CDN_link: story.cdn_url,
              seq_number: index,
              client_id: clientId
            });
          });
        }
      });
    }
    
    return files.sort((a, b) => (a.seq_number || 0) - (b.seq_number || 0));
  };

  const sortedFiles = getSortedFiles();

  /**
   * Handles file selection in the modal.
   * 
   * @function handleFileSelect
   * @param {Object} file - Selected file
   */
  const handleFileSelect = (file) => {
    if (videoRef.current) {
      videoRef.current.pause();
    }
    const newIndex = sortedFiles.findIndex(f => f.file_name === file.file_name);
    setSelectedFile(file);
    setCurrentIndex(newIndex);
  };

  /**
   * Gets the current session folder path.
   * 
   * @function getSessionFolder
   * @returns {string} Session folder path
   */
  const getSessionFolder = () => {
    const date = new Date(media.date);
    const formattedDate = date.toLocaleDateString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    }).replace(/\//g, '-');
    
    return `F(${formattedDate})_${media.client_id}`;
  };

  /**
   * Handles saving notes for media content.
   * 
   * @async
   * @function handleSaveNote
   * @returns {Promise<void>}
   */
  const handleSaveNote = async () => {
    try {
      const sessionFolder = getSessionFolder();
      
      const noteData = {
        client_ID: media.client_ID,
        session_folder: sessionFolder,
        file_data: {
          file_name: selectedFile.file_name,
          cdn_url: selectedFile.CDN_link,
          note: noteText,
          created_at: new Date().toISOString()
        }
      };
      
      console.log('Saving note with data:', noteData);
      
      const response = await axios.post(
        `${API_ENDPOINTS.UPLOAD.CONTENT_NOTES}/add`,
        noteData
      );
      
      if (response.status === 200) {
        setNoteText('');
        setShowNoteModal(false);
        toast.success('Note saved successfully');
        fetchNotes();
      }
    } catch (err) {
      console.error('Error saving note:', err);
      toast.error('Failed to save note');
    }
  };

  /**
   * Handles sending session notes.
   * 
   * @async
   * @function handleSessionNoteSend
   * @returns {Promise<void>}
   */
  const handleSessionNoteSend = async () => {
    try {
      const sessionFolder = getSessionFolder();
      
      // Include CDN links when mapping files
      const sessionFiles = media.files.map(file => ({
        file_name: file.file_name,
        cdn_url: file.CDN_link
      }));
      
      const noteData = {
        client_ID: media.client_ID,
        session_folder: sessionFolder,
        file_data: {
          file_name: null,
          note: sessionNoteText,
          created_at: new Date().toISOString(),
          session_files: sessionFiles
        }
      };

      console.log('Saving session note with data:', noteData);

      const response = await axios.post(
        `${API_ENDPOINTS.UPLOAD.CONTENT_NOTES}/add`,
        noteData
      );
      
      if (response.status === 200) {
        setSessionNoteText('');
        setShowSessionNoteModal(false);
        toast.success('Session note saved successfully');
        fetchNotes();
      }
    } catch (err) {
      console.error('Error saving session note:', err);
      toast.error('Failed to save note');
    }
  };

  /**
   * Handles deleting notes.
   * 
   * @async
   * @function handleDeleteNote
   * @param {Object} note - Note to delete
   * @returns {Promise<void>}
   */
  const handleDeleteNote = async (note) => {
    try {
      const sessionFolder = getSessionFolder();

      const response = await axios.delete(
        `${API_ENDPOINTS.CONTENT_NOTES}/delete-note/${media.client_ID}/${sessionFolder}`,
        { data: note }
      );
      
      if (response.status === 200) {
        fetchNotes();
        toast.success('Note deleted successfully');
      }
    } catch (err) {
      console.error('Error deleting note:', err);
      toast.error('Failed to delete note');
    }
  };

  /**
   * Renders note modal content.
   * 
   * @function renderNoteModalContent
   * @returns {React.ReactElement} Rendered note modal content
   */
  const renderNoteModalContent = () => (
    <div className="note-modal" onClick={() => setShowNoteModal(false)}>
      <div className="note-modal-content" onClick={e => e.stopPropagation()}>
        <h3>Creator Note</h3>
        <textarea
          value={noteText}
          onChange={(e) => setNoteText(e.target.value)}
          placeholder="Enter your note here..."
          rows={4}
        />
        <div className="note-modal-buttons">
          <button onClick={() => setShowNoteModal(false)}>Cancel</button>
          <button 
            onClick={handleSaveNote}
            disabled={!noteText.trim()}
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );

  /**
   * Handles clicking on note icons.
   * 
   * @function handleNoteClick
   * @param {Event} e - Click event
   * @param {Object} file - File associated with the note
   */
  const handleNoteClick = (e, file) => {
    e.stopPropagation();
    setSelectedFile(file);
    setShowNoteModal(true);
  };

  /**
   * Fetches notes for media content.
   * 
   * @async
   * @function fetchNotes
   * @returns {Promise<void>}
   */
  const fetchNotes = async () => {
    try {
      const sessionFolder = getSessionFolder();
      
      const response = await axios.get(
        `${API_ENDPOINTS.UPLOAD.CONTENT_NOTES}/get/${media.client_ID}/${sessionFolder}`
      );
      
      if (response.data && response.data.notes) {
        setSessionNotes(response.data.notes);
      } else {
        setSessionNotes([]);
      }
    } catch (err) {
      console.error('Error fetching notes:', err);
      toast.error('Failed to fetch notes');
    }
  };

  /**
   * Renders notes list modal.
   * 
   * @function renderNotesListModal
   * @returns {React.ReactElement} Rendered notes list modal
   */
  const renderNotesListModal = () => (
    <div className="creator-notes-view-modal" onClick={() => setShowNotesListModal(false)}>
      <div className="creator-notes-view-content" onClick={e => e.stopPropagation()}>
        <h3>Creator Notes</h3>
        {sessionNotes.map((note, index) => (
          <div key={index} className="creator-notes-item">
            <div className="creator-notes-item-header">
              <span>{new Date(note.created_at).toLocaleString()}</span>
              {note.file_name && <span>File: {note.file_name}</span>}
            </div>
            <p className="creator-notes-item-content">{note.note}</p>
            <button 
              className="creator-notes-delete-btn"
              onClick={() => handleDeleteNote(note)}
            >
              Delete
            </button>
          </div>
        ))}
      </div>
    </div>
  );

  /**
   * Handles clicking on post button.
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
        // Get the queue data for this date
        console.log('Client data for post:', client); // Debug log
        
        // Use _id as the client_id for the API call
        const clientId = client._id;
        console.log('Using client ID:', clientId); // Debug log
        
        const response = await axios.get(`${API_ENDPOINTS.POST_ACTIVITY_DETAILS}/${clientId}/${date}`);
        
        // Get the stories from the client's queue
        const stories = response.data?.client_queues?.[clientId]?.stories || [];
        
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
          client_ID: clientId  // Make sure we're passing the correct client ID
        });
      } catch (err) {
        console.error('Error fetching post files:', err);
        setSelectedPost({
          clientName: client.clientName,
          date: date,
          isLoading: false,
          stats: postStats,
          type: 'post',
          files: [],
          client_ID: client._id  // Make sure we're passing the correct client ID here too
        });
      }
    }
  };

  /**
   * Formats numbers for display.
   * 
   * @function formatNumber
   * @param {number} num - Number to format
   * @returns {string} Formatted number string
   */
  const formatNumber = (num) => {
    if (num === 0) return '0';
    
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  return (
    <div className="media-modal-overlay">
      <div className="media-modal">
        <button className="close-button-media-modal" onClick={onClose}>×</button>
        
        <div className="media-modal-inner">
          <div className="media-content-container">
            <div className="snapped-media-player">
              {selectedFile?.file_type?.includes('video') ? (
                <video 
                  ref={videoRef}
                  controls 
                  src={mediaUrl}
                  autoPlay
                  onError={(e) => {
                    console.error('Video loading error:', e);
                    toast.error('Failed to load video');
                  }}
                  onLoadStart={() => console.log('Video loading started')}
                  onLoadedData={() => console.log('Video data loaded')}
                >
                  Your browser does not support the video tag.
                </video>
              ) : selectedFile?.file_type?.includes('image') ? (
                <img 
                  src={mediaUrl}
                  alt={selectedFile.file_name}
                  onError={(e) => {
                    console.error('Image loading error:', e);
                    toast.error('Failed to load image');
                  }}
                />
              ) : (
                <div className="media-error">
                  Unsupported media type: {selectedFile?.file_type}
                </div>
              )}
            </div>
            
            {videoSummary && (
              <div className="video-summary-section">
                <div className="summary-header">
                  <h3>AI Video Summary</h3>
                  <span className="beta-tag">(beta)</span>
                </div>
                <div className="video-summary-content">
                  <div className="summary-blocks">
                    <div className="summary-block left-block">
                      <div className="summary-theme">
                        <strong>Theme:</strong> {
                          videoSummary.split('\n')[0]
                            .replace(/Theme \(≤4 words\):/g, '')
                            .trim()
                        }
                      </div>
                      <div className="summary-overview">
                        <strong>Overview:</strong> {
                          videoSummary
                            .split('\n')
                            .find(line => line.includes('Overview'))
                            ?.replace(/Overview \(≤10 words\):/g, '')
                            .replace(/Overview:/g, '')
                            .trim()
                        }
                      </div>
                      <div className="summary-bullet-points">
                        <strong>Actions:</strong>
                        <ul>
                          {(() => {
                            // Find the start and end of Actions section
                            const lines = videoSummary.split('\n');
                            const startIndex = lines.findIndex(line => line.includes('Actions:'));
                            const endIndex = lines.findIndex(line => line.includes('Similar Ideas:'));
                            
                            if (startIndex === -1) return [];
                            
                            // Get all lines between Actions and Similar Ideas
                            return lines
                              .slice(startIndex + 1, endIndex)
                              .filter(line => line.trim().startsWith('-'))
                              .map((action, i) => (
                                <li key={i}>{action.substring(2).trim()}</li>
                              ));
                          })()}
                        </ul>
                      </div>
                    </div>
                    <div className="summary-block right-block">
                      <div className="summary-improvements">
                        <strong>Improvements:</strong>
                        <ul>
                          {videoSummary
                            .split('\n')
                            .filter(line => line.includes('Improvement Suggestion'))
                            .map((line, i) => (
                              <li key={i}>{line.split(':')[1]?.trim()}</li>
                            ))}
                        </ul>
                      </div>
                      <div className="summary-similar">
                        <strong>Similar Ideas:</strong>
                        <ul>
                          {(() => {
                            // Find the start and end of Similar Ideas section
                            const lines = videoSummary.split('\n');
                            const startIndex = lines.findIndex(line => line.includes('Similar Ideas:'));
                            const endIndex = lines.findIndex(line => line.includes('Environment:'));
                            
                            if (startIndex === -1) return [];
                            
                            // Get all lines between Similar Ideas and Environment
                            return lines
                              .slice(startIndex + 1, endIndex)
                              .filter(line => line.trim().startsWith('-'))
                              .map((idea, i) => (
                                <li key={i}>{idea.substring(2).trim()}</li>
                              ));
                          })()}
                        </ul>
                      </div>
                      <div className="summary-environment">
                        <strong>Environment:</strong> {
                          videoSummary
                            .split('\n')
                            .find(line => line.includes('Environment'))
                            ?.split(':')[1]?.trim()
                        }
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
          
          <div className="file-list">
            <div className="session-note-header-mediamodeldata">
              <div className="post-stats-header-mediamodeldata">
                <div className="date-display-media-modal-mediamodeldata">
                  {(() => {
                    const displayDate = new Date(media.date);
                    displayDate.setDate(displayDate.getDate() + 1); // Add 1 day
                    return displayDate.toLocaleDateString('en-US', {
                      month: 'long',
                      day: 'numeric',
                      year: 'numeric'
                    });
                  })()}
                </div>
                <div className="stats-grid-mediamodeldata">
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">Posts</span>
                    <span className="stat-value-mediamodeldata">{media.stats?.postCount || 0}</span>
                  </div>
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">Views</span>
                    <span className="stat-value-mediamodeldata">{formatNumber(analyticsData.story_views)}</span>
                  </div>
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">Impressions</span>
                    <span className="stat-value-mediamodeldata">{formatNumber(analyticsData.impressions)}</span>
                  </div>
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">Followers</span>
                    <span className="stat-value-mediamodeldata">{analyticsData.follower_change > 0 ? '+' : ''}{formatNumber(analyticsData.follower_change)}</span>
                  </div>
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">Reach</span>
                    <span className="stat-value-mediamodeldata">{formatNumber(analyticsData.reach)}</span>
                  </div>
                  <div className="stat-item-mediamodeldata">
                    <span className="stat-label-mediamodeldata">View Time</span>
                    <span className="stat-value-mediamodeldata">
                      {formatNumber(Math.floor(analyticsData.story_view_time / 60))}m
                    </span>
                  </div>
                </div>
              </div>
              <div className="media-modal-buttons-mediamodeldata">
                <div className="button-row">
                  <button 
                    className="session-note-button"
                    onClick={() => setShowSessionNoteModal(true)}
                  >
                    Add Creator Note
                  </button>
                  <button
                    className="view-notes-button"
                    onClick={() => {
                      fetchNotes();
                      setShowNotesListModal(true);
                    }}
                  >
                    View Notes
                  </button>
                </div>
              </div>
            </div>
            
            <div className="file-items-container">
              {sortedFiles.map((file, index) => (
                <div
                  key={file.file_name}
                  className={`file-item2 ${selectedFile?.file_name === file.file_name ? 'selected' : ''}`}
                  onClick={() => handleFileSelect(file)}
                >
                  <span className="file-emoji">
                    {file.file_type?.includes('video') ? '📹' : '📸'}
                  </span>
                  <span className="file-name2">{file.file_name}</span>
                  <button 
                    className="note-emoji"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleNoteClick(e, file);
                    }}
                  >
                    📝
                  </button>
                </div>
              ))}
            </div>
          </div>

          {showSessionNoteModal && (
            <div className="note-modal" onClick={() => setShowSessionNoteModal(false)}>
              <div className="note-modal-content" onClick={e => e.stopPropagation()}>
                <h3>Add Add Note</h3>
                <textarea
                  value={sessionNoteText}
                  onChange={(e) => setSessionNoteText(e.target.value)}
                  placeholder="Enter session note here..."
                  rows={4}
                />
                <div className="note-modal-buttons">
                  <button onClick={() => setShowSessionNoteModal(false)}>Cancel</button>
                  <button 
                    onClick={handleSessionNoteSend}
                    disabled={!sessionNoteText.trim()}
                  >
                    Save
                  </button>
                </div>
              </div>
            </div>
          )}

          {showNotesListModal && renderNotesListModal()}
          {showNoteModal && renderNoteModalContent()}
        </div>
      </div>
    </div>
  );
};

export default MediaModal; 