/**
 * @fileoverview MediaModal component for displaying and managing media content.
 * Provides functionality for viewing, editing, approving, and managing media files.
 */

import React, { useState, useEffect, useRef } from 'react';
import './MediaModal.css';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { toast } from 'react-hot-toast';
import { fetchAuthSession, getCurrentUser } from '@aws-amplify/auth';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faExclamationTriangle, faTrash } from '@fortawesome/free-solid-svg-icons';
import { useAuth } from '../../contexts/AuthContext';

/**
 * @typedef {Object} MediaFile
 * @property {string} id - Unique identifier for the file
 * @property {string} type - Type of media (video, image, etc.)
 * @property {string} url - URL to access the file
 * @property {string} name - File name
 * @property {string} status - Current status of the file
 * @property {Object} metadata - Additional metadata about the file
 */

/**
 * @typedef {Object} MediaModalProps
 * @property {Object} media - Media data to display
 * @property {Function} onClose - Callback function to close the modal
 * @property {Function} onContentDeleted - Callback function when content is deleted
 */

/**
 * MediaModal component for displaying and managing media content.
 * 
 * @component
 * @param {MediaModalProps} props - Component props
 * @returns {React.ReactElement} The rendered MediaModal component
 */
const MediaModal = ({ media, onClose, onContentDeleted }) => {
  const { getAccessToken } = useAuth();
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
  const [showApproveModal, setShowApproveModal] = useState(false);
  const [showEditorNoteModal, setShowEditorNoteModal] = useState(false);
  const [editorNoteText, setEditorNoteText] = useState('');
  const [isPinned, setIsPinned] = useState(false);
  const [showFlagModal, setShowFlagModal] = useState(false);
  const [selectedFlag, setSelectedFlag] = useState(null);
  const [currentFlagIndex, setCurrentFlagIndex] = useState(0);
  const [allFlags, setAllFlags] = useState([]);
  const [flagStatus, setFlagStatus] = useState(null);
  const [videoSummary, setVideoSummary] = useState(null);
  const [currentUser, setCurrentUser] = useState(null);
  const [showDeleteConfirmModal, setShowDeleteConfirmModal] = useState(false);

  console.log('MediaModal rendered with:', {
    media,
    files: media?.files,
    fileCount: media?.files?.length
  });

  // Sort files by sequence number and set initial selected file
  useEffect(() => {
    if (media?.files?.length > 0) {
      const sortedFiles = [...media.files].sort((a, b) => 
        (a.seq_number || 0) - (b.seq_number || 0)
      );
      setSelectedFile(sortedFiles[0]);
      setCurrentIndex(0);
    }
  }, [media]);

  // Update media URL when selected file changes
  useEffect(() => {
    const getAuthenticatedUrl = async () => {
      if (!selectedFile) return;
      
      try {
        const { tokens } = await fetchAuthSession();
        const token = tokens.idToken.toString();
        const url = selectedFile.CDN_link;
        const separator = url.includes('?') ? '&' : '?';
        const authenticatedUrl = `${url}${separator}token=${token}`;
        
        console.log('Selected File:', {
          fileName: selectedFile.file_name,
          originalCDNLink: selectedFile.CDN_link,
          authenticatedUrl: authenticatedUrl
        });
        
        setMediaUrl(authenticatedUrl);
      } catch (error) {
        console.error('Error getting authenticated URL:', error);
      }
    };

    getAuthenticatedUrl();
  }, [selectedFile]);

  // Add keyboard navigation
  useEffect(() => {
    const handleKeyPress = (e) => {
      if (!media?.files?.length) return;
      
      const sortedFiles = [...media.files].sort((a, b) => 
        (a.seq_number || 0) - (b.seq_number || 0)
      );

      if (e.key === 'ArrowUp' || e.key === 'ArrowLeft') {
        // Go to previous file
        const newIndex = Math.max(0, currentIndex - 1);
        setCurrentIndex(newIndex);
        setSelectedFile(sortedFiles[newIndex]);
      } else if (e.key === 'ArrowDown' || e.key === 'ArrowRight') {
        // Go to next file
        const newIndex = Math.min(sortedFiles.length - 1, currentIndex + 1);
        setCurrentIndex(newIndex);
        setSelectedFile(sortedFiles[newIndex]);
      }
    };

    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [media, currentIndex]);

  // Add this useEffect to initialize editor note text from media data
  useEffect(() => {
    if (media?.sessions?.[0]?.editor_note) {
      setEditorNoteText(media.sessions[0].editor_note);
    }
  }, [media]);

  // Add this new effect to fetch video summary when selected file changes
  useEffect(() => {
    if (!selectedFile) return;
    
    // Only display existing video summaries
    if (selectedFile.video_summary) {
      console.log('Found video summary in file:', selectedFile.video_summary);
      setVideoSummary(selectedFile.video_summary);
    } else {
      console.log('No video summary available');
      setVideoSummary(null);
    }
  }, [selectedFile]);

  // Add this useEffect to get current user info
  useEffect(() => {
    const fetchCurrentUser = async () => {
      try {
        const userInfo = await getCurrentUser();
        // Fetch session with tokens to get custom attributes
        const { tokens } = await fetchAuthSession();
        // Get the payload from ID token which contains the custom attributes
        const payload = tokens.idToken.toString().split('.')[1];
        // Decode the base64 payload
        const decodedPayload = JSON.parse(atob(payload));
        
        // Store user info with custom attributes
        setCurrentUser({
          ...userInfo,
          customAttributes: decodedPayload
        });
        
        console.log('Current user:', userInfo);
        console.log('Token payload:', decodedPayload);
      } catch (error) {
        console.error('Error fetching current user:', error);
      }
    };
    
    fetchCurrentUser();
  }, []);

  if (!media?.files?.length) {
    return null;
  }

  const sortedFiles = [...media.files].sort((a, b) => 
    (a.seq_number || 0) - (b.seq_number || 0)
  );

  /**
   * Handles file selection in the modal.
   * 
   * @function handleFileSelect
   * @param {MediaFile} file - Selected media file
   */
  const handleFileSelect = (file) => {
    if (videoRef.current) {
      videoRef.current.pause();
    }
    const newIndex = sortedFiles.findIndex(f => f.file_name === file.file_name);
    setSelectedFile(file);
    setCurrentIndex(newIndex);
    setVideoSummary(null);
  };

  /**
   * Gets the session ID from CDN link or media data.
   * 
   * @function getSessionId
   * @returns {string} Session ID
   */
  const getSessionId = () => {
    try {
      // Get the first file's CDN link
      const firstFile = media.files[0];
      if (!firstFile?.CDN_link) {
        return null;
      }

      // Extract the date folder from the CDN link
      // Example: https://c.snapped.cc/public/th10021994/STO/062225/file.mp4
      const match = firstFile.CDN_link.match(/\/STO\/(\d{6})\//);
      if (!match) {
        return null;
      }

      // Return the date folder (MMDDYY)
      return match[1];
    } catch (err) {
      console.error('Error getting session ID:', err);
      return null;
    }
  };

  /**
   * Gets the S3 folder path from CDN link.
   * 
   * @function getS3FolderPath
   * @returns {string} S3 folder path
   */
  const getS3FolderPath = () => {
    if (selectedFile?.CDN_link) {
      const parts = selectedFile.CDN_link.split('/');
      const dateFolder = parts[6]; // This should be in MMDDYY format
      if (dateFolder) {
        return dateFolder;
      }
    }
    return null;
  };

  /**
   * Gets the MongoDB session folder path.
   * 
   * @function getMongoFolderPath
   * @returns {string} MongoDB folder path
   */
  const getMongoFolderPath = () => {
    if (media?.client_ID && media?.date) {
      const [year, month, day] = media.date.split('-');
      const paddedMonth = month.padStart(2, '0');
      const paddedDay = day.padStart(2, '0');
      return `${media.client_ID}/STORIES/F(${paddedMonth}-${paddedDay}-${year})_${media.client_ID}`;
    }
    return null;
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
      const folderId = getSessionId();
      
      const noteData = {
        client_ID: media.client_ID,
        session_folder: folderId,
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
      const folderId = getSessionId();
      
      // Include CDN links when mapping files
      const sessionFiles = media.files.map(file => ({
        file_name: file.file_name,
        cdn_url: file.CDN_link
      }));
      
      const noteData = {
        client_ID: media.client_ID,
        session_folder: folderId,
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
      const folderId = getSessionId();
      
      // Log the delete attempt
      console.log('Attempting to delete note:', {
        folderId,
        note
      });

      const response = await axios.delete(
        `${API_ENDPOINTS.UPLOAD.CONTENT_NOTES}/delete-note/${media.client_ID}/${folderId}`,
        {
          data: {
            created_at: note.created_at,
            note: note.note
          }
        }
      );
      
      if (response.data.status === 'success') {
        fetchNotes();  // Refresh notes list
        toast.success('Note deleted successfully');
      }
    } catch (err) {
      console.error('Error deleting note:', err);
      toast.error('Failed to delete note');
    }
  };

  /**
   * Handles approving media content.
   * 
   * @async
   * @function handleApprove
   * @returns {Promise<void>}
   */
  const handleApprove = async () => {
    try {
      // Get the correct session folder ID that matches the database format
      const sessionFolder = getSessionId();
      
      // Get the custom:UserID attribute for approver
      let approverID = "unknown";
      try {
        if (currentUser?.customAttributes) {
          approverID = currentUser.customAttributes['custom:UserID'];
          console.log('Found custom:UserID:', approverID);
        }
        
        if (!approverID || approverID === "unknown") {
          const { tokens } = await fetchAuthSession();
          const payload = tokens.idToken.toString().split('.')[1];
          const decodedPayload = JSON.parse(atob(payload));
          approverID = decodedPayload['custom:UserID'];
          console.log('Fetched custom:UserID directly:', approverID);
        }
        
        if (!approverID || approverID === "unknown") {
          approverID = media.client_ID;  // fallback
        }
      } catch (error) {
        console.error('Error getting custom:UserID:', error);
        approverID = media.client_ID;  // fallback on error
      }
      
      console.log('Session folder for approval:', sessionFolder);
      
      // Format data to match what the backend expects and what's in MongoDB
      const approvalData = {
        client_ID: media.client_ID,
        session_folder: sessionFolder,  // This should match session_id in MongoDB
        approved_by: approverID,
        files: media.files.map(file => ({
          file_name: file.file_name,
          cdn_url: file.CDN_link
        }))
      };

      console.log('Sending approval data:', approvalData);

      // Send request to API
      const response = await axios.post(
        API_ENDPOINTS.UPLOAD.APPROVE,
        approvalData
      );

      // Handle success
      if (response.status === 200) {
        if (response.data.matched_count > 0) {
          toast.success('Content approved successfully');
        } else {
          toast.error('Approval processed but no content was updated');
          console.warn('Approval response:', response.data);
        }
        setShowApproveModal(false);
      }
    } catch (err) {
      console.error('Error approving content:', err);
      toast.error(err.response?.data?.detail || 'Failed to approve content');
    }
  };

  /**
   * Handles saving editor notes.
   * 
   * @async
   * @function handleEditorNoteSave
   * @returns {Promise<void>}
   */
  const handleEditorNoteSave = async () => {
    try {
      const sessionFolder = getSessionId();
      
      const noteData = {
        client_ID: media.client_ID,
        folder_id: sessionFolder,
        note: editorNoteText,
        file_name: selectedFile.file_name,
        cdn_url: selectedFile.CDN_link,
        created_at: new Date().toISOString(),
        pinned: isPinned
      };

      const response = await axios.post(API_ENDPOINTS.CDN.ADD_EDITOR_NOTE, noteData);

      if (response.data.status === 'success') {
        toast.success('Editor note saved successfully');
        setShowEditorNoteModal(false);
        setEditorNoteText('');
        setIsPinned(false);
      }
    } catch (error) {
      console.error('Error saving editor note:', error);
      toast.error('Failed to save editor note');
    }
  };

  /**
   * Handles clicking on flag icons.
   * 
   * @function handleFlagClick
   * @param {Event} e - Click event
   * @param {MediaFile} file - Media file being flagged
   */
  const handleFlagClick = (e, file) => {
    e.stopPropagation();
    setAllFlags(file.content_matches);
    setCurrentFlagIndex(0);
    setSelectedFlag(file.content_matches[0]);
    setFlagStatus(file.content_matches_status);
    setShowFlagModal(true);
  };

  /**
   * Handles navigation between flagged items.
   * 
   * @function handleFlagNavigation
   * @param {string} direction - Navigation direction ('prev' or 'next')
   */
  const handleFlagNavigation = (direction) => {
    const newIndex = direction === 'next' 
      ? (currentFlagIndex + 1) % allFlags.length 
      : (currentFlagIndex - 1 + allFlags.length) % allFlags.length;
    setCurrentFlagIndex(newIndex);
    setSelectedFlag(allFlags[newIndex]);
  };

  /**
   * Handles flag action submission.
   * 
   * @async
   * @function handleFlagAction
   * @returns {Promise<void>}
   */
  const handleFlagAction = async () => {
    try {
      const action = flagStatus === "removed" ? "reinstate" : "remove";
      const response = await axios.put(
        `${API_ENDPOINTS.UPLOAD.UPDATE_FLAG_STATUS(
          media.client_ID,
          selectedFile.folder_id,
          selectedFile.file_name
        )}?action=${action}`
      );
      
      if (response.data.status === "success") {
        setFlagStatus(action === "remove" ? "removed" : null);
        toast.success(response.data.message);
      }
    } catch (error) {
      toast.error(`Failed to ${flagStatus === "removed" ? "reinstate" : "remove"} flag`);
      console.error('Error removing flag:', error);
    }
  };

  /**
   * Handles clicking on note icons.
   * 
   * @function handleNoteClick
   * @param {Event} e - Click event
   * @param {MediaFile} file - Media file associated with the note
   */
  const handleNoteClick = (e, file) => {
    e.stopPropagation();
    setSelectedFile(file);
    setShowNoteModal(true);
  };

  /**
   * Handles deleting media content.
   * 
   * @async
   * @function handleDelete
   * @returns {Promise<void>}
   */
  const handleDelete = async () => {
    try {
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      // We already have the folder_id from when we loaded the data
      const deleteUrl = API_ENDPOINTS.UPLOAD.DELETE_CONTENT(media.client_ID, media.folder_id);
      
      const response = await axios.delete(
        deleteUrl,
        {
          headers: {
            Authorization: `Bearer ${token}`
          }
        }
      );G

      console.log('Delete response:', response.data);
      toast.success('Content deleted successfully');
      onClose();
      if (onContentDeleted) {
        onContentDeleted();
      }

    } catch (err) {
      console.error('Error deleting content:', err);
      toast.error('Failed to delete content');
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
      <div className="creator-note-modal-content" onClick={e => e.stopPropagation()}>
        <h3 className="creator-note-title">Creator Notes</h3>
        <textarea
          value={noteText}
          onChange={(e) => setNoteText(e.target.value)}
          placeholder="Enter your note here..."
          rows={4}
          className="creator-note-textarea"
        />
        <div className="creator-note-buttons">
          <button 
            className="creator-note-button cancel" 
            onClick={() => setShowNoteModal(false)}
          >
            Cancel
          </button>
          <button 
            className="creator-note-button save"
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
   * Fetches notes for media content.
   * 
   * @async
   * @function fetchNotes
   * @returns {Promise<void>}
   */
  const fetchNotes = async () => {
    try {
      const folderId = getSessionId();
      
      const response = await axios.get(
        `${API_ENDPOINTS.UPLOAD.CONTENT_NOTES}/get/${media.client_ID}/${folderId}`
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
              className="delete-button-media-note"
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
   * Renders approve modal content.
   * 
   * @function renderApproveModal
   * @returns {React.ReactElement} Rendered approve modal content
   */
  const renderApproveModal = () => (
    <div className="note-modal" onClick={() => setShowApproveModal(false)}>
      <div className="note-modal-content" onClick={e => e.stopPropagation()}>
        <h3>Approve Content</h3>
        <p className="approval-confirmation-text">
          Are you sure you want to approve all content for this session?
        </p>
        <div className="note-modal-buttons">
          <button onClick={() => setShowApproveModal(false)}>Cancel</button>
          <button 
            onClick={handleApprove}
            className="approve-button"
          >
            Approve
          </button>
        </div>
      </div>
    </div>
  );

  /**
   * Renders editor note modal content.
   * 
   * @function renderEditorNoteModal
   * @returns {React.ReactElement} Rendered editor note modal content
   */
  const renderEditorNoteModal = () => (
    <div className="note-modal" onClick={() => setShowEditorNoteModal(false)}>
      <div className="note-modal-content" onClick={e => e.stopPropagation()}>
        <h3>Editor Note</h3>
        <textarea
          value={editorNoteText}
          onChange={(e) => setEditorNoteText(e.target.value)}
          placeholder="Enter editor note here..."
          rows={4}
        />
        <div className="pin-note-container">
          <label className="pin-note-label">
            <input
              type="checkbox"
              checked={isPinned}
              onChange={(e) => setIsPinned(e.target.checked)}
            />
            Pin Note
          </label>
        </div>
        <div className="note-modal-buttons">
          <button onClick={() => setShowEditorNoteModal(false)}>Cancel</button>
          <button 
            onClick={handleEditorNoteSave}
            disabled={!editorNoteText.trim()}
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );

  /**
   * Renders flag modal content.
   * 
   * @function renderFlagModal
   * @returns {React.ReactElement} Rendered flag modal content
   */
  const renderFlagModal = () => (
    <div className="flag-modal" onClick={() => setShowFlagModal(false)}>
      <div className="flag-modal-content" onClick={e => e.stopPropagation()}>
        <h3>Content Flags</h3>
        {allFlags.length > 1 && (
          <div className="flag-navigation">
            <button 
              className="flag-nav-btn prev"
              onClick={(e) => {
                e.stopPropagation();
                handleFlagNavigation('prev');
              }}
            >
              ←
            </button>
            <span className="flag-count">
              {currentFlagIndex + 1} of {allFlags.length}
            </span>
            <button 
              className="flag-nav-btn next"
              onClick={(e) => {
                e.stopPropagation();
                handleFlagNavigation('next');
              }}
            >
              →
            </button>
          </div>
        )}
        <div className="flag-details">
          <div className="flag-detail-row">
            <span className="detail-label">Type:</span>
            <span className="detail-value">{selectedFlag?.type || selectedFlag?.search_query}</span>
          </div>
          <div className="flag-detail-row">
            <span className="detail-label">Confidence:</span>
            <span className="detail-value">{selectedFlag?.confidence}</span>
          </div>
          <div className="flag-detail-row">
            <span className="detail-label">Severity:</span>
            <span className="detail-value">{selectedFlag?.severity}</span>
          </div>
          <div className="flag-detail-row">
            <span className="detail-label">Score:</span>
            <span className="detail-value">{selectedFlag?.score}</span>
          </div>
        </div>
        <div className="flag-modal-buttons">
          <button 
            className={flagStatus === "removed" ? "reinstate-flag-btn" : "remove-flag-btn"}
            onClick={handleFlagAction}
          >
            {flagStatus === "removed" ? "Reinstate Flag" : "Remove Flag"}
          </button>
        </div>
      </div>
    </div>
  );

  /**
   * Renders delete confirmation modal.
   * 
   * @function renderDeleteConfirmModal
   * @returns {React.ReactElement} Rendered delete confirmation modal
   */
  const renderDeleteConfirmModal = () => (
    <div className="note-modal" onClick={() => setShowDeleteConfirmModal(false)}>
      <div className="note-modal-content" onClick={e => e.stopPropagation()}>
        <h3>Delete Content</h3>
        <p className="delete-confirmation-text">
          Are you sure you want to delete all content for this session? This action cannot be undone.
        </p>
        <div className="note-modal-buttons">
          <button onClick={() => setShowDeleteConfirmModal(false)}>Cancel</button>
          <button 
            onClick={handleDelete}
            className="delete-button"
          >
            Delete
          </button>
        </div>
      </div>
    </div>
  );

  /**
   * Renders video summary content.
   * 
   * @function renderVideoSummary
   * @returns {React.ReactElement} Rendered video summary content
   */
  const renderVideoSummary = () => {
    if (!videoSummary) return null;

    // Parse the video summary text to get different sections
    const sections = videoSummary.split('\n').reduce((acc, line) => {
      if (line.includes('Similar Ideas:')) {
        acc.similarIdeas = [];
      } else if (line.startsWith('-') && acc.similarIdeas) {
        acc.similarIdeas.push(line.substring(2).trim());
      }
      return acc;
    }, { similarIdeas: [] });

    return (
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
                    const lines = videoSummary.split('\n');
                    // Find where Actions section starts (look for "Actions" without requiring the colon)
                    const actionsStart = lines.findIndex(line => line.includes('Actions'));
                    if (actionsStart === -1) return [];

                    // Get all bullet points until we hit the next section
                    const actionLines = [];
                    for (let i = actionsStart + 1; i < lines.length; i++) {
                      const line = lines[i].trim();
                      // Stop when we hit the next section (Overview, Similar Ideas, or Improvements)
                      if (line.includes('Overview:') || line.includes('Similar Ideas:') || line.includes('Improvements:')) break;
                      // Add bullet points
                      if (line.startsWith('-')) {
                        actionLines.push(line);
                      }
                    }

                    return actionLines.map((action, i) => (
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
    );
  };

  return (
    <div className="media-modal-overlay">
      <div className="media-modal">
        <button className="media-close-button" onClick={onClose}>×</button>
        
        <div className="media-modal-inner">
          <div className="media-content-container">
            <div className="snapped-media-player">
              {selectedFile?.file_type?.includes('video') ? (
                <video 
                  ref={videoRef}
                  controls 
                  src={mediaUrl}
                  autoPlay
                >
                  Your browser does not support the video tag.
                </video>
              ) : selectedFile?.file_type?.includes('image') ? (
                <img 
                  src={mediaUrl}
                  alt={selectedFile.file_name}
                />
              ) : null}
            </div>
            
            {renderVideoSummary()}
          </div>
          
          <div className="file-list">
            <div className="session-note-header">
              <div className="date-display-media-modal">
                {(() => {
                  const [year, month, day] = media.date.split('-');
                  const displayDate = new Date(year, month - 1, day);
                  return displayDate.toLocaleDateString('en-US', {
                    month: 'long',
                    day: 'numeric',
                    year: 'numeric'
                  });
                })()}
              </div>
              <div className="media-modal-buttons">
                <div className="button-row">
                  <button 
                    className="session-note-button"
                    onClick={() => setShowSessionNoteModal(true)}
                  >
                    Creator Note
                  </button>
                  <button
                    className="view-notes-button"
                    onClick={() => {
                      fetchNotes();
                      setShowNotesListModal(true);
                    }}
                  >
                    View Creator Notes
                  </button>
                </div>
                <div className="button-row">
                  <button
                    className="approve-button"
                    onClick={() => setShowApproveModal(true)}
                  >
                    Approve Content
                  </button>
                  <button 
                    className="editor-note-button"
                    onClick={() => setShowEditorNoteModal(true)}
                  >
                    Editor Note
                  </button>
                  <button 
                    className="delete-content-button-standalone" 
                    onClick={() => setShowDeleteConfirmModal(true)}
                    title="Delete content"
                  >
                    <FontAwesomeIcon icon={faTrash} />
                  </button>
                </div>
              </div>
            </div>
            <div className="file-items-container">
              {sortedFiles.map((file, index) => (
                <div
                  key={file.file_name}
                  className={`file-item2 
                    ${selectedFile?.file_name === file.file_name ? 'selected' : ''} 
                    ${file.content_matches ? 'flagged' : ''}`}
                  onClick={() => handleFileSelect(file)}
                >
                  <span className="file-emoji">
                    {file.file_type?.includes('video') ? '📹' : '📸'}
                  </span>
                  <span className="file-name2">{file.file_name}</span>
                  {file.content_matches && (
                    <span 
                      className="flag-badge"
                      onClick={(e) => handleFlagClick(e, file)}
                    >
                      <FontAwesomeIcon icon={faExclamationTriangle} className="fa-icon warning" />
                    </span>
                  )}
                  <div className="file-actions">
                    <button 
                      className="note-emoji"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleNoteClick(e, file);
                      }}
                      title="Add Note"
                    >
                      📝
                    </button>
                    <button 
                      className="editor-note-emoji"
                      onClick={(e) => {
                        e.stopPropagation();
                        setSelectedFile(file);
                        setShowEditorNoteModal(true);
                      }}
                      title="Add Editor Note"
                    >
                      ✏️
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {showSessionNoteModal && (
            <div className="note-modal" onClick={() => setShowSessionNoteModal(false)}>
              <div className="note-modal-content" onClick={e => e.stopPropagation()}>
                <h3>Add Note</h3>
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
          {showApproveModal && renderApproveModal()}
          {showEditorNoteModal && renderEditorNoteModal()}
          {showFlagModal && renderFlagModal()}
        </div>

        {showDeleteConfirmModal && renderDeleteConfirmModal()}
      </div>
    </div>
  );
};

export default MediaModal; 