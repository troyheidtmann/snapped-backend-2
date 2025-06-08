import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { useAuth } from '../../contexts/AuthContext';
import { toast } from 'react-toastify';
import './styles/EditorNotes.css';

// Set refresh interval to 5 minutes (300000 ms)
const REFRESH_INTERVAL = 300000;

const EditorNotesSection = ({ clientId, onNoteClick, showPinnedOnly = false }) => {
  const { user } = useAuth();
  const [editorNotes, setEditorNotes] = useState([]);
  const [loading, setLoading] = useState(false);
  const lastFetchRef = useRef(0);
  const timeoutRef = useRef(null);

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric'
    });
  };

  const fetchEditorNotes = async (force = false) => {
    // Don't fetch if no clientId
    if (!clientId) return;

    // Check if enough time has passed since last fetch
    const now = Date.now();
    if (!force && now - lastFetchRef.current < REFRESH_INTERVAL) {
      return;
    }

    try {
      setLoading(true);
      const response = await axios.get(API_ENDPOINTS.CDN_MONGO.EDITOR_NOTES(clientId));
      
      if (response.data?.status === 'success' && Array.isArray(response.data.notes)) {
        console.log('Fetched notes:', response.data.notes);
        // Sort notes - pinned first, then by creation date
        const sortedNotes = response.data.notes.sort((a, b) => {
          if (a.pinned && !b.pinned) return -1;
          if (!a.pinned && b.pinned) return 1;
          return new Date(b.created_at) - new Date(a.created_at);
        });
        setEditorNotes(sortedNotes);
        lastFetchRef.current = now;
      }
    } catch (err) {
      console.error('Error fetching editor notes:', err);
      toast.error('Failed to load editor notes');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Initial fetch
    fetchEditorNotes(true);

    // Set up periodic refresh
    timeoutRef.current = setInterval(() => {
      fetchEditorNotes();
    }, REFRESH_INTERVAL);

    // Cleanup
    return () => {
      if (timeoutRef.current) {
        clearInterval(timeoutRef.current);
      }
    };
  }, [clientId]);

  const handleMediaClick = (note) => {
    console.log('Handling media click for note:', note);

    if (!note || !note.cdn_url) {
      console.error('Note or CDN URL is missing:', note);
      toast.error('Media URL not available');
      return;
    }

    try {
      // Structure the media object exactly as FileMover expects
      const mediaObject = {
        CDN_link: note.cdn_url,
        file_name: note.file_name,
        file_type: note.file_name?.toLowerCase().endsWith('.mp4') ? 'video' : 'image',
        caption: note.note || '',
        session_id: note.folder_id || '',
        is_thumbnail: false,
        seq_number: 0,
        upload_time: note.created_at || '',
        video_length: 0,
        is_indexed: false
      };

      console.log('Sending media object to viewer:', mediaObject);
      onNoteClick && onNoteClick(mediaObject);
    } catch (err) {
      console.error('Error handling media click:', err);
      toast.error('Failed to play media');
    }
  };

  const renderNote = (note) => (
    <div 
      key={`${note.folder_id}-${note.created_at}`}
      className={`cdn-note-item ${note.pinned ? 'pinned' : ''}`}
    >
      <span className="cdn-note-text">{note.note}</span>
    </div>
  );

  if (loading && editorNotes.length === 0) {
    return <div className="cdn-notes-loading">Loading editor notes...</div>;
  }

  if (!loading && editorNotes.length === 0) {
    return <div className="cdn-notes-empty">No editor notes found</div>;
  }

  const filteredNotes = showPinnedOnly 
    ? editorNotes.filter(note => note.pinned)
    : editorNotes;

  return (
    <div className="cdn-notes-container">
      <div className="cdn-notes-list">
        {filteredNotes.map(renderNote)}
      </div>
    </div>
  );
};

export default EditorNotesSection; 