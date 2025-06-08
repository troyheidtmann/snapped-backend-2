/**
 * @fileoverview Sidebar component for CDN navigation.
 * Provides folder tree navigation and quick access features.
 */

import React, { memo, useState, useMemo, useEffect } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faFolder, faArrowLeft, faGrip, faSave, faFolderPlus, faChevronLeft, faImage, faSync, faFolderOpen, faChevronRight, faChevronDown } from '@fortawesome/free-solid-svg-icons';
import MediaPlayer from './MediaPlayer-cdn';
import MultiSelectButton from './MultiSelectButton';
import RenumberButton from './RenumberButton';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import EditorNotesSection from './EditorNotesSection';
import { useAuth } from '../../contexts/AuthContext';
import { pathUtils } from './utils/pathUtils';
import { formatDate } from './utils/dateUtils';
import './styles/Sidebar.css';

const CONTENT_TYPE_LABELS = {
  'STORIES': 'Stories',
  'SPOTLIGHT': 'Spotlight',
  'SAVED': 'Saved',
  'CONTENT_DUMP': 'Content Dump'
};

/**
 * Type definition for a folder node in the tree.
 * @typedef {Object} FolderNode
 * @property {string} name - Name of the folder
 * @property {string} path - Full path to the folder
 * @property {boolean} isExpanded - Whether the folder is expanded
 * @property {Array<FolderNode>} children - Child folders
 */

const MiddleEllipsis = ({ text }) => {
  if (!text) return null;
  
  // Extract date from path if it exists
  const dateMatch = text.match(/F\(([\d-]+)\)/);
  if (dateMatch) {
    return <div className="middle-ellipsis">{dateMatch[1]}</div>;
  }
  
  // Fall back to original ellipsis behavior for non-date paths
  const parts = text.split('/');
  if (parts.length <= 2) return text;

  const start = parts.slice(0, 1).join('/');
  const end = parts.slice(-2).join('/');

  return (
    <div className="middle-ellipsis">
      <span className="start">{start}/</span>
      <span className="ellipsis">...</span>
      <span className="end">/{end}</span>
    </div>
  );
};

const getContentType = (path) => {
  if (path.includes('/SPOTLIGHT/')) {
    return 'SPOTLIGHT';
  } else if (path.includes('/SAVED/')) {
    return 'SAVED';
  }
  return 'STORY';
};

/**
 * Component for rendering a folder tree item.
 * Features:
 * - Expandable/collapsible folders
 * - Visual feedback
 * - Path-based navigation
 * 
 * @param {Object} props - Component properties
 * @param {FolderNode} props.folder - Folder node to render
 * @param {Function} props.onSelect - Selection handler
 * @param {Function} props.onToggle - Expansion toggle handler
 * @param {string} props.selectedPath - Currently selected path
 * @returns {React.ReactElement} The folder tree item interface
 */
const FolderTreeItem = ({ folder, onSelect, onToggle, selectedPath }) => {
  const isSelected = selectedPath === folder.path;
  const hasChildren = folder.children && folder.children.length > 0;

  return (
    <div className="folder-tree-item">
      <div 
        className={`folder-row ${isSelected ? 'selected' : ''}`}
        onClick={() => onSelect(folder.path)}
      >
        {hasChildren && (
          <FontAwesomeIcon
            icon={folder.isExpanded ? faChevronDown : faChevronRight}
            className="folder-chevron"
            onClick={(e) => {
              e.stopPropagation();
              onToggle(folder.path);
            }}
          />
        )}
        <FontAwesomeIcon
          icon={folder.isExpanded ? faFolderOpen : faFolder}
          className="folder-icon"
        />
        <span className="folder-name">{folder.name}</span>
      </div>
      {hasChildren && folder.isExpanded && (
        <div className="folder-children">
          {folder.children.map(child => (
            <FolderTreeItem
              key={child.path}
              folder={child}
              onSelect={onSelect}
              onToggle={onToggle}
              selectedPath={selectedPath}
            />
          ))}
        </div>
      )}
    </div>
  );
};

/**
 * Sidebar component for CDN navigation.
 * Features:
 * - Folder tree navigation
 * - Quick access shortcuts
 * - Path selection
 * - Tree state management
 * 
 * @param {Object} props - Component properties
 * @param {Array<FolderNode>} props.folders - Root level folders
 * @param {string} props.selectedPath - Currently selected path
 * @param {Function} props.onPathSelect - Path selection handler
 * @returns {React.ReactElement} The sidebar interface
 */
const Sidebar = memo(({ 
  path, 
  folders, 
  onNavigate, 
  selectedFile, 
  onFileSelect,
  isRight,
  isReorderMode,
  setIsReorderMode,
  onReorderSave,
  onCreateFolder,
  getMediaUrl,
  showMultiSelect,
  isMultiSelectMode,
  onToggleMultiSelect,
  selectedCount,
  onSetThumbnails,
  isThumbSelectMode,
  onSaveThumbnails,
  setIsThumbSelectMode,
  onContentsUpdate,
  onMoveSelected,
  caption,
  setCaption,
  onCaptionSubmit
}) => {
  const { user } = useAuth();
  const [searchTerm, setSearchTerm] = useState('');
  const [hasContent, setHasContent] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);
  const [expandedPaths, setExpandedPaths] = useState(new Set());

  // Check if current folder has content
  useEffect(() => {
    const currentFolder = folders.find(f => {
      // For root level (public/)
      if (path === 'public/' || path === 'public') {
        return f.name === 'public';
      }
      
      // For nested paths, compare the full path
      const folderPath = path.endsWith('/') ? path.slice(0, -1) : path;
      const expectedName = folderPath.split('/').pop();
      return f.type === 'folder' && f.name === expectedName;
    });
    
    // Check specifically for files, not just any contents
    const hasFiles = currentFolder?.contents?.some(item => item.type === 'file' || item.type?.includes('image') || item.type?.includes('video'));
    setHasContent(hasFiles || false);
  }, [folders, path]);

  // Check if we're in a valid path for creating folders
  const isValidFolderPath = useMemo(() => {
    // First check if we're in one of the main folders
    const hasMainFolder = path.includes('/STORIES') || path.includes('/SPOTLIGHT') || path.includes('/SAVED');
    if (!hasMainFolder) return false;
    
    // Now check if we're exactly one level deep
    const pathParts = path.split('/').filter(Boolean);
    
    // Find the index of our main folder
    const mainFolderIndex = pathParts.findIndex(part => 
      part === 'STORIES' || part === 'SPOTLIGHT' || part === 'SAVED'
    );
    
    // We need to be exactly at the main folder - not deeper
    return mainFolderIndex !== -1 && pathParts.length === mainFolderIndex + 1;
  }, [path]);

  // Format folder display name
  const getDisplayName = (folder) => {
    const { name, type, file_count = 0 } = folder;

    // For session folders - show date and file count
    if (name.startsWith('F(')) {
      const date = name.match(/F\((\d{6})\)/)?.[1];
      if (date) {
        return (
          <div>
            <div className="font-medium">{formatDate(date)}</div>
            <div className="text-xs text-gray-500">{file_count} files</div>
          </div>
        );
      }
    }

    return name;
  };

  // Filter and sort folders
  const filteredFolders = useMemo(() => {
    let filtered = folders;
    
    if (searchTerm) {
      filtered = folders.filter(item => {
        const displayName = typeof getDisplayName(item) === 'string' 
          ? getDisplayName(item) 
          : item.name;
        return displayName && displayName.toLowerCase().includes(searchTerm.toLowerCase());
      });
    }

    // Sort based on folder type
    return filtered.sort((a, b) => {
      // At root level, sort by client ID
      if (path === 'public/') {
        return a.name.localeCompare(b.name);
      }

      // Sort content types in specific order
      const contentTypeOrder = {
        'STORIES': 1,
        'SPOTLIGHT': 2,
        'SAVED': 3,
        'content_dump': 4
      };

      if (contentTypeOrder[a.name] && contentTypeOrder[b.name]) {
        return contentTypeOrder[a.name] - contentTypeOrder[b.name];
      }

      // Sort session folders by date if they have dates
      const dateA = a.name.match(/F\(([\d-]+)\)/);
      const dateB = b.name.match(/F\(([\d-]+)\)/);
      
      if (dateA && dateB) {
        // Convert dates to timestamps for comparison
        const [monthA, dayA, yearA] = dateA[1].split('-');
        const [monthB, dayB, yearB] = dateB[1].split('-');
        const dateObjA = new Date(yearA, monthA - 1, dayA);
        const dateObjB = new Date(yearB, monthB - 1, dayB);
        return dateObjB - dateObjA; // Newest first
      }
      
      // If one has a date and the other doesn't, prioritize the dated one
      if (dateA) return -1;
      if (dateB) return 1;
      
      // For everything else, maintain alphabetical order
      return a.name.localeCompare(b.name);
    });
  }, [folders, searchTerm, path]);

  const handleReorderMode = () => {
    if (isThumbSelectMode) {
      setIsThumbSelectMode(false);
    }
    setIsReorderMode(!isReorderMode);
  };

  const renderFiles = () => {
    // Don't show files in the tree view when in a session folder
    if (pathUtils.isSessionPath(path)) {
      return null;
    }

    // Get the current folder's contents
    const currentFolder = folders.find(f => {
      if (path === 'public/' || path === 'public') {
        return f.name === 'public';
      }
      
      const folderPath = path.endsWith('/') ? path.slice(0, -1) : path;
      const expectedName = folderPath.split('/').pop();
      return f.type === 'folder' && f.name === expectedName;
    });
    
    if (!currentFolder || !currentFolder.contents) {
      return null;
    }

    // Only show files in non-session folders
    return currentFolder.contents
      .filter(item => item.type === 'file')
      .map((file) => (
        <div 
          key={file.name}
          className={`file-item ${selectedFile?.name === file.name ? 'selected' : ''}`}
          onClick={() => handleFileSelect(file)}
        >
          <span className="file-emoji">
            {file.type?.includes('video') ? '📹' : '📸'}
          </span>
          <span className="file-name">{file.name}</span>
        </div>
      ));
  };

  const handleFileSelect = (file) => {
    if (!file) return;
    onFileSelect?.(file);
    // Load the existing caption if it exists
    setCaption(file.caption || '');
  };

  const handleSync = async () => {
    if (!path) return;
    
    try {
      setIsSyncing(true);
      console.log('Starting sync for path:', path);
      
      await axios.post(API_ENDPOINTS.CDN.SCAN_PATH, {
        path: path
      });
      
      console.log('Sync completed successfully');
      // Refresh the contents after sync
      onContentsUpdate?.();
      
      // Show success message (you might want to add a toast/notification system)
      alert('Sync completed successfully');
    } catch (error) {
      console.error('Sync failed:', error);
      // Show error message
      alert(`Sync failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setIsSyncing(false);
    }
  };

  // Load folders using new endpoint
  const loadFolders = async () => {
    const parts = path.split('/').filter(Boolean);
    const collectionMap = {
      'STORIES': 'Uploads',
      'SPOTLIGHT': 'Spotlights',
      'SAVED': 'Saved',
      'CONTENT_DUMP': 'Content_Dump'
    };

    try {
      // If we're at root, show collections
      if (!parts.length || parts[0] === 'public') {
        return Object.entries(collectionMap).map(([key, value]) => ({
          name: key,
          type: 'folder',
          path: `public/${key}/`
        }));
      }

      // Get client ID and collection type
      const clientId = parts.find(p => /^[a-z]{2}\d+$/i.test(p));
      const collectionType = Object.keys(collectionMap).find(key => path.includes(key));
      
      // If we don't have both client ID and collection type, we might be at a higher level
      if (!clientId || !collectionType) {
        // If we're in a client folder (public/clientId/), show content types
        if (parts.length === 2 && /^[a-z]{2}\d+$/i.test(parts[1])) {
          return Object.entries(collectionMap).map(([key, value]) => ({
            name: key,
            type: 'folder',
            path: `${path}${key}/`
          }));
        }
        return [];
      }

      const collection = collectionMap[collectionType];
      console.log('Loading folders for:', { clientId, collection, path });
      
      const response = await axios.get(
        API_ENDPOINTS.CDN.COLLECTION(collection),
        { 
          params: { 
            path: `${clientId}.sessions`
          } 
        }
      );

      // Convert response format to match existing code
      if (!response.data.arrays?.[0]) {
        console.log('No sessions found in response:', response.data);
        return [];
      }

      return response.data.arrays[0].map(session => ({
        name: session.session_id,
        type: 'folder',
        path: `${path}${session.session_id}/`,
        scan_date: session.scan_date,
        total_files: session.total_files_count
      }));

    } catch (error) {
      console.error('Error loading folders:', error.response || error);
      return [];
    }
  };

  useEffect(() => {
    const fetchFolders = async () => {
      try {
        const loadedFolders = await loadFolders();
        // Instead of setFolders (which doesn't exist), use onContentsUpdate
        if (Array.isArray(loadedFolders)) {
          onContentsUpdate?.(loadedFolders);
        } else {
          // If we got an invalid response, at least show root folders
          const rootFolders = Object.entries(CONTENT_TYPE_LABELS).map(([key]) => ({
            name: key,
            type: 'folder',
            path: `public/${key}/`
          }));
          onContentsUpdate?.(rootFolders);
        }
      } catch (error) {
        console.error('Error fetching folders:', error);
        // On error, reset to root folders to allow navigation
        const rootFolders = Object.entries(CONTENT_TYPE_LABELS).map(([key]) => ({
          name: key,
          type: 'folder',
          path: `public/${key}/`
        }));
        onContentsUpdate?.(rootFolders);
      }
    };
    fetchFolders();
  }, [path]);

  // Handle navigation
  const handleNavigate = (target) => {
    try {
      if (target === '..') {
        // Use pathUtils for consistent navigation
        const newPath = pathUtils.navigateUp(path);
        if (newPath) {
          onNavigate(newPath);
        } else {
          // If navigation fails, go to root
          onNavigate('public/');
        }
      } else {
        onNavigate(target);
      }
    } catch (error) {
      console.error('Navigation error:', error);
      // If navigation fails, go to root
      onNavigate('public/');
    }
  };

  /**
   * Toggles folder expansion state.
   * 
   * @param {string} path - Path to toggle
   */
  const handleToggle = (path) => {
    setExpandedPaths(prev => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  return (
    <div className="sidebar">
      <div className="navigation-bar">
        <button 
          className="back-button" 
          onClick={() => handleNavigate('..')} 
          disabled={path === 'public/'}
        >
          <FontAwesomeIcon icon={faChevronLeft} />
        </button>
        <MiddleEllipsis text={path} />
      </div>

      <div className="search-bar">
        <input 
          type="text" 
          placeholder="Search folders..." 
          className="search-input"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
        {showMultiSelect && (
          <MultiSelectButton 
            active={isMultiSelectMode}
            onClick={onToggleMultiSelect}
            selectedCount={selectedCount}
            onMoveSelected={onMoveSelected}
          />
        )}
        {isRight && (
          <>
            <button 
              className={`toolbar-button ${isThumbSelectMode ? 'active' : ''}`}
              onClick={onSetThumbnails}
              title={`Select Thumbnails`}
            >
              <FontAwesomeIcon icon={faImage} />
            </button>
          </>
        )}
      </div>

      <div className="tree-section">
        {filteredFolders.filter(item => item.type === 'folder').map((item) => (
          <FolderTreeItem
            key={item.name}
            folder={{
              ...item,
              isExpanded: expandedPaths.has(item.path)
            }}
            onSelect={handleNavigate}
            onToggle={handleToggle}
            selectedPath={path}
          />
        ))}
        {renderFiles()}
      </div>

      {isRight && selectedFile && (
        <div className="caption-section-vertical">
          <input
            type="text"
            className="caption-input-vertical"
            placeholder="Add caption..."
            value={caption}
            onChange={(e) => setCaption(e.target.value)}
          />
          <button 
            className="caption-send-button-vertical"
            onClick={async () => {
                try {
                    const sessionMatch = path.match(/F\([\d-]+\)_[^/]+/);
                    if (!sessionMatch) {
                        console.error('No session ID found in path');
                        return;
                    }

                    await axios.post(API_ENDPOINTS.CDN.UPDATE_CAPTION, {
                        session_id: sessionMatch[0],
                        file_name: selectedFile.name,
                        caption: caption.trim(),
                        path: path
                    });
                    setCaption('');
                    onContentsUpdate?.();
                } catch (error) {
                    console.error('Caption update failed:', error);
                }
            }}
          >
            Send
          </button>
        </div>
      )}

      {isRight && (
        <div className="media-toolbar">
          <button 
            className={`toolbar-button create-folder ${(!isValidFolderPath || hasContent) ? 'disabled' : ''}`}
            onClick={onCreateFolder}
            title={
              !isValidFolderPath ? "Only available in STORIES, SPOTLIGHT, or SAVED folders" : 
              hasContent ? "Cannot create folder when content exists" : 
              "Create Next Folder"
            }
            disabled={!isValidFolderPath || hasContent}
          >
            <FontAwesomeIcon icon={faFolderPlus} />
            <span>Folder</span>
          </button>
          <button 
            className={`toolbar-button ${isReorderMode ? 'active' : ''}`}
            onClick={handleReorderMode}
            title="Reorder files"
          >
            <FontAwesomeIcon icon={faGrip} />
            <span>Reorder Files</span>
          </button>
          {isReorderMode && (
            <button 
              className="toolbar-button save"
              onClick={onReorderSave}
              title="Save order"
            >
              <FontAwesomeIcon icon={faSave} />
              <span>Save Order</span>
            </button>
          )}
          <button 
            className={`toolbar-button ${isSyncing ? 'syncing' : ''}`}
            onClick={handleSync}
            disabled={isSyncing}
            title="Sync current folder with database"
          >
            <FontAwesomeIcon icon={faSync} spin={isSyncing} />
            <span>Sync</span>
          </button>
        </div>
      )}

      <MediaPlayer file={selectedFile} currentPath={path} getMediaUrl={getMediaUrl} />
    </div>
  );
});

export default Sidebar; 