import React, { memo, useState, useEffect, useRef, useCallback } from 'react';
import { useDrop } from 'react-dnd';
import DraggableFile from './DraggableFile-cdn';
import ClientService from '../../services/ClientService';
import { toast } from 'react-toastify';
import axios from 'axios';
import { pathUtils } from './utils/pathUtils';

const ContentArea = memo(({ 
  path, 
  contents = [], 
  onFileClick, 
  isRight = false, 
  isReorderMode,
  onDrop,
  getMediaUrl,
  getThumbnail,
  onReorderChange,
  isMultiSelectMode,
  selectedFiles,
  onFileSelect,
  isThumbSelectMode,
  onThumbnailSelect,
  thumbnailSelectedFiles = [],
  isLoading,
  hasMore,
  onLoadMore,
  error,
  retryCount,
  onForceRefresh,
  leftDir,
  rightDir,
  doClientsMatch,
  onContentsUpdate
}) => {
  const [isDraggingFromHere, setIsDraggingFromHere] = useState(false);
  const [clientNames, setClientNames] = useState({});
  const MAX_RETRIES = 3;
  
  // Extract client ID from path and fetch client name
  useEffect(() => {
    const fetchClientNames = async () => {
      const clientId = pathUtils.getClientId(path);
      if (clientId) {
        try {
          const clientName = await ClientService.getClientName(clientId);
          setClientNames(prev => ({
            ...prev,
            [clientId]: clientName
          }));
        } catch (error) {
          console.error('Error fetching client name:', error);
        }
      }
    };
    fetchClientNames();
  }, [path]);

  const [{ isOver }, drop] = useDrop({
    accept: ['FILE', 'REORDER'],
    drop: (item, monitor) => {
      const itemType = monitor.getItemType();
      if (itemType === 'REORDER') {
        return;
      }
      onDrop(item, path);
    },
    collect: (monitor) => ({
      isOver: monitor.isOver() && monitor.canDrop(),
    }),
    canDrop: (item, monitor) => {
      const itemType = monitor.getItemType();
      if (itemType === 'REORDER') {
        return isReorderMode;
      }
      return !isDraggingFromHere && (typeof doClientsMatch === 'function' ? doClientsMatch() : true);
    },
  });

  const [localReorderedFiles, setLocalReorderedFiles] = useState([]);

  // Only set initial order when entering reorder mode
  useEffect(() => {
    if (isRight && isReorderMode) {
      const fileContents = contents.filter(item => item.type === 'file');
      setLocalReorderedFiles(fileContents);
      onReorderChange(fileContents);
    }
  }, [isReorderMode]); 

  const handleReorderMove = useCallback((dragIndex, hoverIndex) => {
    if (!isReorderMode) return;
    
    setLocalReorderedFiles(prevFiles => {
      const newFiles = [...prevFiles];
      const [removed] = newFiles.splice(dragIndex, 1);
      newFiles.splice(hoverIndex, 0, removed);
      onReorderChange(newFiles);
      return newFiles;
    });
  }, [isReorderMode, onReorderChange]);

  const getDisplayName = (path) => {
    const clientId = pathUtils.getClientId(path);
    return clientId ? (clientNames[clientId] || clientId) : pathUtils.getDisplayPath(path);
  };

  // Filter to show only files (no folders) in the grid
  const files = contents.filter(item => item.type === 'file');

  const handleDragStart = () => setIsDraggingFromHere(true);
  const handleDragEnd = () => setIsDraggingFromHere(false);

  const handleFileClick = useCallback((file) => {
    if (isThumbSelectMode) {
      onThumbnailSelect(file);
      return;
    }
    
    if (isMultiSelectMode) {
      onFileSelect(file);
    } else {
      onFileClick(file);
    }
  }, [isThumbSelectMode, isMultiSelectMode, onFileSelect, onFileClick, onThumbnailSelect]);

  // Add intersection observer for infinite scroll
  const observerRef = useRef();
  const lastFileRef = useCallback(node => {
    if (isLoading) return;
    
    if (observerRef.current) {
      observerRef.current.disconnect();
    }

    observerRef.current = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting && hasMore) {
        onLoadMore();
      }
    });

    if (node) {
      observerRef.current.observe(node);
    }
  }, [isLoading, hasMore, onLoadMore]);

  const isImage = (fileName) => {
    return /\.(jpg|jpeg|png|gif|webp)$/i.test(fileName);
  };

  const isVideo = (fileName) => {
    return /\.(mp4|mov|avi|webm)$/i.test(fileName);
  };

  useEffect(() => {
    const loadContents = async () => {
      try {
        // Make request to collections endpoint with whatever path we have
        const response = await axios.get(
          API_ENDPOINTS.CDN.COLLECTION('Uploads'),
          { params: { path } }
        );

        // Just display whatever the backend sends us
        if (response.data.media?.length) {
          const files = response.data.media.map(item => ({
            name: item.file_name,
            type: item.file_type || (item.CDN_link?.match(/\.(mp4|mov|avi|webm)$/i) ? 'video' : 'image'),
            CDN_link: item.CDN_link,
            caption: item.caption,
            thumbnail: item.thumbnail,
            seq_number: item.seq_number || 0,
            is_thumbnail: item.is_thumbnail || false,
            upload_time: item.upload_time,
            video_length: item.video_length || 0,
            is_indexed: item.is_indexed || false
          }));
          onContentsUpdate?.(files);
        } else if (response.data.arrays?.[0]) {
          const folders = response.data.arrays[0].map(item => ({
            name: item.session_id || item.client_id || item,
            type: 'folder',
            path: item.path || item.session_id || item.client_id || item
          }));
          onContentsUpdate?.(folders);
        } else {
          onContentsUpdate?.([]);
        }
      } catch (error) {
        console.error('Error loading contents:', error);
        onContentsUpdate?.([]);
      }
    };

    loadContents();
  }, [path]);

  // Only show files in the media gallery when in a session folder
  const filesToDisplay = pathUtils.isSessionPath(path) 
    ? contents.filter(item => item.type === 'file' || item.type?.includes('image') || item.type?.includes('video'))
    : [];

  return (
    <div className="content-wrapper">
      <div 
        ref={drop} 
        className={`content-area ${isOver && !isDraggingFromHere ? 'drop-target' : ''} 
          ${isDraggingFromHere ? 'dragging-source' : ''} 
          ${!doClientsMatch || !doClientsMatch() ? 'content-area--disabled' : ''}`}
        style={{
          pointerEvents: isDraggingFromHere || !doClientsMatch || !doClientsMatch() ? 'none' : 'auto',
          opacity: isDraggingFromHere ? 0.5 : doClientsMatch && doClientsMatch() ? 1 : 0.5,
          height: '100%'
        }}
      >
        {(!doClientsMatch || !doClientsMatch()) && (
          <div className="mismatch-overlay">
            <div className="mismatch-message">
              Client folders must match to enable file operations
            </div>
          </div>
        )}
        
        <div className="file-grid">
          {filesToDisplay.map((file, index) => {
            const isLast = index === filesToDisplay.length - 1;
            const ref = isLast ? lastFileRef : null;
            
            return (
              <DraggableFile
                key={file.name}
                ref={ref}
                file={file}
                index={index}
                onClick={() => handleFileClick(file)}
                onDragStart={handleDragStart}
                onDragEnd={handleDragEnd}
                onMove={handleReorderMove}
                isReorderMode={isReorderMode}
                isSelected={selectedFiles?.some(f => f.name === file.name)}
                isThumbSelected={thumbnailSelectedFiles?.some(f => f.name === file.name)}
                previewUrl={isImage(file.name) ? file.CDN_link : null}
                videoUrl={isVideo(file.name) ? file.CDN_link : null}
                isRight={isRight}
                currentPath={path}
                setFiles={onReorderChange}
              />
            );
          })}
        </div>
        
        {isLoading && (
          <div className="loading-indicator">Loading...</div>
        )}
        
        {error && (
          <div className="error-message">
            {error}
            {retryCount < MAX_RETRIES && (
              <button onClick={onForceRefresh}>Retry</button>
            )}
          </div>
        )}
      </div>
    </div>
  );
});

export default ContentArea; 