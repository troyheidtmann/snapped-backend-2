// ===== IMPORTS =====
import React, { useCallback, useState, useEffect } from 'react';
import { DndProvider } from 'react-dnd';
import { HTML5Backend } from 'react-dnd-html5-backend';
import './styles/FileManager.css';
import { createFolder, reorderFiles } from './operations/FolderOperations';
import { moveFile } from './operations/FileOperations';
import { useFileManagerState } from './hooks/useFileManagerState';
import Sidebar from './Sidebar-cdn';
import ContentArea from './ContentArea-cdn';
import MultiSelectButton from './MultiSelectButton';
import { ThumbnailSelectionService } from './ThumbnailSelectionService';
import { toast } from 'react-toastify';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import ClientService from '../../services/ClientService';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faChevronLeft, faChevronRight, faDatabase } from '@fortawesome/free-solid-svg-icons';
import { useAuth } from '../../contexts/AuthContext';
import EditorNotesSection from './EditorNotesSection';
import { signOut } from 'aws-amplify/auth';
import { useNavigate } from 'react-router-dom';
import { pathUtils } from './utils/pathUtils';

const FileManager = () => {
  const { user } = useAuth();
  const {
    leftDir,
    rightDir,
    selectedLeftFile,
    setSelectedLeftFile,
    selectedRightFile,
    setSelectedRightFile,
    isReorderMode,
    setIsReorderMode,
    reorderedFiles,
    setReorderedFiles,
    getMediaUrl,
    doClientsMatch
  } = useFileManagerState();

  const [isMultiSelectMode, setIsMultiSelectMode] = useState(false);
  const [multiSelectedFiles, setMultiSelectedFiles] = useState([]);
  const [isThumbSelectMode, setIsThumbSelectMode] = useState(false);
  const [thumbnailSelectedFiles, setThumbnailSelectedFiles] = useState([]);
  const [currentOrder, setCurrentOrder] = useState([]);
  const [initialOrder, setInitialOrder] = useState([]);
  const [selectionSide, setSelectionSide] = useState(null); // 'left' or 'right'
  const [thumbnailCache, setThumbnailCache] = useState(new Map());
  const [caption, setCaption] = useState('');
  const [currentClientId, setCurrentClientId] = useState(null);
  const [showCombinedStorage, setShowCombinedStorage] = useState(true);
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [leftLoading, setLeftLoading] = useState(false);
  const [rightLoading, setRightLoading] = useState(false);

  useEffect(() => {
    document.title = 'Content Manager | Snapped';
  }, []);

  // Load thumbnails once when contents change
  useEffect(() => {
    if (!rightDir.contents?.length) return;
    
    // Check if we're in a STO folder
    if (!rightDir.isSTOPath) return;

    const thumbnailFiles = rightDir.contents.filter(f => f.is_thumbnail);
    setThumbnailSelectedFiles(thumbnailFiles);
    
    // Update cache
    const newCache = new Map();
    rightDir.contents.forEach(file => {
      newCache.set(file.name, file.is_thumbnail || false);
    });
    setThumbnailCache(newCache);

  }, [rightDir.contents, rightDir.isSTOPath]);

  // Add effect to load caption when right file is selected
  useEffect(() => {
    if (selectedRightFile) {
      // Set caption from the selected file's data
      setCaption(selectedRightFile.caption || '');
    } else {
      // Clear caption when no file is selected
      setCaption('');
    }
  }, [selectedRightFile]);

  const handleToggleMultiSelect = () => {
    setIsMultiSelectMode(!isMultiSelectMode);
    setMultiSelectedFiles([]); // Clear selection when toggling
  };

  const handleFileSelect = (file, isRight) => {
    if (isRight && isMultiSelectMode) {
      toast.info('Multi-select is only available in the left panel');
      return;
    }
    setMultiSelectedFiles(prev => {
      const isSelected = prev.some(f => f.name === file.name);
      if (isSelected) {
        return prev.filter(f => f.name !== file.name);
      } else {
        return [...prev, file];
      }
    });
  };

  // ===== FILE OPERATIONS =====
  const handleDrop = useCallback(async (item, targetPath) => {
    const refreshCallbacks = {
      [leftDir.path]: () => {
        // Preserve current page when refreshing
        const currentLeftPage = leftDir.pagination.currentPage;
        leftDir.refresh();
        // Restore the page after refresh
        setTimeout(() => {
          for (let i = 1; i < currentLeftPage; i++) {
            leftDir.pagination.nextPage();
          }
        }, 100);
      },
      [rightDir.path]: () => {
        const currentRightPage = rightDir.pagination.currentPage;
        rightDir.refresh();
        setTimeout(() => {
          for (let i = 1; i < currentRightPage; i++) {
            rightDir.pagination.nextPage();
          }
        }, 100);
      }
    };
    
    try {
      if (isMultiSelectMode && multiSelectedFiles.length > 0) {
        // Move all selected files from left to right only
        const movePromises = multiSelectedFiles.map(file => 
          moveFile(
            { 
              name: file.name,
              type: 'file',
              sourcePath: leftDir.path,
              ...file 
            }, 
            targetPath,
            refreshCallbacks
          )
        );

        await Promise.all(movePromises);
        setMultiSelectedFiles([]);
        setIsMultiSelectMode(false);
        refreshCallbacks[leftDir.path]();
        refreshCallbacks[rightDir.path]();
      } else {
        // Handle single file
        await moveFile(
          item, 
          targetPath, 
          refreshCallbacks
        );
      }
    } catch (error) {
      console.error('Error in handleDrop:', error);
      toast.error('Failed to move file(s)');
    }
  }, [leftDir, rightDir, isMultiSelectMode, multiSelectedFiles]);

  // ===== FOLDER OPERATIONS =====
  const handleReorderChange = useCallback((newOrder) => {
    setCurrentOrder(newOrder);
  }, []);

  const handleReorderSave = useCallback(() => {
    if (currentOrder.length > 0) {
      console.log('Saving order:', currentOrder);
      const currentPage = rightDir.pagination.currentPage;
      
      reorderFiles(
        rightDir.path, 
        currentOrder,
        initialOrder,
        () => {
          setIsReorderMode(false);
          rightDir.refresh();
          // Restore the page after refresh
          setTimeout(() => {
            for (let i = 1; i < currentPage; i++) {
              rightDir.pagination.nextPage();
            }
          }, 100);
        }
      ).catch(error => console.error('Error in handleReorder:', error));
    }
  }, [currentOrder, rightDir, setIsReorderMode, initialOrder, rightDir.refresh]);

  const handleCreateFolder = useCallback(() => {
    createFolder(
      rightDir.path, 
      rightDir.folders, 
      rightDir.refresh
    ).catch(error => console.error('Error in handleCreateFolder:', error));
  }, [rightDir.path, rightDir.folders, rightDir.refresh]);

  // Create separate navigation handlers for left and right
  const handleLeftNavigate = useCallback((folder) => {
    setSelectedLeftFile(null);
    leftDir.navigate(folder);
  }, [leftDir]);

  const handleRightNavigate = useCallback((folder) => {
    setSelectedRightFile(null);
    rightDir.navigate(folder);
  }, [rightDir]);

  // Add thumbnail handlers
  const handleSetThumbnails = useCallback(() => {
    if (isReorderMode) {
      setIsReorderMode(false);
    }
    setIsThumbSelectMode(!isThumbSelectMode);
  }, [isThumbSelectMode, isReorderMode]);

  // Add handleSaveThumbnails function
  const handleSaveThumbnails = useCallback(async () => {
    if (!thumbnailSelectedFiles.length) {
      toast.warning('No thumbnails selected');
      return;
    }

    const sessionMatch = rightDir.path.match(/F\([\d-]+\)_[^/]+/);
    if (!sessionMatch) {
      toast.error('Invalid session path');
      return;
    }

    try {
      const response = await axios.post(API_ENDPOINTS.CDN_MONGO.UPDATE_THUMBNAILS, {
        session_id: sessionMatch[0],
        thumbnails: thumbnailSelectedFiles.map(file => ({
          file_name: file.name,
          path: file.path
        }))
      });

      if (response.data.status === 'success') {
        toast.success('Thumbnails saved successfully');
        setIsThumbSelectMode(false);
        rightDir.refresh();
      } else {
        throw new Error(response.data.message || 'Failed to save thumbnails');
      }
    } catch (error) {
      console.error('Error saving thumbnails:', error);
      toast.error(error.message || 'Failed to save thumbnails');
    }
  }, [thumbnailSelectedFiles, rightDir.path, rightDir.refresh]);

  // Update thumbnail selection and database
  const handleThumbnailSelect = useCallback(async (file) => {
    const isSelected = thumbnailCache.get(file.name);
    const sessionMatch = rightDir.path.match(/F\([\d-]+\)_[^/]+/);
    if (!sessionMatch) return;
    
    const sessionId = sessionMatch[0];
    let updatedFiles;

    if (isSelected) {
      updatedFiles = thumbnailSelectedFiles.filter(f => f.name !== file.name);
    } else if (thumbnailSelectedFiles.length < 3) {
      updatedFiles = [...thumbnailSelectedFiles, file];
    } else {
      toast.warning('You can only select 3 files for thumbnails');
      return;
    }

    try {
      // Log what we're sending to the server
      console.log('Updating thumbnails:', {
        sessionId,
        selected_files: updatedFiles
      });

      await ThumbnailSelectionService.updateThumbnails(sessionId, updatedFiles);
      setThumbnailSelectedFiles(updatedFiles);
      setThumbnailCache(prev => new Map(prev.set(file.name, !isSelected)));
      
      rightDir.refresh();

    } catch (error) {
      console.error('Error updating thumbnails:', error);
      toast.error('Failed to update thumbnails');
    }
  }, [rightDir.path, thumbnailSelectedFiles, thumbnailCache, rightDir.refresh]);

  const handleReorderMode = useCallback(() => {
    if (isThumbSelectMode) {
      setIsThumbSelectMode(false);
      setMultiSelectedFiles([]);
    }
    if (!isReorderMode) {
      // Store initial order with all file properties
      const initialFiles = rightDir.contents
        .filter(item => item.type === 'file')
        .sort((a, b) => {
          const seqA = a.seq_number || 0;
          const seqB = b.seq_number || 0;
          return seqA - seqB;
        })
        .map(file => ({
          ...file,
          seq_number: file.seq_number || 0  // Ensure seq_number exists
        }));
      console.log('Initial files with sequence numbers:', initialFiles);
      setInitialOrder(initialFiles);
    }
    setIsReorderMode(!isReorderMode);
  }, [isThumbSelectMode, isReorderMode, rightDir.contents]);

  // Function to get display name - just show the current folder name
  const getDisplayName = useCallback((path) => {
    const parts = path.split('/').filter(Boolean);
    return parts[parts.length - 1] || 'public';
  }, []);

  // Add handler for caption submission
  const handleCaptionSubmit = useCallback(async () => {
    if (!selectedRightFile || !caption.trim()) return;

    // Extract the session ID from the STO path (e.g., "040225")
    const stoMatch = rightDir.path.match(/\/STO\/([^/]+)/);
    if (!stoMatch) {
      toast.error('Invalid storage path');
      return;
    }

    try {
      await axios.post('/api/cdn/update-caption', {
        session_id: stoMatch[1],
        file_name: selectedRightFile.name,
        caption: caption.trim()
      });
      
      toast.success('Caption updated');
      setCaption('');
      rightDir.refresh();
    } catch (error) {
      console.error('Error updating caption:', error);
      toast.error('Failed to update caption');
    }
  }, [caption, selectedRightFile, rightDir.path, rightDir.refresh]);

  const handleNoteClick = (file) => {
    // Update the left media player with the clicked note's file
    setSelectedLeftFile(file);
  };

  const handleSignOut = async () => {
    try {
      await signOut();
      navigate('/cdn-login');
    } catch (error) {
      console.error('Error signing out:', error);
    }
  };

  const leftSidebarProps = {
    path: leftDir.path,
    folders: leftDir.folders,
    onNavigate: handleLeftNavigate,
    selectedFile: selectedLeftFile,
    isRight: false,
    getMediaUrl,
    isMultiSelectMode,
    onToggleMultiSelect: handleToggleMultiSelect,
    selectedCount: multiSelectedFiles.length,
    onMoveSelected: handleDrop,
    isThumbSelectMode,
    onSetThumbnails: handleSetThumbnails,
    setIsThumbSelectMode,
    onReorderSave: handleReorderSave,
    showMultiSelect: true,
    storageType: 'both',
  };

  const leftContentProps = {
    path: leftDir.path,
    contents: leftDir.contents,
    onFileClick: setSelectedLeftFile,
    isRight: false,
    onDrop: handleDrop,
    getMediaUrl,
    getThumbnail: leftDir.getThumbnail,
    isMultiSelectMode,
    selectedFiles: multiSelectedFiles,
    onFileSelect: handleFileSelect,
    isThumbSelectMode,
    onReorderChange: handleReorderChange,
    isLoading: leftDir.isLoading,
    hasMore: leftDir.hasMore,
    onLoadMore: leftDir.loadMore,
    error: leftDir.error,
    retryCount: leftDir.retryCount,
    onForceRefresh: leftDir.forceRefresh,
    leftDir,
    rightDir,
    storageType: 'both',
    doClientsMatch
  };

  const rightSidebarProps = {
    path: rightDir.path,
    folders: rightDir.folders,
    onNavigate: handleRightNavigate,
    selectedFile: selectedRightFile,
    isRight: true,
    isReorderMode,
    setIsReorderMode: handleReorderMode,
    onReorderSave: handleReorderSave,
    onCreateFolder: handleCreateFolder,
    getMediaUrl,
    showMultiSelect: false,
    onSetThumbnails: handleSetThumbnails,
    isThumbSelectMode,
    onSaveThumbnails: handleSaveThumbnails,
    setIsThumbSelectMode,
    caption,
    setCaption,
    onCaptionSubmit: handleCaptionSubmit,
    selectedRightFile,
  };

  const rightContentProps = {
    contents: rightDir.contents.sort((a, b) => {
      // First try to sort by sequence number
      const seqA = a.seq_number || 0;
      const seqB = b.seq_number || 0;
      
      if (seqA !== seqB) {
        return seqA - seqB;
      }
      
      // If seq numbers are same or both 0, sort by filename
      const numA = parseInt(a.name.match(/(\d+)/)?.[0] || 0);
      const numB = parseInt(b.name.match(/(\d+)/)?.[0] || 0);
      
      return numA - numB;
    }),
    path: rightDir.path,
    onFileClick: setSelectedRightFile,
    isRight: true,
    onDrop: handleDrop,
    getMediaUrl,
    getThumbnail: (file, path) => getMediaUrl(file, path),
    isReorderMode,
    onReorderChange: handleReorderChange,
    isMultiSelectMode: false,
    selectedFiles: [],
    onFileSelect: null,
    isThumbSelectMode,
    onThumbnailSelect: handleThumbnailSelect,
    thumbnailSelectedFiles,
    isLoading: rightDir.isLoading,
    hasMore: rightDir.hasMore,
    onLoadMore: rightDir.loadMore,
    error: rightDir.error,
    retryCount: rightDir.retryCount,
    onForceRefresh: rightDir.forceRefresh,
    leftDir,
    rightDir,
    storageType: 'both',
    doClientsMatch
  };

  return (
    <DndProvider backend={HTML5Backend}>
      <div className="file-manager">
        <button 
          onClick={handleSignOut}
          className="cdn-logout-button"
          style={{
            position: 'absolute',
            top: '10px',
            right: '10px',
            padding: '3px 7px',
            background: 'none',
            border: '1px solid rgb(189 187 187)',
            borderRadius: '4px',
            color: 'rgb(189 187 187)',
            cursor: 'pointer',
            fontSize: '12px',
            zIndex: 1000
          }}
        >
          Sign Out
        </button>
        <div className="cdn-panels">
          <div className="file-browser">
            <Sidebar {...leftSidebarProps} />
            
            <div className="content-section">
              <div className={`client-banner ${doClientsMatch() ? 'matching' : 'mismatched'}`}>
                <button 
                  className="page-nav-button" 
                  onClick={leftDir.pagination.prevPage}
                  disabled={!leftDir.pagination.hasPrevPage}
                >
                  <FontAwesomeIcon icon={faChevronLeft} />
                </button>
                <span className="client-name">
                  {getDisplayName(leftDir.displayPath)}
                </span>
                <div className="page-indicator">
                  {leftDir.pagination.currentPage} / {leftDir.pagination.totalPages}
                </div>
                <button 
                  className="page-nav-button" 
                  onClick={leftDir.pagination.nextPage}
                  disabled={!leftDir.pagination.hasNextPage}
                >
                  <FontAwesomeIcon icon={faChevronRight} />
                </button>
              </div>
              <ContentArea {...leftContentProps} />
            </div>

            <div className="content-section">
              <div className={`client-banner ${doClientsMatch() ? 'matching' : 'mismatched'}`}>
                <button 
                  className="page-nav-button" 
                  onClick={rightDir.pagination.prevPage}
                  disabled={!rightDir.pagination.hasPrevPage}
                >
                  <FontAwesomeIcon icon={faChevronLeft} />
                </button>
                <span className="client-name">
                  {getDisplayName(rightDir.displayPath)}
                </span>
                <div className="page-indicator">
                  {rightDir.pagination.currentPage} / {rightDir.pagination.totalPages}
                </div>
                <button 
                  className="page-nav-button" 
                  onClick={rightDir.pagination.nextPage}
                  disabled={!rightDir.pagination.hasNextPage}
                >
                  <FontAwesomeIcon icon={faChevronRight} />
                </button>
              </div>
              <ContentArea {...rightContentProps} />
            </div>

            <Sidebar {...rightSidebarProps} />
          </div>
        </div>

        <div className="cdn-notes-container">
          <EditorNotesSection 
            clientId={currentClientId} 
            onNoteClick={handleNoteClick}
          />
        </div>
      </div>
    </DndProvider>
  );
};

export default React.memo(FileManager);