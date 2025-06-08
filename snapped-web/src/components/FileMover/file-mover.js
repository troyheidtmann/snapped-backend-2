/**
 * @fileoverview File mover component for managing and transferring files between CDN locations.
 * Provides a split-view interface for browsing and moving files between different content types and sessions.
 */

import React, { useState, useEffect } from 'react';
import { Amplify } from 'aws-amplify';
import { getCurrentUser, fetchAuthSession } from 'aws-amplify/auth';
import './file-mover.css';
import axios from 'axios';
import EditorNotesSection from '../CDN/EditorNotesSection';
import { toast } from 'react-hot-toast';

/**
 * @typedef {Object} FileMetadata
 * @property {string} name - Name of the file
 * @property {string} type - Type of file ('video'|'image')
 * @property {string} CDN_link - URL to the file in CDN
 * @property {string} [thumbnail] - Base64 encoded thumbnail or URL
 * @property {boolean} [thumbnailFailed] - Whether thumbnail generation failed
 * @property {boolean} [is_thumbnail] - Whether this file is marked as a thumbnail
 * @property {Object} session_info - Session information for the file
 */

/**
 * @typedef {Object} SessionInfo
 * @property {string} session_id - Unique identifier for the session
 * @property {string} folder_id - Folder identifier
 * @property {string} client_id - Client identifier
 */

/**
 * @typedef {Object} ClientInfo
 * @property {string} First_Legal_Name - Client's first name
 * @property {string} Last_Legal_Name - Client's last name
 * @property {string} client_ID - Unique client identifier
 */

const API_BASE_URL = 'https://track.snapped.cc';

/**
 * Formats a duration in seconds to a human-readable string.
 * 
 * @param {number} seconds - Duration in seconds
 * @returns {string} Formatted duration string (e.g., "1:30")
 */
const formatDuration = (seconds) => {
    if (!seconds) return '0:00';
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.floor(seconds % 60);
    return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
};

/**
 * Cleans a session name by removing the F() wrapper.
 * 
 * @param {string} sessionId - Session identifier
 * @returns {string} Cleaned session name
 */
const cleanSessionName = (sessionId) => {
    const match = sessionId.match(/F\((\d{2}-\d{2}-\d{4})\)/);
    return match ? match[1] : sessionId;
};

/**
 * File mover component for managing and transferring files between CDN locations.
 * Features:
 * - Split-view file browsing
 * - Drag and drop file movement
 * - Thumbnail management
 * - File organization and reordering
 * - Comment/caption management
 * - Session creation and management
 * - Media preview and playback
 * - Error handling and user feedback
 * 
 * @returns {React.ReactElement} The file mover interface
 */
const FileMover = () => {
    // Left tree state
    const [selectedClient, setSelectedClient] = useState(null);
    const [selectedContentType, setSelectedContentType] = useState(null);
    const [users, setUsers] = useState([]);
    const [contentTypes, setContentTypes] = useState([]);
    const [sessions, setSessions] = useState([]);
    const [files, setFiles] = useState([]);
    const [clientInfo, setClientInfo] = useState(null);
    
    // Right tree state (destination)
    const [destSelectedClient, setDestSelectedClient] = useState(null);
    const [destSelectedContentType, setDestSelectedContentType] = useState(null);
    const [destUsers, setDestUsers] = useState([]);
    const [destSessions, setDestSessions] = useState([]);
    const [destClientInfo, setDestClientInfo] = useState(null);
    const [destFiles, setDestFiles] = useState([]);

    // Shared state
    const [currentPath, setCurrentPath] = useState('/');
    const [isSourceLoading, setIsSourceLoading] = useState(false);
    const [isDestLoading, setIsDestLoading] = useState(false);
    const [currentPage, setCurrentPage] = useState(1);
    const [destCurrentPage, setDestCurrentPage] = useState(1);
    const [selectedFile, setSelectedFile] = useState(null);
    const [selectedDestFile, setSelectedDestFile] = useState(null);
    const itemsPerPage = 20;

    // Add pagination helpers
    const totalPages = Math.ceil(files.length / itemsPerPage);
    const destTotalPages = Math.ceil(destFiles.length / itemsPerPage);

    // Add state for tracking current sessions
    const [currentSourceSession, setCurrentSourceSession] = useState(null);
    const [currentDestSession, setCurrentDestSession] = useState(null);

    // Add new state for organize mode
    const [isOrganizeMode, setIsOrganizeMode] = useState(false);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
    const [draggedFile, setDraggedFile] = useState(null);
    const [dragOverFile, setDragOverFile] = useState(null);

    // Add new state for thumbnail mode
    const [isThumbnailMode, setIsThumbnailMode] = useState(false);
    const [hasUnsavedThumbnailChanges, setHasUnsavedThumbnailChanges] = useState(false);
    const [selectedThumbnails, setSelectedThumbnails] = useState(new Set());

    // Add new state for comment functionality
    const [isCommentModalOpen, setIsCommentModalOpen] = useState(false);
    const [commentText, setCommentText] = useState('');
    const [selectedFileForComment, setSelectedFileForComment] = useState(null);

    const getCurrentPageItems = () => {
        const startIndex = (currentPage - 1) * itemsPerPage;
        const endIndex = startIndex + itemsPerPage;
        return files.slice(startIndex, endIndex);
    };

    const getDestCurrentPageItems = () => {
        const startIndex = (destCurrentPage - 1) * itemsPerPage;
        const endIndex = startIndex + itemsPerPage;
        return destFiles.slice(startIndex, endIndex);
    };

    const goToNextPage = () => {
        if (currentPage < totalPages) {
            setCurrentPage(currentPage + 1);
        }
    };

    const goToPrevPage = () => {
        if (currentPage > 1) {
            setCurrentPage(currentPage - 1);
        }
    };

    const goToNextDestPage = () => {
        if (destCurrentPage < destTotalPages) {
            setDestCurrentPage(destCurrentPage + 1);
        }
    };

    const goToPrevDestPage = () => {
        if (destCurrentPage > 1) {
            setDestCurrentPage(destCurrentPage - 1);
        }
    };

    useEffect(() => {
        loadContentTypes();
    }, []);

    const getAuthToken = async () => {
        try {
            const { tokens } = await fetchAuthSession();
            return tokens.idToken.toString();
        } catch (error) {
            console.error('Error getting auth token:', error);
            throw error;
        }
    };

    const fetchWithAuth = async (url, options = {}) => {
        try {
            const token = await getAuthToken();
            const response = await fetch(`${API_BASE_URL}${url}`, {
            ...options,
            headers: {
                ...options.headers,
                'Authorization': `Bearer ${token}`
            }
        });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return response;
        } catch (error) {
            console.error('Fetch error:', error);
            throw error;
        }
    };

    const loadContentTypes = async () => {
        try {
            setIsSourceLoading(true);
            const response = await fetchWithAuth('/api/cdn-mongo/collections');
            const data = await response.json();
            
            if (data.status === 'success') {
                setContentTypes(data.collections);
            } else {
                throw new Error('Failed to load content types');
            }
        } catch (error) {
            console.error('Error loading content types:', error);
        } finally {
            setIsSourceLoading(false);
        }
    };

    const selectContentType = async (contentType) => {
        setSelectedContentType(contentType);
        setSelectedClient(null);
        setClientInfo(null);
        setFiles([]);
        loadUsersForContentType(contentType, false);
    };

    const selectDestContentType = async (contentType) => {
        setDestSelectedContentType(contentType);
        setDestSelectedClient(null);
        setDestClientInfo(null);
        loadUsersForContentType(contentType, true);
    };

    const loadUsersForContentType = async (contentType, isDestination) => {
        try {
            if (isDestination) {
                setIsDestLoading(true);
            } else {
                setIsSourceLoading(true);
            }
            const response = await fetchWithAuth(`/api/cdn-mongo/collections/${contentType}/clients`);
            const data = await response.json();
            
            if (data.status === 'success') {
                // Process users sequentially to avoid overwhelming the server
                const authorizedUsers = [];
                for (const user of data.clients) {
                    try {
                        const infoResponse = await fetchWithAuth(`/api/cdn-mongo/get-client-info/${user.client_ID}`);
                        if (infoResponse.status === 403) {
                            // Skip unauthorized users
                            continue;
                        }
                        const infoData = await infoResponse.json();
                        authorizedUsers.push({
                            ...user,
                            clientInfo: infoData.client_info
                        });
                    } catch (error) {
                        // If we get a 403, skip this user
                        if (error.message.includes('403')) {
                            continue;
                        }
                        console.error(`Error fetching info for ${user.client_ID}:`, error);
                    }
                }
                
                if (isDestination) {
                    setDestUsers(authorizedUsers);
                } else {
                    setUsers(authorizedUsers);
                }
            }
        } catch (error) {
            showError('Failed to load users');
        } finally {
            if (isDestination) {
                setIsDestLoading(false);
            } else {
                setIsSourceLoading(false);
            }
        }
    };

    const selectUser = async (clientId, isDestination) => {
        try {
            if (isDestination) {
                setIsDestLoading(true);
                setDestSelectedClient(clientId);
                setDestSessions([]);
            } else {
                setIsSourceLoading(true);
                setSelectedClient(clientId);
                setSessions([]);
                setFiles([]);
            }
            
            // Load user info and sessions
            const [infoResponse, sessionsResponse] = await Promise.all([
                fetchWithAuth(`/api/cdn-mongo/get-client-info/${clientId}`),
                fetchWithAuth(`/api/cdn-mongo/collections/${isDestination ? destSelectedContentType : selectedContentType}/clients/${clientId}/sessions`)
            ]);
            
            const infoData = await infoResponse.json();
            const sessionsData = await sessionsResponse.json();
            
            if (infoData.status === 'success') {
                if (isDestination) {
                    setDestClientInfo(infoData.client_info);
                } else {
                    setClientInfo(infoData.client_info);
                }
            }
            
            if (sessionsData.status === 'success') {
                if (isDestination) {
                    setDestSessions(sessionsData.sessions);
                } else {
                    setSessions(sessionsData.sessions);
                }
            }
        } catch (error) {
            showError('Failed to load client info and sessions');
        } finally {
            if (isDestination) {
                setIsDestLoading(false);
            } else {
                setIsSourceLoading(false);
            }
        }
    };

    const loadGallery = async (sessionId, skipLoadingState = false) => {
        setCurrentSourceSession(sessionId);
        if (!selectedClient || !selectedContentType) return;

        try {
            if (!skipLoadingState) {
                setIsSourceLoading(true);
            }
            const collectionMap = {
                "Uploads": "STORIES",
                "Spotlights": "SPOTLIGHT",
                "Saved": "SAVED",
                "Content_Dump": "Content_Dump"
            };
            
            const mongoCollection = collectionMap[selectedContentType];
            if (!mongoCollection) {
                throw new Error(`Invalid content type: ${selectedContentType}`);
            }
            
            // Log the request details for debugging
            console.log('Loading gallery with:', {
                selectedClient,
                selectedContentType,
                mongoCollection,
                sessionId
            });
            
            const path = `${selectedClient}/${mongoCollection}/${sessionId}`;
            console.log('Gallery request path:', path);
            
            const response = await fetchWithAuth(`/api/cdn-mongo/file-gallery?folder_path=${path}`);
            const data = await response.json();
            
            if (data.status === 'success') {
                const processedFiles = data.files.map(file => ({
                    ...file,
                    video_length: typeof file.video_length === 'string' ? 
                        parseInt(file.video_length, 10) : 
                        file.video_length,
                    thumbnail: file.thumbnail || null,
                    thumbnailFailed: file.thumbnail_failed || false,
                    is_thumbnail: file.is_thumbnail || false,
                    session_info: {
                        ...file.session_info,
                        session_id: sessionId
                    }
                }));

                setFiles(processedFiles);
            }
        } catch (error) {
            console.error('Gallery error:', error);
            showError('Failed to load gallery');
        } finally {
            if (!skipLoadingState) {
                setIsSourceLoading(false);
            }
        }
    };

    const handleDragStart = (e, file, sourceSessionId) => {
        // Get the correct session ID from the file's session info or current session
        let actualSourceSession = file.session_info?.session_id || currentSourceSession;
        
        if (!actualSourceSession) {
            showError('Source session not found');
            return;
        }

        // Log the drag start details for debugging
        console.log('Drag start details:', {
            fileName: file.name,
            sourceSessionId: actualSourceSession,
            fileSessionInfo: file.session_info,
            currentSourceSession,
            providedSourceSessionId: sourceSessionId,
            sourceContentType: selectedContentType
        });

        // Include source content type in the drag data
        e.dataTransfer.setData('application/json', JSON.stringify({
            file_name: file.name,
            source_session_id: actualSourceSession,  // Use the original session ID format
            client_id: selectedClient,
            source_content_type: selectedContentType
        }));
        e.currentTarget.classList.add('dragging');
    };

    const handleDragEnd = (e) => {
        // Remove dragging class when drag ends
        e.currentTarget.classList.remove('dragging');
    };

    const handleDragOver = (e) => {
        e.preventDefault();
        // Add drag-over class to the gallery
        e.currentTarget.classList.add('drag-over');
    };

    const handleDragLeave = (e) => {
        // Remove drag-over class when leaving the gallery
        e.currentTarget.classList.remove('drag-over');
    };

    const handleDrop = async (e, targetSessionId) => {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');

        try {
            const dragData = JSON.parse(e.dataTransfer.getData('application/json'));
            const { file_name, source_session_id, client_id, source_content_type } = dragData;

            // Log the drop details for debugging
            console.log('Drop details:', {
                fileName: file_name,
                sourceSessionId: source_session_id,
                targetSessionId,
                clientId: client_id,
                sourceContentType: source_content_type,
                targetContentType: destSelectedContentType
            });

            // Use original session IDs without transformation
            const formattedSourceSessionId = source_session_id;

            // Log the session IDs for debugging
            console.log('Session IDs:', {
                source: formattedSourceSessionId,
                target: targetSessionId,
                sourceContentType: source_content_type,
                targetContentType: destSelectedContentType
            });

            // Make the move file request with proper error handling
            const response = await fetchWithAuth(`/api/cdn-mongo/move-file/${client_id}/${encodeURIComponent(formattedSourceSessionId)}/${encodeURIComponent(targetSessionId)}/${file_name}`, {
                method: 'POST'
            });

            if (!response.ok) {
                const errorData = await response.json();
                const errorMessage = errorData.detail || `Failed to move file: ${response.status}`;
                console.error('Move file error:', errorMessage);
                throw new Error(errorMessage);
            }

            const result = await response.json();
            if (result.status === 'success') {
                // Refresh both source and target galleries
                await loadGallery(source_session_id, true);
                await loadDestGallery(targetSessionId, true);
                toast.success('File moved successfully');
            } else {
                throw new Error(result.message || 'Failed to move file');
            }
        } catch (error) {
            console.error('Error moving file:', error);
            toast.error(error.message || 'Failed to move file');
        }
    };

    const renderGalleryItem = (file, sessionId) => {
        const truncatedName = file.name.substring(0, 15);
        const thumbnailUrl = file.thumbnail ? 
            file.thumbnail.startsWith('http') ? file.thumbnail : `data:image/jpeg;base64,${file.thumbnail}` : 
            null;
            
        const isSelected = (file === selectedDestFile) || (file === selectedFile);
            
        return (
            <div 
                className={`gallery-item-wrapper ${file.is_thumbnail ? 'is-thumbnail' : ''} 
                          ${selectedThumbnails.has(file.name) ? 'selected-thumbnail' : ''} 
                          ${isSelected ? 'selected' : ''}`}
                draggable={isOrganizeMode}
                onDragStart={(e) => handleDragStart(e, file, sessionId)}
                onDragEnd={handleDragEnd}
                onClick={(e) => {
                    if (isThumbnailMode) {
                        handleThumbnailClick(file);
                    } else {
                        handleGalleryItemClick(file, e, sessionId === currentDestSession);
                    }
                }}
            >
                {file.is_thumbnail && <span className="thumbnail-star">★</span>}
                <div className={`gallery-item-thumbnail ${file.thumbnailFailed ? 'thumbnail-failed' : ''}`}>
                    {file.type === 'video' ? (
                        <div className="video-thumbnail">
                            {file.thumbnailFailed || !thumbnailUrl ? (
                                <div className="thumbnail-placeholder video">
                                    <span className="placeholder-icon">▶</span>
                                </div>
                            ) : (
                                <>
                                    <img 
                                        src={thumbnailUrl}
                                        alt={truncatedName}
                                        loading="lazy"
                                        style={{
                                            width: '100%',
                                            height: '100%',
                                            objectFit: 'cover',
                                            objectPosition: 'center'
                                        }}
                                        onError={(e) => {
                                            setFiles(prevFiles => prevFiles.map(f => 
                                                f.name === file.name 
                                                    ? { ...f, thumbnailFailed: true }
                                                    : f
                                            ));
                                        }}
                                    />
                                    <div className="video-overlay">▶</div>
                                    {typeof file.video_length === 'number' && file.video_length > 0 && (
                                        <div className="video-duration">
                                            {formatDuration(file.video_length)}
                                        </div>
                                    )}
                                </>
                            )}
                        </div>
                    ) : (
                        file.thumbnailFailed || !thumbnailUrl ? (
                            <div className="thumbnail-placeholder image">
                                <span className="placeholder-icon">🖼</span>
                            </div>
                        ) : (
                            <img 
                                src={thumbnailUrl}
                                alt={truncatedName}
                                loading="lazy"
                                style={{
                                    width: '100%',
                                    height: '100%',
                                    objectFit: 'cover',
                                    objectPosition: 'center'
                                }}
                                onError={(e) => {
                                    setFiles(prevFiles => prevFiles.map(f => 
                                        f.name === file.name 
                                            ? { ...f, thumbnailFailed: true }
                                            : f
                                    ));
                                }}
                            />
                        )
                    )}
                </div>
                <div className="gallery-item-info">
                    <span className="file-name">{truncatedName}</span>
                    {file.caption && <span className="file-caption">{file.caption}</span>}
                </div>
            </div>
        );
    };

    const formatFileSize = (bytes) => {
        if (!bytes) return '';
        const units = ['B', 'KB', 'MB', 'GB'];
        let size = bytes;
        let unitIndex = 0;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return `${size.toFixed(1)} ${units[unitIndex]}`;
    };

    const navigateBack = () => {
        if (files.length > 0) {
            setFiles([]);
        } else if (sessions.length > 0) {
            setSessions([]);
            setSelectedClient(null);
            setClientInfo(null);
        } else if (selectedClient) {
            setSelectedClient(null);
            setClientInfo(null);
        } else if (selectedContentType) {
            setSelectedContentType(null);
            setUsers([]);
        }
    };

    const navigateBackDest = () => {
        if (destFiles.length > 0) {
            setDestFiles([]);
        } else if (destSessions.length > 0) {
            setDestSessions([]);
            setDestSelectedClient(null);
            setDestClientInfo(null);
        } else if (destSelectedClient) {
            setDestSelectedClient(null);
            setDestClientInfo(null);
        } else if (destSelectedContentType) {
            setDestSelectedContentType(null);
            setDestUsers([]);
        }
    };

    const showError = (message) => {
        // You might want to use a proper toast/notification system here
        console.error(message);
    };

    const getBreadcrumb = () => {
        const parts = ['Home'];
        if (selectedContentType) parts.push(selectedContentType);
        if (selectedClient && clientInfo) {
            const firstName = clientInfo.First_Legal_Name || '';
            const lastName = clientInfo.Last_Legal_Name || '';
            parts.push(firstName && lastName ? `${firstName} ${lastName}` : selectedClient);
        }
        return parts;
    };

    const handleFileSelect = (file) => {
        setSelectedFile(file);
    };

    const renderMediaPlayer = (file, isDestination = false) => {
        console.log('Rendering media player:', {
            file,
            isDestination,
            CDN_link: file?.CDN_link,
            type: file?.type
        });

        if (!file) {
            return (
                <div className="media-player-empty">
                    <p>Select a file to preview</p>
                </div>
            );
        }

        const renderInfo = () => {
            const truncatedName = file.name.substring(0, 15);
            return (
                <div className="media-player-info">
                    <span className="media-player-filename">{truncatedName}</span>
                    {file.type === 'video' && typeof file.video_length === 'number' && file.video_length > 0 && (
                        <span className="media-player-details">
                            {formatDuration(file.video_length)}
                        </span>
                    )}
                </div>
            );
        };

        const renderVideoSummary = () => {
            if (!file.video_summary) return null;
            
            const summaryLines = file.video_summary.split('\n');
            const theme = summaryLines.find(line => line.startsWith('Theme'));
            const points = summaryLines.filter(line => !line.startsWith('Theme'));
            
            return (
                <div className="video-summary-container">
                    {theme && <div className="video-summary-theme">{theme}</div>}
                    {points.map((point, index) => (
                        <div key={index} className="video-summary-points">{point}</div>
                    ))}
                </div>
            );
        };

        return (
            <div className={`media-player-content ${isDestination ? 'destination' : 'source'}`}>
                {renderInfo()}
                {file.type === 'video' ? (
                    <div className="video-player-container">
                        <video 
                            key={file.CDN_link}
                            controls
                            autoPlay
                            playsInline
                            src={file.CDN_link}
                            poster={file.thumbnail ? 
                                file.thumbnail.startsWith('http') ? file.thumbnail : `data:image/jpeg;base64,${file.thumbnail}` 
                                : undefined}
                            preload="auto"
                            onError={(e) => {
                                console.error('Video playback error:', e);
                            }}
                            style={{
                                width: '100%',
                                height: '100%',
                                objectFit: 'contain',
                                backgroundColor: '#000'
                            }}
                        />
                    </div>
                ) : (
                    <div className="media-player-thumbnail">
                        <img 
                            src={file.CDN_link}
                            alt={file.name}
                            style={{
                                width: '100%',
                                height: '100%',
                                objectFit: 'contain',
                                backgroundColor: '#000'
                            }}
                        />
                    </div>
                )}
                {file.type === 'video' && renderVideoSummary()}
            </div>
        );
    };

    const handleGalleryItemClick = (file, e, isDestination = false) => {
        e.preventDefault();
        console.log('Gallery item clicked:', {
            file,
            isDestination,
            CDN_link: file.CDN_link,
            type: file.type
        });
        
        // Stop any currently playing videos before switching
        const videoElements = document.querySelectorAll('video');
        videoElements.forEach(video => {
            video.pause();
            video.currentTime = 0;
        });
        
        if (isDestination) {
            setSelectedDestFile(file);
        } else {
            setSelectedFile(file);
        }
    };

    const loadDestGallery = async (sessionId, skipLoadingState = false) => {
        setCurrentDestSession(sessionId);
        if (!destSelectedClient || !destSelectedContentType) return;

        try {
            if (!skipLoadingState) {
                setIsDestLoading(true);
            }
            const collectionMap = {
                "Uploads": "STORIES",
                "Spotlights": "SPOTLIGHT",
                "Saved": "SAVED",
                "Content_Dump": "Content_Dump"
            };
            
            const mongoCollection = collectionMap[destSelectedContentType];
            if (!mongoCollection) {
                throw new Error(`Invalid content type: ${destSelectedContentType}`);
            }
            
            // Log the request details for debugging
            console.log('Loading destination gallery with:', {
                destSelectedClient,
                destSelectedContentType,
                mongoCollection,
                sessionId
            });
            
            const path = `${destSelectedClient}/${mongoCollection}/${sessionId}`;
            console.log('Destination gallery request path:', path);
            
            const response = await fetchWithAuth(`/api/cdn-mongo/file-gallery?folder_path=${path}`);
            const data = await response.json();
            
            if (data.status === 'success') {
                const processedFiles = data.files.map(file => ({
                    ...file,
                    video_length: typeof file.video_length === 'string' ? 
                        parseInt(file.video_length, 10) : 
                        file.video_length,
                    thumbnail: file.thumbnail || null,
                    thumbnailFailed: file.thumbnail_failed || false,
                    is_thumbnail: file.is_thumbnail || false,
                    session_info: {
                        ...file.session_info,
                        session_id: sessionId
                    }
                }));
                
                const sortedFiles = processedFiles.sort((a, b) => a.seq_number - b.seq_number);
                setDestFiles(sortedFiles);
            }
        } catch (error) {
            console.error('Gallery error:', error);
            showError('Failed to load destination gallery');
        } finally {
            if (!skipLoadingState) {
                setIsDestLoading(false);
            }
        }
    };

    const createSession = async () => {
        try {
            const token = await getAuthToken();
            // Map content type to match backend expectations
            const contentTypeMap = {
                "Uploads": "STORIES",
                "Spotlights": "SPOTLIGHT",
                "Saved": "SAVED",
                "Content_Dump": "CONTENT_DUMP"
            };
            const mappedContentType = contentTypeMap[destSelectedContentType] || destSelectedContentType;
            
            const response = await fetch(`${API_BASE_URL}/api/cdn-mongo/create-session`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    client_id: destSelectedClient,
                    content_type: mappedContentType
                })
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            if (result.status === 'success') {
                // Refresh the sessions list for the right gallery
                await selectUser(destSelectedClient, true);
            } else {
                showError('Failed to create session');
            }
        } catch (error) {
            console.error('Error creating session:', error);
            showError('Failed to create session');
        }
    };

    const toggleOrganizeMode = () => {
        if (isOrganizeMode) {
            // Reset organize mode state
            setHasUnsavedChanges(false);
            setDraggedFile(null);
            setDragOverFile(null);
        }
        setIsOrganizeMode(!isOrganizeMode);
    };

    const handleFileDragStart = (file) => {
        if (!isOrganizeMode) return;
        setDraggedFile(file);
    };

    const handleFileDragOver = (e, file) => {
        if (!isOrganizeMode) return;
        e.preventDefault();
        if (file !== dragOverFile) {
            setDragOverFile(file);
        }
    };

    const handleFileDragEnd = () => {
        if (!isOrganizeMode) return;
        setDraggedFile(null);
        setDragOverFile(null);
    };

    const handleFileDrop = (e, targetFile) => {
        if (!isOrganizeMode || !draggedFile || draggedFile === targetFile) return;
        e.preventDefault();

        const sourceIndex = destFiles.findIndex(f => f.name === draggedFile.name);
        const targetIndex = destFiles.findIndex(f => f.name === targetFile.name);
        
        if (sourceIndex === -1 || targetIndex === -1) return;

        const newFiles = [...destFiles];
        const [movedFile] = newFiles.splice(sourceIndex, 1);
        newFiles.splice(targetIndex, 0, movedFile);
        
        // Update sequence numbers in the UI to match new order
        const updatedFiles = newFiles.map((file, index) => ({
            ...file,
            seq_number: index + 1
        }));
        
        setDestFiles(updatedFiles);
        setHasUnsavedChanges(true);
        setDraggedFile(null);
        setDragOverFile(null);
    };

    const saveOrganizedFiles = async () => {
        try {
            const token = await getAuthToken();
            
            // Create updates array with files in their current visual order
            const updates = destFiles.map((file, index) => ({
                file_name: file.name,
                seq_number: index + 1
            }));

            const response = await fetch(`${API_BASE_URL}/api/cdn-mongo/update-sequence-numbers`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    client_id: destSelectedClient,
                    session_id: currentDestSession,
                    file_updates: updates
                })
            });

            if (!response.ok) {
                throw new Error('Failed to save file order');
            }

            // Refresh the gallery to show updated sequence numbers
            await loadDestGallery(currentDestSession, true);
            setHasUnsavedChanges(false);
            toast.success('File order saved successfully');
        } catch (error) {
            console.error('Error saving file order:', error);
            toast.error('Failed to save file order');
        }
    };

    const toggleThumbnailMode = () => {
        if (isThumbnailMode) {
            // Reset thumbnail mode state
            setHasUnsavedThumbnailChanges(false);
            setSelectedThumbnails(new Set());
        }
        setIsThumbnailMode(!isThumbnailMode);
    };

    const handleThumbnailClick = (file) => {
        if (!isThumbnailMode) return;
        
        const newSelectedThumbnails = new Set(selectedThumbnails);
        if (newSelectedThumbnails.has(file.name)) {
            newSelectedThumbnails.delete(file.name);
        } else {
            newSelectedThumbnails.add(file.name);
        }
        
        setSelectedThumbnails(newSelectedThumbnails);
        setHasUnsavedThumbnailChanges(true);
    };

    const saveThumbnailChanges = async () => {
        try {
            const token = await getAuthToken();
            
            // Process each selected thumbnail
            const promises = Array.from(selectedThumbnails).map(fileName => 
                fetch(`${API_BASE_URL}/api/cdn-mongo/toggle-thumbnail/${destSelectedClient}/${currentDestSession}/${fileName}`, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                })
            );

            await Promise.all(promises);

            // Refresh the gallery to show updated thumbnail statuses
            await loadDestGallery(currentDestSession, true);
            setHasUnsavedThumbnailChanges(false);
            setSelectedThumbnails(new Set());
            toast.success('Thumbnails updated successfully');
        } catch (error) {
            console.error('Error saving thumbnails:', error);
            toast.error('Failed to update thumbnails');
        }
    };

    const handleAddComment = () => {
        if (!selectedFile && !selectedDestFile) {
            toast.error('Please select a file first');
            return;
        }
        const fileToComment = selectedFile || selectedDestFile;
        setSelectedFileForComment(fileToComment);
        setCommentText(fileToComment.caption || '');
        setIsCommentModalOpen(true);
    };

    const handleCommentSubmit = async () => {
        try {
            if (!selectedFileForComment) return;

            // First determine which context (source or destination) the file is from
            let clientId, contentType;
            
            // Check if the file exists in the source files
            const isInSourceFiles = files.some(f => f.name === selectedFileForComment.name);
            // Check if the file exists in the destination files
            const isInDestFiles = destFiles.some(f => f.name === selectedFileForComment.name);
            
            if (isInSourceFiles) {
                clientId = selectedClient;
                contentType = selectedContentType;
            } else if (isInDestFiles) {
                clientId = destSelectedClient;
                contentType = destSelectedContentType;
            } else {
                throw new Error('Could not determine file context');
            }

            if (!clientId || !contentType) {
                throw new Error('Missing client ID or content type');
            }

            // Get the session ID from the file's session info
            const sessionId = selectedFileForComment.session_info.session_id;
            if (!sessionId) {
                throw new Error('Missing session ID');
            }

            // Map collection type to content type
            const contentTypeMap = {
                "Uploads": "STORIES",
                "Spotlights": "SPOTLIGHT",
                "Saved": "SAVED",
                "Content_Dump": "CONTENT_DUMP"
            };
            const mappedContentType = contentTypeMap[contentType] || contentType;

            // Construct the file path in the correct format: sc/clientId/contentType/sessionId/fileName
            const filePath = `${clientId}/${mappedContentType}/${sessionId}/${selectedFileForComment.name}`;

            // Log the path construction details for debugging
            console.log('Update caption request details:', {
                clientId,
                contentType,
                mappedContentType,
                sessionId,
                fileName: selectedFileForComment.name,
                constructedPath: filePath,
                isInSourceFiles,
                isInDestFiles,
                sourceFiles: files.map(f => f.name),
                destFiles: destFiles.map(f => f.name)
            });

            const token = await getAuthToken();
            const response = await fetch(`${API_BASE_URL}/api/cdn-mongo/update-caption`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    file_path: filePath,
                    caption: commentText
                })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(`${response.status}: ${errorData.detail || 'Failed to update comment'}`);
            }

            const result = await response.json();
            console.log('Update caption response:', result);

            // Refresh the galleries to show updated caption
            if (currentSourceSession) {
                loadGallery(currentSourceSession, true);
            }
            if (currentDestSession) {
                loadDestGallery(currentDestSession, true);
            }

            toast.success('Comment updated successfully');
            setIsCommentModalOpen(false);
            setCommentText('');
            setSelectedFileForComment(null);
        } catch (error) {
            console.error('Error updating comment:', error);
            toast.error(error.message || 'Failed to update comment');
        }
    };

    return (
        <div className="file-mover-container">
            {console.log('Files in state:', files)}
            <header className="file-mover-header">
                <div className="file-mover-nav">
                    <div className="file-mover-title">File Mover</div>
                    <div className="file-mover-actions">
                        {destSelectedClient && destSelectedContentType && (
                            <button 
                                className="create-session-button"
                                onClick={createSession}
                            >
                                + Create Session
                            </button>
                        )}
                        {currentDestSession && (
                            <>
                                <button 
                                    className={`organize-button ${isOrganizeMode ? 'active' : ''}`}
                                    onClick={toggleOrganizeMode}
                                    disabled={isThumbnailMode}
                                >
                                    {isOrganizeMode ? 'Exit Organize' : 'Organize'}
                                </button>
                                <button 
                                    className={`thumbnail-button ${isThumbnailMode ? 'active' : ''}`}
                                    onClick={toggleThumbnailMode}
                                    disabled={isOrganizeMode}
                                >
                                    {isThumbnailMode ? 'Exit Thumbnail' : 'Set Thumbnails'}
                                </button>
                                <button
                                    className="add-comment-button"
                                    onClick={handleAddComment}
                                    disabled={!selectedFile && !selectedDestFile}
                                >
                                    Add Comment
                                </button>
                            </>
                        )}
                        {isOrganizeMode && hasUnsavedChanges && (
                            <button 
                                className="save-changes-button"
                                onClick={saveOrganizedFiles}
                            >
                                Save Changes
                            </button>
                        )}
                        {isThumbnailMode && hasUnsavedThumbnailChanges && (
                            <button 
                                className="save-changes-button"
                                onClick={saveThumbnailChanges}
                            >
                                Save Thumbnails
                            </button>
                        )}
                    </div>
                </div>
            </header>

            <div className="file-mover-layout">
                {/* Left Tree */}
                <div className="file-mover-tree-left">
                    {!selectedContentType ? (
                        <div className="file-mover-content-types">
                            <div className="destination-header">
                                <button 
                                    className="destination-back-button"
                                    onClick={navigateBack}
                                >
                                    ←
                                </button>
                                <h3>Content Types</h3>
                            </div>
                            <div className="file-mover-tree">
                                {contentTypes.map(type => (
                                    <div 
                                        key={type}
                                        className={`file-mover-tree-item ${selectedContentType === type ? 'selected' : ''}`}
                                        onClick={() => selectContentType(type)}
                                    >
                                        {type === 'Content_Dump' ? 'Dump' : type}
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : !selectedClient && users.length > 0 && (
                        <div className="file-mover-user-list">
                            <div className="destination-header">
                                <button 
                                    className="destination-back-button"
                                    onClick={navigateBack}
                                >
                                    ←
                                </button>
                                <h3>Users</h3>
                            </div>
                            {users.map(user => {
                                const firstName = user.clientInfo?.First_Legal_Name || '';
                                const lastName = user.clientInfo?.Last_Legal_Name || '';
                                const displayName = firstName && lastName ? 
                                    `${firstName} ${lastName}` : 
                                    user.client_ID;
                                
                                return (
                                    <div 
                                        key={user.client_ID}
                                        className={`file-mover-user-item ${selectedClient === user.client_ID ? 'selected' : ''}`}
                                        onClick={() => selectUser(user.client_ID, false)}
                                    >
                                        {displayName}
                                    </div>
                                );
                            })}
                        </div>
                    )}

                    {selectedClient && (
                        <div className="file-mover-left-container">
                            {/* Sessions list - At the top */}
                            <div className="file-mover-sessions">
                                <div className="destination-header">
                                    <button 
                                        className="destination-back-button"
                                        onClick={navigateBack}
                                    >
                                        ←
                                    </button>
                                    <h3>Sessions - {selectedContentType === 'Content_Dump' ? 'Dump' : selectedContentType}</h3>
                                </div>
                                {sessions.map((session, index) => {
                                    let displayName = session.session_id;
                                    if (selectedContentType === 'Content_Dump') {
                                        // For Content_Dump, keep the original CONTENTDUMP_ format
                                        displayName = session.session_id;
                                    } else {
                                        // Keep existing date extraction for other types
                                        const dateMatch = session.session_id.match(/F\((\d{2}-\d{2}-\d{4})\)/);
                                        if (dateMatch) {
                                            displayName = dateMatch[1];
                                        }
                                    }
                                    
                                    return (
                                        <div 
                                            key={`${session.session_id}-${session.folder_id}-${index}`}
                                            className={`session-item ${files.length > 0 && files[0].session_info?.folder_id === session.folder_id ? 'selected' : ''}`}
                                            onClick={() => loadGallery(session.session_id)}
                                        >
                                            <span className="session-date">
                                                {selectedContentType === 'Content_Dump' ? displayName : cleanSessionName(session.session_id)}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>

                            {/* Media Player Section - In the middle */}
                            <div className="media-player-section">
                                {renderMediaPlayer(selectedFile, false)}
                            </div>
                        </div>
                    )}
                </div>

                {/* File Windows */}
                <div className="file-mover-windows">
                    {/* Left Window */}
                    <div className="file-window">
                        {isSourceLoading && (
                            <div className="loading-overlay">
                                <div className="spinner"></div>
                            </div>
                        )}
                        <div className="file-window-header">
                            <div className="file-window-title">
                                Source {clientInfo && `- ${clientInfo.First_Legal_Name} ${clientInfo.Last_Legal_Name}`}
                            </div>
                            {files.length > 0 && (
                                <div className="pagination-controls">
                                    <button 
                                        className="pagination-button"
                                        onClick={goToPrevPage}
                                        disabled={currentPage === 1}
                                    >
                                        ←
                                    </button>
                                    <span className="pagination-info">
                                        {currentPage} / {totalPages}
                                    </span>
                                    <button 
                                        className="pagination-button"
                                        onClick={goToNextPage}
                                        disabled={currentPage === totalPages}
                                    >
                                        →
                                    </button>
                                </div>
                            )}
                        </div>
                        <div className="file-window-content">
                            {currentSourceSession ? (
                                <>
                                    <div className="file-mover-gallery"
                                        onDragOver={handleDragOver}
                                        onDragLeave={handleDragLeave}
                                        onDrop={(e) => handleDrop(e, currentSourceSession)}>
                                        {files.length > 0 ? getCurrentPageItems().map((file, index) => (
                                            <div 
                                                key={file.name}
                                                draggable={isOrganizeMode}
                                                onDragStart={(e) => handleFileDragStart(file)}
                                                onDragOver={(e) => handleFileDragOver(e, file)}
                                                onDragEnd={handleFileDragEnd}
                                                onDrop={(e) => handleFileDrop(e, file)}
                                                className={`file-item ${dragOverFile === file ? 'drag-over' : ''} ${draggedFile === file ? 'dragging' : ''}`}
                                            >
                                                {renderGalleryItem(file, selectedClient)}
                                            </div>
                                        )) : (
                                            <div className="file-mover-empty">
                                                <p>Drop files here</p>
                                            </div>
                                        )}
                                    </div>
                                    {selectedClient && (
                                        <div className="pinned-notes-section">
                                            <div className="pinned-notes-header">
                                                Pinned Notes
                                            </div>
                                            <div className="pinned-notes-content">
                                                <EditorNotesSection 
                                                    clientId={selectedClient} 
                                                    onNoteClick={handleFileSelect}
                                                    showPinnedOnly={true}
                                                />
                                            </div>
                                        </div>
                                    )}
                                </>
                            ) : (
                                <div className="file-mover-empty">
                                    <p>Select a session to view files</p>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Right Window */}
                    <div className="file-window">
                        {isDestLoading && (
                            <div className="loading-overlay">
                                <div className="spinner"></div>
                            </div>
                        )}
                        <div className="file-window-header">
                            <div className="file-window-title">
                                Destination {destClientInfo && `- ${destClientInfo.First_Legal_Name} ${destClientInfo.Last_Legal_Name}`}
                            </div>
                            {destFiles.length > 0 && (
                                <div className="pagination-controls">
                                    <button 
                                        className="pagination-button"
                                        onClick={goToPrevDestPage}
                                        disabled={destCurrentPage === 1}
                                    >
                                        ←
                                    </button>
                                    <span className="pagination-info">
                                        {destCurrentPage} / {destTotalPages}
                                    </span>
                                    <button 
                                        className="pagination-button"
                                        onClick={goToNextDestPage}
                                        disabled={destCurrentPage === destTotalPages}
                                    >
                                        →
                                    </button>
                                </div>
                            )}
                        </div>
                        <div className="file-window-content">
                            {currentDestSession ? (
                                <>
                                    <div className="file-mover-gallery"
                                        onDragOver={handleDragOver}
                                        onDragLeave={handleDragLeave}
                                        onDrop={(e) => handleDrop(e, currentDestSession)}>
                                        {destFiles.length > 0 ? getDestCurrentPageItems().map((file, index) => (
                                            <div 
                                                key={file.name}
                                                draggable={isOrganizeMode}
                                                onDragStart={(e) => handleFileDragStart(file)}
                                                onDragOver={(e) => handleFileDragOver(e, file)}
                                                onDragEnd={handleFileDragEnd}
                                                onDrop={(e) => handleFileDrop(e, file)}
                                                className={`file-item ${dragOverFile === file ? 'drag-over' : ''} ${draggedFile === file ? 'dragging' : ''}`}
                                            >
                                                {renderGalleryItem(file, currentDestSession)}
                                            </div>
                                        )) : (
                                            <div className="file-mover-empty">
                                                <p>Drop files here</p>
                                            </div>
                                        )}
                                    </div>
                                    {destSelectedClient && (
                                        <div className="editor-notes-section">
                                            <div className="editor-notes-header">
                                                All Notes
                                            </div>
                                            <div className="editor-notes-content">
                                                <EditorNotesSection 
                                                    clientId={destSelectedClient} 
                                                    onNoteClick={(file) => handleGalleryItemClick(file, new Event('click'), true)}
                                                    showPinnedOnly={false}
                                                />
                                            </div>
                                        </div>
                                    )}
                                </>
                            ) : (
                                <div className="file-mover-empty">
                                    <p>Select a session to view files</p>
                                </div>
                            )}
                        </div>
                    </div>
                </div>

                {/* Right Tree */}
                <div className="file-mover-tree-right">
                    {!destSelectedContentType ? (
                        <div className="file-mover-content-types">
                            <div className="destination-header">
                                <button 
                                    className="destination-back-button"
                                    onClick={navigateBackDest}
                                >
                                    ←
                                </button>
                                <h3>Content Types</h3>
                            </div>
                            <div className="file-mover-tree">
                                {contentTypes.map(type => (
                                    <div 
                                        key={type}
                                        className={`file-mover-tree-item ${destSelectedContentType === type ? 'selected' : ''}`}
                                        onClick={() => selectDestContentType(type)}
                                    >
                                        {type === 'Content_Dump' ? 'Dump' : type}
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : !destSelectedClient && destUsers.length > 0 ? (
                        <div className="file-mover-user-list">
                            <div className="destination-header">
                                <button 
                                    className="destination-back-button"
                                    onClick={navigateBackDest}
                                >
                                    ←
                                </button>
                                <h3>Users</h3>
                            </div>
                            {destUsers.map(user => {
                                const firstName = user.clientInfo?.First_Legal_Name || '';
                                const lastName = user.clientInfo?.Last_Legal_Name || '';
                                const displayName = firstName && lastName ? 
                                    `${firstName} ${lastName}` : 
                                    user.client_ID;
                                
                                return (
                                    <div 
                                        key={user.client_ID}
                                        className={`file-mover-user-item ${destSelectedClient === user.client_ID ? 'selected' : ''}`}
                                        onClick={() => selectUser(user.client_ID, true)}
                                    >
                                        {displayName}
                                    </div>
                                );
                            })}
                        </div>
                    ) : destSelectedClient && (
                        <div className="file-mover-right-container">
                            {/* Sessions list */}
                            <div className="file-mover-sessions">
                                <div className="destination-header">
                                    <button 
                                        className="destination-back-button"
                                        onClick={navigateBackDest}
                                    >
                                        ←
                                    </button>
                                    <h3>Sessions - {destSelectedContentType === 'Content_Dump' ? 'Dump' : destSelectedContentType}</h3>
                                </div>
                                {destSessions.map((session, index) => {
                                    let displayName = session.session_id;
                                    if (destSelectedContentType === 'Content_Dump') {
                                        // For Content_Dump, keep the original CONTENTDUMP_ format
                                        displayName = session.session_id;
                                    } else {
                                        // Keep existing date extraction for other types
                                        const dateMatch = session.session_id.match(/F\((\d{2}-\d{2}-\d{4})\)/);
                                        if (dateMatch) {
                                            displayName = dateMatch[1];
                                        }
                                    }
                                    
                                    return (
                                        <div 
                                            key={`${session.session_id}-${session.folder_id}-${index}`}
                                            className={`session-item ${destFiles.length > 0 && destFiles[0].session_info?.folder_id === session.folder_id ? 'selected' : ''}`}
                                            onClick={() => loadDestGallery(session.session_id)}
                                        >
                                            <span className="session-date">
                                                {destSelectedContentType === 'Content_Dump' ? displayName : cleanSessionName(session.session_id)}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>

                            {/* Media Player Section - In the middle */}
                            <div className="media-player-section">
                                {renderMediaPlayer(selectedDestFile, true)}
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* Add Comment Modal */}
            {isCommentModalOpen && (
                <div className="comment-modal-overlay" onClick={() => setIsCommentModalOpen(false)}>
                    <div className="comment-modal" onClick={e => e.stopPropagation()}>
                        <div className="comment-modal-header">
                            Add Comment
                        </div>
                        <textarea
                            className="comment-modal-textarea"
                            value={commentText}
                            onChange={(e) => setCommentText(e.target.value)}
                            placeholder="Enter your comment..."
                            autoFocus
                        />
                        <div className="comment-modal-actions">
                            <button 
                                className="comment-modal-cancel"
                                onClick={() => setIsCommentModalOpen(false)}
                            >
                                Cancel
                            </button>
                            <button 
                                className="comment-modal-submit"
                                onClick={handleCommentSubmit}
                            >
                                Submit
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export { FileMover }; 