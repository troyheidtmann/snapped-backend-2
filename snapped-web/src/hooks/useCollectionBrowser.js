/**
 * @fileoverview Custom hook for browsing and managing collections of media content.
 * Provides functionality for navigating through collection hierarchies and managing media arrays.
 */

import { useState, useCallback } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../config/api';

/**
 * @typedef {Object} MediaItem
 * @property {string} id - Unique identifier for the media item
 * @property {string} type - Type of media (video, image, etc.)
 * @property {string} url - URL to access the media
 * @property {string} name - Name of the media file
 * @property {Object} metadata - Additional metadata about the media
 */

/**
 * @typedef {Object} CollectionArray
 * @property {string} id - Unique identifier for the array
 * @property {string} name - Name of the array
 * @property {Array<MediaItem>} items - Media items in the array
 */

/**
 * @typedef {Object} CollectionBrowserState
 * @property {string|null} currentCollection - Currently selected collection
 * @property {string|null} currentPath - Current path within the collection
 * @property {Array<CollectionArray>} arrays - Arrays in the current path
 * @property {Array<MediaItem>} media - Media items in the current path
 * @property {boolean} isLoading - Loading state
 * @property {string|null} error - Error message if any
 */

/**
 * Custom hook for browsing and managing collections
 * @returns {Object} Collection browser state and functions
 */
export const useCollectionBrowser = () => {
    const [currentCollection, setCurrentCollection] = useState(null);
    const [currentPath, setCurrentPath] = useState(null);
    const [arrays, setArrays] = useState([]);
    const [media, setMedia] = useState([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);

    /**
     * Loads content for the current collection and path
     * @async
     * @function loadContent
     * @returns {Promise<void>}
     */
    const loadContent = useCallback(async () => {
        if (!currentCollection) return;
        
        setIsLoading(true);
        setError(null);
        
        try {
            const response = await axios.get(
                API_ENDPOINTS.CDN.COLLECTION(currentCollection),
                { params: { path: currentPath } }
            );
            
            setArrays(response.data.arrays);
            setMedia(response.data.media);
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to load content');
            console.error('Error loading collection content:', err);
        } finally {
            setIsLoading(false);
        }
    }, [currentCollection, currentPath]);

    /**
     * Navigates into a specific array in the collection
     * @function navigateInto
     * @param {number} arrayIndex - Index of the array to navigate into
     */
    const navigateInto = useCallback((arrayIndex) => {
        const newPath = currentPath 
            ? `${currentPath}.${arrayIndex}`
            : String(arrayIndex);
        setCurrentPath(newPath);
    }, [currentPath]);

    /**
     * Navigates up one level in the collection hierarchy
     * @function navigateUp
     */
    const navigateUp = useCallback(() => {
        if (!currentPath) {
            setCurrentCollection(null);
            return;
        }
        const parts = currentPath.split('.');
        parts.pop();
        setCurrentPath(parts.length ? parts.join('.') : null);
    }, [currentPath]);

    /**
     * Selects a collection to browse
     * @function selectCollection
     * @param {string} collectionName - Name of the collection to select
     */
    const selectCollection = useCallback((collectionName) => {
        setCurrentCollection(collectionName);
        setCurrentPath(null);
        setArrays([]);
        setMedia([]);
    }, []);

    return {
        currentCollection,
        currentPath,
        arrays,
        media,
        isLoading,
        error,
        loadContent,
        navigateInto,
        navigateUp,
        selectCollection
    };
}; 