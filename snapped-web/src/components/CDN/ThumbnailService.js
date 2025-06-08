/**
 * @fileoverview Service for managing thumbnail generation and caching in the CDN system.
 * Handles thumbnail creation, caching, and error management for media files.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';

/**
 * Service class for managing thumbnail operations.
 * Features:
 * - Thumbnail generation and caching
 * - Error handling and retry logic
 * - Cache management and cleanup
 * - Request deduplication
 */
class ThumbnailService {
  static thumbnailCache = new Map();
  static pendingRequests = new Map();
  static errorCache = new Map();  // Add error caching
  
  /**
   * Generates a thumbnail for a video file.
   * Features:
   * - Caching with 5-minute error cooldown
   * - Request deduplication
   * - Error handling with retry logic
   * 
   * @param {string} path - Path to the video file
   * @param {Object} file - File object containing metadata
   * @param {string} file.name - Name of the video file
   * @returns {Promise<string|null>} URL of the generated thumbnail or null if generation fails
   */
  static async generateThumbnail(path, file) {
    // Validate inputs
    if (!path || !file || !file.name) {
      console.warn('Invalid input to generateThumbnail:', { path, file });
      return null;
    }

    const cacheKey = `${path}/${file.name}`;
    
    // Check cache
    if (this.thumbnailCache.has(cacheKey)) {
      return this.thumbnailCache.get(cacheKey);
    }

    // Check error cache - don't retry failed requests for 5 minutes
    const errorEntry = this.errorCache.get(cacheKey);
    if (errorEntry && (Date.now() - errorEntry.timestamp) < 300000) {
      return null;
    }

    // Check if there's already a pending request for this thumbnail
    if (this.pendingRequests.has(cacheKey)) {
      return this.pendingRequests.get(cacheKey);
    }

    // Create a new request promise
    const requestPromise = (async () => {
      try {
        console.log(`Requesting thumbnail for: ${cacheKey}`); // Debug log
        const response = await axios.post(API_ENDPOINTS.CDN.GENERATE_THUMBNAIL, {
          video_path: path,
          video_name: file.name
        });
        
        if (response.data.status === 'success' && response.data.thumbnailUrl) {
          this.thumbnailCache.set(cacheKey, response.data.thumbnailUrl);
          this.errorCache.delete(cacheKey);  // Clear any previous errors
          return response.data.thumbnailUrl;
        }
        return null;
      } catch (error) {
        console.error('Thumbnail generation failed:', error);
        // Cache the error
        this.errorCache.set(cacheKey, { timestamp: Date.now(), error: error.message });
        return null;
      } finally {
        // Clean up pending request
        this.pendingRequests.delete(cacheKey);
      }
    })();

    // Store the pending request
    this.pendingRequests.set(cacheKey, requestPromise);
    return requestPromise;
  }

  /**
   * Gets the URL for a thumbnail based on file path and name.
   * 
   * @param {string} path - Path to the file
   * @param {Object} file - File object containing metadata
   * @param {string} file.name - Name of the file
   * @returns {string} URL to the thumbnail
   */
  static getThumbnailUrl(path, file) {
    return `${API_ENDPOINTS.CDN.MEDIA_URL}/${path.replace(/^\/+|\/+$/g, '')}/.thumbnails/${file.name}`;
  }

  /**
   * Clears all cached thumbnails and pending requests.
   */
  static clearCache() {
    this.thumbnailCache.clear();
    this.pendingRequests.clear();
    this.errorCache.clear();
  }

  /**
   * Clears cached thumbnails for a specific path.
   * 
   * @param {string} path - Path to clear cache for
   */
  static clearCacheForPath(path) {
    // Clear cache entries that start with this path
    for (const key of this.thumbnailCache.keys()) {
      if (key.startsWith(path)) {
        this.thumbnailCache.delete(key);
        this.errorCache.delete(key);
      }
    }
  }
}

export default ThumbnailService; 