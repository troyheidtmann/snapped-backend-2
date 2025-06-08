/**
 * @fileoverview Service for managing video-related operations in the CDN system.
 * Handles video metadata retrieval and processing.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';

/**
 * Service class for managing video operations.
 * Features:
 * - Video duration retrieval
 * - Error handling
 * - API integration
 */
class VideoService {
  /**
   * Retrieves the duration of a video by its ID.
   * Features:
   * - Duration fetching from backend
   * - Error handling with fallback
   * 
   * @param {string} videoId - Unique identifier for the video
   * @returns {Promise<number|null>} Video duration in seconds or null if retrieval fails
   */
  static async getVideoDuration(videoId) {
    try {
      const response = await axios.get(API_ENDPOINTS.CDN.VIDEO_DURATION(videoId));
      if (response.data.status === 'success') {
        return response.data.duration;
      }
      return null;
    } catch (error) {
      console.error('Error fetching video duration:', error);
      return null;
    }
  }
}

export default VideoService; 