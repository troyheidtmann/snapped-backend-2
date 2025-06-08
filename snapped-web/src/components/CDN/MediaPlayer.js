/**
 * @fileoverview Media player component for displaying images and videos.
 * Handles playback, time tracking, and media display.
 */

import React, { useRef, useState, useEffect } from 'react';

/**
 * Formats duration in seconds to MM:SS format.
 * 
 * @param {number} seconds - Duration in seconds
 * @returns {string} Formatted duration string
 */
const formatDuration = (seconds) => {
  if (!seconds) return '0:00';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins}:${secs.toString().padStart(2, '0')}`;
};

/**
 * Component for displaying and controlling media content.
 * Features:
 * - Video playback with time tracking
 * - Image display
 * - Duration formatting
 * - Media type detection
 * - Error handling
 * 
 * @param {Object} props - Component properties
 * @param {Object} props.file - File object containing media information
 * @param {Function} props.getMediaUrl - Function to get media URL
 * @returns {React.ReactElement} The media player interface
 */
const MediaPlayer = ({ file, getMediaUrl }) => {
  const videoRef = useRef(null);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  
  useEffect(() => {
    console.log('MediaPlayer file:', file);
    console.log('MediaPlayer metadata:', file?.metadata);
  }, [file]);

  /**
   * Handles video time update events.
   */
  const handleTimeUpdate = () => {
    if (videoRef.current) {
      setCurrentTime(videoRef.current.currentTime);
    }
  };

  /**
   * Handles video metadata load events.
   */
  const handleLoadedMetadata = () => {
    if (videoRef.current) {
      setDuration(videoRef.current.duration);
    }
  };

  if (!file) return null;

  const isVideo = file.name.match(/\.(mp4|webm|mov)$/i);
  
  if (isVideo) {
    return (
      <div className="video-container">
        <video
          ref={videoRef}
          className="video-player"
          src={getMediaUrl(file.path, file.name)}
          controls
          onTimeUpdate={handleTimeUpdate}
          onLoadedMetadata={handleLoadedMetadata}
        />
        {duration > 0 && (
          <div className="video-time">
            {formatDuration(currentTime)} / {formatDuration(duration)}
          </div>
        )}
      </div>
    );
  }

  return (
    <img 
      className="media-image"
      src={getMediaUrl(file.path, file.name)}
      alt={file.name}
    />
  );
};

export default MediaPlayer; 