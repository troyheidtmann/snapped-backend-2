/**
 * @fileoverview CDN-specific media player component.
 * Handles direct CDN URL playback and display.
 */

import React, { memo } from 'react';

/**
 * Component for displaying media content from CDN URLs.
 * Features:
 * - Video playback with native controls
 * - Image display
 * - Media type detection
 * - Error handling
 * 
 * @param {Object} props - Component properties
 * @param {Object} props.file - File object containing media information
 * @param {string} props.currentPath - Current directory path
 * @param {Function} props.getMediaUrl - Function to get media URL
 * @returns {React.ReactElement|null} The media player interface or null if no file
 */
const MediaPlayer = memo(({ file, currentPath, getMediaUrl }) => {
  if (!file) return null;

  const isVideo = file.name.match(/\.(mp4|webm|mov)$/i);
  const isImage = file.name.match(/\.(jpg|jpeg|png|gif)$/i);
  
  // Use the direct CDN URL
  const mediaUrl = file.url;

  return (
    <div className="media-player">
      {isVideo ? (
        <video 
          controls
          className="video-player"
          key={file.name}
          src={file.url}
          onError={(e) => console.error('Video error:', e)}
          playsInline
          preload="metadata"
        >
          <source src={file.url} type="video/mp4" />
          Your browser does not support the video tag.
        </video>
      ) : isImage ? (
        <img 
          src={file.url}
          alt={file.name}
          className="media-image"
        />
      ) : null}
    </div>
  );
});

export default MediaPlayer; 