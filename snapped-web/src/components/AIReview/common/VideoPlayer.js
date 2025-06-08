/**
 * @fileoverview A reusable video player component that renders an HTML5 video element
 * with consistent styling and aspect ratio.
 */

/**
 * @typedef {Object} VideoPlayerProps
 * @property {string} url - The URL of the video to be played
 */

/**
 * A video player component that maintains a 9:16 aspect ratio and responsive sizing.
 * 
 * @param {VideoPlayerProps} props - The component props
 * @returns {React.ReactElement|null} Returns null if no URL is provided
 */
import React from 'react';

const VideoPlayer = ({ url }) => {
  if (!url) return null;

  return (
    <video 
      controls 
      style={{ 
        height: '600px',
        maxHeight: '80vh',
        width: 'auto',
        aspectRatio: '9/16',
        backgroundColor: '#000',
        display: 'block'
      }}
    >
      <source src={url} type="video/mp4" />
      Your browser does not support the video tag.
    </video>
  );
};

export default VideoPlayer; 