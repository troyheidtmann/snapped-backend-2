/**
 * @fileoverview Demo site component for showcasing Snapped features.
 * Provides an interactive video player interface with chapter navigation.
 */

import React, { useState, useRef } from 'react';
import ReactPlayer from 'react-player';
import './DemoSite.css';

/**
 * @typedef {Object} VideoChapter
 * @property {number} time - Start time of the chapter in seconds
 * @property {string} title - Title of the chapter
 */

/**
 * @typedef {Object} VideoData
 * @property {string} url - URL of the video
 * @property {string} title - Title of the video section
 * @property {Array<VideoChapter>} chapters - List of chapters in the video
 */

/**
 * Demo site component for showcasing Snapped features through video presentations.
 * Features:
 * - Interactive video player with custom controls
 * - Chapter-based navigation
 * - Playback speed control
 * - Progress tracking
 * - Full screen support
 * - Loading state management
 * 
 * @returns {React.ReactElement} The demo site interface
 */
const DemoSite = () => {
  const [currentVideo, setCurrentVideo] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [playbackRate, setPlaybackRate] = useState(1.25);
  const [progress, setProgress] = useState(0);
  const [loading, setLoading] = useState(false);
  const playerRef = useRef(null);
  const [isFullScreen, setIsFullScreen] = useState(false);

  // Add useEffect for title
  React.useEffect(() => {
    document.title = 'Demo Video | Snapped';
  }, []);

  const videos = [
    {
      url: 'https://snapped2.b-cdn.net/demo/finalcreator.mp4',
      title: 'Creators',
      chapters: [
        { time: 0, title: 'Onboarding' },
        { time: 81, title: 'Content Upload' },
        { time: 187, title: 'Analytics' }
      ]
    },
    {
      url: 'https://snapped2.b-cdn.net/demo/editorbayfinal.mp4',
      title: 'Editors',
      chapters: [
        { time: 84, title: 'Notes' },
        { time: 133, title: 'Spotlights' },
        { time: 148, title: 'Time Sheets' },
      ]
    },
    {
      url: 'https://snapped2.b-cdn.net/demo/fixedmanagersection_1.mp4',
      title: 'Managers',
      chapters: [
        { time: 270, title: 'Tasks' },
        { time: 300, title: 'Upload Manager' },
        { time: 417, title: 'Analytics' }
      ]
    },
    {
      url: 'https://snapped2.b-cdn.net/demo/newfeaturesfinal.mp4',
      title: 'New Features',
      chapters: [
        { time: 0, title: 'Sign Up' },
        { time: 92, title: 'AI Messages' },
        { time: 169, title: 'Approve Content' },
        { time: 252, title: 'Analytics' },
        { time: 306, title: 'Editor Notes' },
        { time: 360, title: 'Time Tracking' }
      ]
    }
  ];

  const handleProgress = (state) => {
    setProgress(state.played);
  };

  const handleSeek = (e) => {
    const bounds = e.currentTarget.getBoundingClientRect();
    const percent = (e.clientX - bounds.left) / bounds.width;
    playerRef.current.seekTo(percent);
  };

  const handleVideoChange = (index) => {
    setLoading(true);
    setCurrentVideo(index);
    setPlaying(true);  // Auto-play when switching videos
  };

  const handleReady = () => {
    setLoading(false);
  };

  const handleVideoClick = () => {
    setPlaying(!playing);
  };

  const handleFullScreen = () => {
    const wrapper = document.querySelector('.player-wrapper');
    if (!document.fullscreenElement) {
      wrapper.requestFullscreen();
      setIsFullScreen(true);
    } else {
      document.exitFullscreen();
      setIsFullScreen(false);
    }
  };

  const handleChapterClick = (time) => {
    playerRef.current.seekTo(time);
    setPlaying(true);
  };

  return (
    <div className="demo-container">
      <div className="video-selector">
        {videos.map((video, index) => (
          <button
            key={index}
            className={`video-button ${currentVideo === index ? 'active' : ''}`}
            onClick={() => handleVideoChange(index)}
          >
            {video.title}
          </button>
        ))}
      </div>

      <div className="player-wrapper">
        {loading && (
          <div className="loading-overlay">
            <div className="loader"></div>
          </div>
        )}
        <div className="frame-top"></div>
        <div className="frame-bottom"></div>
        <div className="frame-left"></div>
        <div className="frame-right"></div>
        <div className="player-container" onClick={handleVideoClick}>
          <ReactPlayer
            ref={playerRef}
            url={videos[currentVideo].url}
            playing={playing}
            playbackRate={playbackRate}
            width="100%"
            height="100%"
            controls={false}
            onProgress={handleProgress}
            onReady={handleReady}
          />
        </div>
      </div>

      <div className="chapters">
        {videos[currentVideo].chapters.map((chapter, index) => (
          <button 
            key={index} 
            className="chapter-button"
            onClick={() => handleChapterClick(chapter.time)}
          >
            {chapter.title}
          </button>
        ))}
      </div>

      <div className="progress-bar" onClick={handleSeek}>
        <div className="progress-bar-filled" style={{ width: `${progress * 100}%` }} />
      </div>

      <div className="player-controls">
        <button 
          onClick={() => setPlaying(!playing)} 
          className={`desktop-control ${playing ? 'playing' : ''}`}
        >
          {playing ? 'Pause' : 'Play'}
        </button>
        <button 
          onClick={() => playerRef.current.seekTo(playerRef.current.getCurrentTime() - 10)} 
          className="desktop-control"
        >
          -10s
        </button>
        <button 
          onClick={() => playerRef.current.seekTo(playerRef.current.getCurrentTime() + 10)} 
          className="desktop-control"
        >
          +10s
        </button>
        <button 
          onClick={() => setPlaybackRate(playbackRate === 1 ? 1.25 : 1)} 
          className={`desktop-control ${playbackRate !== 1 ? 'active' : ''}`}
        >
          {playbackRate}x
        </button>
        <button onClick={handleFullScreen} className="mobile-control">
          Full Screen
        </button>
      </div>
    </div>
  );
};

export default DemoSite;
