/**
 * @fileoverview A comprehensive video review interface that allows users to review AI-generated
 * video summaries and provide structured feedback. The component includes prompt management,
 * video playback, and detailed rating systems.
 */

import React, { useState, useEffect } from 'react';
import VideoPlayer from './common/VideoPlayer';
import { API_ENDPOINTS } from '../../config/api';
import { Amplify } from 'aws-amplify';
import { getCurrentUser, fetchAuthSession } from '@aws-amplify/auth';
import './AIPromptReview.css';

/**
 * @typedef {Object} VideoReview
 * @property {string} video_id - Unique identifier for the video
 * @property {string} file_name - Original filename of the video
 * @property {string} client_id - Client identifier
 * @property {string} cdn_url - URL to the video file
 * @property {string} summary - AI-generated summary text
 */

/**
 * @typedef {Object} Prompt
 * @property {string} id - Unique identifier for the prompt
 * @property {string} prompt_id - Version identifier for the prompt
 * @property {string} text - The actual prompt text
 */

/**
 * @typedef {Object} PromptSuggestion
 * @property {string} explanation - Explanation of the suggested changes
 * @property {string} prompt_text - The suggested prompt text
 */

/**
 * @typedef {Object} Ratings
 * @property {number} theme_accuracy - Rating for theme accuracy (1-5)
 * @property {number} bullet_points_clarity - Rating for bullet points clarity (1-5)
 * @property {number} overview_completeness - Rating for overview completeness (1-5)
 * @property {number} suggestions_usefulness - Rating for suggestions usefulness (1-5)
 * @property {number} similar_ideas_relevance - Rating for similar ideas relevance (1-5)
 * @property {number} environment_accuracy - Rating for environment accuracy (1-5)
 */

/**
 * Main component for video summary review functionality.
 * Manages the review workflow including:
 * - Loading and displaying pending reviews
 * - Prompt management and generation
 * - Video playback integration
 * - Structured feedback collection
 * 
 * @returns {React.ReactElement} The rendered video summary review interface
 */
const VideoSummary = () => {
  const [loading, setLoading] = useState(false);
  const [pendingReviews, setPendingReviews] = useState([]);
  const [currentPrompt, setCurrentPrompt] = useState(null);
  const [showPromptModal, setShowPromptModal] = useState(false);
  const [promptSuggestions, setPromptSuggestions] = useState([]);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(null);
  const [generatingPrompt, setGeneratingPrompt] = useState(false);
  const [savingPrompt, setSavingPrompt] = useState(false);
  const [editedPromptText, setEditedPromptText] = useState("");

  useEffect(() => {
    if (currentPrompt) {
      setEditedPromptText(currentPrompt.text);
    }
  }, [currentPrompt]);

  // Load videos automatically on component mount
  useEffect(() => {
    handleSyncContent();
  }, []);

  const handleSyncContent = async () => {
    try {
      setLoading(true);
      
      // Get the current authenticated session
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      
      const response = await fetch(API_ENDPOINTS.VIDEO_SUMMARY.SYNC_CONTENT, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) throw new Error('Failed to sync content');
      
      const data = await response.json();
      setPendingReviews(data.videos || []);
      setCurrentPrompt(data.current_prompt);
      
    } catch (error) {
      alert('Failed to sync content: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleOpenPromptModal = async () => {
    try {
      setShowPromptModal(true);
      setPromptSuggestions([]);
      setSelectedSuggestionIndex(null);
      setGeneratingPrompt(false);
    } catch (error) {
      if (error.message.includes('unreviewed videos')) {
        alert('Please review all pending videos before generating new prompts.');
        setShowPromptModal(false);
      } else {
        alert('Error opening prompt modal: ' + error.message);
      }
    }
  };

  const handleGeneratePrompt = async () => {
    try {
      setGeneratingPrompt(true);
      setSelectedSuggestionIndex(null);
      
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      
      const response = await fetch(API_ENDPOINTS.VIDEO_SUMMARY.GENERATE_PROMPT, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        throw new Error(data.detail || 'Failed to generate prompt suggestions');
      }
      
      setPromptSuggestions(data.suggestions);
    } catch (error) {
      if (error.message.includes('unreviewed videos')) {
        alert('Please review all pending videos before generating new prompts.');
        setShowPromptModal(false);
      } else {
        alert('Failed to generate prompt suggestions: ' + error.message);
      }
    } finally {
      setGeneratingPrompt(false);
    }
  };

  const handleActivatePrompt = async () => {
    if (selectedSuggestionIndex === null) {
      alert('Please select a suggestion first');
      return;
    }

    try {
      setSavingPrompt(true);
      
      const selectedSuggestion = promptSuggestions[selectedSuggestionIndex];
      
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();
      
      const response = await fetch(API_ENDPOINTS.VIDEO_SUMMARY.ACTIVATE_PROMPT, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          prompt_text: selectedSuggestion.prompt_text
        })
      });
      
      if (!response.ok) throw new Error('Failed to save prompt');
      
      const data = await response.json();
      setCurrentPrompt({
        id: data.id,
        prompt_id: data.prompt_id,
        text: data.prompt_text
      });
      
      setShowPromptModal(false);
      alert('New prompt version activated successfully');
    } catch (error) {
      alert('Failed to activate prompt: ' + error.message);
    } finally {
      setSavingPrompt(false);
    }
  };

  return (
    <div className="videoSummaryReviewContainer_SummaryReview">
      {/* Header Section */}
      <div className="videoSummaryReviewHeaderCard_SummaryReview">
        <div className="videoSummaryReviewHeader_SummaryReview">
          <h3>Video Summary Review</h3>
        </div>
        <div className="videoSummaryReviewHeaderButtons_SummaryReview">
          <button 
            onClick={handleSyncContent} 
            disabled={loading}
            className="videoSummaryReviewSaveButton_SummaryReview"
          >
            {loading ? 'Syncing...' : 'Sync New Content'}
          </button>
          <button 
            onClick={handleOpenPromptModal}
            disabled={!currentPrompt}
            className="videoSummaryReviewGenerateButton_SummaryReview"
          >
            Prompt Controls
          </button>
        </div>
      </div>

      {/* Current Prompt Section */}
      {currentPrompt && (
        <div className="videoSummaryReviewCurrentPromptSection_SummaryReview">
          <div className="videoSummaryReviewPromptEditorHeader_SummaryReview">
            <h4>Current Prompt</h4>
            <div className="videoSummaryReviewPromptVersion_SummaryReview">
              Version: {currentPrompt.prompt_id}
            </div>
          </div>
          <div className="videoSummaryReviewPromptText_SummaryReview">
            {currentPrompt.text}
          </div>
        </div>
      )}

      {/* Prompt Generation Modal */}
      {showPromptModal && (
        <div className="videoSummaryReviewModalOverlay_SummaryReview">
          <div className="videoSummaryReviewModalContent_SummaryReview">
            <button 
              onClick={() => {
                setShowPromptModal(false);
                setPromptSuggestions([]);
                setSelectedSuggestionIndex(null);
              }}
              className="videoSummaryReviewModalCloseButton_SummaryReview"
              aria-label="Close modal"
            >
              ×
            </button>
            <h3>Prompt Generation Controls</h3>
            
            {promptSuggestions.length === 0 ? (
              <div className="videoSummaryReviewPromptControls_SummaryReview">
                <p>Click the button below to generate new prompt suggestions based on current feedback.</p>
                <button 
                  onClick={handleGeneratePrompt}
                  disabled={generatingPrompt}
                  className="videoSummaryReviewGenerateButton_SummaryReview"
                >
                  {generatingPrompt ? 'Generating...' : 'Generate New Prompt'}
                </button>
              </div>
            ) : (
              <>
                <div className="videoSummaryReviewSuggestionGrid_SummaryReview">
                  {promptSuggestions.map((suggestion, index) => (
                    <div 
                      key={index}
                      className={`videoSummaryReviewSuggestionCard_SummaryReview ${selectedSuggestionIndex === index ? 'selected' : ''}`}
                      onClick={() => setSelectedSuggestionIndex(index)}
                    >
                      <h4>Option {index + 1}</h4>
                      <p className="videoSummaryReviewPromptExplanation_SummaryReview">
                        {suggestion.explanation}
                      </p>
                      <pre className="videoSummaryReviewPromptTextPreview_SummaryReview">
                        {suggestion.prompt_text}
                      </pre>
                    </div>
                  ))}
                </div>

                <div className="videoSummaryReviewModalActions_SummaryReview">
                  <button 
                    onClick={() => {
                      setPromptSuggestions([]);
                      setSelectedSuggestionIndex(null);
                    }}
                    className="videoSummaryReviewCancelButton_SummaryReview"
                  >
                    Generate Different Options
                  </button>
                  <button 
                    onClick={() => setShowPromptModal(false)}
                    className="videoSummaryReviewCancelButton_SummaryReview"
                  >
                    Cancel
                  </button>
                  <button 
                    onClick={handleActivatePrompt}
                    disabled={savingPrompt || selectedSuggestionIndex === null}
                    className="videoSummaryReviewSaveButton_SummaryReview"
                  >
                    {savingPrompt ? 'Activating...' : 'Activate Selected Prompt'}
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Review Cards */}
      {loading ? (
        <div className="videoSummaryReviewCard_SummaryReview" style={{ textAlign: 'center', padding: '40px' }}>
          Loading...
        </div>
      ) : (
        pendingReviews.map(review => (
          <ReviewCard key={review.video_id} review={review} />
        ))
      )}
    </div>
  );
};

/**
 * Sub-component that renders an individual video review card.
 * Includes video playback, summary display, and rating interface.
 * 
 * @param {Object} props
 * @param {VideoReview} props.review - The review data to display
 * @returns {React.ReactElement} The rendered review card
 */
const ReviewCard = ({ review }) => {
  const [ratings, setRatings] = useState({
    theme_accuracy: 0,
    bullet_points_clarity: 0,
    overview_completeness: 0,
    suggestions_usefulness: 0,
    similar_ideas_relevance: 0,
    environment_accuracy: 0
  });
  const [feedback, setFeedback] = useState('');
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    try {
      setSaving(true);
      
      const { tokens } = await fetchAuthSession();
      const token = tokens.idToken.toString();

      const response = await fetch(API_ENDPOINTS.VIDEO_SUMMARY.SAVE_REVIEW(review.video_id), {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ratings,
          feedback
        })
      });

      if (!response.ok) throw new Error('Failed to save review');
      
      alert('Review saved successfully');
    } catch (error) {
      alert('Failed to save review: ' + error.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="videoSummaryReviewCard_SummaryReview">
      {/* Left Column - Review Content */}
      <div className="videoSummaryReviewContent_SummaryReview">
        <div className="videoSummaryReviewHeader_SummaryReview">
          <h3>{review.file_name}</h3>
          <div className="videoSummaryReviewClientId_SummaryReview">Client ID: {review.client_id}</div>
        </div>

        {/* Summary Display */}
        <div className="videoSummaryReviewSummarySection_SummaryReview">
          <h4>Generated Summary</h4>
          <div className="videoSummaryReviewThemeText_SummaryReview">Theme: {review.summary.split('\n')[0]}</div>
          <ul className="videoSummaryReviewBulletPoints_SummaryReview">
            {review.summary
              .split('\n')
              .filter(line => line.startsWith('-'))
              .slice(0, 3)
              .map((point, i) => (
                <li key={i}>{point.substring(2)}</li>
              ))}
          </ul>
          <div className="videoSummaryReviewOverviewText_SummaryReview">
            {review.summary.split('\n').find(line => line.startsWith('Overview'))}
          </div>
          <div className="videoSummaryReviewImprovementText_SummaryReview">
            {review.summary.split('\n').find(line => line.startsWith('Improvement'))}
          </div>
          <div className="videoSummaryReviewSimilarIdeas_SummaryReview">
            {review.summary
              .split('\n')
              .filter(line => line.startsWith('- [Alternative'))
              .map((idea, i) => (
                <div key={i}>{idea}</div>
              ))}
          </div>
          <div className="videoSummaryReviewEnvironmentText_SummaryReview">
            {review.summary.split('\n').find(line => line.startsWith('Environment'))}
          </div>
        </div>

        {/* Review Form */}
        <div className="videoSummaryReviewRatingGroup_SummaryReview">
          {Object.entries({
            'Theme Accuracy': 'theme_accuracy',
            'Bullet Points Clarity': 'bullet_points_clarity',
            'Overview Completeness': 'overview_completeness',
            'Suggestions Usefulness': 'suggestions_usefulness',
            'Similar Ideas Relevance': 'similar_ideas_relevance',
            'Environment Description': 'environment_accuracy'
          }).map(([label, key]) => (
            <div key={key} className="videoSummaryReviewRatingRow_SummaryReview">
              <label className="videoSummaryReviewRatingLabel_SummaryReview">{label}</label>
              <select 
                className="videoSummaryReviewRatingSelect_SummaryReview"
                value={ratings[key]} 
                onChange={e => setRatings(prev => ({ ...prev, [key]: Number(e.target.value) }))}
              >
                <option value="0">Select rating</option>
                {[1,2,3,4,5].map(num => (
                  <option key={num} value={num}>{num}</option>
                ))}
              </select>
            </div>
          ))}
        </div>

        <textarea
          className="videoSummaryReviewFeedbackTextarea_SummaryReview"
          placeholder="Additional feedback or suggestions for improvement"
          value={feedback}
          onChange={e => setFeedback(e.target.value)}
        />

        <button 
          onClick={handleSave}
          disabled={saving || Object.values(ratings).some(r => r === 0)}
          className="videoSummaryReviewSaveButton_SummaryReview"
        >
          {saving ? 'Saving...' : 'Save Review'}
        </button>
      </div>

      {/* Right Column - Video Player */}
      <div className="videoSummaryReviewVideoContainer_SummaryReview">
        <VideoPlayer url={review.cdn_url} />
      </div>
    </div>
  );
};

export default VideoSummary;