/**
 * @fileoverview Performance calculator utilities for computing lead/client performance scores.
 * Provides functions for calculating engagement, follower counts, and overall performance metrics.
 */

import algorithmSettings from '../config/algorithmSettings.json';

/**
 * @typedef {Object} Lead
 * @property {number} IG_Followers - Instagram follower count
 * @property {number} TT_Followers - TikTok follower count
 * @property {number} YT_Followers - YouTube follower count
 * @property {boolean} IG_Verified - Instagram verification status
 * @property {boolean} TT_Verified - TikTok verification status
 * @property {boolean} YT_Verified - YouTube verification status
 * @property {number} IG_Engagement - Instagram engagement rate
 * @property {number} TT_Engagement - TikTok engagement rate
 * @property {number} YT_Engagement - YouTube engagement rate
 */

/**
 * Calculates the overall performance score for a lead
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated performance score
 */
export const calculateScore = (lead) => {
  let score = 0;
  
  // Add debugging
  console.log('Calculating score for:', lead);
  
  // Engagement Scores
  const engagementScore = calculateEngagementScore(lead);
  console.log('Engagement Score:', engagementScore);
  score += engagementScore;
  
  // Follower Base
  const followerScore = calculateFollowerScore(lead);
  console.log('Follower Score:', followerScore);
  score += followerScore;
  
  // Recent Performance
  const performanceScore = calculateRecentPerformanceScore(lead);
  console.log('Performance Score:', performanceScore);
  score += performanceScore;
  
  // Verification Points
  const verificationScore = calculateVerificationScore(lead);
  console.log('Verification Score:', verificationScore);
  score += verificationScore;
  
  // Cross-Platform Presence
  const crossPlatformScore = calculateCrossPlatformScore(lead);
  console.log('Cross Platform Score:', crossPlatformScore);
  score += crossPlatformScore;
  
  // Add Rank Score
  const rankScore = calculateRankScore(lead);
  console.log('Rank Score:', rankScore);
  score += rankScore;
  
  console.log('Final Score:', score);
  return score;
};

/**
 * Calculates the engagement score based on platform engagement rates
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated engagement score
 */
const calculateEngagementScore = (lead) => {
  let score = 0;
  const settings = algorithmSettings.engagement;

  // TikTok Engagement
  const ttEng = parseFloat(lead.TT_Engagement) || 0;
  if (ttEng > settings.tiktok.high) score += settings.tiktok.points.high;
  else if (ttEng > settings.tiktok.medium) score += settings.tiktok.points.medium;
  else if (ttEng > 0) score += settings.tiktok.points.low;

  // Instagram Engagement
  const igEng = parseFloat(lead.IG_Engagement) || 0;
  if (igEng > settings.instagram.high) score += settings.instagram.points.high;
  else if (igEng > settings.instagram.medium) score += settings.instagram.points.medium;
  else if (igEng > 0) score += settings.instagram.points.low;

  // Average Engagement
  const avgEng = parseFloat(lead.Average_Engagement) || 0;
  if (avgEng > settings.average.high) score += settings.average.points.high;
  else if (avgEng > settings.average.medium) score += settings.average.points.medium;
  else if (avgEng > 0) score += settings.average.points.low;

  return score;
};

/**
 * Calculates the follower score based on platform follower counts
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated follower score
 */
const calculateFollowerScore = (lead) => {
  let score = 0;
  const settings = algorithmSettings.followers;

  // Snapchat Followers
  const snapFol = parseInt(lead.Snapchat_Followers) || 0;
  if (snapFol > settings.snapchat.high) score += settings.snapchat.points.high;
  else if (snapFol > settings.snapchat.medium) score += settings.snapchat.points.medium;
  else if (snapFol > 0) score += settings.snapchat.points.low;

  // TikTok Followers
  const ttFol = parseInt(lead.TT_Followers) || 0;
  if (ttFol > settings.tiktok.high) score += settings.tiktok.points.high;
  else if (ttFol > settings.tiktok.medium) score += settings.tiktok.points.medium;
  else if (ttFol > 0) score += settings.tiktok.points.low;

  // Instagram Followers
  const igFol = parseInt(lead.IG_Followers) || 0;
  if (igFol > settings.instagram.high) score += settings.instagram.points.high;
  else if (igFol > settings.instagram.medium) score += settings.instagram.points.medium;
  else if (igFol > 0) score += settings.instagram.points.low;

  // YouTube Subscribers
  const ytSub = parseInt(lead.YT_Subscribers) || 0;
  if (ytSub > settings.youtube.high) score += settings.youtube.points.high;
  else if (ytSub > settings.youtube.medium) score += settings.youtube.points.medium;
  else if (ytSub > 0) score += settings.youtube.points.low;

  return score;
};

/**
 * Calculates the recent performance score based on recent metrics
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated recent performance score
 */
const calculateRecentPerformanceScore = (lead) => {
  let score = 0;
  const settings = algorithmSettings.recentPerformance;

  // TikTok Recent Performance
  const ttFol = parseInt(lead.TT_Followers) || 0;
  const ttLikes = parseInt(lead.TT_Recent_Likes) || 0;
  const ttLikeRate = ttFol > 0 ? (ttLikes / ttFol) * 100 : 0;
  
  if (ttLikeRate > settings.tiktok.high) score += settings.tiktok.points.high;
  else if (ttLikeRate > settings.tiktok.medium) score += settings.tiktok.points.medium;
  else if (ttLikeRate > settings.tiktok.low) score += settings.tiktok.points.low;

  // Instagram Recent Performance
  const igFol = parseInt(lead.IG_Followers) || 0;
  const igLikes = parseInt(lead.IG_Recent_Likes) || 0;
  const igLikeRate = igFol > 0 ? (igLikes / igFol) * 100 : 0;
  
  if (igLikeRate > settings.instagram.high) score += settings.instagram.points.high;
  else if (igLikeRate > settings.instagram.medium) score += settings.instagram.points.medium;
  else if (igLikeRate > settings.instagram.low) score += settings.instagram.points.low;

  // YouTube Recent Performance
  const ytSub = parseInt(lead.YT_Subscribers) || 0;
  const ytViews = parseInt(lead.YT_Recent_Views) || 0;
  const ytViewRate = ytSub > 0 ? (ytViews / ytSub) * 100 : 0;
  
  if (ytViewRate > settings.youtube.high) score += settings.youtube.points.high;
  else if (ytViewRate > settings.youtube.medium) score += settings.youtube.points.medium;
  else if (ytViewRate > settings.youtube.low) score += settings.youtube.points.low;

  return score;
};

/**
 * Calculates the verification score based on platform verification status
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated verification score
 */
const calculateVerificationScore = (lead) => {
  let score = 0;
  const settings = algorithmSettings.verification;

  if (lead.Snap_Star) score += settings.snapchat;
  if (lead.TT_Verified) score += settings.tiktok;
  if (lead.IG_Verified) score += settings.instagram;
  if (lead.YT_Verified) score += settings.youtube;

  return score;
};

/**
 * Calculates the cross-platform score based on presence across platforms
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated cross-platform score
 */
const calculateCrossPlatformScore = (lead) => {
  // Check if the lead has presence on all platforms
  if (lead.Snapchat_Username && 
      lead.IG_Username && 
      lead.TT_Username && 
      lead.YT_Username) {
    return algorithmSettings.crossPlatform;
  }
  return 0;
};

/**
 * Calculates the rank score based on platform rankings
 * @param {Lead} lead - Lead data object
 * @returns {number} The calculated rank score
 */
const calculateRankScore = (lead) => {
  let score = 0;
  const settings = algorithmSettings.ranks;

  // Instagram Ranks
  const igRank = parseInt(lead.IG_Rank) || 0;
  if (igRank <= settings.instagram.high) score += settings.instagram.points.high;
  else if (igRank <= settings.instagram.medium) score += settings.instagram.points.medium;
  else if (igRank > 0) score += settings.instagram.points.low;

  // TikTok Ranks
  const ttRank = parseInt(lead.TT_Rank) || 0;
  if (ttRank <= settings.tiktok.high) score += settings.tiktok.points.high;
  else if (ttRank <= settings.tiktok.medium) score += settings.tiktok.points.medium;
  else if (ttRank > 0) score += settings.tiktok.points.low;

  // YouTube Ranks
  const ytRank = parseInt(lead.YT_Rank) || 0;
  if (ytRank <= settings.youtube.high) score += settings.youtube.points.high;
  else if (ytRank <= settings.youtube.medium) score += settings.youtube.points.medium;
  else if (ytRank > 0) score += settings.youtube.points.low;

  return score;
};

/**
 * Gets the cell style based on parameter values
 * @param {Object} params - Grid cell parameters
 * @returns {Object} Cell style object
 */
export const getCellStyle = (params) => {
  const baseStyle = {
    textAlign: 'center',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%'
  };
  
  // Return only base styling for score cells
  if (params.colDef.field === 'Performance_Score') {
    return baseStyle;
  }

  // Keep other cell styling logic...
  return baseStyle;
}; 