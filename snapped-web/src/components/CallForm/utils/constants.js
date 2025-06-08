/**
 * @fileoverview Constants used throughout the call form components.
 * Includes timezone options and initial form state definitions.
 */

/**
 * @typedef {Object} TimezoneOption
 * @property {string} value - The timezone code/identifier
 * @property {string} label - Human-readable timezone label
 * @property {boolean} [disabled] - Whether this option is disabled (used for separators)
 */

/**
 * Available timezone options for the form.
 * Organized by region with separators.
 * @type {TimezoneOption[]}
 */
export const TIMEZONE_OPTIONS = [
  { value: '', label: '-- Select Timezone --' },
  
  // North America
  { value: 'separator1', label: '------- North America -------', disabled: true },
  { value: 'PT', label: 'Pacific Time (PT)' },
  { value: 'MT', label: 'Mountain Time (MT)' },
  { value: 'CT', label: 'Central Time (CT)' },
  { value: 'ET', label: 'Eastern Time (ET)' },
  { value: 'AT', label: 'Atlantic Time (AT)' },
  
  // Europe
  { value: 'separator2', label: '------- Europe -------', disabled: true },
  { value: 'GMT', label: 'GMT/BST' },
  { value: 'CET', label: 'Central European (CET)' },
  { value: 'EET', label: 'Eastern European (EET)' },
  
  // Asia & Pacific
  { value: 'separator3', label: '------- Asia & Pacific -------', disabled: true },
  { value: 'GT', label: 'Gulf Time (GT)' },
  { value: 'SGT', label: 'Singapore (SGT)' },
  { value: 'JST', label: 'Japan (JST)' },
  { value: 'AEST', label: 'Australian Eastern' },
  { value: 'AWST', label: 'Australian Western' },
  { value: 'NZT', label: 'New Zealand' }
];

/**
 * Initial state for the call form.
 * Includes all possible fields with default values.
 * @type {Object}
 */
export const INITIAL_FORM_STATE = {
  client_id: '',
  First_Legal_Name: '',
  Last_Legal_Name: '',
  Email_Address: '',
  Stage_Name: '',
  DOB: '',
  IG_Username: '',
  IG_Followers: '',
  IG_Verified: false,
  IG_Engagement: '',
  TT_Username: '',
  TT_Followers: '',
  TT_Verified: false,
  YT_Username: '',
  YT_Followers: '',
  YT_Verified: false,
  Snap_Username: '',
  Snap_Followers: '',
  Snap_Star: false,
  Snap_Monetized: false,
  Timezone: '',
  IG_Rank: 0,
  IG_Views_Rank: 0,
  TT_Rank: 0,
  TT_Views_Rank: 0,
  YT_Rank: 0,
  YT_Views_Rank: 0
}; 