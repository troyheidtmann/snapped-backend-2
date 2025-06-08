/**
 * @fileoverview API endpoint configuration for the Snapped application.
 * Contains all API endpoint URLs and helper functions for constructing endpoint paths.
 * Centralizes API route management and provides a single source of truth for API URLs.
 */

// Get the API base URL from environment variable or fallback to production
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'https://track.snapped.cc';
const BUNNY_CDN_URL = 'https://snapped2.b-cdn.net';

/**
 * @namespace API_ENDPOINTS
 * @description Collection of all API endpoints used in the application.
 * Organized by feature/domain for better maintainability.
 */
export const API_ENDPOINTS = {
  /** @property {string} CALL_FORM - Endpoint for call form submissions */
  CALL_FORM: `${API_BASE_URL}/call`,
  
  /** @property {string} CALL_FORM_MOXY - Endpoint for Moxy-specific call form submissions */
  CALL_FORM_MOXY: `${API_BASE_URL}/call-moxy`,
  
  /** 
   * @function SOCIAL_STATS
   * @param {string} platform - Social media platform
   * @param {string} username - User's social media handle
   * @returns {string} Endpoint for fetching social media statistics
   */
  SOCIAL_STATS: (platform, username) => 
    `${API_BASE_URL}/api/social-stats/${platform}/${username}`,
  
  /** @property {string} UPLOAD_ACTIVITY - Endpoint for upload activity tracking */
  UPLOAD_ACTIVITY: '/api/uploadapp/upload-activity',
  
  /** @property {string} STORY_METRICS - Endpoint for story metrics and analytics */
  STORY_METRICS: '/api/uploadapp/story-metrics',
  
  /** @property {string} UPLOAD_MEDIA_DETAILS - Endpoint for media upload details */
  UPLOAD_MEDIA_DETAILS: '/api/uploadapp/upload-media-details',
  
  /** @property {string} CONTENT_DUMPS - Endpoint for content dump management */
  CONTENT_DUMPS: '/api/uploadapp/content-dumps',
  
  /** @property {string} CONTENT_DUMP_DETAILS - Endpoint for content dump details */
  CONTENT_DUMP_DETAILS: '/api/uploadapp/content-dump-details',
  
  /** @property {string} POST_ACTIVITY - Endpoint for post activity tracking */
  POST_ACTIVITY: '/api/uploadapp/post-activity',
  
  /** @property {string} POST_ACTIVITY_DETAILS - Endpoint for detailed post activity */
  POST_ACTIVITY_DETAILS: '/api/uploadapp/post-activity/details',
  
  /** 
   * @namespace LEADS
   * @description Endpoints for lead management and tracking
   */
  LEADS: {
    /** @property {string} GRID - Endpoint for lead grid view */
    GRID: '/api/leads/grid',
    /** @property {string} RAW - Endpoint for raw lead data */
    RAW: '/api/leads/raw',
    /** @property {string} RANKS - Endpoint for lead rankings */
    RANKS: '/api/leads/ranks',
    /** 
     * @function UPDATE
     * @param {string} leadId - ID of the lead to update
     * @returns {string} Lead update endpoint
     */
    UPDATE: (leadId) => `${API_BASE_URL}/api/leads/${leadId}`,
    /** 
     * @function SYNC
     * @param {string} leadId - ID of the lead to sync
     * @returns {string} Lead sync endpoint
     */
    SYNC: (leadId) => `${API_BASE_URL}/api/leads/sync/${leadId}`,
    
    /** 
     * @namespace NOTES
     * @description Endpoints for lead note management
     */
    NOTES: {
      GET: (leadId) => `${API_BASE_URL}/api/leads/notes/${leadId}`,
      CREATE: (leadId, noteType) => `${API_BASE_URL}/api/leads/notes/${leadId}/${noteType}`,
      UPDATE: (leadId, noteId) => `${API_BASE_URL}/api/leads/notes/${leadId}/${noteId}`,
      DELETE: (leadId, noteType, timestamp) => `${API_BASE_URL}/api/leads/notes/${leadId}/${noteType}/${timestamp}`
    },
    
    PAYOUT_INFO: (clientId) => `${API_BASE_URL}/api/leads/payout-info/${clientId}`,
    SYNC_PAYOUT: `${API_BASE_URL}/api/leads/sync-payout-email`,
    UPDATE_APPROVAL_STATUS: `${API_BASE_URL}/api/leads/update-approval-status`,
    APPROVAL_STATUS: (clientId) => `${API_BASE_URL}/api/leads/approval-status/${clientId}`
  },
  
  /** 
   * @namespace CONTRACTS
   * @description Endpoints for contract management
   */
  CONTRACTS: {
    SAVE_DRAFT: `${API_BASE_URL}/api/contracts/save_draft`,
    SIGN: `${API_BASE_URL}/api/contracts/sign`,
    SIGN_REP: `${API_BASE_URL}/api/contracts/sign/representative`,
    DOWNLOAD: `${API_BASE_URL}/api/contracts/download`,
    SEND_TO_CLIENT: `${API_BASE_URL}/api/contracts/send_to_client`,
    VERSIONS: (clientId) => `${API_BASE_URL}/api/contracts/versions/${clientId}`,
    CONTRACT: (clientId) => `${API_BASE_URL}/api/contracts/contract/${clientId}`
  },
  
  /** @property {string} ONBOARDING - Endpoint for user onboarding */
  ONBOARDING: `${API_BASE_URL}/api/onboarding`,
  
  /** @property {string} TASKS - Endpoint for task management */
  TASKS: `${API_BASE_URL}/api/tasks`,
  
  /** 
   * @namespace VISTA_GROUP
   * @description Endpoints for Vista group management
   */
  VISTA_GROUP: {
    CREATE: `${API_BASE_URL}/api/vista-group/create`,
  },
  
  /** 
   * @namespace TIKTOK
   * @description Endpoints for TikTok integration
   */
  TIKTOK: {
    DOWNLOAD: `${API_BASE_URL}/api/tiktok/download`
  },
  
  /** 
   * @namespace PARTNERS
   * @description Endpoints for partner management
   */
  PARTNERS: {
    LIST: '/api/partners',
    ADD: '/api/partners',
    MONETIZED: '/api/partners/monetized',
    REFERRED: '/api/partners/referred',
    GET_MONETIZED: (clientId) => `/api/partners/monetized/${clientId}`,
    GET_REFERRED: (clientId) => `/api/partners/referred/${clientId}`,
  },
  
  /** 
   * @namespace CDN
   * @description Endpoints for CDN management and operations
   */
  CDN: {
    BASE_URL: API_BASE_URL,
    CLIENT_INFO: (clientId) => `${API_BASE_URL}/api/cdn/client-info/${clientId}`,
    EDITOR_NOTES: '/api/cdn/editor-notes',
    ADD_EDITOR_NOTE: '/api/cdn/editor-notes/add',
    FOLDER_OPERATIONS: `${API_BASE_URL}/api/cdn/folder-operations`,
    DUMP_CONTENTS: `${API_BASE_URL}/api/cdn/dump-contents`,
    LIST_CONTENTS: `${API_BASE_URL}/api/cdn/list-contents`,
    SEARCH: `${API_BASE_URL}/api/cdn/search`,
    MEDIA_URL: BUNNY_CDN_URL,
    REORDER_FILES: `${API_BASE_URL}/api/cdn/reorder-files`,
    GENERATE_THUMBNAIL: `${API_BASE_URL}/api/cdn/generate-thumbnail`,
    TEST_THUMB_INSERT: `${API_BASE_URL}/api/cdn/test-thumb-insert`,
    UPDATE_CAPTION: `${API_BASE_URL}/api/cdn/update-caption`,
    SCAN_PATH: '/api/bunnyscan/scan-path',
    REMOVE_FILE_RECORD: `${API_BASE_URL}/api/cdn/remove-file-record`,
    LIST_FOLDERS: '/api/cdn-mongo/list-folders',
    FILE_GALLERY: '/api/cdn-mongo/file-gallery',
    COLLECTIONS: '/api/cdn-mongo/collections',
    COLLECTION: (name) => `/api/cdn-mongo/collections/${name}`,
  },
  
  /** 
   * @namespace CONTENT_DUMP
   * @description Endpoints for content dump operations
   */
  CONTENT_DUMP: {
    GET: (userId) => `${API_BASE_URL}/api/content-dump/${userId}`,
    CREATE: (userId) => `${API_BASE_URL}/api/content-dump/${userId}`,
    UPDATE: (userId) => `${API_BASE_URL}/api/content-dump/${userId}`,
  },
  
  /** 
   * @namespace UPLOAD
   * @description Endpoints for upload management
   */
  UPLOAD: {
    INIT_SESSION: `${API_BASE_URL}/api/uploadapp/init-session`,
    MEDIA_DETAILS: `${API_BASE_URL}/api/uploadapp/upload-media-details`,
    CONTENT_NOTES: `${API_BASE_URL}/api/uploadapp/content-notes`,
    APPROVE: `/api/uploadapp/content-notes/approve`,
    UPDATE_EDITOR_NOTE: `/api/uploadapp/update-editor-note`,
    GET_EDITOR_NOTES: `/api/uploadapp/editor-notes`,
    UPLOAD_VIDEOS: `/api/uploadapp/upload-videos`,
    SCAN_MEDIA: `${API_BASE_URL}/api/content-scan/scan-media`,
    UPDATE_FLAG_STATUS: (clientId, folderId, fileName) => 
      `${API_BASE_URL}/api/uploadapp/content-flags/${clientId}/${folderId}/${fileName}/status`,
    VIDEO_SUMMARY: (clientId, sessionId, fileName) => 
      `${API_BASE_URL}/api/uploadapp/video-summary/${clientId}/${sessionId}/${fileName}`,
    DELETE_CONTENT: (clientId, sessionFolder) =>
      `${API_BASE_URL}/api/uploadapp/delete-content/${clientId}/${sessionFolder}`,
  },
  
  /** @property {string} SPOTLIGHTS - Endpoint for spotlight content */
  SPOTLIGHTS: '/api/uploadapp/spotlights',
  
  /** @property {string} SPOTLIGHT_DETAILS - Endpoint for spotlight content details */
  SPOTLIGHT_DETAILS: '/api/uploadapp/spotlight-details',
  
  /** 
   * @namespace EMPLOYEES
   * @description Endpoints for employee management
   */
  EMPLOYEES: {
    SIGNUP: `${API_BASE_URL}/api/employees/signup`,
    PARTNER_SEARCH: (query) => `${API_BASE_URL}/api/partners/search/${query}`,
    SEARCH: `${API_BASE_URL}/api/leads/employees/search`,
    GET_ASSIGNED: `${API_BASE_URL}/api/leads/employees/assigned`,
    LIST: `${API_BASE_URL}/api/employees/`,
    GET: (userId) => `${API_BASE_URL}/api/employees/${userId}`,
    METRICS: (userId) => `${API_BASE_URL}/api/employees/${userId}/metrics`,
    TIMESHEET: (userId) => `${API_BASE_URL}/api/employees/${userId}/timesheet`,
    INVOICES: (userId) => `${API_BASE_URL}/api/employees/${userId}/invoices`,
    DOWNLOAD_INVOICE: (userId, invoiceId) => `${API_BASE_URL}/api/employees/${userId}/invoices/${invoiceId}/download`
  },
  
  /** 
   * @namespace ANALYTICS
   * @description Endpoints for analytics and reporting
   */
  ANALYTICS: {
    GET_CLIENTS: '/api/analytics/clients',
    GET_SNAPCHAT: '/api/analytics/snapchat',
    GET_SNAPCHAT_SINGLE: '/api/analytics/snapchat/single',
    UPLOAD_SNAPCHAT: '/api/analytics/upload/snapchat',
    GET_SNAPCHAT_METRICS: '/api/analytics/snapchat/metrics',
    GET_MOBILE: '/api/analytics/mobile',
    SYNC_VISTA: '/api/analytics/sync/vista'
  },
  
  /** 
   * @namespace TIMESHEET
   * @description Endpoints for timesheet management
   */
  TIMESHEET: {
    ENTRIES: '/api/timesheet/entries',
    SEARCH_ASSIGNEES: (query) => `/api/timesheet/search_assignees?query=${query}`,
    PREVIEW_INVOICE: '/api/timesheet/preview-invoice',
    SUBMIT_INVOICE: '/api/timesheet/submit-invoice',
    SET_QB_ID: '/api/timesheet/set-qb-id',
    UPDATE_ENTRY: (entryId) => `/api/timesheet/entries/${entryId}`,
    DELETE_ENTRY: (entryId) => `${API_BASE_URL}/api/timesheet/entries/${entryId}`,
  },
  
  /** 
   * @namespace DEMO
   * @description Endpoints for demo functionality
   */
  DEMO: {
    VERIFY_PASSWORD: `${API_BASE_URL}/api/demo/verify-password`,
  },
  
  /** 
   * @namespace SUPPORT
   * @description Endpoints for support functionality
   */
  SUPPORT: {
    SUBMIT: `${API_BASE_URL}/api/support/submit`,
  },
  
  /** 
   * @namespace QUEUE
   * @description Endpoints for queue management
   */
  QUEUE: {
    SPOTLIGHT: '/api/spot-queue/build',
    SAVED: '/api/saved-queue/build',
    PROCESS_SAVED: '/api/saved-queue/process-saved-make'
  },
  
  /** @property {string} SAVED_ACTIVITY - Endpoint for saved activity tracking */
  SAVED_ACTIVITY: '/api/uploadapp/saved-activity',
  
  /** @property {string} SAVED_MEDIA_DETAILS - Endpoint for saved media details */
  SAVED_MEDIA_DETAILS: '/api/uploadapp/saved-media-details',
  
  /** 
   * @namespace TWELVE_LABS
   * @description Endpoints for Twelve Labs integration
   */
  TWELVE_LABS: {
    SCAN_UPLOADS: `${API_BASE_URL}/api/twelve-labs/scan-uploads`,
    SCAN_INDEXES: `${API_BASE_URL}/api/twelve-labs/scan-for-indexes`,
    UPLOAD_VIDEOS: `${API_BASE_URL}/api/twelve-labs/upload-videos`,
    SEARCH_INDEX: `${API_BASE_URL}/api/twelve-labs/search-index`,
    SEARCH_ALL: `${API_BASE_URL}/api/twelve-labs/search-all-indexes`,
    MATCH_RESULTS: `${API_BASE_URL}/api/twelve-labs/match-search-results`,
    SUMMARIZE_VIDEOS: `${API_BASE_URL}/api/twelve-labs/summarize-videos`,
    SUMMARIZE_VIDEO: (videoId) => `${API_BASE_URL}/api/twelve-labs/summarize-video/${videoId}`,
  },
  
  /** 
   * @namespace CONTENT_SCAN
   * @description Endpoints for content scanning functionality
   */
  CONTENT_SCAN: {
    SCAN_SESSION: `${API_BASE_URL}/api/content-scan/scan-session`,
    SCAN_MEDIA: `${API_BASE_URL}/api/content-scan/scan-media`,
  },
  
  /** @property {string} CONTENT_FLAGS - Endpoint for content flag management */
  CONTENT_FLAGS: '/api/uploadapp/content-flags',
  
  /** 
   * @namespace VIDEO_SUMMARY
   * @description Endpoints for video summary functionality
   */
  VIDEO_SUMMARY: {
    SYNC_CONTENT: `${API_BASE_URL}/api/ai-review/video-summary/sync-content`,
    SAVE_REVIEW: (videoId) => `${API_BASE_URL}/api/ai-review/video-summary/save-review/${videoId}`,
    GET_PROMPTS: `${API_BASE_URL}/api/ai-review/video-summary/prompts`,
    GENERATE_PROMPT: `${API_BASE_URL}/api/ai-review/video-summary/generate-prompt`,
    ACTIVATE_PROMPT: `${API_BASE_URL}/api/ai-review/video-summary/activate-prompt`,
  },
  
  /** 
   * @namespace SURVEY
   * @description Endpoints for survey functionality
   */
  SURVEY: {
    QUESTIONS: `${API_BASE_URL}/api/client_survey/questions`,
    SUBMIT: `${API_BASE_URL}/api/client_survey/submit`,
    SUBMIT_KEY_INFO: `${API_BASE_URL}/api/client_survey/key_info`,
    RESPONSES: {
      KEY: `${API_BASE_URL}/api/client_survey/responses/key`,
      DETAILED: `${API_BASE_URL}/api/client_survey/responses/detailed`,
      USERS: `${API_BASE_URL}/api/client_survey/users`
    },
    ADMIN: {
      ADD_QUESTION: `${API_BASE_URL}/api/client_survey/questions/add`,
      UPDATE_QUESTIONS: `${API_BASE_URL}/api/client_survey/questions/update`,
      GET_SECTIONS: `${API_BASE_URL}/api/client_survey/sections`
    }
  },
  
  /** 
   * @namespace CHAT
   * @description Endpoints for chat functionality
   */
  CHAT: {
    SEND: `${API_BASE_URL}/api/chat`
  },
  
  /** 
   * @namespace DESKTOP_UPLOAD
   * @description Endpoints for desktop upload functionality
   */
  DESKTOP_UPLOAD: {
    UPLOAD: `${API_BASE_URL}/api/desktop-upload/upload`,
    PROGRESS: (fileName) => `${API_BASE_URL}/api/desktop-upload/progress/${fileName}`,
    USERS: `${API_BASE_URL}/api/desktop-upload/users`,
    FOLDERS: (userId) => `${API_BASE_URL}/api/desktop-upload/folders/${userId}`
  },
  
  /** 
   * @namespace PAYMENTS
   * @description Endpoints for payment processing
   */
  PAYMENTS: {
    CREATE_PAYEE: `${API_BASE_URL}/api/payments/payee`,
    GET_PAYEE: (email) => `${API_BASE_URL}/api/payments/payee/${email}`,
    SEARCH_PAYOUTS: `${API_BASE_URL}/api/payments/search-payouts`,
    SEARCH_PAYEES: `${API_BASE_URL}/api/payments/search-payees`,
    GET_SPLIT_PROFILE: (clientId) => `${API_BASE_URL}/api/payments/split-profile/${clientId}`,
    SAVE_SPLIT_PROFILE: `${API_BASE_URL}/api/payments/split-profile`,
    DELETE_SPLIT_PROFILE: (clientId, payeeId) => `${API_BASE_URL}/api/payments/split-profile/${clientId}/${payeeId}`,
    PAYEE_FORM: `${API_BASE_URL}/payee-form`,
    SPLITS: {
      LIST: '/api/payments/splits',
      CREATE: '/api/payments/splits',
      UPDATE: (splitId) => `/api/payments/splits/${splitId}`,
      DELETE: (splitId) => `/api/payments/splits/${splitId}`,
      GET: (splitId) => `/api/payments/splits/${splitId}`,
    },
    QUICKBOOKS: {
      SYNC: '/api/payments/quickbooks/sync',
      STATUS: (email) => `/api/payments/quickbooks/sync-status/${email}`,
      BULK_SYNC: '/api/payments/quickbooks/bulk-sync'
    }
  },
  
  /** 
   * @namespace CDN_MONGO
   * @description Endpoints for CDN MongoDB operations
   */
  CDN_MONGO: {
    GET_USERS: `${API_BASE_URL}/api/cdn-mongo/get-users`,
    LIST_FOLDERS: `${API_BASE_URL}/api/cdn-mongo/list-folders`,
    FILE_GALLERY: `${API_BASE_URL}/api/cdn-mongo/file-gallery`,
    GET_CLIENT_INFO: (clientId) => `${API_BASE_URL}/api/cdn-mongo/get-client-info/${clientId}`,
    PARENT_PATH: `${API_BASE_URL}/api/cdn-mongo/parent-path`,
    EDITOR_NOTES: (clientId) => `/api/cdn-mongo/editor-notes/${clientId}`
  },
  
  /** 
   * @namespace CLIENTS
   * @description Endpoints for client management
   */
  CLIENTS: {
    SIGNED: `${API_BASE_URL}/api/clients/signed`,
  },
  
  /** 
   * @namespace MESSAGES
   * @description Endpoints for message management
   */
  MESSAGES: {
    GET_AI_NOTES: (clientId, date) => `${API_BASE_URL}/api/messages/ai-notes/${clientId}/${date}`,
    GET_AI_TASKS: (clientId, date) => `${API_BASE_URL}/api/messages/tasks/${clientId}/${date}`
  },
};