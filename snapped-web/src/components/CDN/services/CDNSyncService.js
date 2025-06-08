/**
 * @fileoverview Service for handling CDN synchronization operations.
 * Manages queuing and execution of file operations with the backend.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../../../config/api';

/**
 * Service class for managing CDN synchronization operations.
 * Features:
 * - Operation queuing by session
 * - Data formatting for different operation types
 * - Batch processing of operations
 * - Error handling and retry logic
 */
class CDNSyncService {
  constructor() {
    this.syncQueue = new Map();
    this.isSyncing = false;
  }

  /**
   * Queues a new operation for synchronization.
   * @param {Object} operation - The operation to queue
   * @param {string} operation.session_id - Unique identifier for the session
   * @param {string} operation.operation_type - Type of operation (move, caption, reorder)
   * @param {string} operation.content_type - Content type (STORIES, SPOTLIGHT)
   * @param {Object} operation.data - Operation-specific data
   * @returns {Promise<void>}
   */
  async queueOperation(operation) {
    const { session_id, operation_type, content_type, data } = operation;
    
    if (!this.syncQueue.has(session_id)) {
      this.syncQueue.set(session_id, []);
    }

    // Format data based on content type
    const formattedData = this.formatOperationData(operation_type, content_type, data);

    this.syncQueue.get(session_id).push({
      operation_type,
      content_type,
      timestamp: new Date().toISOString(),
      data: formattedData
    });

    if (!this.isSyncing) {
      await this.startSync();
    }
  }

  /**
   * Formats operation data based on operation type.
   * @param {string} operation_type - Type of operation
   * @param {string} content_type - Type of content
   * @param {Object} data - Raw operation data
   * @returns {Object} Formatted data for backend
   */
  formatOperationData(operation_type, content_type, data) {
    // Format data to match database field capitalization
    switch (operation_type) {
      case 'move':
        return {
          source_path: data.source_path,
          destination_path: data.destination_path,
          file_name: data.file_name,
          content_type: content_type,  // STORIES or SPOTLIGHT
          is_thumbnail: data.is_thumbnail || false,
          seq_number: data.seq_number || null
        };

      case 'caption':
        return {
          file_name: data.file_name,
          caption: data.caption,
          content_type: content_type
        };

      case 'reorder':
        return {
          files: data.files.map(file => ({
            file_name: file.name,
            seq_number: file.seq_number,
            is_thumbnail: file.is_thumbnail || false
          }))
        };

      default:
        return data;
    }
  }

  /**
   * Starts the synchronization process for queued operations.
   * Features:
   * - Batch processing by session
   * - Operation grouping
   * - Error handling
   * - Auto-retry on completion
   * @returns {Promise<void>}
   */
  async startSync() {
    if (this.isSyncing) return;
    this.isSyncing = true;

    try {
      for (const [session_id, operations] of this.syncQueue) {
        if (operations.length === 0) continue;

        const groupedOps = this.groupOperations(operations);
        
        await axios.post(API_ENDPOINTS.CDN.SYNC, {
          session_id,
          operations: groupedOps
        });

        this.syncQueue.delete(session_id);
      }
    } catch (error) {
      console.error('CDN sync error:', error);
    } finally {
      this.isSyncing = false;
      
      if (this.syncQueue.size > 0) {
        await this.startSync();
      }
    }
  }

  /**
   * Groups operations by type for batch processing.
   * @param {Array<Object>} operations - List of operations to group
   * @returns {Object} Operations grouped by type
   */
  groupOperations(operations) {
    return operations.reduce((groups, op) => {
      if (!groups[op.operation_type]) {
        groups[op.operation_type] = [];
      }
      groups[op.operation_type].push(op);
      return groups;
    }, {});
  }
}

export const cdnSyncService = new CDNSyncService(); 