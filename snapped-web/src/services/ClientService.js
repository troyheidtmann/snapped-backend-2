/**
 * @fileoverview Service class for managing client-related operations.
 * Provides functionality for retrieving and formatting client information.
 */

import axios from 'axios';
import { API_ENDPOINTS } from '../config/api';

/**
 * @typedef {Object} ClientInfo
 * @property {string} [First_Legal_Name] - Client's legal first name
 * @property {string} [Last_Legal_Name] - Client's legal last name
 * @property {string} [Stage_Name] - Client's stage or professional name
 * @property {Object} [metadata] - Additional client metadata
 */

/**
 * Service class for client operations
 * @class
 */
class ClientService {
  /**
   * Retrieves detailed information about a client
   * 
   * @static
   * @async
   * @param {string} clientId - The ID of the client
   * @returns {Promise<ClientInfo|null>} Client information or null if not found
   */
  static async getClientInfo(clientId) {
    if (!clientId) {
      return null;
    }

    try {
      const response = await axios.get(API_ENDPOINTS.CDN.CLIENT_INFO(clientId));
      return response.data;
    } catch (error) {
      console.error('Error fetching client info:', error);
      return null;
    }
  }

  /**
   * Retrieves and formats a client's display name
   * Prioritizes stage name over legal name, falls back to client ID
   * 
   * @static
   * @async
   * @param {string} clientId - The ID of the client
   * @returns {Promise<string>} Formatted client name or client ID if not found
   */
  static async getClientName(clientId) {
    if (!clientId) {
      return '';
    }

    try {
      const clientInfo = await this.getClientInfo(clientId);
      if (clientInfo) {
        const firstName = clientInfo.First_Legal_Name || '';
        const lastName = clientInfo.Last_Legal_Name || '';
        const stageName = clientInfo.Stage_Name;
        
        if (stageName) {
          return stageName;
        }
        return `${firstName} ${lastName}`.trim() || clientId;
      }
      return clientId;
    } catch (error) {
      console.error('Error getting client name:', error);
      return clientId;
    }
  }
}

export default ClientService; 