/**
 * @fileoverview Dropdown component for selecting clients in the analytics interface.
 * Handles client data formatting and selection state management.
 */

import React from 'react';
import './ClientSelector.css';

/**
 * @typedef {Object} Client
 * @property {string} [user_id] - Unique identifier for the user
 * @property {string} [client_id] - Alternative identifier for the client
 * @property {string} [snap_profile_name] - Snapchat profile name
 * @property {string} [username] - Username of the client
 */

/**
 * @typedef {Object} ClientSelectorProps
 * @property {Client[]} clients - Array of available clients
 * @property {string} selectedClient - Currently selected client ID
 * @property {(clientId: string) => void} onClientChange - Selection change handler
 */

/**
 * Dropdown component for selecting clients with proper formatting and fallbacks.
 * Features:
 * - Handles multiple ID formats (user_id or client_id)
 * - Displays formatted client names with platform indicator
 * - Provides fallback for unknown clients
 * - Includes debug logging for development
 * 
 * @param {ClientSelectorProps} props - Component properties
 * @returns {React.ReactElement|null} The client selector dropdown or null if no clients
 */
const ClientSelector = ({ clients, selectedClient, onClientChange }) => {
  console.log('ClientSelector props:', { clients, selectedClient });

  if (!clients?.length) {
    console.log('No clients available');
    return null;
  }

  return (
    <div className="client-selector">
      <select 
        value={selectedClient} 
        onChange={(e) => onClientChange(e.target.value)}
        className="client-select"
      >
        <option value="">Select Client</option>
        {clients.map((client) => {
          const id = client.user_id || client.client_id;
          const name = client.snap_profile_name || client.username || 'Unknown Client';
          
          console.log('Rendering client option:', { id, name, client });
          
          return (
            <option key={id} value={id}>
              {name} (Snapchat)
            </option>
          );
        })}
      </select>
    </div>
  );
};

export default ClientSelector; 