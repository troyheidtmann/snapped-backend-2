/**
 * @fileoverview Partners Modal component for managing partner information.
 * Provides functionality for adding and managing partner details in the system.
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { API_ENDPOINTS } from '../../config/api';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTimes, faTrash, faEdit } from '@fortawesome/free-solid-svg-icons';

/**
 * @typedef {Object} PartnersModalProps
 * @property {boolean} isOpen - Whether the modal is open
 * @property {Function} onClose - Callback function to close the modal
 * @property {Function} onPartnerAdded - Callback function called when a new partner is added
 */

/**
 * @typedef {Object} Partner
 * @property {string} name - Partner name
 * @property {string} email - Partner email
 * @property {string} company - Partner company name
 * @property {string} role - Partner role in the system
 */

/**
 * Partners Modal component for managing partner information.
 * Features include:
 * - Adding new partners
 * - Viewing partner details
 * - Managing partner relationships
 * 
 * @param {PartnersModalProps} props - Component props
 * @returns {React.ReactElement|null} The rendered modal or null if not open
 */
const PartnersModal = ({ isOpen, onClose, onPartnerAdded }) => {
  const [newPartner, setNewPartner] = useState('');
  const [partners, setPartners] = useState([]);

  useEffect(() => {
    if (isOpen) {
      fetchPartners();
    }
  }, [isOpen]);

  /**
   * Fetches the list of existing partners
   * @async
   * @returns {Promise<void>}
   */
  const fetchPartners = async () => {
    try {
      const response = await axios.get(API_ENDPOINTS.PARTNERS.LIST);
      setPartners(response.data);
    } catch (error) {
      console.error('Error fetching partners:', error);
    }
  };

  /**
   * Handles form submission for adding a new partner
   * @async
   * @param {React.FormEvent} e - Form event
   * @returns {Promise<void>}
   */
  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const response = await axios.post(API_ENDPOINTS.PARTNERS.ADD, {
        name: newPartner
      });
      if (onPartnerAdded) {
        onPartnerAdded(response.data);
      }
      setPartners(prev => [...prev, response.data]);
      setNewPartner('');
    } catch (error) {
      console.error('Error adding partner:', error);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="partners-modal-overlay" onClick={(e) => {
      if (e.target === e.currentTarget) {
        onClose();
      }
    }}>
      <div className="partners-modal" onClick={(e) => e.stopPropagation()}>
        <h2>Manage Partners</h2>
        <button onClick={(e) => {
          e.stopPropagation();
          onClose();
        }} className="close-button">
          <FontAwesomeIcon icon={faTimes} />
        </button>

        <form onSubmit={(e) => {
          e.preventDefault();
          e.stopPropagation();
          handleSubmit(e);
        }} className="add-partner-form">
          <input
            type="text"
            value={newPartner}
            onChange={(e) => setNewPartner(e.target.value)}
            placeholder="Enter partner name"
            required
          />
          <button type="submit" className="add-partner-button">Add Partner</button>
        </form>

        <div className="partners-list">
          {partners.map(partner => (
            <div key={partner.id} className="partner-item">
              <span className="partner-name">{partner.name}</span>
              <div className="partner-actions">
                <button className="edit-button" title="Edit">
                  <FontAwesomeIcon icon={faEdit} />
                </button>
                <button className="delete-button" title="Delete">
                  <FontAwesomeIcon icon={faTrash} />
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default PartnersModal; 