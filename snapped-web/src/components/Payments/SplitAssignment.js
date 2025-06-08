/**
 * @fileoverview Split Assignment component for managing payment splits between payees.
 * Provides functionality for assigning and managing revenue sharing between multiple parties.
 */

import React, { useState, useEffect } from 'react';
import {
  Select,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  MenuItem,
  Alert,
  Snackbar
} from '@mui/material';
import { Add as AddIcon, Save as SaveIcon } from '@mui/icons-material';
import { API_ENDPOINTS } from '../../config/api';
import { useAuth } from '../../contexts/AuthContext';
import './SplitAssignment.css';

/**
 * @typedef {Object} Split
 * @property {string} payeeId - ID of the payee
 * @property {string} payeeName - Name of the payee
 * @property {string} companyName - Company name of the payee
 * @property {number} percentage - Split percentage
 */

/**
 * @typedef {Object} Payee
 * @property {string} _id - Unique identifier
 * @property {string} name - Payee name
 * @property {string} company_name - Company name
 */

/**
 * @typedef {Object} Payout
 * @property {string} payout_email - Email associated with the payout
 * @property {string} client_id - Client identifier
 */

/**
 * Split Assignment component for managing payment splits between payees.
 * Features include:
 * - Payout selection
 * - Split creation and management
 * - Percentage allocation
 * - Payee management
 * - Real-time validation
 * 
 * @returns {React.ReactElement} The rendered split assignment interface
 */
const SplitAssignment = () => {
  const { getAccessToken } = useAuth();
  const [selectedPayout, setSelectedPayout] = useState(null);
  const [payouts, setPayouts] = useState([]);
  const [splits, setSplits] = useState([]);
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [payees, setPayees] = useState([]);
  const [selectedPayee, setSelectedPayee] = useState(null);
  const [splitPercentage, setSplitPercentage] = useState('');
  const [loading, setLoading] = useState(false);
  const [loadingPayees, setLoadingPayees] = useState(false);
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [savingChanges, setSavingChanges] = useState(false);
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' });

  /**
   * Loads available payouts from the API
   * @async
   * @returns {Promise<void>}
   */
  const loadPayouts = async () => {
    try {
      setLoading(true);
      const token = await getAccessToken();
      console.log('Got token:', token?.substring(0, 20) + '...');
      
      const response = await fetch('/api/payments/search-payouts', {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch payouts: ${errorText}`);
      }
      
      const data = await response.json();
      console.log('Raw payout data:', data);
      
      if (!data.payouts || !Array.isArray(data.payouts)) {
        console.error('Invalid payouts data structure:', data);
        throw new Error('Invalid data structure received from server');
      }
      
      setPayouts(data.payouts);
      console.log('Payouts set in state:', data.payouts.length);
    } catch (error) {
      console.error('Error loading payouts:', error);
      setSnackbar({
        open: true,
        message: 'Failed to load payouts: ' + error.message,
        severity: 'error'
      });
    } finally {
      setLoading(false);
    }
  };

  /**
   * Loads available payees from the API
   * @async
   * @returns {Promise<void>}
   */
  const loadPayees = async () => {
    try {
      setLoadingPayees(true);
      const token = await getAccessToken();
      const response = await fetch('/api/payments/search-payees', {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch payees: ${errorText}`);
      }

      const data = await response.json();
      setPayees(data.payees || []);
    } catch (error) {
      console.error('Error loading payees:', error);
      setSnackbar({
        open: true,
        message: 'Failed to load payees: ' + error.message,
        severity: 'error'
      });
    } finally {
      setLoadingPayees(false);
    }
  };

  /**
   * Handles payout selection and loads associated splits
   * @async
   * @param {string} email - Email of the selected payout
   * @returns {Promise<void>}
   */
  const handlePayoutSelect = async (email) => {
    try {
      const payout = payouts.find(p => p.payout_email === email);
      if (!payout) {
        setSnackbar({
          open: true,
          message: 'Invalid payout selected',
          severity: 'error'
        });
        return;
      }

      setSelectedPayout(payout);
      setSplits([]);
      
      if (!payout.client_id) {
        setSnackbar({
          open: true,
          message: 'No client ID associated with this payout',
          severity: 'warning'
        });
        return;
      }

      const token = await getAccessToken();
      console.info(`Loading splits for client_id: ${payout.client_id}`);
      
      const response = await fetch(`/api/payments/split-profile/${payout.client_id}`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch splits: ${errorText}`);
      }

      const data = await response.json();
      console.info('Received splits data:', data);
      
      setSplits(data.splits || []);
      setHasUnsavedChanges(false);
    } catch (error) {
      console.error('Error loading splits:', error);
      setSnackbar({
        open: true,
        message: 'Failed to load splits: ' + error.message,
        severity: 'error'
      });
    }
  };

  /**
   * Adds a new split for the selected payout
   * @returns {void}
   */
  const handleAddSplit = () => {
    if (!selectedPayee || !splitPercentage || !selectedPayout) {
      setSnackbar({
        open: true,
        message: 'Please select a payee and enter a percentage',
        severity: 'error'
      });
      return;
    }

    const percentage = parseInt(splitPercentage);
    if (isNaN(percentage) || percentage <= 0 || percentage > 100) {
      setSnackbar({
        open: true,
        message: 'Please enter a valid percentage between 1 and 100',
        severity: 'error'
      });
      return;
    }

    const newSplit = {
      payeeId: selectedPayee._id,
      payeeName: selectedPayee.name,
      companyName: selectedPayee.company_name || '',
      percentage: percentage
    };

    setSplits(prev => [...prev, newSplit]);
    setIsModalVisible(false);
    setHasUnsavedChanges(true);
  };

  /**
   * Saves all splits for the current payout
   * @async
   * @returns {Promise<void>}
   */
  const handleSaveAllSplits = async () => {
    if (!selectedPayout || !splits.length) {
      setSnackbar({
        open: true,
        message: 'No splits to save',
        severity: 'error'
      });
      return;
    }

    try {
      setSavingChanges(true);
      const token = await getAccessToken();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.SAVE_SPLIT_PROFILE, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          clientId: selectedPayout.client_id,
          splits: splits
        })
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to save splits: ${errorText}`);
      }

      setSnackbar({
        open: true,
        message: 'All splits saved successfully',
        severity: 'success'
      });
      setHasUnsavedChanges(false);
    } catch (error) {
      console.error('Error saving splits:', error);
      setSnackbar({
        open: true,
        message: 'Failed to save splits: ' + error.message,
        severity: 'error'
      });
    } finally {
      setSavingChanges(false);
    }
  };

  /**
   * Removes a split for a specific payee
   * @async
   * @param {string} payeeId - ID of the payee to remove
   * @returns {Promise<void>}
   */
  const handleRemoveSplit = async (payeeId) => {
    try {
      if (!selectedPayout?.client_id) {
        setSnackbar({
          open: true,
          message: 'No client ID available',
          severity: 'error'
        });
        return;
      }

      const token = await getAccessToken();
      const response = await fetch(API_ENDPOINTS.PAYMENTS.DELETE_SPLIT_PROFILE(selectedPayout.client_id, payeeId), {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to delete split: ${errorText}`);
      }

      setSplits(prev => prev.filter(split => split.payeeId !== payeeId));
      setSnackbar({
        open: true,
        message: 'Split deleted successfully',
        severity: 'success'
      });
    } catch (error) {
      console.error('Error deleting split:', error);
      setSnackbar({
        open: true,
        message: 'Failed to delete split: ' + error.message,
        severity: 'error'
      });
    }
  };

  // Load initial data
  useEffect(() => {
    loadPayouts();
    loadPayees();
  }, []);

  return (
    <div className="split-assignment-container">
      <div className="controls-section">
        <Select
          fullWidth
          value={selectedPayout?.payout_email || ''}
          onChange={(e) => handlePayoutSelect(e.target.value)}
          disabled={loading}
          displayEmpty
          sx={{ mb: 2 }}
        >
          <MenuItem value="" disabled>
            Select a payout
          </MenuItem>
          {payouts.map((payout) => (
            <MenuItem key={payout.payout_email} value={payout.payout_email}>
              {payout.payout_email}
            </MenuItem>
          ))}
        </Select>

        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setIsModalVisible(true)}
          disabled={!selectedPayout}
          sx={{ mr: 1 }}
        >
          Add Split
        </Button>

        <Button
          variant="contained"
          color="primary"
          startIcon={<SaveIcon />}
          onClick={handleSaveAllSplits}
          disabled={!hasUnsavedChanges || savingChanges}
        >
          Save All
        </Button>
      </div>

      <TableContainer component={Paper} sx={{ mt: 2 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Payee Name</TableCell>
              <TableCell>Company</TableCell>
              <TableCell>Percentage</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {splits.map((split) => (
              <TableRow key={split.payeeId}>
                <TableCell>{split.payeeName}</TableCell>
                <TableCell>{split.companyName}</TableCell>
                <TableCell>{split.percentage}%</TableCell>
                <TableCell>
                  <Button
                    variant="outlined"
                    color="error"
                    size="small"
                    onClick={() => handleRemoveSplit(split.payeeId)}
                  >
                    Remove
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog
        open={isModalVisible}
        onClose={() => setIsModalVisible(false)}
      >
        <DialogTitle>Add Split</DialogTitle>
        <DialogContent>
          <Select
            fullWidth
            value={selectedPayee?._id || ''}
            onChange={(e) => setSelectedPayee(payees.find(p => p._id === e.target.value))}
            sx={{ mb: 2, mt: 1 }}
            displayEmpty
          >
            <MenuItem value="" disabled>
              Select a payee
            </MenuItem>
            {payees.map((payee) => (
              <MenuItem key={payee._id} value={payee._id}>
                {payee.name} ({payee.company_name || 'No Company'})
              </MenuItem>
            ))}
          </Select>

          <TextField
            fullWidth
            label="Percentage"
            type="number"
            value={splitPercentage}
            onChange={(e) => setSplitPercentage(e.target.value)}
            InputProps={{
              inputProps: { min: 1, max: 100 }
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setIsModalVisible(false)}>Cancel</Button>
          <Button onClick={handleAddSplit} variant="contained" color="primary">
            Add
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          sx={{ width: '100%' }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </div>
  );
};

export default SplitAssignment; 
