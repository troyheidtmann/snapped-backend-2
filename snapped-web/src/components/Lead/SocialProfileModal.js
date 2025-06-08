/**
 * @fileoverview Social Profile Modal component for displaying social media profile previews.
 * Provides an embedded view of social media profiles within a modal interface.
 */

import React from 'react';
import {
  Modal,
  Box,
  IconButton,
  Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

/**
 * Social Profile Modal component for displaying social media profiles.
 * Features include:
 * - Embedded profile preview
 * - Platform-specific display handling
 * - Responsive modal interface
 * 
 * @param {SocialProfileModalProps} props - Component props
 * @returns {React.ReactElement} The rendered modal
 */
const SocialProfileModal = ({ open, onClose, url, platform }) => {
  return (
    <Modal
      open={open}
      onClose={onClose}
      aria-labelledby="social-profile-modal"
    >
      <Box sx={{
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: '90vw',
        height: '80vh',
        bgcolor: 'background.paper',
        boxShadow: 24,
        borderRadius: 1,
        display: 'flex',
        flexDirection: 'column'
      }}>
        <Box sx={{ 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          p: 2,
          borderBottom: '1px solid rgba(0, 0, 0, 0.12)'
        }}>
          <Typography variant="h6">
            {platform} Profile
          </Typography>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
        
        <Box sx={{ 
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden'
        }}>
          <iframe
            src={url}
            style={{
              width: '100%',
              height: '100%',
              border: 'none'
            }}
            title={`${platform} Profile`}
            sandbox="allow-same-origin allow-scripts allow-popups allow-forms"
          />
          <Typography 
            variant="caption" 
            color="text.secondary" 
            sx={{ p: 1, textAlign: 'center' }}
          >
            If the content doesn't load, you can <a href={url} target="_blank" rel="noopener noreferrer">open it in a new tab</a>
          </Typography>
        </Box>
      </Box>
    </Modal>
  );
};

export default SocialProfileModal; 