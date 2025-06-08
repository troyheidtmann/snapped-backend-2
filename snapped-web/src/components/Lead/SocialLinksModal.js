/**
 * @fileoverview Social Links Modal component for displaying and managing social media links.
 * Provides a modal interface for viewing and interacting with a client's social media profiles.
 */

import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Box,
  Typography,
  Link,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import InstagramIcon from '@mui/icons-material/Instagram';
import YouTubeIcon from '@mui/icons-material/YouTube';
import MusicNoteIcon from '@mui/icons-material/MusicNote'; // For TikTok
import PhotoCameraIcon from '@mui/icons-material/PhotoCamera'; // For Snapchat

/**
 * @typedef {Object} SocialLinksModalProps
 * @property {boolean} open - Whether the modal is open
 * @property {Function} onClose - Callback function to close the modal
 * @property {Object} lead - Lead/client data object containing social media information
 */

/**
 * Social Links Modal component for displaying social media links.
 * Features include:
 * - Displaying social media profile links
 * - Opening social media profiles in new tabs
 * - Showing verification status and follower counts
 * 
 * @param {SocialLinksModalProps} props - Component props
 * @returns {React.ReactElement} The rendered modal
 */
const SocialLinksModal = ({ open, onClose, lead }) => {
  if (!lead) return null;

  const socialLinks = [
    {
      platform: 'Instagram',
      username: lead.IG_Username,
      url: `https://instagram.com/${lead.IG_Username}`,
      icon: <InstagramIcon sx={{ color: '#E1306C' }} />,
      followers: parseInt(lead.IG_Followers || 0).toLocaleString(),
      verified: lead.IG_Verified,
    },
    {
      platform: 'TikTok',
      username: lead.TT_Username,
      url: `https://tiktok.com/@${lead.TT_Username}`,
      icon: <MusicNoteIcon sx={{ color: '#000000' }} />,
      followers: parseInt(lead.TT_Followers || 0).toLocaleString(),
      verified: lead.TT_Verified,
    },
    {
      platform: 'YouTube',
      username: lead.YT_Username,
      url: `https://youtube.com/@${lead.YT_Username}`,
      icon: <YouTubeIcon sx={{ color: '#FF0000' }} />,
      followers: parseInt(lead.YT_Followers || 0).toLocaleString(),
      verified: lead.YT_Verified,
    },
    {
      platform: 'Snapchat',
      username: lead.Snap_Username,
      url: `https://snapchat.com/add/${lead.Snap_Username}`,
      icon: <PhotoCameraIcon sx={{ color: '#FFFC00' }} />,
      followers: lead.Snap_Followers === "na" ? '-' : parseInt(lead.Snap_Followers || 0).toLocaleString(),
      verified: lead.Snap_Star,
    },
  ];

  return (
    <Dialog 
      open={open} 
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      onClick={(e) => e.stopPropagation()}
    >
      <DialogTitle>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">
            {lead.First_Legal_Name} {lead.Last_Legal_Name}'s Social Media
          </Typography>
          <IconButton onClick={onClose}>
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent>
        {socialLinks.map((social) => (
          social.username && (
            <Box 
              key={social.platform}
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 2,
                py: 2,
                borderBottom: '1px solid #eee',
                '&:last-child': {
                  borderBottom: 'none'
                }
              }}
            >
              {social.icon}
              <Box flex={1}>
                <Typography variant="subtitle1">
                  {social.platform}
                  {social.verified && ' ✓'}
                </Typography>
                <Link 
                  href={social.url} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  sx={{ textDecoration: 'none' }}
                >
                  @{social.username}
                </Link>
                <Typography variant="caption" display="block" color="text.secondary">
                  {social.followers} followers
                </Typography>
              </Box>
            </Box>
          )
        ))}
      </DialogContent>
    </Dialog>
  );
};

export default SocialLinksModal; 