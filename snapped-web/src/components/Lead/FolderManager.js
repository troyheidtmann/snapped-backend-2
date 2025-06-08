/**
 * @fileoverview Folder Manager component for handling folder operations in the CDN system.
 * Provides functionality for creating, moving, and managing folders and their contents.
 */

import React, { useState } from 'react';
import {
  Breadcrumbs,
  Button,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Typography,
  Link,
  Paper,
  Box
} from '@mui/material';
import { Folder as FolderIcon, InsertDriveFile as FileIcon, CreateNewFolder as CreateNewFolderIcon } from '@mui/icons-material';
import { API_ENDPOINTS } from '../../config/api';
import axios from 'axios';

/**
 * @typedef {Object} FolderManagerProps
 * @property {Array<Object>} contents - Array of folder contents
 * @property {Function} onRefresh - Callback function to refresh folder contents
 */

/**
 * Folder Manager component for managing folder operations.
 * Features include:
 * - Creating new folders
 * - Moving files between folders
 * - Managing folder contents
 * 
 * @param {FolderManagerProps} props - Component props
 * @returns {React.ReactElement} The rendered component
 */
export default function FolderManager({ contents, onRefresh }) {
  const [currentPath, setCurrentPath] = useState('/');
  const [selectedItems, setSelectedItems] = useState([]);
  const [showNewFolderModal, setShowNewFolderModal] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');

  /**
   * Creates a new folder with the specified name
   * @async
   * @returns {Promise<void>}
   */
  const handleCreateFolder = async () => {
    if (!newFolderName) return;
    
    try {
      const response = await fetch('/api/cdn/folder-operations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          operation: 'create_folder',
          destination_path: `${currentPath}/${newFolderName}`.replace('//', '/')
        })
      });

      if (!response.ok) throw new Error('Failed to create folder');
      
      setShowNewFolderModal(false);
      setNewFolderName('');
      onRefresh();
    } catch (error) {
      console.error('Failed to create folder:', error);
    }
  };

  /**
   * Moves selected files to a destination folder
   * @async
   * @param {string} destination - Destination folder path
   * @returns {Promise<void>}
   */
  const handleMove = async (destination) => {
    try {
      const response = await fetch('/api/cdn/folder-operations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          operation: 'move',
          source_path: currentPath,
          destination_path: destination,
          items: selectedItems
        })
      });

      if (!response.ok) throw new Error('Failed to move items');

      setSelectedItems([]);
      onRefresh();
    } catch (error) {
      console.error('Failed to move items:', error);
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Paper sx={{ p: 3, borderRadius: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
          <Breadcrumbs sx={{ flex: 1 }}>
            {currentPath.split('/').map((segment, i) => {
              const path = currentPath.split('/').slice(0, i + 1).join('/');
              return (
                <Link
                  key={i}
                  component="button"
                  variant="body1"
                  onClick={() => setCurrentPath(path)}
                  sx={{
                    cursor: 'pointer',
                    color: 'primary.main',
                    textDecoration: 'none',
                    '&:hover': {
                      textDecoration: 'underline'
                    }
                  }}
                >
                  {segment || 'Root'}
                </Link>
              );
            })}
          </Breadcrumbs>
          
          <Button
            variant="contained"
            color="primary"
            onClick={() => setShowNewFolderModal(true)}
            startIcon={<CreateNewFolderIcon />}
            sx={{ ml: 2 }}
          >
            Create Folder
          </Button>
        </Box>

        <List sx={{ 
          bgcolor: 'background.paper',
          borderRadius: 1,
          border: '1px solid',
          borderColor: 'divider'
        }}>
          {contents
            .filter(item => item.folder_path === currentPath)
            .map(item => (
              <ListItem
                key={item.path}
                button
                selected={selectedItems.includes(item.path)}
                onClick={() => {
                  if (item.type === 'folder') {
                    setCurrentPath(item.path);
                  } else {
                    setSelectedItems(prev => 
                      prev.includes(item.path) 
                        ? prev.filter(p => p !== item.path)
                        : [...prev, item.path]
                    );
                  }
                }}
                sx={{
                  borderRadius: 1,
                  mb: 0.5,
                  '&:hover': {
                    bgcolor: 'action.hover'
                  },
                  '&.Mui-selected': {
                    bgcolor: 'primary.light',
                    '&:hover': {
                      bgcolor: 'primary.light'
                    }
                  }
                }}
              >
                <ListItemIcon>
                  {item.type === 'folder' ? (
                    <FolderIcon color="primary" />
                  ) : (
                    <FileIcon color="action" />
                  )}
                </ListItemIcon>
                <ListItemText 
                  primary={item.name}
                  primaryTypographyProps={{
                    color: item.type === 'folder' ? 'primary' : 'textPrimary'
                  }}
                />
              </ListItem>
            ))}
        </List>
      </Paper>

      <Dialog
        open={showNewFolderModal}
        onClose={() => setShowNewFolderModal(false)}
        PaperProps={{
          sx: {
            borderRadius: 2,
            width: '100%',
            maxWidth: 400
          }
        }}
      >
        <DialogTitle sx={{ 
          bgcolor: 'primary.main',
          color: 'primary.contrastText'
        }}>
          Create New Folder
        </DialogTitle>
        <DialogContent sx={{ mt: 2 }}>
          <TextField
            autoFocus
            margin="dense"
            label="Folder Name"
            type="text"
            fullWidth
            value={newFolderName}
            onChange={(e) => setNewFolderName(e.target.value)}
            variant="outlined"
          />
        </DialogContent>
        <DialogActions sx={{ p: 2, pt: 0 }}>
          <Button 
            onClick={() => setShowNewFolderModal(false)}
            color="inherit"
          >
            Cancel
          </Button>
          <Button 
            onClick={handleCreateFolder}
            variant="contained"
            color="primary"
            disabled={!newFolderName}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
} 