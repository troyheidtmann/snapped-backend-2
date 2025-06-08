/**
 * @fileoverview Draggable file component for CDN operations.
 * Handles drag and drop functionality for file management.
 */

import React, { forwardRef } from 'react';
import { useDrag, useDrop } from 'react-dnd';
import { toast } from 'react-hot-toast';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faFile, faFolder, faImage, faVideo } from '@fortawesome/free-solid-svg-icons';

/**
 * Component for rendering a draggable file or folder item.
 * Features:
 * - Drag and drop functionality
 * - File type detection and icons
 * - Reordering support
 * - Selection handling
 * 
 * @param {Object} props - Component properties
 * @param {Object} props.file - File object containing metadata
 * @param {string} props.file.name - Name of the file
 * @param {string} props.file.type - Type of the file (file/folder)
 * @param {number} props.index - Index in the file list
 * @param {Function} props.moveFile - Handler for file movement
 * @param {Function} props.onSelect - Selection handler
 * @param {boolean} props.isSelected - Whether the file is selected
 * @param {boolean} props.isMultiSelectMode - Whether multi-select mode is active
 * @returns {React.ReactElement} The draggable file interface
 */
const DraggableFile = forwardRef(({
  file,
  index,
  onClick,
  onDragStart,
  onDragEnd,
  onMove,
  isReorderMode,
  isSelected,
  isThumbSelected,
  previewUrl,
  videoUrl,
  isRight,
  setFiles,
  currentPath,
  moveFile,
  onSelect,
  isMultiSelectMode
}, ref) => {
  const refInternal = useRef(null);

  /**
   * Determines the icon to display based on file type.
   * 
   * @param {Object} file - File object
   * @returns {Object} FontAwesome icon object
   */
  const getFileIcon = (file) => {
    if (file.type === 'folder') return faFolder;
    if (file.name.match(/\.(jpg|jpeg|png|gif)$/i)) return faImage;
    if (file.name.match(/\.(mp4|webm|mov)$/i)) return faVideo;
    return faFile;
  };

  const [{ isDragging }, dragRef] = useDrag(() => ({
    type: isReorderMode ? 'REORDER' : 'FILE',
    item: isReorderMode 
      ? { index, name: file.name }
      : {
          name: file.name,
          path: file.path,
          sourcePath: currentPath,
          type: 'file',
          url: file.url,
          match: file.match,
          file: file
        },
    collect: (monitor) => ({
      isDragging: monitor.isDragging()
    }),
    canDrag: () => isReorderMode || !isRight
  }), [file, isReorderMode, isRight, currentPath]);

  const [, dropRef] = useDrop(() => ({
    accept: 'REORDER',
    hover(draggedItem) {
      if (!draggedItem || draggedItem.index === index) return;
      onMove(draggedItem.index, index);
      draggedItem.index = index;
    }
  }), [index, onMove]);

  const renderPreview = () => {
    if (file.thumbnail) {
      return (
        <img 
          src={`data:image/jpeg;base64,${file.thumbnail}`}
          alt={file.name}
          className="file-preview-cdn"
          onError={(e) => {
            e.target.onerror = null;
            e.target.src = '/placeholder-image.png';
          }}
        />
      );
    }

    if (previewUrl) {
      return (
        <img 
          src={previewUrl} 
          alt={file.name}
          className="file-preview-cdn"
          onError={(e) => {
            e.target.onerror = null;
            e.target.src = '/placeholder-image.png';
          }}
        />
      );
    }

    if (videoUrl) {
      return (
        <video 
          className="file-preview-cdn"
          controls={false}
          muted
          preload="metadata"
        >
          <source src={videoUrl} type={`video/${file.name.split('.').pop()}`} />
        </video>
      );
    }

    return (
      <div className="file-icon">
        <FontAwesomeIcon icon={getFileIcon(file)} className="file-icon" />
      </div>
    );
  };

  return (
    <div
      ref={(node) => {
        const dragDropRef = dragRef(dropRef(node));
        if (typeof ref === 'function') {
          ref(dragDropRef);
        } else if (ref) {
          ref.current = dragDropRef;
        }
      }}
      className={`draggable-file 
        ${isDragging ? 'dragging' : ''} 
        ${isSelected ? 'selected' : ''} 
        ${isThumbSelected ? 'thumb-selected' : ''}`}
      onClick={(e) => {
        onClick(e, file);
        if (onSelect) {
          onSelect(e, file);
        }
      }}
      style={{ 
        opacity: isDragging ? 0.4 : 1,
        cursor: isReorderMode ? 'move' : 'pointer',
        transform: isDragging ? 'scale(0.95)' : 'scale(1)',
        transition: 'transform 0.2s, opacity 0.2s'
      }}
    >
      <div className="file-preview-container">
        {renderPreview()}
      </div>
      {isMultiSelectMode && (
        <input
          type="checkbox"
          checked={isSelected}
          onChange={(e) => {
            e.stopPropagation();
            if (onSelect) {
              onSelect(e, file);
            }
          }}
          className="file-checkbox"
        />
      )}
    </div>
  );
});

export default DraggableFile; 