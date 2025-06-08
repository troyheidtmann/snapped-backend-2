/**
 * @fileoverview Button component for managing multi-select operations.
 * Handles selection mode toggling and selected item actions.
 */

import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faObjectGroup } from '@fortawesome/free-solid-svg-icons';

/**
 * Button component for multi-select functionality.
 * Features:
 * - Selection mode toggle
 * - Selected item count display
 * - Batch operation trigger
 * - Visual feedback
 * 
 * @param {Object} props - Component properties
 * @param {boolean} props.active - Whether multi-select mode is active
 * @param {Function} props.onClick - Click handler for mode toggle
 * @param {number} props.selectedCount - Number of selected items
 * @param {Function} props.onMoveSelected - Handler for moving selected items
 * @returns {React.ReactElement} The multi-select button interface
 */
const MultiSelectButton = ({ active, onClick, selectedCount, onMoveSelected }) => {
  /**
   * Handles button click based on current state.
   * - If active with selections, triggers move operation
   * - Otherwise toggles selection mode
   * 
   * @param {React.MouseEvent} e - Click event
   */
  const handleClick = (e) => {
    if (active && selectedCount > 0) {
      // If we're in multi-select mode and have selections, trigger the move
      onMoveSelected();
    } else {
      // Otherwise toggle multi-select mode
      onClick(e);
    }
  };

  return (
    <button 
      className={`multi-select-button ${active ? 'active' : ''}`}
      onClick={handleClick}
      title={active ? (selectedCount > 0 ? 'Move Selected Files' : 'Exit Multi-Select') : 'Enter Multi-Select Mode'}
    >
      <FontAwesomeIcon icon={faObjectGroup} />
      {active && selectedCount > 0 && (
        <span>{selectedCount}</span>
      )}
    </button>
  );
};

export default MultiSelectButton; 