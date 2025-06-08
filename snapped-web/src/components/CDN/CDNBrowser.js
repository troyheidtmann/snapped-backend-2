/**
 * @fileoverview Main browser component for the CDN system.
 * Provides a container for browsing and managing CDN content.
 */

import React from 'react';
import { CollectionBrowser } from './CollectionBrowser';
import './styles/CollectionBrowser.css';

/**
 * Root component for the CDN browser interface.
 * Features:
 * - Collection browsing
 * - Content management
 * - Responsive layout
 * 
 * @returns {React.ReactElement} The CDN browser interface
 */
const CDNBrowser = () => {
    return (
        <div className="cdn-container">
            <CollectionBrowser />
        </div>
    );
};

export default CDNBrowser; 