/**
 * @fileoverview Collection browser component for the CDN system.
 * Provides a split-view interface for browsing and managing CDN collections.
 */

import React, { useEffect } from 'react';
import { useCollectionBrowser } from '../../hooks/useCollectionBrowser';
import { MediaItem } from './MediaItem';

const CollectionList = ({ onSelect }) => {
    const collections = ['Uploads', 'Spotlights', 'Saved', 'Content_Dump'];
    
    return (
        <div className="collection-list">
            {collections.map(name => (
                <button 
                    key={name}
                    onClick={() => onSelect(name)}
                    className="collection-button"
                >
                    {name}
                </button>
            ))}
        </div>
    );
};

const ContentViewer = ({ arrays, media, onNavigateInto }) => {
    return (
        <div className="content-viewer">
            {/* Arrays section */}
            {arrays.length > 0 && (
                <div className="arrays-section">
                    <h3>Arrays</h3>
                    {arrays.map((array, index) => (
                        <div 
                            key={index}
                            onClick={() => onNavigateInto(index)}
                            className="array-item"
                        >
                            Array ({array.length} items)
                        </div>
                    ))}
                </div>
            )}

            {/* Media grid */}
            {media.length > 0 && (
                <div className="media-grid">
                    {media.map((item, index) => (
                        <MediaItem
                            key={index}
                            url={item.CDN_link}
                            thumbnail={item.thumbnail}
                            caption={item.caption}
                            type={item.type}
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

/**
 * Component for browsing and managing CDN collections.
 * Features:
 * - Split-view file management
 * - Drag and drop file operations
 * - File movement between directories
 * - Error handling and user feedback
 * 
 * @returns {React.ReactElement} The collection browser interface
 */
export const CollectionBrowser = () => {
    const {
        currentCollection,
        arrays,
        media,
        isLoading,
        error,
        loadContent,
        navigateInto,
        navigateUp,
        selectCollection
    } = useCollectionBrowser();

    // Load content when collection or path changes
    useEffect(() => {
        loadContent();
    }, [currentCollection, loadContent]);

    if (error) {
        return <div className="error-message">{error}</div>;
    }

    return (
        <div className="collection-browser">
            {/* Navigation header */}
            <div className="browser-header">
                {currentCollection ? (
                    <>
                        <button onClick={navigateUp} className="back-button">
                            Back
                        </button>
                        <h2>{currentCollection}</h2>
                    </>
                ) : (
                    <h2>Collections</h2>
                )}
            </div>

            {/* Loading state */}
            {isLoading ? (
                <div className="loading">Loading...</div>
            ) : (
                <>
                    {!currentCollection ? (
                        <CollectionList onSelect={selectCollection} />
                    ) : (
                        <ContentViewer
                            arrays={arrays}
                            media={media}
                            onNavigateInto={navigateInto}
                        />
                    )}
                </>
            )}
        </div>
    );
}; 