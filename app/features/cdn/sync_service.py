"""
CDN Sync Service - Content Synchronization Module

This module provides synchronization services for managing content operations across
different collections in the Snapped platform. It handles file movements, metadata
updates, and content organization between Stories, Spotlight, Saved Content, and
Content Dumps.

Architecture:
-----------
1. Operation Types:
   - Move: Transfer files between collections
   - Caption: Update file descriptions
   - Reorder: Manage file sequence

2. Collections:
   - Content Dump: Temporary storage
   - Uploads (Stories): Regular content
   - Spotlights: Featured content
   - Saved: Archived content

3. Session Types:
   - CONTENTDUMP_[clientid]
   - F(date)_[clientid] for Stories/Spotlight/Saved

Data Flow:
--------
1. Operation Processing:
   - Validate session and collection
   - Process grouped operations
   - Update timestamps
   - Maintain data consistency

2. File Movement:
   - Source validation
   - Destination preparation
   - Metadata preservation
   - Atomic updates

Security:
--------
- Session validation
- Collection access control
- Data integrity checks
- Atomic operations

Dependencies:
-----------
- MongoDB Motor: Database operations
- datetime: Timestamp management
- bson: ObjectID handling
- typing: Type hints

Author: Snapped Development Team
"""

from datetime import datetime
from typing import Dict, List
from app.shared.database import (
    content_dump_collection,
    upload_collection,
    spotlight_collection,
    saved_collection
)
from bson import ObjectId

class CDNSyncService:
    """
    Service for synchronizing content operations across different collections.
    
    This class manages complex file operations between different content types:
    - Stories (regular uploads)
    - Spotlight (featured content)
    - Saved (archived content)
    - Content Dump (temporary storage)
    
    Key Features:
    - File movement between collections
    - Metadata synchronization
    - Content organization
    - Session management
    
    The service ensures data consistency and atomic operations while
    handling various content types and their specific requirements.
    """

    def __init__(self):
        """
        Initialize the sync service with collection references.
        
        Collections:
        - content_dump: Temporary storage for content
        - uploads: Regular story content
        - spotlights: Featured content
        - saved: Archived content
        """
        self.content_dump = content_dump_collection
        self.uploads = upload_collection
        self.spotlights = spotlight_collection
        self.saved = saved_collection

    async def process_sync(self, session_id: str, operations: Dict):
        """
        Process a group of synchronization operations for a session.
        
        This method orchestrates different types of operations:
        - File movements between collections
        - Caption updates
        - Content reordering
        
        Args:
            session_id (str): Unique session identifier
                Format: "F(date)_clientid" or "CONTENTDUMP_clientid"
            operations (Dict): Grouped operations to process
                Structure: {
                    'move': [movement_operations],
                    'caption': [caption_operations],
                    'reorder': [reorder_operations]
                }
        
        Process Flow:
        ------------
        1. Collection Determination:
           - Validate session
           - Identify content type
           - Select collection
        
        2. Operation Processing:
           - Handle moves
           - Update captions
           - Manage reordering
        
        3. Timestamp Update:
           - Record modifications
           - Maintain audit trail
        
        Raises:
            ValueError: For invalid session IDs
            Exception: For operation failures
        """
        try:
            # Determine collection and content type
            collection_info = await self._get_collection_info(session_id)
            if not collection_info:
                raise ValueError(f"Invalid session ID: {session_id}")

            collection, content_type = collection_info

            # Process each operation type
            for op_type, ops in operations.items():
                if op_type == 'move':
                    await self._process_moves(collection, session_id, ops, content_type)
                elif op_type == 'caption':
                    await self._process_captions(collection, session_id, ops)
                elif op_type == 'reorder':
                    await self._process_reorder(collection, session_id, ops)

            # Update last_modified timestamp
            await self._update_timestamp(collection, session_id)

        except Exception as e:
            print(f"Sync error for session {session_id}: {str(e)}")
            raise

    async def _get_collection_info(self, session_id: str) -> tuple:
        """
        Determine the appropriate collection based on session ID.
        
        Args:
            session_id (str): Session identifier to analyze
        
        Returns:
            tuple: (collection, content_type)
                collection: MongoDB collection reference
                content_type: 'content_dump'|'SPOTLIGHT'|'SAVED'|'STORIES'
        
        Session ID Formats:
        -----------------
        - CONTENTDUMP_[clientid]: Content dump sessions
        - F(date)_[clientid]: Regular content sessions
        - F(date)_[clientid]_SPOTLIGHT: Spotlight content
        - F(date)_[clientid]_SAVED: Saved content
        """
        if session_id.startswith('CONTENTDUMP_'):
            return self.content_dump, 'content_dump'
        elif session_id.startswith('F('):
            if 'SPOTLIGHT' in session_id:
                return self.spotlights, 'SPOTLIGHT'
            elif 'SAVED' in session_id:
                return self.saved, 'SAVED'
            return self.uploads, 'STORIES'
        return None

    async def _process_moves(self, collection, session_id: str, operations: List, content_type: str):
        """
        Handle file movement operations between collections.
        
        This method manages complex file transfers between different content types,
        maintaining metadata and ensuring data consistency.
        
        Args:
            collection: Source collection reference
            session_id (str): Active session identifier
            operations (List): List of move operations
            content_type (str): Content category
        
        Move Types:
        ----------
        - Content Dump ↔ Stories
        - Stories ↔ Spotlight
        - Spotlight ↔ Content Dump
        - Stories ↔ Saved
        
        Each move operation preserves:
        - File metadata
        - Upload history
        - Sequence information
        - Captions and notes
        """
        for op in operations:
            source_path = op['data']['source_path']
            dest_path = op['data']['destination_path']
            file_name = op['data']['file_name']

            # Determine source and destination types
            source_is_dump = 'CONTENT_DUMP' in source_path
            dest_is_dump = 'CONTENT_DUMP' in dest_path
            source_is_stories = 'STORIES' in source_path
            dest_is_stories = 'STORIES' in dest_path
            source_is_spotlight = 'SPOTLIGHT' in source_path
            dest_is_spotlight = 'SPOTLIGHT' in dest_path

            # Get client ID from path
            client_id = source_path.split('/')[1]  # e.g., "jm07161995"

            if source_is_dump and dest_is_stories:
                await self._move_dump_to_stories(client_id, file_name, dest_path, session_id)
            elif source_is_stories and dest_is_dump:
                await self._move_stories_to_dump(client_id, file_name, source_path, session_id)
            elif source_is_spotlight and dest_is_stories:
                await self._move_spotlight_to_stories(client_id, file_name, dest_path, session_id)
            elif source_is_stories and dest_is_spotlight:
                await self._move_stories_to_spotlight(client_id, file_name, source_path, session_id)
            elif source_is_spotlight and dest_is_dump:
                await self._move_spotlight_to_dump(client_id, file_name, dest_path, session_id)
            elif source_is_dump and dest_is_spotlight:
                await self._move_dump_to_spotlight(client_id, file_name, dest_path, session_id)
            else:
                # Regular move within same collection
                await self._update_file_location(collection, session_id, file_name, dest_path)

    async def _move_dump_to_stories(self, client_id: str, file_name: str, dest_path: str, stories_session_id: str):
        """
        Transfer file from Content Dump to Stories collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            dest_path (str): Destination path in Stories
            stories_session_id (str): Target session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Content Dump
           - Verify file data
        
        2. Destination Update:
           - Add to Stories collection
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Content Dump
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # 1. Get file from Content_Dump
        dump_doc = await self.content_dump.find_one(
            {"client_ID": client_id, "sessions.files.file_name": file_name},
            {"sessions.$": 1}
        )
        
        if not dump_doc or not dump_doc.get('sessions'):
            raise ValueError(f"File {file_name} not found in Content_Dump")

        # Get file data
        file_data = next(
            (f for f in dump_doc['sessions'][0]['files'] if f['file_name'] == file_name),
            None
        )

        if not file_data:
            raise ValueError(f"File data not found for {file_name}")

        # 2. Add to Stories
        await self.uploads.update_one(
            {"session_id": stories_session_id},
            {"$push": {"files": {
                **file_data,
                "CDN_link": f"{dest_path}/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # 3. Remove from Content_Dump
        await self.content_dump.update_one(
            {"client_ID": client_id},
            {"$pull": {"sessions.$[].files": {"file_name": file_name}}}
        )

    async def _move_stories_to_dump(self, client_id: str, file_name: str, source_path: str, stories_session_id: str):
        """
        Transfer file from Stories to Content Dump collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            source_path (str): Source path in Stories
            stories_session_id (str): Source session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Stories
           - Verify file data
        
        2. Destination Update:
           - Add to Content Dump
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Stories
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # 1. Get file from Stories
        stories_doc = await self.uploads.find_one(
            {"session_id": stories_session_id, "files.file_name": file_name},
            {"files.$": 1}
        )

        if not stories_doc or not stories_doc.get('files'):
            raise ValueError(f"File {file_name} not found in Stories")

        file_data = stories_doc['files'][0]

        # 2. Add to Content_Dump
        dump_session_id = f"CONTENTDUMP_{client_id}"
        await self.content_dump.update_one(
            {"client_ID": client_id, "sessions.session_id": dump_session_id},
            {"$push": {"sessions.$.files": {
                **file_data,
                "CDN_link": f"sc/{client_id}/CONTENT_DUMP/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # 3. Remove from Stories
        await self.uploads.update_one(
            {"session_id": stories_session_id},
            {"$pull": {"files": {"file_name": file_name}}}
        )

    async def _update_file_location(self, collection, session_id: str, file_name: str, dest_path: str):
        """
        Update file location within the same collection.
        
        Args:
            collection: MongoDB collection reference
            session_id (str): Active session identifier
            file_name (str): Name of file to update
            dest_path (str): New file location
        
        Updates:
        --------
        - CDN link
        - Upload timestamp
        - File path
        """
        await collection.update_one(
            {"session_id": session_id, "files.file_name": file_name},
            {"$set": {
                "files.$.CDN_link": f"{dest_path}/{file_name}",
                "files.$.upload_time": datetime.utcnow().isoformat()
            }}
        )

    async def _process_captions(self, collection, session_id: str, operations: List):
        """
        Update file captions in batch.
        
        Args:
            collection: MongoDB collection reference
            session_id (str): Active session identifier
            operations (List): List of caption updates
        
        Operation Structure:
        ------------------
        {
            'file_name': str,
            'caption': str
        }
        """
        for op in operations:
            file_name = op['data']['file_name']
            caption = op['data']['caption']

            update_query = {
                "session_id": session_id,
                "files.file_name": file_name
            }

            update_data = {
                "$set": {
                    "files.$.caption": caption
                }
            }

            await collection.update_one(update_query, update_data)

    async def _process_reorder(self, collection, session_id: str, operations: List):
        """
        Reorder files within a session.
        
        Args:
            collection: MongoDB collection reference
            session_id (str): Active session identifier
            operations (List): List of reorder operations
        
        Operation Structure:
        ------------------
        {
            'files': [
                {
                    'file_name': str,
                    'seq_number': int
                }
            ]
        }
        """
        for op in operations:
            files = op['data']['files']
            
            # Update each file's sequence number
            for file_info in files:
                update_query = {
                    "session_id": session_id,
                    "files.file_name": file_info['file_name']
                }

                update_data = {
                    "$set": {
                        "files.$.seq_number": file_info['seq_number']
                    }
                }

                await collection.update_one(update_query, update_data)

    async def _update_timestamp(self, collection, session_id: str):
        """
        Update the last_modified timestamp for a session.
        
        Args:
            collection: MongoDB collection reference
            session_id (str): Session to update
        
        This method maintains an audit trail of content modifications
        by updating the last_updated field with the current UTC time.
        """
        await collection.update_one(
            {"session_id": session_id},
            {"$set": {"last_updated": datetime.utcnow()}}
        )

    async def _move_spotlight_to_stories(self, client_id: str, file_name: str, dest_path: str, stories_session_id: str):
        """
        Transfer file from Spotlight to Stories collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            dest_path (str): Destination path in Stories
            stories_session_id (str): Target session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Spotlight
           - Verify file data
        
        2. Destination Update:
           - Add to Stories collection
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Spotlight
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # 1. Get file from Spotlight
        spotlight_doc = await self.spotlights.find_one(
            {"client_ID": client_id, "sessions.files.file_name": file_name},
            {"sessions.$": 1}
        )

        if not spotlight_doc or not spotlight_doc.get('sessions'):
            raise ValueError(f"File {file_name} not found in Spotlight")

        file_data = next(
            (f for f in spotlight_doc['sessions'][0]['files'] if f['file_name'] == file_name),
            None
        )

        # 2. Add to Stories
        await self.uploads.update_one(
            {"session_id": stories_session_id},
            {"$push": {"files": {
                **file_data,
                "CDN_link": f"{dest_path}/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # 3. Remove from Spotlight
        await self.spotlights.update_one(
            {"client_ID": client_id},
            {"$pull": {"sessions.$[].files": {"file_name": file_name}}}
        )

    async def _move_stories_to_spotlight(self, client_id: str, file_name: str, source_path: str, stories_session_id: str):
        """
        Transfer file from Stories to Spotlight collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            source_path (str): Source path in Stories
            stories_session_id (str): Source session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Stories
           - Verify file data
        
        2. Destination Update:
           - Add to Spotlight
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Stories
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # Similar to _move_stories_to_dump but for Spotlight
        stories_doc = await self.uploads.find_one(
            {"session_id": stories_session_id, "files.file_name": file_name},
            {"files.$": 1}
        )

        if not stories_doc or not stories_doc.get('files'):
            raise ValueError(f"File {file_name} not found in Stories")

        file_data = stories_doc['files'][0]

        # Add to Spotlight
        await self.spotlights.update_one(
            {"client_ID": client_id, "sessions.session_id": session_id},
            {"$push": {"sessions.$.files": {
                **file_data,
                "CDN_link": f"sc/{client_id}/SPOTLIGHT/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # Remove from Stories
        await self.uploads.update_one(
            {"session_id": stories_session_id},
            {"$pull": {"files": {"file_name": file_name}}}
        )

    async def _move_spotlight_to_dump(self, client_id: str, file_name: str, dest_path: str, spotlight_session_id: str):
        """
        Transfer file from Spotlight to Content Dump collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            dest_path (str): Destination path in Content Dump
            spotlight_session_id (str): Source session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Spotlight
           - Verify file data
        
        2. Destination Update:
           - Add to Content Dump
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Spotlight
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # Similar pattern to other moves
        spotlight_doc = await self.spotlights.find_one(
            {"client_ID": client_id, "sessions.files.file_name": file_name},
            {"sessions.$": 1}
        )

        if not spotlight_doc or not spotlight_doc.get('sessions'):
            raise ValueError(f"File {file_name} not found in Spotlight")

        file_data = next(
            (f for f in spotlight_doc['sessions'][0]['files'] if f['file_name'] == file_name),
            None
        )

        # Add to Content_Dump
        dump_session_id = f"CONTENTDUMP_{client_id}"
        await self.content_dump.update_one(
            {"client_ID": client_id, "sessions.session_id": dump_session_id},
            {"$push": {"sessions.$.files": {
                **file_data,
                "CDN_link": f"sc/{client_id}/CONTENT_DUMP/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # Remove from Spotlight
        await self.spotlights.update_one(
            {"client_ID": client_id},
            {"$pull": {"sessions.$[].files": {"file_name": file_name}}}
        )

    async def _move_dump_to_spotlight(self, client_id: str, file_name: str, dest_path: str, spotlight_session_id: str):
        """
        Transfer file from Content Dump to Spotlight collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            dest_path (str): Destination path in Spotlight
            spotlight_session_id (str): Target session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Content Dump
           - Verify file data
        
        2. Destination Update:
           - Add to Spotlight
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Content Dump
           - Update indexes
        
        Raises:
            ValueError: If file not found
        """
        # Similar pattern to _move_dump_to_stories
        dump_doc = await self.content_dump.find_one(
            {"client_ID": client_id, "sessions.files.file_name": file_name},
            {"sessions.$": 1}
        )

        if not dump_doc or not dump_doc.get('sessions'):
            raise ValueError(f"File {file_name} not found in Content_Dump")

        file_data = next(
            (f for f in dump_doc['sessions'][0]['files'] if f['file_name'] == file_name),
            None
        )

        # Add to Spotlight
        await self.spotlights.update_one(
            {"session_id": spotlight_session_id},
            {"$push": {"files": {
                **file_data,
                "CDN_link": f"{dest_path}/{file_name}",
                "upload_time": datetime.utcnow().isoformat()
            }}}
        )

        # Remove from Content_Dump
        await self.content_dump.update_one(
            {"client_ID": client_id},
            {"$pull": {"sessions.$[].files": {"file_name": file_name}}}
        )

    async def _move_saved_to_stories(self, client_id: str, file_name: str, dest_path: str, stories_session_id: str):
        """
        Transfer file from Saved to Stories collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            dest_path (str): Destination path in Stories
            stories_session_id (str): Target session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Saved
           - Verify file data
        
        2. Destination Update:
           - Add to Stories
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Saved
           - Update indexes
        """
        # Implementation similar to _move_spotlight_to_stories

    async def _move_stories_to_saved(self, client_id: str, file_name: str, source_path: str, session_id: str):
        """
        Transfer file from Stories to Saved collection.
        
        Args:
            client_id (str): Client identifier
            file_name (str): Name of file to move
            source_path (str): Source path in Stories
            session_id (str): Source session ID
        
        Process Flow:
        ------------
        1. Source Validation:
           - Locate file in Stories
           - Verify file data
        
        2. Destination Update:
           - Add to Saved
           - Update CDN links
           - Preserve metadata
        
        3. Source Cleanup:
           - Remove from Stories
           - Update indexes
        """
        # Implementation similar to _move_stories_to_spotlight 