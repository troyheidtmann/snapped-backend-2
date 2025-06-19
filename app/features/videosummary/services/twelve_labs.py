"""
Twelve Labs Integration Module

This module provides integration with Twelve Labs API for video analysis,
content moderation, and summarization.

Features:
- Video indexing
- Content analysis
- Moderation checks
- Video summaries
- Search functionality

Data Model:
- Video indexes
- Analysis results
- Content flags
- Search results
- Summaries

Security:
- API key validation
- Content validation
- Access control
- Error handling

Dependencies:
- Twelve Labs SDK
- FastAPI for routing
- MongoDB for storage
- aiohttp for async
- logging for tracking

Author: Snapped Development Team
"""

import os
import logging
import traceback
from typing import Dict, List, Set
from datetime import datetime, timezone
import aiohttp
from app.shared.database import video_analysis_collection, analysis_queue_collection, upload_collection, summary_prompt_collection, MONGODB_URL, MONGO_SETTINGS
from app.features.videosummary.insights import store_video_analysis_results, extract_insights, update_best_practices
from twelvelabs import TwelveLabs
import asyncio
from fastapi import APIRouter, HTTPException, Form, File, UploadFile
from motor.motor_asyncio import AsyncIOMotorClient
import json
from pymongo import UpdateOne
from asyncio import gather, create_task, Queue, Task
import tempfile
import ffmpeg
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/twelve-labs", tags=["twelve-labs"])

class TwelveLabsService:
    """
    Twelve Labs service handler.
    
    Manages integration with Twelve Labs API for video processing
    and content analysis.
    
    Attributes:
        api_key (str): Twelve Labs API key
        client (TwelveLabs): API client instance
        video_extensions (tuple): Supported video formats
        max_concurrent_uploads (int): Upload concurrency limit
    """
    
    def __init__(self):
        """Initialize service with API credentials and settings."""
        self.api_key = "tlk_2Q2CT173542GEK2CR57HP1M09VBZ"
        
        # Video extensions we care about
        self.video_extensions = ('.mp4', '.mov', '.avi')
        
        # Increase concurrent uploads (adjust based on testing)
        self.max_concurrent_uploads = 15  # or higher if needed
        self.active_tasks: Set[Task] = set()
        self._client = None

    async def __aenter__(self):
        """Async context manager entry"""
        if not self._client:
            self._client = TwelveLabs(api_key=self.api_key, version="v1.3")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._client:
            if hasattr(self._client, '__aexit__'):
                await self._client.__aexit__(exc_type, exc_val, exc_tb)
            elif hasattr(self._client, '__exit__'):
                self._client.__exit__(exc_type, exc_val, exc_tb)
            self._client = None

    @property
    def client(self):
        """Get the Twelve Labs client instance"""
        if not self._client:
            raise RuntimeError("TwelveLabsService must be used as an async context manager")
        return self._client

    async def create_client_index(self, client_id: str) -> str:
        """
        Create or get client's video index.
        
        Args:
            client_id: Client identifier
            
        Returns:
            str: Index identifier
            
        Raises:
            Exception: For API errors
            
        Notes:
            - Checks existing index
            - Creates index with Marengo 2.7
            - Updates database
            - Handles errors
        """
        try:
            logger.info(f"Getting/creating index for client: {client_id}")
            
            # First check if index exists in UploadDB.Uploads
            upload_doc = await upload_collection.find_one({"client_ID": client_id})
            if upload_doc and upload_doc.get("twelve_labs_index"):
                logger.info(f"Found existing index in uploads: {upload_doc['twelve_labs_index']}")
                return upload_doc["twelve_labs_index"]

            # If not found, create new index with Marengo 2.7
            models = [
                {
                    "name": "marengo2.7",
                    "options": ["visual", "audio"]
                }
            ]
            
            # Create index synchronously
            index = self.client.index.create(
                name=client_id,  # Use client_ID as index name
                models=models,
                addons=["thumbnail"]
            )
            
            # Save index to AIVideo.indexes
            await video_analysis_collection.insert_one({
                "client_id": client_id,
                "index_id": index.id,
                "created_at": datetime.now(timezone.utc)
            })
            
            # Update UploadDB.Uploads with index
            await upload_collection.update_one(
                {"client_ID": client_id},
                {"$set": {"twelve_labs_index": index.id}},
                upsert=True
            )
            
            logger.info(f"Created new Marengo 2.7 index: {index.id}")
            return index.id

        except Exception as e:
            logger.error(f"Error creating index for {client_id}: {str(e)}")
            raise

    async def process_videos_concurrent(self, videos_to_process: List[Dict]) -> List[Dict]:
        """
        Process multiple videos concurrently.
        
        Args:
            videos_to_process: List of video data
            
        Returns:
            List[Dict]: Processing results
            
        Notes:
            - Uses worker pool
            - Handles failures
            - Limits concurrency
            - Tracks results
        """
        results = []
        processing_queue = Queue()
        
        # Add all videos to queue
        for video in videos_to_process:
            await processing_queue.put(video)
            
        async def process_queue_item():
            while not processing_queue.empty():
                video = await processing_queue.get()
                try:
                    result = await self.process_single_video(
                        client_id=video['client_id'],
                        file_data=video['file_data']
                    )
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing video {video['file_data'].get('file_name')}: {str(e)}")
                finally:
                    processing_queue.task_done()

        # Create workers up to max_concurrent_uploads
        workers = [
            create_task(process_queue_item())
            for _ in range(min(self.max_concurrent_uploads, len(videos_to_process)))
        ]
        
        # Wait for all workers to complete
        await gather(*workers)
        return results

    async def _preprocess_video(self, video_url: str) -> str:
        """Preprocess video to ensure it meets Twelve Labs requirements"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download video
                input_path = os.path.join(temp_dir, 'input.mp4')
                output_path = os.path.join(temp_dir, 'output.mp4')

                async with aiohttp.ClientSession() as session:
                    async with session.get(video_url) as response:
                        if response.status != 200:
                            raise Exception(f"Failed to download video: {response.status}")
                        with open(input_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)

                # Get video dimensions
                probe = ffmpeg.probe(input_path)
                video_stream = next((stream for stream in probe['streams'] 
                                   if stream['codec_type'] == 'video'), None)
                if not video_stream:
                    raise Exception("No video stream found")

                width = int(video_stream['width'])
                height = int(video_stream['height'])
                aspect_ratio = width / height

                logger.info(f"Original video dimensions: {width}x{height}, aspect ratio: {aspect_ratio:.2f}")

                # If aspect ratio is outside 1:1 to 16:9 range, resize
                if aspect_ratio < 1 or aspect_ratio > 16/9:
                    logger.info("Video needs resizing to meet Twelve Labs requirements")
                    
                    # Calculate new dimensions to fit within 16:9
                    if aspect_ratio < 1:  # Too narrow/tall
                        new_height = min(height, 1920)  # Cap height at 1920
                        new_width = new_height  # Make it square (1:1)
                    else:  # Too wide
                        new_width = min(width, 1920)  # Cap width at 1920
                        new_height = int(new_width * 9/16)  # 16:9 aspect ratio

                    logger.info(f"Resizing to {new_width}x{new_height}")

                    # Process with ffmpeg
                    stream = ffmpeg.input(input_path)
                    stream = ffmpeg.filter(stream, 'scale', width=new_width, height=new_height)
                    stream = ffmpeg.output(stream, output_path,
                                         vcodec='libx264',
                                         preset='ultrafast',
                                         acodec='aac')
                    ffmpeg.run(stream, overwrite_output=True)

                    # Return path to processed video
                    return output_path

                # If no processing needed, return original path
                return input_path

        except Exception as e:
            logger.error(f"Error preprocessing video: {str(e)}")
            raise

    async def process_single_video(self, client_id: str, file_data: Dict):
        """Process a single video with non-blocking status checks"""
        temp_dir = None
        try:
            # Get index and validate
            upload_doc = await upload_collection.find_one({"client_ID": client_id})
            if not upload_doc or not upload_doc.get("twelve_labs_index"):
                return {"status": "skipped", "reason": "no_index"}
            
            index_id = upload_doc["twelve_labs_index"]
            if file_data.get("is_indexed"):
                return {"status": "skipped", "reason": "already_indexed"}

            video_url = file_data.get("CDN_link")
            if not video_url:
                return {"status": "error", "reason": "no_cdn_link"}

            # Create temp directory that will persist through the whole process
            temp_dir = tempfile.mkdtemp()
            
            # Preprocess video if needed
            try:
                # Download video - use original filename
                original_filename = file_data.get("file_name", "video.mp4")
                input_path = os.path.join(temp_dir, original_filename)
                output_path = os.path.join(temp_dir, f"processed_{original_filename}")

                async with aiohttp.ClientSession() as session:
                    async with session.get(video_url) as response:
                        if response.status != 200:
                            raise Exception(f"Failed to download video: {response.status}")
                        with open(input_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)

                # Get video dimensions
                probe = ffmpeg.probe(input_path)
                video_stream = next((stream for stream in probe['streams'] 
                                   if stream['codec_type'] == 'video'), None)
                if not video_stream:
                    raise Exception("No video stream found")

                width = int(video_stream['width'])
                height = int(video_stream['height'])
                aspect_ratio = width / height

                logger.info(f"Original video dimensions: {width}x{height}, aspect ratio: {aspect_ratio:.2f}")

                # If aspect ratio is outside 1:1 to 16:9 range, resize
                if aspect_ratio < 1 or aspect_ratio > 16/9:
                    logger.info("Video needs resizing to meet Twelve Labs requirements")
                    
                    # Calculate new dimensions to fit within 16:9
                    if aspect_ratio < 1:  # Too narrow/tall
                        new_height = min(height, 1920)  # Cap height at 1920
                        new_width = new_height  # Make it square (1:1)
                    else:  # Too wide
                        new_width = min(width, 1920)  # Cap width at 1920
                        new_height = int(new_width * 9/16)  # 16:9 aspect ratio

                    logger.info(f"Resizing to {new_width}x{new_height}")

                    # Process with ffmpeg
                    stream = ffmpeg.input(input_path)
                    stream = ffmpeg.filter(stream, 'scale', width=new_width, height=new_height)
                    stream = ffmpeg.output(stream, output_path,
                                         vcodec='libx264',
                                         preset='ultrafast',
                                         acodec='aac')
                    ffmpeg.run(stream, overwrite_output=True)
                    video_path = output_path
                else:
                    video_path = input_path

                logger.info(f"Video preprocessing completed for {original_filename}")

                # Create file object from video path
                with open(video_path, 'rb') as f:
                    # Submit video for indexing using file upload with original filename
                    task = self.client.task.create(
                        index_id=index_id,
                        file=f,  # Pass file object directly
                        language="en",
                        filename=original_filename  # Pass original filename to Twelve Labs
                    )
            except Exception as e:
                logger.error(f"Video preprocessing failed for {file_data.get('file_name')}: {str(e)}")
                return {"status": "error", "reason": f"preprocessing_failed: {str(e)}"}
            
            # Monitor task status asynchronously
            video_id = await self._monitor_task_completion(task)
            
            # Update databases
            await gather(
                self._update_upload_collection(client_id, file_data, task.id, video_id),
                self._update_video_analysis(client_id, file_data, task.id, video_id, index_id, video_url)
            )

            return {
                "status": "success",
                "video_id": video_id,
                "file_name": file_data["file_name"]
            }

        except Exception as e:
            logger.error(f"Error in process_single_video: {str(e)}")
            return {"status": "error", "reason": str(e)}
        finally:
            # Clean up temp directory
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)

    async def _monitor_task_completion(self, task) -> str:
        """Monitor task completion with timeout"""
        max_attempts = 60
        attempt = 0
        
        while attempt < max_attempts:
            task = self.client.task.retrieve(task.id)
            if task.status == "ready":
                return task.video_id
            elif task.status == "failed":
                raise RuntimeError(f"Indexing failed with status {task.status}")
            
            await asyncio.sleep(5)
            attempt += 1
            
        raise RuntimeError("Indexing timed out after 5 minutes")

    async def _update_upload_collection(self, client_id: str, file_data: Dict, task_id: str, video_id: str):
        """Update upload collection with video results"""
        await upload_collection.update_one(
            {
                "client_ID": client_id,
                "sessions.session_id": file_data["session_id"],
                "sessions.files.file_name": file_data["file_name"]
            },
            {
                "$set": {
                    "sessions.$[session].files.$[file].is_indexed": True,
                    "sessions.$[session].files.$[file].twelve_labs_task_id": task_id,
                    "sessions.$[session].files.$[file].twelve_labs_video_id": video_id
                }
            },
            array_filters=[
                {"session.session_id": file_data["session_id"]},
                {"file.file_name": file_data["file_name"]}
            ]
        )

    async def _update_video_analysis(self, client_id: str, file_data: Dict, task_id: str, video_id: str, index_id: str, video_url: str):
        """Update video analysis collection with results"""
        await video_analysis_collection.update_one(
            {"client_id": client_id},
            {
                "$push": {
                    "STORY": {
                        "video_id": video_id,
                        "task_id": task_id,
                        "file_name": file_data["file_name"],
                        "session_id": file_data.get("session_id", ""),
                        "index_id": index_id,
                        "cdn_url": video_url,
                        "indexed_at": datetime.now(timezone.utc)
                    }
                }
            },
            upsert=True
        )

    async def search_index(self, index_id: str, query: str) -> Dict:
        """Search an index with a query"""
        try:
            logger.info(f"Searching index {index_id} for: {query}")
            
            # Use the SDK's search functionality
            results = self.client.search.query(
                index_id=index_id,
                options=["visual", "audio"],
                query_text=query,
                threshold="medium",
                group_by="clip",
                page_limit=12,
                adjust_confidence_level=0.84  # This means 84% confidence
            )
            
            # Format results
            formatted_results = {
                "page_info": {
                    "total_results": len(results.data)
                },
                "search_pool": {
                    "total_duration": sum(clip.end - clip.start for clip in results.data)
                },
                "data": []
            }
            
            # Format each clip
            for clip in results.data:
                formatted_results["data"].append({
                    "score": clip.score,
                    "confidence": clip.confidence,
                    "start": clip.start,
                    "end": clip.end,
                    "video_id": clip.video_id,
                    "thumbnail_url": clip.thumbnail_url if hasattr(clip, "thumbnail_url") else None
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching index: {str(e)}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    async def video_id_upload_match(self, search_results: List[Dict], client_id: str):
        """Match search results with uploads based on video_id and record key data
        
        Args:
            search_results: List of search result objects from video_analysis collection
            client_id: The client ID to match against
        """
        try:
            logger.info(f"Starting video ID matching for client {client_id}")
            
            # Get the upload document for this client
            upload_doc = await upload_collection.find_one({"client_ID": client_id})
            if not upload_doc:
                logger.error(f"No upload document found for client {client_id}")
                return
                
            # Track updates to make
            updates = []
            
            # Go through each session and file to find matches
            for session in upload_doc.get("sessions", []):
                for file in session.get("files", []):
                    # Check if this file has a twelve_labs_video_id
                    video_id = file.get("twelve_labs_video_id")
                    if not video_id:
                        continue
                        
                    # Find matching search results for this video_id
                    matching_results = [
                        result for result in search_results 
                        if result.get("video_id") == video_id
                    ]
                    
                    if matching_results:
                        logger.info(f"Found {len(matching_results)} matches for video {video_id}")
                        
                        # Format the content data to store
                        content_data = []
                        for result in matching_results:
                            content_data.append({
                                "search_query": result.get("search_query"),
                                "search_date": result.get("search_date"),
                                "score": result.get("score"),
                                "confidence": result.get("confidence"),
                                "start_time": result.get("start_time"),
                                "end_time": result.get("end_time"),
                                "thumbnail_url": result.get("thumbnail_url"),
                                "severity": self._calculate_severity(result.get("score", 0))
                            })
                        
                        # Create update operation for this file
                        updates.append(UpdateOne(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": session.get("session_id"),
                                "sessions.files.twelve_labs_video_id": video_id
                            },
                            {
                                "$set": {
                                    "sessions.$[session].files.$[file].content_matches": content_data,
                                    "sessions.$[session].files.$[file].last_content_match": datetime.now(timezone.utc)
                                }
                            },
                            array_filters=[
                                {"session.session_id": session.get("session_id")},
                                {"file.twelve_labs_video_id": video_id}
                            ]
                        ))
            
            # Execute all updates in bulk if we have any
            if updates:
                logger.info(f"Executing {len(updates)} bulk updates")
                result = await upload_collection.bulk_write(updates)
                logger.info(f"Updated {result.modified_count} files with content matches")
            else:
                logger.info("No matches found to update")
                
        except Exception as e:
            logger.error(f"Error in video_id_upload_match: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def search_all_indexes(self, query: str = None) -> Dict:
    
        """Search all indexes and save flags to video_analysis collection"""
        try:
            logger.info("Fetching all indexes from AIVideo.video_analysis...")
            
            # Define search queries for content moderation
            search_queries = [
                {
                    "description": "Unauthorized Commercial Promotion",
                    "query_text": "OnlyFans OR Fanfix OR Passes",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.75,
                    "options": ["visual", "audio"]
                },
                {
                    "description": "External Platform Promotions",
                    "query_text": "TikTok OR Instagram OR Facebook OR Twitch OR Linktree OR Shopify",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.73,  # Lower threshold to catch more
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Explicit Content",
                    "query_text": "nudity OR explicit sexual content OR sex acts",
                    "threshold": "low",
                    "adjust_confidence_level": 0.73,
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Hateful Content",
                    "query_text": "hate speech OR discrimination OR violent extremism",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.85,
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Harassment and Bullying",
                    "query_text": "harassment OR bullying OR belittling language OR invasions of privacy OR demeaning content OR targeted attacks OR doxxing OR mean pranks OR humiliation OR threats",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.84,  # Standard threshold
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Threats, Violence, and Harm",
                    "query_text": "threats OR violence OR harm OR graphic violence OR animal abuse OR self-harm OR suicide OR eating disorders OR dangerous behavior OR violent content OR disturbing imagery",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.85,  # Higher threshold
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Harmful False Information",
                    "query_text": "false information OR misinformation OR deceptive content OR unverified claims OR manipulated media OR conspiracy theories OR misleading information OR fake news OR hoaxes",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.86,  # Higher threshold
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Illegal or Regulated Activities",
                    "query_text": "illegal activity OR drug use OR contraband OR weapons OR counterfeit goods OR human trafficking OR gambling OR tobacco products OR vape products OR alcohol OR criminal activity",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.85,
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Dangerous Activities",
                    "query_text": "driving OR reckless behavior OR dangerous stunts OR risky challenges OR unsafe activities OR distracted driving OR hazardous actions OR perilous behavior OR endangering safety",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.84,
                    "options": ["visual", "audio"]
                },
                {
                    "description": "Sexualized Content",
                    "query_text": "sexualized OR suggestive content OR provocative imagery OR adult themes OR erotic content OR sensual material OR sexually suggestive OR lascivious content OR risqué material",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.73,  # Lower threshold to catch more
                    "options": ["visual", "audio"]
                },
                {
                    "description": "General Promotions",
                    "query_text": "promotion OR advertising OR sponsored content OR marketing OR endorsements OR commercial content OR promotional material OR advertisements OR brand promotion",
                    "threshold": "medium",
                    "adjust_confidence_level": 0.84,  # Standard threshold
                    "options": ["visual", "audio"]
                }
            ]

            # Get all unique index IDs
            indexes = await video_analysis_collection.distinct("index_id")
            logger.info(f"Found {len(indexes)} indexes to search")
            
            total_flags = 0
            
            # Search each index
            for index_id in indexes:
                try:
                    # Get client_id for this index
                    index_doc = await video_analysis_collection.find_one({"index_id": index_id})
                    client_id = index_doc.get("client_id") if index_doc else None
                    
                    # Run either specific query or all queries
                    queries_to_run = [{"query_text": query}] if query else search_queries
                    
                    for search_query in queries_to_run:
                        logger.info(f"\nSearching index {index_id} for: {search_query['description'] if 'description' in search_query else search_query['query_text']}")
                        
                        results = self.client.search.query(
                            index_id=index_id,
                            options=search_query.get("options", ["visual", "audio"]),
                            query_text=search_query["query_text"],
                            threshold=search_query.get("threshold", "low"),
                            group_by="clip",
                            page_limit=12,
                            adjust_confidence_level=search_query.get("adjust_confidence_level", 0.70)
                        )
                        
                        # Save flags to video_analysis collection
                        for clip in results.data:
                            flag_data = {
                                "search_query": search_query["query_text"],
                                "search_description": search_query.get("description", "Custom Query"),
                                "search_date": datetime.now(timezone.utc),
                                "video_id": clip.video_id,
                                "score": clip.score,
                                "confidence": clip.confidence,
                                "start_time": clip.start,
                                "end_time": clip.end,
                                "thumbnail_url": clip.thumbnail_url if hasattr(clip, "thumbnail_url") else None,
                                "severity": self._calculate_severity(clip.score)
                            }
                            
                            await video_analysis_collection.update_one(
                                {"index_id": index_id},
                                {"$push": {"content_flags": flag_data}},
                                upsert=True
                            )
                            
                            total_flags += 1
                            logger.info(f"Saved flag - Type: {search_query.get('description', 'Custom Query')}, Score: {clip.score}, Video: {clip.video_id}")
                
                except Exception as e:
                    logger.error(f"Error searching index {index_id}: {str(e)}")
                    continue

            return {
                "status": "success",
                "total_indexes_searched": len(indexes),
                "total_flags_saved": total_flags
            }
                
        except Exception as e:
            logger.error(f"Error searching all indexes: {str(e)}")
            logger.error(traceback.format_exc())
            return {"error": str(e)}

    def _calculate_severity(self, score: float) -> str:
        """Calculate severity level based on score"""
        if score >= 90:
            return "critical"
        elif score >= 80:
            return "high"
        elif score >= 70:
            return "medium"
        else:
            return "low"

    async def generate_video_summary(self, video_id: str, index_id: str) -> Dict:
        """Generate structured summary for a video"""
        try:
            logger.info(f"Generating summary for video {video_id} in index {index_id}")
            
            # Get active prompt from database
            active_prompt = await summary_prompt_collection.find_one({"active": True})
            if not active_prompt:
                raise Exception("No active prompt found in database")
            
            prompt = active_prompt["prompt_text"]
            prompt_id = active_prompt["prompt_id"]

            # Call Twelve Labs summarize endpoint
            result = self.client.generate.summarize(
                video_id=video_id,
                type="summary",
                prompt=prompt
            )
            
            if not result or not result.summary:
                raise Exception("No summary generated")

            formatted_summary = result.summary

            # Update UploadDB.Uploads with summary and prompt version
            await upload_collection.update_one(
                {
                    "sessions.files.twelve_labs_video_id": video_id
                },
                {
                    "$set": {
                        "sessions.$[].files.$[file].video_summary": formatted_summary,
                        "sessions.$[].files.$[file].summary_prompt_version": prompt_id
                    }
                },
                array_filters=[{"file.twelve_labs_video_id": video_id}]
            )

            # Update AIVideo.video_analysis
            await video_analysis_collection.update_one(
                {
                    "STORY.video_id": video_id
                },
                {
                    "$set": {
                        "STORY.$.video_summary": formatted_summary,
                        "STORY.$.summary_prompt_version": prompt_id
                    }
                }
            )

            return {
                "status": "success",
                "video_id": video_id,
                "summary": formatted_summary,
                "prompt_version": prompt_id
            }

        except Exception as e:
            logger.error(f"Error generating summary for video {video_id}: {str(e)}")
            return {
                "status": "error",
                "video_id": video_id,
                "error": str(e)
            }

    async def summarize_all_new_videos(self) -> Dict:
        """Generate summaries for all newly indexed videos without summaries"""
        try:
            results = []
            
            # Find all indexed videos without summaries
            async for doc in upload_collection.find(
                {
                    "sessions.files": {
                        "$elemMatch": {
                            "is_indexed": True,
                            "video_summary": {"$exists": False}
                        }
                    }
                }
            ):
                client_id = doc.get("client_ID")
                index_id = doc.get("twelve_labs_index")
                
                if not index_id:
                    continue
                    
                for session in doc.get("sessions", []):
                    for file in session.get("files", []):
                        if (file.get("is_indexed") and 
                            file.get("twelve_labs_video_id") and 
                            not file.get("video_summary")):
                            
                            result = await self.generate_video_summary(
                                video_id=file["twelve_labs_video_id"],
                                index_id=index_id
                            )
                            results.append(result)
            
            return {
                "status": "success",
                "total_processed": len(results),
                "successes": len([r for r in results if r["status"] == "success"]),
                "failures": len([r for r in results if r["status"] == "error"]),
                "results": results
            }
                        
        except Exception as e:
            logger.error(f"Error in summarize_all_new_videos: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

@router.post("/scan-uploads")
async def scan_uploads():
    """Endpoint to scan all existing uploads into Twelve Labs"""
    try:
        async with TwelveLabsService() as service:
            stats = await service.scan_existing_uploads()
            return {
                "status": "success", 
                "message": "Completed scanning uploads",
                "statistics": stats
            }
    except Exception as e:
        logger.error(f"Error in scan_uploads endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/scan-for-indexes")
async def scan_for_indexes():
    """Scan UploadDB.Uploads for documents needing 12labs indexes"""
    try:
        async with TwelveLabsService() as service:
            stats = {
                "total_scanned": 0,
                "indexes_created": 0,
                "errors": []
            }

            # Find all documents without an index
            async for doc in upload_collection.find(
                {"$or": [
                    {"twelve_labs_index": {"$exists": False}},
                    {"twelve_labs_index": {"$in": [None, ""]}}
                ]},
                {"client_ID": 1}
            ):
                stats["total_scanned"] += 1
                client_id = doc.get("client_ID")
                
                if not client_id:
                    continue

                try:
                    await service.create_client_index(client_id)
                    stats["indexes_created"] += 1
                except Exception as e:
                    stats["errors"].append({"client_id": client_id, "error": str(e)})

            return {"status": "success", "statistics": stats}

    except Exception as e:
        logger.error(f"Error in scan_for_indexes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload-videos")
async def upload_videos():
    """Process multiple videos concurrently"""
    try:
        async with TwelveLabsService() as service:
            videos_to_process = []
            
            # Get today's date in MM-DD-YYYY format
            today = datetime.now().strftime("%m-%d-%Y")
            
            # Collect all videos that need processing from today's sessions
            async for doc in upload_collection.find({
                "twelve_labs_index": {"$exists": True, "$ne": ""},
                "sessions": {
                    "$elemMatch": {
                        "session_id": {"$regex": f"F\\({today}\\)"},
                        "files": {
                            "$elemMatch": {
                                "is_indexed": {"$ne": True},
                                "file_type": {"$regex": "video", "$options": "i"},
                                "file_name": {"$exists": True, "$ne": ""}  # Ensure filename exists
                            }
                        }
                    }
                }
            }):
                client_id = doc.get("client_ID")
                for session in doc.get("sessions", []):
                    # Only process today's sessions
                    if not session.get("session_id", "").startswith(f"F({today})"):
                        continue
                        
                    for file in session.get("files", []):
                        # Only process videos with valid filenames that aren't indexed
                        if (not file.get("is_indexed") and 
                            file.get("file_type", "").lower() == "video" and
                            file.get("file_name")):  # Ensure filename exists
                            
                            logger.info(f"Queueing video for processing: {file.get('file_name')} from session {session.get('session_id')}")
                            videos_to_process.append({
                                "client_id": client_id,
                                "file_data": {**file, "session_id": session.get("session_id", "")}
                            })

            # Process videos concurrently
            results = await service.process_videos_concurrent(videos_to_process)
            
            # Compile statistics
            stats = {
                "total": len(videos_to_process),
                "processed": len([r for r in results if r["status"] == "success"]),
                "skipped": len([r for r in results if r["status"] == "skipped"]),
                "failed": len([r for r in results if r["status"] == "error"]),
                "errors": [r for r in results if r["status"] == "error"]
            }

            return {
                "status": "success",
                "message": "Completed processing videos",
                "statistics": stats
            }

    except Exception as e:
        logger.error(f"Error in upload_videos endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search-index")
async def search_index(index_id: str, query: str):
    """Search an index with specific query"""
    try:
        async with TwelveLabsService() as service:
            results = await service.search_index(index_id, query)
            return results
    except Exception as e:
        logger.error(f"Error in search endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search-all-indexes")
async def search_all_indexes(query: str = None):
    """Search all available indexes with specific query or run all moderation queries"""
    try:
        async with TwelveLabsService() as service:
            results = await service.search_all_indexes(query)
            return results
    except Exception as e:
        logger.error(f"Error in search-all endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/match-search-results/{client_id}")
@router.post("/match-search-results")  # Added route without client_id
async def match_search_results(client_id: str = None):
    """Match and record search results in uploads collection for all clients or specific client"""
    try:
        async with TwelveLabsService() as service:
            total_processed = 0
            
            if client_id:
                # Process single client
                video_analysis_doc = await video_analysis_collection.find_one({"client_id": client_id})
                if not video_analysis_doc:
                    raise HTTPException(status_code=404, detail=f"No video analysis found for client {client_id}")
                    
                content_flags = video_analysis_doc.get("content_flags", [])
                if content_flags:
                    await service.video_id_upload_match(content_flags, client_id)
                    total_processed = len(content_flags)
                    
                return {
                    "status": "success",
                    "message": f"Matched content flags for client {client_id}",
                    "client_id": client_id,
                    "total_flags": total_processed
                }
            else:
                # Process all clients
                results = []
                async for doc in video_analysis_collection.find({"content_flags": {"$exists": True}}):
                    client_id = doc.get("client_id")
                    content_flags = doc.get("content_flags", [])
                    
                    if client_id and content_flags:
                        try:
                            await service.video_id_upload_match(content_flags, client_id)
                            total_processed += len(content_flags)
                            results.append({
                                "client_id": client_id,
                                "flags_processed": len(content_flags)
                            })
                        except Exception as e:
                            logger.error(f"Error processing client {client_id}: {str(e)}")
                            results.append({
                                "client_id": client_id,
                                "error": str(e)
                            })
                
                return {
                    "status": "success",
                    "message": "Completed matching content flags for all clients",
                    "total_flags_processed": total_processed,
                    "results": results
                }
                
    except Exception as e:
        logger.error(f"Error in match_search_results endpoint: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summarize-videos")
async def summarize_videos():
    """Generate summaries for all newly indexed videos"""
    try:
        async with TwelveLabsService() as service:
            results = await service.summarize_all_new_videos()
            return results
    except Exception as e:
        logger.error(f"Error in summarize_videos endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summarize-video/{video_id}")
async def summarize_video(video_id: str, index_id: str):
    """Generate summary for a specific video"""
    try:
        async with TwelveLabsService() as service:
            result = await service.generate_video_summary(video_id, index_id)
            return result
    except Exception as e:
        logger.error(f"Error in summarize_video endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


