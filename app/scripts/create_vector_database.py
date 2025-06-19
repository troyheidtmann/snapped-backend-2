"""
Vector Database Creation Module

This module creates and manages a vector database for storing and searching
content embeddings from various sources.

Features:
- Vector database creation
- Embedding generation
- Content processing
- Search index management
- Client data handling

Data Model:
- Survey responses
- Video analysis
- Best practices
- Client metrics
- Vector embeddings

Security:
- MongoDB authentication
- API key validation
- Access control
- Error handling

Dependencies:
- OpenAI for embeddings
- MongoDB for storage
- Motor for async DB
- Certifi for TLS
- urllib for encoding

Author: Snapped Development Team
"""

import asyncio
from datetime import datetime, UTC
from openai import AsyncOpenAI
from motor.motor_asyncio import AsyncIOMotorClient
from app.shared.database import (
    survey_responses,
    content_data_collection,
    video_analysis_collection,
    MONGODB_URL,
    MONGO_SETTINGS
)
import re
import certifi
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get OpenAI API key from environment
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

# Initialize OpenAI client with environment variable
client = AsyncOpenAI(api_key=api_key)

# URL encode username and password
username = quote_plus("troyheidtmann")
password = quote_plus("Gunit1500!!!!@@@@")

# Build connection string with encoded credentials
VECTOR_DB_URL = f"mongodb+srv://{username}:{password}@chatai.3v7ig.mongodb.net/?retryWrites=true&w=majority&appName=ChatAI"

# Connection settings for the new shared cluster
VECTOR_DB_SETTINGS = {
    "tlsCAFile": certifi.where(),
    "tls": True,
    "serverSelectionTimeoutMS": 10000,
    "connectTimeoutMS": 20000,
    "maxPoolSize": 100,
    "retryWrites": True
}

async def generate_embeddings_batch(texts, batch_size=100):
    """
    Generate embeddings for a batch of texts.
    
    Args:
        texts: List of text strings to embed
        batch_size: Number of texts per batch
        
    Returns:
        list: Generated embeddings
        
    Notes:
        - Uses OpenAI API
        - Handles batching
        - Error handling
        - None for failed items
    """
    embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        try:
            response = await client.embeddings.create(
                model="text-embedding-ada-002",
                input=batch
            )
            embeddings.extend([data.embedding for data in response.data])
        except Exception as e:
            print(f"Error generating embeddings for batch: {str(e)}")
            # Fill with None for failed embeddings
            embeddings.extend([None] * len(batch))
    return embeddings

async def create_vector_search_index(collection):
    """
    Create vector search index for collection.
    
    Args:
        collection: MongoDB collection
        
    Returns:
        bool: Success status
        
    Notes:
        - Creates dummy doc
        - Defines index
        - Handles errors
        - Checks existing
    """
    try:
        # Create a dummy document to ensure collection exists
        try:
            await collection.insert_one({
                "_id": "dummy_init",
                "temp": True,
                "created_at": datetime.now(UTC)
            })
            # Delete the dummy document
            await collection.delete_one({"_id": "dummy_init"})
        except Exception as e:
            if not "duplicate key" in str(e).lower():
                print(f"Warning: Error creating dummy document: {str(e)}")
            
        # Try to create the index, but don't fail if Atlas Search is not enabled
        try:
            # Check if index already exists
            existing_indexes = []
            async for index in collection.list_search_indexes():
                existing_indexes.append(index)
                
            if any(index['name'] == 'vector_index' for index in existing_indexes):
                print("Vector search index already exists")
                return True

            # Define the index configuration for Atlas Vector Search
            index_config = {
                "name": "vector_index",
                "type": "search",
                "definition": {
                    "mappings": {
                        "dynamic": True,
                        "fields": {
                            "vectors.survey_chunks.embedding": {
                                "dimensions": 1536,
                                "similarity": "cosine",
                                "type": "knnVector"
                            },
                            "vectors.video_chunks.embedding": {
                                "dimensions": 1536,
                                "similarity": "cosine",
                                "type": "knnVector"
                            },
                            "vectors.best_practice_chunks.embedding": {
                                "dimensions": 1536,
                                "similarity": "cosine",
                                "type": "knnVector"
                            }
                        }
                    }
                }
            }
            
            # Create the index
            await collection.create_search_index(index_config)
            print("Vector search index created successfully")
            return True
        except Exception as search_error:
            if "SearchNotEnabled" in str(search_error):
                print("Warning: Atlas Search is not enabled. Skipping search index creation.")
                print("To enable Atlas Search, please visit your MongoDB Atlas dashboard:")
                print("1. Go to your cluster")
                print("2. Click on 'Search' tab")
                print("3. Click 'Create Search Index'")
                print("4. Choose 'JSON Editor' and use the index configuration from this script")
                return False
            raise  # Re-raise other types of errors
            
    except Exception as e:
        print(f"Error creating vector search index: {str(e)}")
        print(f"Full error: {e}")
        return False

def chunk_text(text, max_chars=1000):
    """
    Split text into smaller chunks.
    
    Args:
        text: Text to split
        max_chars: Maximum characters per chunk
        
    Returns:
        list: Text chunks
        
    Notes:
        - Preserves words
        - Handles empty input
        - Length control
    """
    if not text:
        return []
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        if current_length + len(word) + 1 > max_chars:
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]
            current_length = len(word)
        else:
            current_chunk.append(word)
            current_length += len(word) + 1
    
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    
    return chunks

def extract_date_from_session_id(session_id: str) -> datetime | None:
    """
    Extract date from session ID.
    
    Args:
        session_id: Session identifier
        
    Returns:
        datetime: Extracted date or None
        
    Notes:
        - Format F(MM-DD-YYYY)
        - Regex parsing
        - UTC timezone
    """
    if not session_id:
        return None
    try:
        # Extract date string between parentheses
        date_match = re.search(r'F\((.*?)\)', session_id)
        if date_match:
            date_str = date_match.group(1)
            # Parse date string in MM-DD-YYYY format
            return datetime.strptime(date_str, '%m-%d-%Y').replace(tzinfo=UTC)
    except Exception:
        return None
    return None

def is_valid_client_id(client_id: str) -> bool:
    """
    Validate client ID format.
    
    Args:
        client_id: Client identifier
        
    Returns:
        bool: Valid status
        
    Notes:
        - 2-3 letters + 8 numbers
        - Case sensitive
        - Regex validation
    """
    if not client_id or not isinstance(client_id, str):
        return False
    # Pattern: 2-3 letters followed by exactly 8 numbers
    pattern = re.compile(r'^[a-zA-Z]{2,3}\d{8}$')
    return bool(pattern.match(client_id))

async def create_vector_database():
    """
    Create vector database with embeddings.
    
    Returns:
        None
        
    Notes:
        - Processes clients
        - Generates embeddings
        - Creates indexes
        - Handles errors
        - Manages connections
    """
    # Create MongoDB client instances
    source_client = AsyncIOMotorClient(MONGODB_URL, **MONGO_SETTINGS)  # Serverless instance
    vector_client = AsyncIOMotorClient(VECTOR_DB_URL, **VECTOR_DB_SETTINGS)  # New shared cluster
    
    try:
        # Get or create vector database collection in the new cluster
        vector_db = vector_client["AIChat"]["VectorDB"]
        
        # Try to create vector search index
        index_created = await create_vector_search_index(vector_db)
        if not index_created:
            print("Note: Vector search capabilities will be limited until Atlas Search is enabled")
        
        # Get all unique client IDs from video analysis collection in source
        unique_client_ids = await video_analysis_collection.distinct("client_id")
        print(f"Found {len(unique_client_ids)} unique clients to process")
        
        # Filter out None client IDs and validate format
        unique_client_ids = [cid for cid in unique_client_ids if cid is not None and is_valid_client_id(cid)]
        print(f"After filtering invalid client IDs: {len(unique_client_ids)} valid clients to process")
        
        # Sort client IDs for consistent processing order
        unique_client_ids.sort()
        print("First few client IDs:", unique_client_ids[:5])
        
        # Get best practices from source
        snapbest_collection = source_client["AIChat"]["SnapBest"]
        best_practices = await snapbest_collection.find_one({})
        
        # Process best practices once since they're common
        best_practice_chunks = []
        if best_practices:
            texts_to_embed = []
            metadata = []
            for section, content in best_practices["SnapchatContentCreatorBestPractices"].items():
                text = f"{section}\n{content['Description']}\nTips:\n" + "\n".join(content["Tips"])
                texts_to_embed.append(text)
                metadata.append({
                    "type": "best_practice",
                    "source": content.get("Source")
                })
            
            embeddings = await generate_embeddings_batch(texts_to_embed)
            for text, embedding, meta in zip(texts_to_embed, embeddings, metadata):
                if embedding:
                    best_practice_chunks.append({
                        "text": text,
                        "embedding": embedding,
                        **meta
                    })
        
        for client_id in unique_client_ids:
            try:
                print(f"Processing client: {client_id}")
                
                # Get all related data using client_id
                survey_data = await survey_responses.find_one({
                    "$or": [
                        {"client_id": client_id},
                        {"user_id": client_id}
                    ]
                })
                
                # Query content_data using client_id (which is the same as user_id)
                print(f"Searching content_data for client: {client_id}")
                content_data = await content_data_collection.find({
                    "$or": [
                        {"client_id": client_id},
                        {"user_id": client_id}  # This is the same as client_id
                    ]
                }).to_list(None)

                # If we found content data, update any documents that are missing client_id
                if content_data:
                    for doc in content_data:
                        if not doc.get("client_id") and doc.get("user_id"):
                            print(f"Updating document to set client_id = user_id: {doc.get('user_id')}")
                            await content_data_collection.update_one(
                                {"_id": doc["_id"]},
                                {"$set": {"client_id": doc["user_id"]}}
                            )

                # If no content data found, try survey data as last resort
                if not content_data:
                    print(f"No content data found with direct ID match, trying survey data...")
                    if survey_data and survey_data.get("responses", {}).get("responses"):
                        snapchat_handle = survey_data["responses"]["responses"].get("what_is_your_snapchat_handle")
                        if snapchat_handle:
                            print(f"Found Snapchat handle in survey: {snapchat_handle}")
                            content_data = await content_data_collection.find({
                                "$or": [
                                    {"client_id": snapchat_handle},
                                    {"user_id": snapchat_handle},
                                    {"snap_profile_name": snapchat_handle}
                                ]
                            }).to_list(None)

                video_analysis = await video_analysis_collection.find({
                    "client_id": client_id
                }).to_list(None)

                # Debug output
                print(f"\nProcessing {client_id}:")
                print(f"- Found survey data: {bool(survey_data)}")
                print(f"- Found content data entries: {len(content_data) if content_data else 0}")
                if content_data:
                    print(f"- Content data matched on:")
                    print(f"  - client_id: {content_data[0].get('client_id')}")
                    print(f"  - user_id: {content_data[0].get('user_id')}")
                    print(f"  - snap_id: {content_data[0].get('snap_id')}")
                    print(f"  - snap_profile_name: {content_data[0].get('snap_profile_name')}")
                print(f"- Found video analysis entries: {len(video_analysis) if video_analysis else 0}")
                
                # Process survey responses in batch
                survey_chunks = []
                if survey_data and survey_data.get("responses", {}).get("responses"):
                    texts_to_embed = []
                    for q, a in survey_data["responses"]["responses"].items():
                        if a:  # Only process non-empty answers
                            texts_to_embed.append(f"Question: {q}\nAnswer: {str(a)}")
                    
                    if texts_to_embed:
                        embeddings = await generate_embeddings_batch(texts_to_embed)
                        for text, embedding in zip(texts_to_embed, embeddings):
                            if embedding:
                                survey_chunks.append({
                                    "text": text,
                                    "embedding": embedding,
                                    "type": "survey_qa"
                                })
                
                # Process video summaries in batch
                video_chunks = []
                texts_to_embed = []
                video_ids = []
                session_dates = []
                for doc in video_analysis:
                    for story in doc.get("STORY", []):
                        if summary := story.get("video_summary"):
                            chunks = chunk_text(summary)
                            texts_to_embed.extend(chunks)
                            video_ids.extend([story.get("video_id")] * len(chunks))
                            session_date = extract_date_from_session_id(story.get("session_id"))
                            session_dates.extend([session_date] * len(chunks))
                
                if texts_to_embed:
                    embeddings = await generate_embeddings_batch(texts_to_embed)
                    for text, embedding, video_id, session_date in zip(texts_to_embed, embeddings, video_ids, session_dates):
                        if embedding:
                            video_chunks.append({
                                "text": text,
                                "embedding": embedding,
                                "type": "video_summary",
                                "video_id": video_id,
                                "session_date": session_date.isoformat() if session_date else None
                            })
                
                # Get client name and metrics from content data first, then fall back to survey data
                client_name = None
                content_metrics = {
                    "platform": "snapchat",
                    "sessions": [],
                    "snap_profile_name": None,
                    "snap_id": None
                }
                
                if content_data:
                    client_doc = content_data[0]
                    content_metrics["snap_profile_name"] = client_doc.get("snap_profile_name")
                    content_metrics["snap_id"] = client_doc.get("snap_id")
                    # Process each content data document
                    for doc in content_data:
                        # Get sessions array from the document
                        sessions = doc.get("sessions", [])
                        for session in sessions:
                            session_metrics = session.get("metrics", {})
                            metrics = {
                                "session_id": session.get("session_id"),
                                "date": session.get("date"),
                                "upload_date": session.get("upload_date"),
                                # Get metrics fromested structure
                                "engagement": {
                                    "followers": session_metrics.get("engagement", {}).get("followers", 0),
                                    "followers_added": session_metrics.get("engagement", {}).get("followers_added", 0),
                                    "followers_lost": session_metrics.get("engagement", {}).get("followers_lost", 0),
                                    "engagement_rate": session_metrics.get("engagement", {}).get("engagement_rate", 0)
                                },
                                "content": {
                                    "posts": session_metrics.get("content", {}).get("posts", 0),
                                    "stories": session_metrics.get("content", {}).get("stories", 0),
                                    "saved_stories": session_metrics.get("content", {}).get("saved_stories", 0),
                                    "spotlights": session_metrics.get("content", {}).get("spotlights", 0)
                                },
                                "interactions": {
                                    "likes": session_metrics.get("interactions", {}).get("likes", 0),
                                    "shares": session_metrics.get("interactions", {}).get("shares", 0),
                                    "replies": session_metrics.get("interactions", {}).get("replies", 0),
                                    "screenshots": session_metrics.get("interactions", {}).get("screenshots", 0),
                                    "swipe_ups": session_metrics.get("interactions", {}).get("swipe_ups", 0),
                                    "swipe_downs": session_metrics.get("interactions", {}).get("swipe_downs", 0)
                                },
                                "views": {
                                    "impressions": session_metrics.get("views", {}).get("impressions", 0),
                                    "reach": session_metrics.get("views", {}).get("reach", 0),
                                    "profile_views": session_metrics.get("views", {}).get("profile_views", 0),
                                    "story_views": session_metrics.get("views", {}).get("story_views", 0),
                                    "spotlight_views": session_metrics.get("views", {}).get("spotlight_views", 0),
                                    "lens_views": session_metrics.get("views", {}).get("lens_views", 0),
                                    "saved_story_views": session_metrics.get("views", {}).get("saved_story_views", 0)
                                },
                                "time_metrics": {
                                    "story_view_time": session_metrics.get("time_metrics", {}).get("story_view_time", 0),
                                    "saved_story_view_time": session_metrics.get("time_metrics", {}).get("saved_story_view_time", 0),
                                    "snap_view_time": session_metrics.get("time_metrics", {}).get("snap_view_time", 0),
                                    "spotlight_view_time": session_metrics.get("time_metrics", {}).get("spotlight_view_time", 0),
                                    "lens_view_time": session_metrics.get("time_metrics", {}).get("lens_view_time", 0)
                                },
                                "other": {
                                    "awareness": session_metrics.get("other", {}).get("awareness", 0),
                                    "scans": session_metrics.get("other", {}).get("scans", 0)
                                }
                            }
                            content_metrics["sessions"].append(metrics)

                if survey_data:
                    client_name = survey_data.get("client_name")
                elif content_data:
                    # Fall back to snap_profile_name if no survey data
                    client_name = content_metrics["snap_profile_name"]
                
                # Structure the unified document with vectors
                vector_doc = {
                    "client_id": client_id,
                    "client_name": client_name,
                    "last_updated": datetime.now(UTC),
                    "vectors": {
                        "survey_chunks": survey_chunks,
                        "video_chunks": video_chunks,
                        "best_practice_chunks": best_practice_chunks
                    },
                    "metadata": {
                        "survey_timestamp": survey_data.get("timestamp") if survey_data else None,
                        "content_metrics": content_metrics
                    },
                    "vector_metadata": {
                        "embedding_model": "text-embedding-ada-002",
                        "embedding_version": "v1",
                        "last_embedded": datetime.now(UTC),
                        "embedding_status": "completed",
                        "total_vectors": len(survey_chunks) + len(video_chunks) + len(best_practice_chunks)
                    }
                }
                
                # Replace the entire document
                await vector_db.replace_one(
                    {"client_id": client_id},
                    vector_doc,
                    upsert=True
                )
                
                print(f"Successfully processed client: {client_id} ({client_name}) with {vector_doc['vector_metadata']['total_vectors']} vectors")
                
            except Exception as e:
                print(f"Error processing client {client_id}: {str(e)}")
                continue
        
        print("Vector database creation completed")
        
    except Exception as e:
        print(f"Error creating vector database: {str(e)}")
    finally:
        # Close both MongoDB clients
        source_client.close()
        vector_client.close()

if __name__ == "__main__":
    # Check if vector DB URL is configured
    if VECTOR_DB_URL == "YOUR_NEW_CLUSTER_URL":
        print("Error: Please configure VECTOR_DB_URL with your new shared cluster connection string")
        exit(1)
    
    asyncio.run(create_vector_database()) 