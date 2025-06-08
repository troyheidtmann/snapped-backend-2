"""
Vista Analytics Service Module

This module handles the integration with Vista Social's analytics API, providing automated
data synchronization for Snapchat creator analytics. It manages data fetching, processing,
and storage in the MongoDB database.

System Architecture:
    - External Integration:
        * Vista Social API
        * ZIP file processing
        * CSV data parsing
    
    - Data Storage:
        * MongoDB Collections:
            - content_data: Stores processed analytics
            - ClientInfo: Client metadata
    
    - Data Processing Pipeline:
        1. API Data Fetching:
            - Authentication via JWT
            - Date range filtering
            - Bulk profile data export
        2. Data Processing:
            - ZIP extraction
            - CSV parsing
            - Metric normalization
        3. Storage:
            - Incremental updates
            - Session-based storage
            - Atomic operations

Security:
    - JWT Authentication
    - Secure cookie handling
    - HTTPS communication
    - Error isolation

Data Models:
    1. Analytics Session:
        - Temporal metrics (daily granularity)
        - Engagement metrics
        - Content metrics
        - View metrics
        - Time-based metrics
    
    2. Profile Data:
        - Snapchat identifiers
        - Platform metadata
        - Update tracking

Error Handling:
    - Request failures
    - Data validation
    - Processing errors
    - Storage conflicts
"""

import aiohttp
import asyncio
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from app.shared.database import async_client
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class VistaAnalyticsService:
    """
    Service class for managing Vista Social analytics data integration.
    
    Responsibilities:
        - API communication with Vista Social
        - Data fetching and processing
        - Database synchronization
        - Error handling and logging
    
    Integration Points:
        - Vista Social API
        - MongoDB storage
        - Logging system
    """
    
    def __init__(self):
        """
        Initialize Vista Analytics Service with configuration.
        
        Components:
            - API endpoint configuration
            - Database connections
            - Authentication tokens
        """
        self.base_url = "https://vistasocial.com/api"
        self.content_data_collection = async_client['ClientDb']['content_data']
        self.client_info_collection = async_client['ClientDb']['ClientInfo']
        self.vista_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjA2NjY3LCJpYXQiOjE3NDg4ODg0NjF9.xFGnuB90CR_5aZnjSZoPBmVPBm8IHKUINR5GNGVXvas"

    async def fetch_vista_data(self, profile_gids: list, date_from: str, date_to: str) -> bytes:
        """
        Fetch analytics data from Vista API.
        
        Request Flow:
            1. Authentication setup
            2. Request formatting
            3. API communication
            4. Response validation
        
        Error Handling:
            - Connection errors
            - Authentication failures
            - Invalid responses
            - Content type validation
        
        Args:
            profile_gids: List of Vista profile IDs
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
        
        Returns:
            ZIP file content as bytes
        """
        url = f"{self.base_url}/export/profiles"
        
        # Prepare the JSON payload first
        payload = {
            "profile_gids": profile_gids,
            "dateFrom": date_from,
            "dateTo": date_to
        }
        json_data = json.dumps(payload)
        
        # Add cookies from the working request
        cookies = {
            "_omappvp": "eQXOJizCjLiiBzDAcuyF8R5Bc00KwCf6OMNy2t7Exs7WkkLUZdjoqCmwrmjjSyJA98sY9rk2rtqHeZGF1ub3K7NOvdtUlYPY",
            "_ga": "GA1.1.1941661738.1736216543",
            "_fbp": "fb.1.1736216543335.397776428709945965",
            "hubspotutk": "a418f68049f551bfa58e6f3b29784ca7",
            "_sleek_session": "%7B%22init%22%3A%222025-02-16T23%3A00%3A57.093Z%22%7D",
            "_gcl_au": "1.1.1130043600.1746498146",
            "connect.sid": "s%3Am70x8sOQAbX5gXh5kkHe9-qdlldFVuOF.crDDen5P8809FKlZjAIuY11s1i33uV8KdT4C0axUf7M",
            "__hstc": "243085085.a418f68049f551bfa58e6f3b29784ca7.1736216543640.1741571486489.1746498146578.11",
            "__hssrc": "1",
            "jwt": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjA2NjY3LCJpYXQiOjE3NDY0OTgxNzV9.wgVJjXarCXXlWLsl8yQ5ncNNq9nCR3U_dAl5aolJZwY",
            "__hssc": "243085085.4.1746498146578",
            "_uetsid": "f38a2d802a2011f085ace311abc8e095",
            "_uetvid": "f38a45702a2011f08078af20c2ae67a1",
            "_ga_6TGX06C7CZ": "GS2.1.s1746498146$o17$g1$t1746499796$j11$l0$h0",
            "AWSALB": "TeFpTtuukDn7oMekCuHW3wWcDqBAtCXDDuetq3AndXCXaGL88HqbcdPxkqcqr3hi5k7EY+AUjpS9ysJ4qhFI4QJUND36Ti2UY2A4fyFZeDzWS37y3hAbidOHEvBD",
            "AWSALBCORS": "TeFpTtuukDn7oMekCuHW3wWcDqBAtCXDDuetq3AndXCXaGL88HqbcdPxkqcqr3hi5k7EY+AUjpS9ysJ4qhFI4QJUND36Ti2UY2A4fyFZeDzWS37y3hAbidOHEvBD"
        }
        
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "authorization": f"Bearer {self.vista_token}",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "content-length": str(len(json_data)),
            "origin": "https://vistasocial.com",
            "pragma": "no-cache",
            "referer": "https://vistasocial.com/listening?network=snapchat&from_date=2025-02-09&to_date=2025-03-09&profile=all&report_type=comprehensive",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }

        logger.info("Vista API Request Details:")
        logger.info(f"URL: {url}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Cookies: {cookies}")
        logger.info(f"Payload: {json_data}")

        async with aiohttp.ClientSession(cookies=cookies) as session:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Vista API error: {response.status}, Response: {error_text}")
                    raise Exception(f"Failed to fetch Vista data: {error_text}")
                
                content_type = response.headers.get('Content-Type', '')
                logger.info(f"Response Content-Type: {content_type}")
                logger.info(f"Response Headers: {dict(response.headers)}")
                
                response_data = await response.read()
                
                # Log the first few bytes to help debug
                logger.info(f"First 50 bytes of response: {response_data[:50]}")
                
                # Verify we got a ZIP file
                if content_type != 'application/zip':
                    error_text = response_data.decode('utf-8', errors='ignore')
                    logger.error(f"Expected application/zip but got {content_type}")
                    logger.error(f"Response: {error_text[:200]}")
                    raise Exception("Response is not a ZIP file")
                
                return response_data

    async def process_csv_data(self, csv_content: str, snap_id: str, client: dict):
        """
        Process CSV data and store in MongoDB.
        
        Processing Steps:
            1. Date validation and filtering
            2. Data parsing and normalization
            3. Metric calculation
            4. Incremental updates
        
        Data Validation:
            - Date format consistency
            - Numeric value handling
            - Missing data management
        
        Storage Strategy:
            - Atomic updates
            - Session-based organization
            - Duplicate prevention
        
        Args:
            csv_content: Raw CSV data
            snap_id: Snapchat profile identifier
            client: Client metadata dictionary
        """
        try:
            logger.info(f"Processing CSV data for snap_id: {snap_id}")
            
            # First get the latest session date for this profile
            existing_doc = await self.content_data_collection.find_one(
                {"snap_profile_name": snap_id, "platform": "snapchat"},
                {"sessions.date": 1}
            )
            
            latest_date = None
            if existing_doc and existing_doc.get('sessions'):
                # Convert all dates to datetime for comparison
                dates = [datetime.strptime(s['date'], '%m-%d-%Y') for s in existing_doc['sessions']]
                latest_date = max(dates)
                logger.info(f"Latest existing session date for {snap_id}: {latest_date.strftime('%m-%d-%Y')}")
            
            df = pd.read_csv(io.StringIO(csv_content))
            logger.info(f"Found {len(df)} rows of data")
            
            # Group all sessions for this snap_id
            sessions = []
            for _, row in df.iterrows():
                # Convert date format
                date_obj = datetime.strptime(row['date'], '%B %d, %Y')
                
                # Skip if this date is not newer than our latest date
                if latest_date and date_obj <= latest_date:
                    logger.info(f"Skipping date {date_obj.strftime('%m-%d-%Y')} - already exists")
                    continue
                    
                formatted_date = date_obj.strftime('%m-%d-%Y')
                logger.info(f"Adding new data for date: {formatted_date}")

                session_data = {
                    "date": formatted_date,
                    "metrics": {
                        "engagement": {
                            "followers": int(row['followers']),
                            "followers_added": int(row['followers_added']),
                            "followers_lost": int(row['followers_lost']),
                            "engagement_rate": float(row['engagement'])
                        },
                        "content": {
                            "posts": int(row['posts']),
                            "stories": int(row.get('stories', 0)),
                            "saved_stories": int(row.get('saved_stories', 0)),
                            "spotlights": int(row.get('spotlights', 0))
                        },
                        "interactions": {
                            "likes": int(row.get('likes', 0)),
                            "shares": int(row.get('shares', 0)),
                            "replies": int(row.get('replies', 0)),
                            "screenshots": int(row.get('screenshots', 0)),
                            "swipe_ups": int(row.get('swipe_ups', 0)),
                            "swipe_downs": int(row.get('swipe_downs', 0))
                        },
                        "views": {
                            "impressions": int(row.get('impressions', 0)),
                            "reach": int(row.get('reach', 0)),
                            "profile_views": int(row.get('profile_views', 0)),
                            "story_views": int(row.get('story_views', 0)),
                            "spotlight_views": int(row.get('spotlight_views', 0)),
                            "lense_views": int(row.get('lense_views', 0)),
                            "saved_story_views": int(row.get('saved_story_views', 0))
                        },
                        "time_metrics": {
                            "story_view_time": int(row.get('story_view_time', 0)),
                            "saved_story_view_time": int(row.get('saved_story_view_time', 0)),
                            "snap_view_time": int(row.get('snap_view_time', 0)),
                            "spotlight_view_time": int(row.get('spotlight_view_time', 0)),
                            "lense_view_time": int(row.get('lense_view_time', 0))
                        },
                        "other": {
                            "awareness": int(row.get('awareness', 0)),
                            "scans": int(row.get('scans', 0))
                        }
                    }
                }
                sessions.append(session_data)

            if not sessions:
                logger.info(f"No new sessions to add for {snap_id}")
                return

            logger.info(f"Adding {len(sessions)} new sessions for {snap_id}")
            
            # Update database - only update sessions and last_updated
            result = await self.content_data_collection.update_one(
                {
                    "snap_profile_name": snap_id,
                    "platform": "snapchat"
                },
                {
                    "$push": {
                        "sessions": {
                            "$each": sessions
                        }
                    },
                    "$set": {
                        "last_updated": datetime.now()
                    },
                    "$setOnInsert": {  # Only set these fields if document doesn't exist
                        "platform": "snapchat",
                        "snap_profile_name": snap_id,
                        "user_id": client.get('client_id')
                    }
                },
                upsert=True
            )
            
            logger.info(f"Database update for {snap_id}:")
            logger.info(f"Modified: {result.modified_count}")
            logger.info(f"Upserted: {result.upserted_id is not None}")

        except Exception as e:
            logger.error(f"Error processing CSV data for snap_id {snap_id}")
            logger.error(f"Error details: {str(e)}")
            raise

    async def sync_analytics(self):
        """
        Main synchronization function for Vista analytics.
        
        Workflow:
            1. Time Range Determination:
                - Calculate sync window (48 hours)
                - Handle timezone considerations
            
            2. Bulk Data Retrieval:
                - Fetch multiple profile data
                - ZIP file handling
                - Error isolation
            
            3. Processing Pipeline:
                - File extraction
                - CSV processing
                - Database updates
            
            4. Monitoring:
                - Success/failure tracking
                - Performance logging
                - Error reporting
        
        Error Handling:
            - Critical errors logged and raised
            - Individual file failures isolated
            - Summary reporting
        """
        try:
            logger.info("=== Starting Vista Analytics Sync ===")
            
            today = datetime.now(timezone.utc)
            
            # Option 1: Always fetch last 48 hours to ensure we get latest data
            date_from = (today - timedelta(days=2)).strftime('%Y-%m-%d')
            date_to = today.strftime('%Y-%m-%d')
            
            logger.info(f"Current UTC time: {today}")
            logger.info(f"Fetching data from: {date_from} to: {date_to}")
            
            # Updated profile IDs list to include all 30 profiles
            profile_ids = [
                460427, 460442, 462587, 462588, 462598, 463075, 463552, 463553,
                463630, 466428, 466550, 466551, 466552, 466553, 466554, 466555,
                466556, 466557, 466558, 475622, 498663, 498841, 512091, 515505,
                521819, 529653, 530860, 530861, 536936, 539122
            ]
            
            # Fetch data with date range
            zip_data = await self.fetch_vista_data(profile_ids, date_from, date_to)
            logger.info("Successfully received ZIP data from Vista API")
            
            processed_count = 0
            error_count = 0
            
            # Process all ZIP contents with detailed logging
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_ref:
                total_files = len(zip_ref.namelist())
                logger.info(f"Processing {total_files} files from ZIP archive")
                
                for filename in zip_ref.namelist():
                    if not filename.endswith('.csv'):
                        logger.info(f"Skipping non-CSV file: {filename}")
                        continue
                    
                    try:
                        # Extract username from filename
                        username = filename.split('_')[0]
                        logger.info(f"\nProcessing file for username: {username}")
                        logger.info(f"Reading CSV content from {filename}")

                        # Process CSV file
                        csv_content = zip_ref.read(filename).decode('utf-8')
                        client = {"username": username}
                        
                        logger.info(f"Processing data for {username}")
                        await self.process_csv_data(csv_content, username, client)
                        
                        processed_count += 1
                        logger.info(f"Successfully processed data for {username}")
                        
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error processing {filename}: {str(e)}")
                        
            # Log final summary
            logger.info("\n=== Vista Analytics Sync Summary ===")
            logger.info(f"Total files processed: {processed_count}")
            logger.info(f"Successful: {processed_count - error_count}")
            logger.info(f"Errors: {error_count}")
            logger.info(f"Sync completed at: {datetime.now()}")
            logger.info("=====================================\n")
            
        except Exception as e:
            logger.error(f"Critical error in sync_analytics: {str(e)}")
            raise 