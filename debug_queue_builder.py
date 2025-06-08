import asyncio
import motor.motor_asyncio
import certifi
from urllib.parse import quote_plus
from datetime import datetime

username = quote_plus('troyheidtmann')
password = quote_plus('Gunit1500!!!!@@@@')
MONGODB_URL = f'mongodb+srv://{username}:{password}@clientdb.fsb2wz0.mongodb.net/?retryWrites=true&w=majority'

class DebugSpotQueueBuilder:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URL, tlsCAFile=certifi.where())
        self.db = self.client['UploadDB']
        self.spotlights = self.db['Spotlights']
        self.MORNING_POST_TIME = 16
        self.POST_START_MINUTES = 0

    async def debug_build_daily_queue(self, queue_date: datetime = None) -> dict:
        """Debug version of build_daily_queue with extensive logging."""
        try:
            if not queue_date:
                queue_date = datetime.now()

            print(f"Building daily queue for {queue_date}")

            # Initialize daily queue
            daily_queue = {
                'queue_date': queue_date.strftime('%Y-%m-%d'),
                'created_at': datetime.now().isoformat(),
                'client_queues': {},
                'total_posts': 0,
                'status': 'pending'
            }

            # Get all clients
            cursor = self.spotlights.find({})
            client_count = 0
            processed_clients = []
            
            print("Starting to iterate through clients...")
            
            async for doc in cursor:
                try:
                    client_count += 1
                    # Get client ID from either field
                    client_id = doc.get('client_ID') or doc.get('client_id')
                    if not client_id:
                        print(f"Document {client_count} missing client_ID/client_id field")
                        continue

                    print(f"\n=== Processing client {client_count}: {client_id} ===")
                    processed_clients.append(client_id)

                    # Get all sessions (both regular and TikTok)
                    all_sessions = []
                    regular_sessions = doc.get('sessions', [])
                    tt_sessions = doc.get('tt_sessions', [])
                    
                    print(f"Client {client_id} has {len(regular_sessions)} regular sessions and {len(tt_sessions)} TikTok sessions")
                    
                    all_sessions.extend(regular_sessions)
                    all_sessions.extend(tt_sessions)

                    if not all_sessions:
                        print(f"Client {client_id} has no sessions, skipping")
                        continue

                    # Try to queue files
                    print(f"Calling _prepare_client_queue for {client_id}")
                    client_queue = await self._prepare_client_queue(client_id, all_sessions)
                    print(f"_prepare_client_queue returned {len(client_queue) if client_queue else 0} files for {client_id}")
                    
                    if client_queue:
                        print(f"Processing {len(client_queue)} files for {client_id}")
                        # Schedule posts 5 minutes apart
                        scheduled_posts = []
                        files_to_queue = []
                        
                        # Calculate time blocks for all possible files
                        time_blocks = [
                            queue_date.replace(hour=self.MORNING_POST_TIME, minute=self.POST_START_MINUTES + i*5, second=0)
                            for i in range(len(client_queue))
                        ]
                        
                        for i, post in enumerate(client_queue):
                            scheduled_time = time_blocks[i].isoformat()
                            queue_time = datetime.now().isoformat()
                            
                            # Update post data with scheduling info
                            post['scheduled_time'] = scheduled_time
                            post['queued'] = True
                            post['queue_time'] = queue_time
                            
                            scheduled_posts.append(post)
                            files_to_queue.append({
                                "file_name": post['file_name'],
                                "session_id": post['session_id'],
                                "source": post['source'],
                                "queue_time": queue_time
                            })
                        
                        print(f"About to mark {len(files_to_queue)} files as queued for {client_id}")
                        # Mark these files as queued
                        await self.mark_files_as_queued(client_id, files_to_queue)
                        print(f"Successfully marked files as queued for {client_id}")
                        
                        daily_queue['client_queues'][client_id] = {
                            "posts": scheduled_posts,
                            "processed": False
                        }
                        daily_queue['total_posts'] += len(scheduled_posts)
                        print(f"Successfully queued {len(scheduled_posts)} posts for {client_id}")
                    else:
                        print(f"No files found to queue for client {client_id}")

                except Exception as e:
                    print(f"ERROR processing client {client_id}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue

            print(f"\nProcessed {len(processed_clients)} clients: {processed_clients}")
            print(f"Daily queue built with {daily_queue['total_posts']} total posts")
            return daily_queue

        except Exception as e:
            print(f"ERROR building daily queue: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    async def _prepare_client_queue(self, client_id: str, client_sessions: list[dict]) -> list[dict]:
        """Prepare a queue for a client by selecting unqueued files from their sessions."""
        print(f"  Preparing queue for client {client_id}")
        queued_files = []

        for session in client_sessions:
            session_id = session.get('session_id')
            if not session_id:
                print(f"  Session for client {client_id} missing session_id")
                continue

            print(f"  Processing session {session_id} for client {client_id}")
            files = session.get('files', [])
            print(f"  Found {len(files)} files in session")
            
            for file in files:
                # Get filename from either file_name, filename, or extract from cdn_url
                file_name = file.get('file_name') or file.get('filename')
                if not file_name and file.get('cdn_url'):
                    # Extract filename from cdn_url if no explicit filename
                    cdn_url = file['cdn_url']
                    file_name = cdn_url.split('/')[-1]
                    print(f"    Extracted filename {file_name} from cdn_url")
                
                if not file_name:
                    print(f"    File in session {session_id} has no filename and no cdn_url")
                    continue

                # Check if file is already queued
                queued = file.get('queued', False)
                stats_queued = file.get('stats', {}).get('queued')
                if queued is True or stats_queued is True:
                    print(f"    File {file_name} is already queued (queued={queued}, stats.queued={stats_queued})")
                    continue

                # Get file type
                file_type = file.get('file_type')
                if not file_type:
                    if file_name.lower().endswith(('.mp4', '.mov')):
                        file_type = 'video/mp4'
                    elif file_name.lower().endswith(('.jpg', '.jpeg')):
                        file_type = 'image/jpeg'
                    elif file_name.lower().endswith('.png'):
                        file_type = 'image/png'
                    else:
                        file_type = 'video/mp4'  # Default to video

                # Get snap ID from either field
                snap_id = file.get('snap_id') or file.get('snap_ID')

                print(f"    Found unqueued file: {file_name} (type={file_type})")
                
                # Prepare post data
                post_data = {
                    'file_name': file_name,
                    'cdn_url': file.get('cdn_url'),
                    'file_type': file_type,
                    'snap_id': snap_id,
                    'scheduled_time': None,  # Will be set by caller
                    'session_id': session_id,
                    'content_type': 'spotlight',
                    'source': 'tiktok' if session_id.startswith('TT_') else 'regular',
                    'caption': file.get('caption', '')[:65],  # Truncate caption to 65 chars
                    'queued': False,
                    'queue_time': None  # Will be set when actually queued
                }
                queued_files.append(post_data)

                if len(queued_files) >= 2:
                    print(f"    Found enough files ({len(queued_files)}) for client {client_id}")
                    return queued_files

        print(f"  Found {len(queued_files)} files to queue for client {client_id}")
        return queued_files

    async def mark_files_as_queued(self, client_id: str, queued_files: list):
        """Mark files as queued in the database."""
        try:
            print(f"    Marking {len(queued_files)} files as queued for {client_id}")
            for file_data in queued_files:
                file_name = file_data['file_name']
                session_id = file_data['session_id']
                source = file_data['source']
                queue_time = file_data['queue_time']

                print(f"      Marking {file_name} in session {session_id} (source: {source})")

                # Update for regular files
                if source == 'regular':
                    result = await self.spotlights.update_one(
                        {
                            "$or": [
                                {"client_ID": client_id},
                                {"client_id": client_id}
                            ],
                            "sessions.session_id": session_id,
                            "sessions.files": {
                                "$elemMatch": {
                                    "$or": [
                                        {"file_name": file_name},
                                        {"filename": file_name}
                                    ]
                                }
                            }
                        },
                        {
                            "$set": {
                                "sessions.$[session].files.$[file].queued": True,
                                "sessions.$[session].files.$[file].queue_time": queue_time
                            }
                        },
                        array_filters=[
                            {"session.session_id": session_id},
                            {
                                "$or": [
                                    {"file.file_name": file_name},
                                    {"file.filename": file_name}
                                ]
                            }
                        ]
                    )
                # Update for TikTok files
                else:
                    result = await self.spotlights.update_one(
                        {
                            "$or": [
                                {"client_ID": client_id},
                                {"client_id": client_id}
                            ],
                            "tt_sessions.session_id": session_id,
                            "tt_sessions.files": {
                                "$elemMatch": {
                                    "$or": [
                                        {"file_name": file_name},
                                        {"filename": file_name}
                                    ]
                                }
                            }
                        },
                        {
                            "$set": {
                                "tt_sessions.$[session].files.$[file].queued": True,
                                "tt_sessions.$[session].files.$[file].queue_time": queue_time
                            }
                        },
                        array_filters=[
                            {"session.session_id": session_id},
                            {
                                "$or": [
                                    {"file.file_name": file_name},
                                    {"file.filename": file_name}
                                ]
                            }
                        ]
                    )
                
                print(f"        Update result: matched={result.matched_count}, modified={result.modified_count}")
                
        except Exception as e:
            print(f"    ERROR marking files as queued for {client_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

async def main():
    builder = DebugSpotQueueBuilder()
    result = await builder.debug_build_daily_queue()
    print(f"\nFinal result: {len(result['client_queues'])} clients queued")
    for client_id, queue in result['client_queues'].items():
        print(f"  {client_id}: {len(queue['posts'])} posts")

if __name__ == "__main__":
    asyncio.run(main()) 