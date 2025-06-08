from bunny_cdn import BunnyCDN  # Import your Bunny CDN client

@router.get("/list-contents")
async def list_contents(path: str):
    try:
        # Get basic file listing
        contents = await get_directory_contents(path)
        
        # For each video file, fetch metadata from Bunny CDN
        for item in contents:
            if item['type'] == 'file' and item['name'].lower().endswith(('.mp4', '.mov', '.webm')):
                try:
                    # Extract video ID from filename or path
                    video_id = item['name']  # Adjust based on how you store video IDs
                    
                    # Fetch video metadata from Bunny CDN
                    metadata = await bunny_cdn.get_video_metadata(video_id)
                    
                    # Add metadata to the file object
                    item['metadata'] = {
                        'duration': metadata.get('length'),  # Video duration in seconds
                        'width': metadata.get('width'),
                        'height': metadata.get('height'),
                        'encoding': metadata.get('encode'),
                        'status': metadata.get('status')
                    }
                except Exception as e:
                    print(f"Error fetching metadata for {item['name']}: {str(e)}")
                    item['metadata'] = None
        
        return {
            "status": "success",
            "contents": contents
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        } 