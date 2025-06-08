import aiohttp
from config import settings

class BunnyCDN:
    def __init__(self):
        self.api_key = settings.BUNNY_API_KEY
        self.library_id = settings.BUNNY_LIBRARY_ID
        self.base_url = f"https://video.bunnycdn.com/library/{self.library_id}"
        self.headers = {
            "AccessKey": self.api_key,
            "accept": "application/json"
        }

    async def get_video_metadata(self, video_id: str):
        """Fetch video metadata from Bunny CDN"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/videos/{video_id}"
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'length': data.get('length'),
                        'width': data.get('width'),
                        'height': data.get('height'),
                        'encode': data.get('encode'),
                        'status': data.get('status')
                    }
                else:
                    raise Exception(f"Failed to fetch metadata: {await response.text()}") 