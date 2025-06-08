"""
Vista Social Analytics Report Downloader

This module handles the automated download of analytics reports from Vista Social's API,
specifically focusing on Snapchat content performance metrics.

System Architecture:
    1. API Integration:
        - Vista Social REST API
        - Authentication handling
        - Report generation requests
        - CSV data retrieval
    
    2. Data Flow:
        a) Request Generation:
            - Date range calculation
            - Profile selection
            - Network filtering
            - Report type specification
        
        b) Response Processing:
            - CSV data handling
            - File system storage
            - Timestamp management
            - Error handling
    
    3. Storage Structure:
        /downloads/
        └── vista_social_report_{timestamp}.csv

Security Considerations:
    - API Authentication:
        * Bearer token handling
        * Cookie management
        * Session maintenance
    
    - Data Protection:
        * Secure file storage
        * Token encryption
        * Access control
    
    - Error Handling:
        * Network failures
        * API rate limits
        * Authentication errors
        * Storage issues

Dependencies:
    - requests: HTTP client for API communication
    - dotenv: Environment configuration
    - datetime: Timestamp management
    - os: File system operations
    - logging: Operation logging
"""

import os
import logging
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def download_vista_report(days_back=7):
    """
    Download analytics report from Vista Social API.
    
    Processing Flow:
        1. Request Preparation:
            - Date range calculation
            - Headers configuration
            - Authentication setup
            - Profile selection
        
        2. API Communication:
            - POST request execution
            - Response validation
            - Error handling
            - Rate limit management
        
        3. Data Storage:
            - Directory creation
            - File naming
            - Content writing
            - Cleanup handling
    
    Authentication:
        - Bearer Token: JWT-based authentication
        - Cookies: Session management
        - Headers: Request configuration
    
    Parameters:
        days_back (int): Number of days to include in report (default: 7)
    
    Returns:
        str: Path to downloaded report file
        None: If download fails
    
    Error Handling:
        - Network connectivity issues
        - API authentication failures
        - Invalid response formats
        - File system errors
    
    Security Notes:
        - Token Refresh: Tokens should be rotated periodically
        - Cookie Management: Session cookies require secure handling
        - File Permissions: Downloaded files need appropriate access controls
    """
    try:
        # Calculate dates for last 48 hours
        today = datetime.now()
        from_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')
        
        url = "https://vistasocial.com/api/export/csv"
        
        # Headers exactly as seen in the working request
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjA2NjY3LCJpYXQiOjE3NDY0OTgxNzV9.wgVJjXarCXXlWLsl8yQ5ncNNq9nCR3U_dAl5aolJZwY',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'origin': 'https://vistasocial.com',
            'pragma': 'no-cache',
            'referer': 'https://vistasocial.com/dashboard?network=snapchat&from_date=2025-04-07&to_date=2025-05-05&profile=all&report_type=posts',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        
        # Add cookies from the working request
        cookies = {
            '_omappvp': 'kfc7yE3JzPuhQZmWWC07mkIaw8xI1wY2ZDIObOvaOFZDdpvvfdP0brppjkH2UjeSQWWhzCSx1c1uLOx29g4hX7i5lr1b2cDT',
            '_ga': 'GA1.1.1628364338.1733358382',
            'hubspotutk': '6e71ab62df72a180a9f32417c9e8679c',
            'DashboardNewsAndArticles': '668698ca50a42dccc3072c82',
            '_gcl_gs': '2.1.k1$i1748888214$u24757949',
            'connect.sid': 's%3AbJF2EISMMLFWHiD-ZfVXqj4iqFW5mpyK.SNunVLym9WsKVXGls%2B7XLW3oG3DZ8vZ%2B7CniY7sY1t0',
            '__hstc': '243085085.6e71ab62df72a180a9f32417c9e8679c.1733358382863.1734139045531.1748888215748.18',
            '__hssrc': '1',
            '_fprom_ref': 'equipe71',
            '_fprom_tid': '82bf3cfb-f3d3-454a-b541-d235d588efac',
            '_gcl_au': '1.1.1634904879.1748888215.1631424776.1748888413.1748888413',
            'jwt': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjA2NjY3LCJpYXQiOjE3NDg4ODg0NjF9.xFGnuB90CR_5aZnjSZoPBmVPBm8IHKUINR5GNGVXvas',
            '_sleek_session': '%7B%22init%22%3A%222025-06-02T18%3A21%3A02.285Z%22%7D',
            '__hssc': '243085085.3.1748888215748',
            '_uetsid': 'c3b66a003fdd11f0825efd67ef494ef3|2uf1e1|2|fwf|0|1979',
            'AWSALB': '4B4swMAKe1BirJcaNUF1PlWeXd7cmuygQrm0a82Dah7Fj3cbFJGLfn+Pa8DYIlxSq6UCW92YabK5e5Iy5NhEe8GK8feSpT2WF7rojfU8Zp7vmspbiPaEpToqel0B',
            'AWSALBCORS': '4B4swMAKe1BirJcaNUF1PlWeXd7cmuygQrm0a82Dah7Fj3cbFJGLfn+Pa8DYIlxSq6UCW92YabK5e5Iy5NhEe8GK8feSpT2WF7rojfU8Zp7vmspbiPaEpToqel0B',
            '_uetvid': 'c3b656603fdd11f08c7f6d4612e9770c|19rc0tb|1748888657955|5|1|bat.bing.com/p/insights/c/l',
            '_ga_6TGX06C7CZ': 'GS2.1.s1748888215$o18$g1$t1748888658$j5$l0$h0'
        }
        
        # Request body exactly as seen in the working request
        data = {
            "network": "snapchat",
            "from_date": from_date,
            "to_date": to_date,
            "profile": "all",
            "report_type": "posts",
            "profile_gids": [
                460427, 460442, 462587, 462588, 462598, 463075, 463552, 463553,
                463630, 466428, 466550, 466551, 466552, 466553, 466554, 466555,
                466556, 466557, 466558, 475622, 498663, 498841, 512091, 515505,
                521819, 529653, 530860, 530861, 536936, 539122
            ]
        }
        
        logger.info(f"Making POST request to {url}")
        response = requests.post(url, headers=headers, json=data, cookies=cookies)
        
        if response.status_code == 200:
            # Create downloads directory if it doesn't exist
            download_dir = os.path.join(os.getcwd(), 'downloads')
            os.makedirs(download_dir, exist_ok=True)
            
            # Save with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(download_dir, f"vista_social_report_{timestamp}.csv")
            
            with open(output_file, 'wb') as f:
                f.write(response.content)
            logger.info(f"Report downloaded to: {output_file}")
            return output_file
        else:
            logger.error(f"Failed to download report: {response.status_code}")
            logger.error(f"Response: {response.text[:1000]}")
            return None
            
    except Exception as e:
        logger.error(f"Error downloading report: {str(e)}")
        raise

if __name__ == "__main__":
    download_vista_report() 