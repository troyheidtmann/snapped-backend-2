from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Body, Depends
from typing import Dict, List, Optional
from app.shared.database import spotlight_collection, upload_collection, saved_collection
from app.shared.auth import get_current_user_group
import openai
import os
from datetime import datetime
import logging
from PIL import Image
import io
import boto3
from botocore.exceptions import ClientError
import json
import emoji
import base64
from openai import AsyncOpenAI
import subprocess
import tempfile

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/captions")

# Initialize OpenAI client
openai.api_key = os.getenv("OPENAI_API_KEY")

CAPTION_PROMPT = '''You are a caption writer. Generate either simple captions or contextual questions based on what's in the image.

Rules:
1. Mix between two types:
   - Simple captions (e.g. "on the boat today", "hike with the girls")
   - Contextual questions based on what's in the image:
     * For daytime pics: "how's your day going?", "what's everyone doing today?"
     * For nighttime: "night plans?", "who's still up?"
     * For food pics: "what's everyone eating?", "favorite snack?"
     * For outdoor pics: "favorite hiking spot?", "beach day anyone?"
     * For workout pics: "morning or night workouts?"
2. NO hashtags or mentions
3. NO emojis unless they appear in examples
4. Keep questions casual and match them to what's happening in the image
5. For activity captions, just state what's happening
6. Keep everything short and simple
7. Return ONLY the captions/questions, one per line'''

class CaptionGenerator:
    def __init__(self):
        self.client = AsyncOpenAI()
        
    async def get_example_captions(self, client_id: str) -> List[str]:
        """Fetch example captions from the database."""
        try:
            # Get document for this client
            doc = await spotlight_collection.find_one(
                {"client_ID": client_id}
            )
            
            if not doc:
                logger.warning(f"No document found for client {client_id}")
                return []
                
            examples = []
            # Get captions directly from tt_sessions[0].files array
            if tt_sessions := doc.get('tt_sessions', []):
                if tt_sessions and tt_sessions[0].get('files', []):
                    files = tt_sessions[0]['files']
                    for file in files:
                        if caption := file.get('caption'):
                            if isinstance(caption, str) and caption.strip():
                                examples.append(caption.strip())
            
            if examples:
                logger.info(f"Found {len(examples)} captions")
                logger.debug(f"Captions: {examples}")
            else:
                logger.warning(f"No captions found for client {client_id}")
                
            # Take only the most recent 5 captions
            return examples[:5]
            
        except Exception as e:
            logger.error(f"Error fetching example captions: {str(e)}")
            logger.exception("Full error details:")
            return []
            
    async def extract_video_frame(self, video_data: bytes) -> bytes:
        """Extract a frame from video using ffmpeg."""
        try:
            # Save video data to a temporary file
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
                temp_video.write(video_data)
                video_path = temp_video.name
                
            # Create a temporary file for the output image
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_img:
                img_path = temp_img.name
            
            # Use ffmpeg to extract a frame at 1 second
            cmd = [
                'ffmpeg',
                '-i', video_path,        # Input file
                '-ss', '00:00:01',       # Seek to 1 second
                '-vframes', '1',         # Extract 1 frame
                '-f', 'image2',          # Force image2 format
                '-y',                    # Overwrite output
                img_path                 # Output file
            ]
            
            # Run ffmpeg
            process = subprocess.run(cmd, capture_output=True, text=True)
            if process.returncode != 0:
                logger.error(f"FFmpeg error: {process.stderr}")
                raise ValueError("Failed to extract frame from video")
            
            # Read the extracted frame
            with open(img_path, 'rb') as f:
                frame_data = f.read()
                
            # Clean up temporary files
            os.unlink(video_path)
            os.unlink(img_path)
            
            return frame_data
            
        except Exception as e:
            logger.error(f"Error extracting video frame: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to extract frame from video")
        
    async def analyze_and_generate(self, file_data: bytes, client_id: str, content_type: str = None) -> List[str]:
        """Analyze image and generate captions using GPT-4 Vision."""
        try:
            # Handle different file types
            if content_type:
                logger.info(f"Processing file of type: {content_type}")
                if content_type.startswith('image/'):
                    # For images, ensure they're in JPEG format
                    image = Image.open(io.BytesIO(file_data))
                    if image.mode != 'RGB':
                        image = image.convert('RGB')
                    img_byte_arr = io.BytesIO()
                    image.save(img_byte_arr, format='JPEG')
                    image_data = img_byte_arr.getvalue()
                elif content_type.startswith('video/'):
                    # Extract a frame from the video
                    logger.info("Extracting frame from video using FFmpeg")
                    image_data = await self.extract_video_frame(file_data)
                else:
                    image_data = file_data
            else:
                image_data = file_data
            
            # Get example captions for this client
            example_captions = await self.get_example_captions(client_id)
            
            # Clean the example captions - remove hashtags and mentions
            cleaned_examples = []
            for caption in example_captions:
                words = [word for word in caption.split() if not word.startswith(('#', '@'))]
                if words:
                    cleaned_examples.append(' '.join(words))
            
            # Build the prompt with examples if available
            prompt = CAPTION_PROMPT
            if cleaned_examples:
                prompt += "\n\nHere are some caption examples. Use similar emoji style if present, but focus on simple captions and questions that match what's in the image:\n"
                prompt += "\n".join(cleaned_examples)
            else:
                prompt += "\n\nNo examples available - generate simple captions and contextual questions without emojis."
            
            # Convert image data to base64
            base64_image = base64.b64encode(image_data).decode('utf-8')
            
            # Create the messages array with the image
            messages = [
                {
                    "role": "system",
                    "content": prompt
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Look at the image and generate a mix of simple captions and questions that match what's happening (time of day, activity, location, etc)."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ]
            
            response = await self.client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                max_tokens=300,
                temperature=0.7,
                response_format={"type": "text"}
            )
            
            # Parse the response into individual captions
            caption_text = response.choices[0].message.content
            if not caption_text:
                logger.warning("Empty response received from the model")
                raise HTTPException(status_code=500, detail="No response received from the model")
                
            captions = [cap.strip() for cap in caption_text.split('\n') if cap.strip()]
            
            # Filter out any non-caption text (like numbering or explanations)
            captions = [cap for cap in captions if not cap.startswith(('Here', 'Caption', '1.', '2.', '3.'))]
            
            # Ensure we have at least one caption
            if not captions:
                logger.warning("No valid captions were generated from the response")
                raise HTTPException(status_code=500, detail="Failed to generate valid captions")
                
            return captions[:3]  # Return top 3 captions
            
        except Exception as e:
            logger.error(f"Error generating captions: {str(e)}")
            if "insufficient_quota" in str(e):
                raise HTTPException(status_code=429, detail="insufficient_quota")
            raise HTTPException(status_code=500, detail=str(e))

caption_generator = CaptionGenerator()

@router.post("/suggest")
async def suggest_caption(
    file: UploadFile = File(...),
    client_id: str = Form(...),
    is_bold: bool = Form(False),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Generate caption suggestions for an uploaded file using GPT-4 Vision.
    
    Args:
        file: The media file to generate captions for
        client_id: Client identifier for finding example captions
        is_bold: Whether to generate bold-style captions (not used but kept for compatibility)
        
    Returns:
        List of caption suggestions
    """
    try:
        logger.info(f"Caption suggestion requested for client_id: {client_id}")
        logger.debug(f"File type: {file.content_type}, Filename: {file.filename}")
        
        # Read file content
        content = await file.read()
        
        # Check if it's a video file
        is_video = file.content_type.startswith('video/')
        
        # Generate captions using GPT-4 Vision
        captions = await caption_generator.analyze_and_generate(content, client_id, file.content_type)
        
        return {
            "status": "success",
            "has_examples": len(captions) > 0,
            "captions": captions
        }
        
    except Exception as e:
        logger.error(f"Error suggesting caption: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/regenerate")
async def regenerate_caption(
    data: Dict = Body(...),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Regenerate a caption using the same parameters but excluding previous suggestions.
    
    Args:
        data: Dictionary containing previous parameters and suggestions
        
    Returns:
        New caption suggestion
    """
    try:
        # Add exclusion of previous suggestions to the prompt
        data["prompt"] += "\n\nPlease generate different options from these previous suggestions:\n"
        data["prompt"] += "\n".join(data["previous_suggestions"])
        
        # Generate new captions
        captions = await caption_generator.analyze_and_generate(data["prompt"].encode('utf-8'), data["client_id"], data["content_type"])
        
        return {
            "status": "success",
            "captions": captions
        }
        
    except Exception as e:
        logger.error(f"Error regenerating caption: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 