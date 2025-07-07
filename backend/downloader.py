import os
import uuid
import asyncio
import logging
from typing import Dict, Optional
import yt_dlp
import ffmpeg
import re
import random
import time
import json

logger = logging.getLogger(__name__)

class VideoDownloader:
    def __init__(self, downloads_dir: str):
        self.downloads_dir = downloads_dir
        self.session_cookies = None
        
    def _format_duration(self, duration: Optional[float]) -> Optional[str]:
        """Convert duration in seconds to HH:MM:SS format"""
        if duration is None:
            return None
        
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    
    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename"""
        # Remove invalid characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Replace spaces with underscores
        sanitized = re.sub(r'\s+', '_', sanitized)
        # Limit length
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized
    
    def _get_user_agents(self):
        """Return a list of common user agents to rotate"""
        return [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
        ]
    
    def _get_extraction_strategies(self):
        """Return different extraction strategies to try"""
        return [
            # Strategy 1: Android client (most reliable)
            {
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android'],
                        'skip': ['hls', 'dash']
                    }
                },
                'format': 'best[height<=720]/best'
            },
            # Strategy 2: iOS client
            {
                'extractor_args': {
                    'youtube': {
                        'player_client': ['ios'],
                        'skip': ['hls', 'dash']
                    }
                },
                'format': 'best[height<=720]/best'
            },
            # Strategy 3: Web client with different format
            {
                'extractor_args': {
                    'youtube': {
                        'player_client': ['web'],
                        'skip': ['hls']
                    }
                },
                'format': 'worst[height>=360]/best[height<=720]/best'
            },
            # Strategy 4: Android with different format selection
            {
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android'],
                        'skip': ['hls', 'dash']
                    }
                },
                'format': 'worst[height>=480]/best[height<=720]/best'
            },
            # Strategy 5: Fallback to any available format
            {
                'extractor_args': {
                    'youtube': {
                        'player_client': ['android', 'web'],
                        'skip': ['hls']
                    }
                },
                'format': 'best/worst'
            }
        ]
    
    async def download_video(self, url: str) -> Dict:
        """
        Download YouTube video and convert to HEVC format with multiple fallback strategies
        """
        unique_id = str(uuid.uuid4())[:8]
        temp_video_path = None
        
        try:
            # Add random delay to avoid rate limiting
            await asyncio.sleep(random.uniform(1.0, 3.0))
            
            user_agents = self._get_user_agents()
            strategies = self._get_extraction_strategies()
            
            info = None
            successful_strategy = None
            
            # Try each strategy until one works
            for i, strategy in enumerate(strategies):
                try:
                    selected_ua = random.choice(user_agents)
                    
                    # Base options
                    ydl_opts = {
                        'noplaylist': True,
                        'extract_flat': False,
                        'quiet': True,
                        'no_warnings': True,
                        'user_agent': selected_ua,
                        'referer': 'https://www.youtube.com/',
                        'headers': {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Accept-Encoding': 'gzip, deflate',
                            'DNT': '1',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Cache-Control': 'max-age=0'
                        },
                        'http_chunk_size': 10485760,
                        'retries': 3,
                        'fragment_retries': 3,
                        'ignoreerrors': False,
                        'no_check_certificate': True,
                        **strategy
                    }
                    
                    logger.info(f"Trying extraction strategy {i+1}/{len(strategies)}")
                    
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                        if info:
                            successful_strategy = ydl_opts
                            logger.info(f"Strategy {i+1} successful!")
                            break
                
                except Exception as e:
                    error_msg = str(e).lower()
                    logger.warning(f"Strategy {i+1} failed: {str(e)}")
                    
                    # If it's a bot detection error, try next strategy
                    if 'bot' in error_msg or 'sign in' in error_msg:
                        # Add longer delay before next attempt
                        await asyncio.sleep(random.uniform(2.0, 5.0))
                        continue
                    elif i == len(strategies) - 1:  # Last strategy
                        raise e
                    else:
                        continue
            
            if not info or not successful_strategy:
                raise Exception("All extraction strategies failed. YouTube may be temporarily blocking this server.")
            
            title = info.get('title', 'Unknown Video')
            duration = info.get('duration')
            thumbnail = info.get('thumbnail')
            
            logger.info(f"Video info extracted: {title}")
            
            # Download video using successful strategy
            video_filename = f"video_{unique_id}.%(ext)s"
            video_path = os.path.join(self.downloads_dir, video_filename)
            
            download_opts = successful_strategy.copy()
            download_opts['outtmpl'] = video_path
            
            # Add another delay before download
            await asyncio.sleep(random.uniform(1.0, 2.0))
            
            logger.info("Starting video download...")
            
            with yt_dlp.YoutubeDL(download_opts) as ydl:
                ydl.download([url])
            
            # Find the actual downloaded file
            for file in os.listdir(self.downloads_dir):
                if file.startswith(f"video_{unique_id}"):
                    temp_video_path = os.path.join(self.downloads_dir, file)
                    break
            
            if not temp_video_path or not os.path.exists(temp_video_path):
                raise Exception("Failed to download video file")
            
            logger.info(f"Video downloaded successfully: {os.path.basename(temp_video_path)}")
            
            # Convert to HEVC format
            output_filename = f"output_{unique_id}.mkv"
            output_path = os.path.join(self.downloads_dir, output_filename)
            
            logger.info(f"Converting video to HEVC format: {output_filename}")
            
            # Run FFmpeg conversion
            await self._convert_to_hevc(temp_video_path, output_path)
            
            # Clean up temporary files
            if temp_video_path and os.path.exists(temp_video_path):
                os.remove(temp_video_path)
            
            if not os.path.exists(output_path):
                raise Exception("Failed to convert video to HEVC format")
            
            logger.info(f"Successfully converted video: {output_filename}")
            
            return {
                'title': title,
                'filename': output_filename,
                'filepath': output_path,
                'duration': self._format_duration(duration),
                'thumbnail': thumbnail
            }
                
        except Exception as e:
            # Clean up temporary files on error
            if temp_video_path and os.path.exists(temp_video_path):
                try:
                    os.remove(temp_video_path)
                except:
                    pass
            
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            
            # Provide more specific error messages
            if 'bot' in error_msg.lower() or 'sign in' in error_msg.lower():
                raise Exception("YouTube is currently blocking automated requests from this server. This is a temporary restriction that affects many hosting providers. Please try again later or use a different video.")
            elif 'video unavailable' in error_msg.lower() or 'unavailable' in error_msg.lower():
                raise Exception("This video is unavailable, private, or has been removed from YouTube.")
            elif 'age' in error_msg.lower() and 'restricted' in error_msg.lower():
                raise Exception("This video is age-restricted and cannot be downloaded.")
            elif 'copyright' in error_msg.lower():
                raise Exception("This video is protected by copyright and cannot be downloaded.")
            elif 'extraction strategies failed' in error_msg:
                raise Exception("YouTube is temporarily blocking this server. This is common with hosting providers. Please try again in 10-15 minutes.")
            else:
                raise Exception(f"Download failed: {error_msg}")
    
    async def _convert_to_hevc(self, input_path: str, output_path: str):
        """Convert video to HEVC format using FFmpeg"""
        try:
            # Use asyncio to run FFmpeg conversion
            process = await asyncio.create_subprocess_exec(
                'ffmpeg',
                '-i', input_path,
                '-c:v', 'libx265',
                '-preset', 'fast',  # Changed to fast for quicker processing
                '-crf', '23',
                '-c:a', 'aac',
                '-b:a', '128k',
                '-movflags', '+faststart',
                '-y',  # Overwrite output file
                output_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown FFmpeg error"
                logger.error(f"FFmpeg conversion failed: {error_msg}")
                raise Exception(f"Video conversion failed: {error_msg}")
            
            logger.info("FFmpeg conversion completed successfully")
            
        except FileNotFoundError:
            raise Exception("FFmpeg not found. Please install FFmpeg with HEVC support.")
        except Exception as e:
            logger.error(f"FFmpeg conversion error: {str(e)}")
            raise e
