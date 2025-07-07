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

logger = logging.getLogger(__name__)

class VideoDownloader:
    def __init__(self, downloads_dir: str):
        self.downloads_dir = downloads_dir
        
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
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
    
    async def download_video(self, url: str) -> Dict:
        """
        Download YouTube video and convert to HEVC format
        """
        unique_id = str(uuid.uuid4())[:8]
        temp_video_path = None
        temp_audio_path = None
        
        try:
            # Add random delay to avoid rate limiting
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            # Configure yt-dlp options with anti-bot measures
            user_agents = self._get_user_agents()
            selected_ua = random.choice(user_agents)
            
            ydl_opts = {
                'format': 'best[height<=720]/best',
                'noplaylist': True,
                'extract_flat': False,
                'quiet': True,
                'no_warnings': True,
                'user_agent': selected_ua,
                'referer': 'https://www.youtube.com/',
                'headers': {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-us,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                },
                'extractor_args': {
                    'youtube': {
                        'skip': ['hls', 'dash'],
                        'player_client': ['android', 'web']
                    }
                },
                'http_chunk_size': 10485760,  # 10MB chunks
            }
            
            # Extract video info
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    info = ydl.extract_info(url, download=False)
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'bot' in error_msg or 'sign in' in error_msg:
                        # Try with different extractor args
                        ydl_opts['extractor_args']['youtube']['player_client'] = ['android']
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl_retry:
                            info = ydl_retry.extract_info(url, download=False)
                    else:
                        raise e
                
                if not info:
                    raise Exception("Failed to extract video information")
                
                title = info.get('title', 'Unknown Video')
                duration = info.get('duration')
                thumbnail = info.get('thumbnail')
                
                logger.info(f"Video info extracted: {title}")
                
                # Sanitize title for filename
                safe_title = self._sanitize_filename(title)
                
                # Download video
                video_filename = f"video_{unique_id}.%(ext)s"
                video_path = os.path.join(self.downloads_dir, video_filename)
                
                ydl_opts_download = {
                    'format': 'best[height<=720]/best',
                    'outtmpl': video_path,
                    'noplaylist': True,
                    'quiet': True,
                    'no_warnings': True,
                    'user_agent': selected_ua,
                    'referer': 'https://www.youtube.com/',
                    'headers': ydl_opts['headers'],
                    'extractor_args': ydl_opts['extractor_args'],
                    'http_chunk_size': 10485760,
                }
                
                # Add another small delay before download
                await asyncio.sleep(random.uniform(1.0, 3.0))
                
                with yt_dlp.YoutubeDL(ydl_opts_download) as ydl:
                    ydl.download([url])
                
                # Find the actual downloaded file
                for file in os.listdir(self.downloads_dir):
                    if file.startswith(f"video_{unique_id}"):
                        temp_video_path = os.path.join(self.downloads_dir, file)
                        break
                
                if not temp_video_path or not os.path.exists(temp_video_path):
                    raise Exception("Failed to download video file")
                
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
            for temp_file in [temp_video_path, temp_audio_path]:
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except:
                        pass
            
            error_msg = str(e)
            logger.error(f"Download failed: {error_msg}")
            
            # Provide more specific error messages
            if 'bot' in error_msg.lower() or 'sign in' in error_msg.lower():
                raise Exception("YouTube is currently blocking automated requests. Please try again in a few minutes or try a different video.")
            elif 'video unavailable' in error_msg.lower():
                raise Exception("This video is unavailable or has been removed.")
            elif 'private' in error_msg.lower():
                raise Exception("This video is private and cannot be downloaded.")
            elif 'age' in error_msg.lower() and 'restricted' in error_msg.lower():
                raise Exception("This video is age-restricted and cannot be downloaded.")
            elif 'copyright' in error_msg.lower():
                raise Exception("This video is protected by copyright and cannot be downloaded.")
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
                '-preset', 'medium',
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
