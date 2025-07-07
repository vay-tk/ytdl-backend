import os
import uuid
import asyncio
import logging
from typing import Dict, Optional
import yt_dlp
import ffmpeg
import re

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
    
    async def download_video(self, url: str) -> Dict:
        """
        Download YouTube video and convert to HEVC format
        """
        unique_id = str(uuid.uuid4())[:8]
        temp_video_path = None
        temp_audio_path = None
        
        try:
            # Configure yt-dlp options
            ydl_opts = {
                'format': 'best[height<=720]/best',
                'noplaylist': True,
                'extract_flat': False,
                'quiet': True,
                'no_warnings': True,
            }
            
            # Extract video info
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
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
                }
                
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
            
            logger.error(f"Download failed: {str(e)}")
            raise e
    
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