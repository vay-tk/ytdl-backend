import os
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import List

logger = logging.getLogger(__name__)

class FileCleanupScheduler:
    def __init__(self, downloads_dir: str, cleanup_interval: int = 3600):
        """
        Initialize file cleanup scheduler
        
        Args:
            downloads_dir: Directory to monitor for file cleanup
            cleanup_interval: Interval in seconds between cleanup runs (default: 1 hour)
        """
        self.downloads_dir = downloads_dir
        self.cleanup_interval = cleanup_interval
        self.running = False
        self.cleanup_thread = None
        self.scheduled_files = {}  # filename -> scheduled_time
        
    def start(self):
        """Start the cleanup scheduler"""
        if self.running:
            return
        
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info("File cleanup scheduler started")
    
    def stop(self):
        """Stop the cleanup scheduler"""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        logger.info("File cleanup scheduler stopped")
    
    def schedule_file_cleanup(self, filepath: str, cleanup_after_hours: int = 1):
        """
        Schedule a file for cleanup after specified hours
        
        Args:
            filepath: Full path to the file to be cleaned up
            cleanup_after_hours: Hours after which the file should be deleted
        """
        filename = os.path.basename(filepath)
        cleanup_time = datetime.now() + timedelta(hours=cleanup_after_hours)
        
        self.scheduled_files[filename] = cleanup_time
        logger.info(f"Scheduled file cleanup: {filename} at {cleanup_time}")
    
    def _cleanup_loop(self):
        """Main cleanup loop that runs in a separate thread"""
        while self.running:
            try:
                self._cleanup_expired_files()
                self._cleanup_old_files()
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _cleanup_expired_files(self):
        """Clean up files that have been scheduled for cleanup"""
        current_time = datetime.now()
        files_to_remove = []
        
        for filename, scheduled_time in self.scheduled_files.items():
            if current_time >= scheduled_time:
                filepath = os.path.join(self.downloads_dir, filename)
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        logger.info(f"Cleaned up scheduled file: {filename}")
                    except Exception as e:
                        logger.error(f"Failed to remove scheduled file {filename}: {str(e)}")
                
                files_to_remove.append(filename)
        
        # Remove cleaned up files from schedule
        for filename in files_to_remove:
            del self.scheduled_files[filename]
    
    def _cleanup_old_files(self):
        """Clean up old files that weren't scheduled but are older than 2 hours"""
        if not os.path.exists(self.downloads_dir):
            return
        
        cutoff_time = time.time() - (2 * 3600)  # 2 hours ago
        cleaned_count = 0
        
        try:
            for filename in os.listdir(self.downloads_dir):
                filepath = os.path.join(self.downloads_dir, filename)
                
                if os.path.isfile(filepath):
                    file_mtime = os.path.getmtime(filepath)
                    
                    if file_mtime < cutoff_time:
                        try:
                            os.remove(filepath)
                            cleaned_count += 1
                            logger.info(f"Cleaned up old file: {filename}")
                        except Exception as e:
                            logger.error(f"Failed to remove old file {filename}: {str(e)}")
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} old files")
                
        except Exception as e:
            logger.error(f"Error during old file cleanup: {str(e)}")
    
    def get_cleanup_stats(self) -> dict:
        """Get statistics about scheduled cleanups"""
        return {
            'scheduled_files': len(self.scheduled_files),
            'next_cleanup': min(self.scheduled_files.values()) if self.scheduled_files else None,
            'running': self.running
        }