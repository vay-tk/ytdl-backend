from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, HttpUrl
import os
import logging
from contextlib import asynccontextmanager

from downloader import VideoDownloader
from scheduler import FileCleanupScheduler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create downloads directory
downloads_dir = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(downloads_dir, exist_ok=True)

# Initialize cleanup scheduler
cleanup_scheduler = FileCleanupScheduler(downloads_dir)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting YouTube Downloader API...")
    cleanup_scheduler.start()
    logger.info("File cleanup scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down YouTube Downloader API...")
    cleanup_scheduler.stop()
    logger.info("File cleanup scheduler stopped")

# Initialize FastAPI app
app = FastAPI(
    title="YouTube Video Downloader API",
    description="Download YouTube videos in HEVC format",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS - Allow all origins for deployment flexibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Mount static files for downloads
app.mount("/files", StaticFiles(directory=downloads_dir), name="downloads")

# Initialize video downloader
downloader = VideoDownloader(downloads_dir)

class DownloadRequest(BaseModel):
    url: HttpUrl

class DownloadResponse(BaseModel):
    title: str
    downloadUrl: str
    thumbnail: str = None
    duration: str = None

@app.get("/")
async def root():
    return {"message": "YouTube Video Downloader API", "status": "running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "youtube-downloader"}

@app.post("/api/download", response_model=DownloadResponse)
async def download_video(request: DownloadRequest, background_tasks: BackgroundTasks):
    """
    Download and convert a YouTube video to HEVC format
    """
    try:
        url = str(request.url)
        logger.info(f"Processing download request for URL: {url}")
        
        # Validate YouTube URL
        if not any(domain in url for domain in ['youtube.com', 'youtu.be']):
            raise HTTPException(status_code=400, detail="Please provide a valid YouTube URL")
        
        # Download and process video
        result = await downloader.download_video(url)
        
        # Schedule file cleanup in background
        background_tasks.add_task(cleanup_scheduler.schedule_file_cleanup, result['filepath'])
        
        logger.info(f"Successfully processed video: {result['title']}")
        
        return DownloadResponse(
            title=result['title'],
            downloadUrl=f"/files/{result['filename']}",
            thumbnail=result.get('thumbnail'),
            duration=result.get('duration')
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        error_msg = str(e)
        
        if "YouTube is currently blocking" in error_msg:
            raise HTTPException(status_code=429, detail=error_msg)
        elif "Video unavailable" in error_msg or "video is unavailable" in error_msg:
            raise HTTPException(status_code=404, detail=error_msg)
        elif "age-restricted" in error_msg.lower():
            raise HTTPException(status_code=400, detail=error_msg)
        elif "private" in error_msg.lower():
            raise HTTPException(status_code=400, detail=error_msg)
        elif "copyright" in error_msg.lower():
            raise HTTPException(status_code=400, detail=error_msg)
        else:
            raise HTTPException(status_code=500, detail="Failed to process video. Please try again later.")

@app.get("/files/{filename}")
async def download_file(filename: str):
    """
    Serve processed video files
    """
    file_path = os.path.join(downloads_dir, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        file_path,
        media_type="video/x-matroska",
        filename=filename,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
