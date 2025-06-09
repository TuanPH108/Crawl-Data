import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, HttpUrl, validator
from typing import List, Optional
from pymongo import MongoClient
import re
from datetime import datetime
import json
from .celery_app import celery_app
from app.routers import news, analytics

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/app.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Cấu hình FastAPI app
app = FastAPI(
    title="Crawling System API",
    description="API for managing web crawling tasks",
    version="1.0.0"
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["*"]
)

# Include routers
app.include_router(news.router, prefix="/api/news", tags=["news"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["analytics"])

class CrawlRequest(BaseModel):
    urls: List[HttpUrl]
    max_pages: Optional[int] = 10
    max_depth: Optional[int] = 2
    allowed_domains: Optional[List[str]] = None
    follow_links: Optional[bool] = True
    extract_images: Optional[bool] = True
    extract_links: Optional[bool] = True
    extract_text: Optional[bool] = True
    respect_robots: Optional[bool] = True
    user_agent: Optional[str] = None
    proxy: Optional[str] = None
    timeout: Optional[int] = 30
    retry_count: Optional[int] = 3
    retry_delay: Optional[int] = 5
    concurrent_requests: Optional[int] = 16
    delay_between_requests: Optional[float] = 1.0
    priority: Optional[int] = 1
    callback_url: Optional[HttpUrl] = None
    metadata: Optional[dict] = None

    @validator('urls')
    def validate_urls(cls, v):
        if not v:
            raise ValueError('At least one URL is required')
        if len(v) > 100:
            raise ValueError('Maximum 100 URLs allowed per request')
        return v

    @validator('max_pages')
    def validate_max_pages(cls, v):
        if v is not None and (v < 1 or v > 1000):
            raise ValueError('max_pages must be between 1 and 1000')
        return v

    @validator('max_depth')
    def validate_max_depth(cls, v):
        if v is not None and (v < 1 or v > 10):
            raise ValueError('max_depth must be between 1 and 10')
        return v

    @validator('timeout')
    def validate_timeout(cls, v):
        if v is not None and (v < 1 or v > 300):
            raise ValueError('timeout must be between 1 and 300 seconds')
        return v

    @validator('retry_count')
    def validate_retry_count(cls, v):
        if v is not None and (v < 0 or v > 10):
            raise ValueError('retry_count must be between 0 and 10')
        return v

    @validator('concurrent_requests')
    def validate_concurrent_requests(cls, v):
        if v is not None and (v < 1 or v > 32):
            raise ValueError('concurrent_requests must be between 1 and 32')
        return v

    @validator('delay_between_requests')
    def validate_delay(cls, v):
        if v is not None and (v < 0 or v > 10):
            raise ValueError('delay_between_requests must be between 0 and 10 seconds')
        return v

    @validator('priority')
    def validate_priority(cls, v):
        if v is not None and (v < 1 or v > 10):
            raise ValueError('priority must be between 1 and 10')
        return v

class CrawlResponse(BaseModel):
    task_id: str
    status: str
    message: str = ""

class ErrorResponse(BaseModel):
    detail: str
    code: str

@celery_app.task(name='delegate_crawl_task', queue='delegate_queue')
def delegate_crawl_task(urls: List[str], timeout: int = 20):
    """
    Task điều phối crawl, gửi các URL đến scraper worker
    """
    try:
        logger.info(f"[DELEGATE] Starting delegate_crawl_task with URLs: {urls} and timeout: {timeout}")
        logger.info(f"[DELEGATE] Current task ID: {celery_app.current_task.request.id}")
        
        # Gửi task đến scraper worker
        result_task = celery_app.send_task(
            'scraper_crawl_task',
            args=[urls, timeout],
            kwargs={'task_id': celery_app.current_task.request.id},
            queue='scraper_queue'
        )
        
        logger.info(f"[DELEGATE] Sent task to scraper worker. Task ID: {result_task.id} for URLs: {urls}")
        
        return {
            "status": "success",
            "message": f"Đã gửi {len(urls)} URL đến scraper worker",
            "scraper_task_id": result_task.id
        }
    except Exception as e:
        logger.error(f"[DELEGATE] Error in delegate_crawl_task: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.post("/api/crawl", 
    response_model=CrawlResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid input"},
        429: {"model": ErrorResponse, "description": "Too many requests"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Start crawling URLs",
    description="Start crawling one or more URLs. Returns a task ID that can be used to check the status."
)
async def start_crawl(request: Request, crawl_request: CrawlRequest):
    """
    API để bắt đầu crawl một hoặc nhiều URL
    """
    try:
        logger.info(f"[API] Received crawl request for URLs: {crawl_request.urls} with timeout: {crawl_request.timeout}")
        
        # Gọi task điều phối
        delegator_task = delegate_crawl_task.delay(
            urls=[str(url) for url in crawl_request.urls],
            timeout=crawl_request.timeout
        )
        
        logger.info(f"[API] Created delegator task with ID: {delegator_task.id} for URLs: {crawl_request.urls}")
        
        return CrawlResponse(
            task_id=delegator_task.id,
            status="started",
            message="Crawl task started successfully"
        )
    except ValueError as e:
        logger.error(f"[API] Validation error in start_crawl: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[API] Error in start_crawl: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/task/{task_id}",
    responses={
        404: {"model": ErrorResponse, "description": "Task not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get task status",
    description="Get the current status of a crawling task by its ID."
)
async def get_task_status(task_id: str):
    try:
        # Lấy kết quả từ Celery
        result = celery_app.AsyncResult(task_id)
        
        # Trả về trạng thái của task
        state = result.state
        
        if state == 'PENDING':
            status_message = "Task đang chờ xử lý"
        elif state == 'STARTED':
            status_message = "Task đang được thực thi"
        elif state == 'SUCCESS':
            status_message = "Task đã hoàn thành"
        elif state == 'FAILURE':
            status_message = f"Task thất bại: {str(result.info)}"
        else:
            status_message = f"Trạng thái không xác định: {state}"
            
        logger.info(f"Checking status for task ID: {task_id}")
        logger.info(f"Task state: {state}")
        logger.info(f"Task response: {{'state': '{state}', 'status': '{status_message}'}}")
        
        return {
            "task_id": task_id,
            "status": state,
            "message": status_message
        }
    except Exception as e:
        logger.error(f"Error getting task status for {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/crawled-data",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="Get crawled data",
    description="Get the most recently crawled data with optional limit parameter."
)
def get_crawled_data(limit: int = 10):
    try:
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://root:example@mongodb:27017/')
        client = MongoClient(mongo_uri)
        db = client.crawling_data
        collection = db.articles
        
        # Lấy dữ liệu mới nhất
        data = list(collection.find().sort("timestamp", -1).limit(limit))
        
        # Convert ObjectId to str for JSON serialization
        for item in data:
            item['_id'] = str(item['_id'])
            
        return data
    except Exception as e:
        logger.error(f"Error fetching crawled data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'client' in locals():
            client.close()

@app.get("/health",
    summary="Health check",
    description="Check if the API is running properly."
)
async def health_check():
    try:
        # Kiểm tra kết nối RabbitMQ thông qua Celery app
        celery_app.control.ping(timeout=1)
        
        # Kiểm tra kết nối MongoDB
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://root:example@mongodb:27017/')
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        client.close()
        
        return {"status": "healthy", "message": "API and dependencies are healthy"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/")
async def root():
    return {"message": "News Crawling System API"}

@app.post("/crawl")
async def crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    try:
        # Log the request
        logger.info(f"Received crawl request for {len(request.urls)} URLs")
        logger.debug(f"Request details: {request.dict()}")

        # Validate URLs
        for url in request.urls:
            if not url.scheme in ['http', 'https']:
                raise HTTPException(status_code=400, detail=f"Invalid URL scheme: {url}")

        # Create crawl task
        task = celery_app.send_task(
            'tasks.crawl',
            args=[request.dict()],
            queue='delegate_queue',
            priority=request.priority
        )

        # Log task creation
        logger.info(f"Created crawl task with ID: {task.id}")

        return {
            "message": "Crawl task created successfully",
            "task_id": task.id,
            "status": "pending"
        }

    except Exception as e:
        logger.error(f"Error creating crawl task: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    try:
        task = celery_app.AsyncResult(task_id)
        return {
            "task_id": task_id,
            "status": task.status,
            "result": task.result if task.ready() else None
        }
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    try:
        # Check Celery connection
        celery_app.control.inspect().active()
        return {"status": "healthy", "celery": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}", exc_info=True)
        return {"status": "unhealthy", "celery": "disconnected"}