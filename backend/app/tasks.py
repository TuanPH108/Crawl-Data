import eventlet
eventlet.monkey_patch()

import subprocess
import os
from celery import Celery, Task
import logging
import sys
from io import StringIO
import time
import traceback
from pathlib import Path
from datetime import datetime
import json
from celery.signals import worker_ready, task_failure, task_success
from prometheus_client import Counter, Histogram, start_http_server
import socket
import requests
from typing import Optional, Dict, Any, List
from pymongo import MongoClient
from app.celery_app import celery_app

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
TASK_COUNTER = Counter('crawl_task_total', 'Total number of crawl tasks', ['status'])
TASK_DURATION = Histogram('crawl_task_duration_seconds', 'Time spent crawling', ['status'])

# Cấu hình Celery
broker_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin123@rabbitmq:5672/')
result_backend = 'rpc://'

logger.info(f"Using broker URL: {broker_url}")
logger.info(f"Using result backend URL: {result_backend}")

celery_app = Celery('scraper')

celery_app.conf.update(
    broker_url=broker_url,
    result_backend=result_backend,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_concurrency=8,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,  # Retry indefinitely
    broker_connection_timeout=120,  # Tăng timeout lên 120s
    broker_connection_retry=True,
    broker_connection_retry_delay=1,  # Giảm delay xuống 1s
    broker_heartbeat=30,  # Tăng heartbeat lên 30s
    broker_pool_limit=50,  # Tăng pool limit lên 50
    broker_transport_options={
        'confirm_publish': True,
        'max_retries': 10,  # Tăng số lần retry
        'interval_start': 0,
        'interval_step': 1,  # Giảm interval step
        'interval_max': 10,  # Giảm interval max
    },
    task_routes={
        'scraper_crawl_task': {'queue': 'scraper_queue'}
    },
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=10,  # Giảm retry delay
    task_max_retries=10,  # Tăng số lần retry
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    worker_max_memory_per_child=200000,
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour
    task_soft_time_limit=3000,  # 50 minutes
    worker_enable_remote_control=True,
    worker_state_db='/app/worker_state.db'
)

class BaseTaskWithRetry(Task):
    """Base task class with retry mechanism"""
    autoretry_for = (Exception,)
    retry_kwargs = {'max_retries': 3, 'countdown': 60}
    retry_backoff = True
    retry_backoff_max = 600
    retry_jitter = True

@worker_ready.connect
def at_start(sender, **kwargs):
    """Start Prometheus metrics server when worker starts"""
    logger.info("Worker is ready!")
    logger.info(f"Celery configuration: {celery_app.conf}")
    
    # Start Prometheus metrics server
    try:
        start_http_server(8000)
        logger.info("Prometheus metrics server started on port 8000")
    except Exception as e:
        logger.error(f"Failed to start Prometheus metrics server: {e}")

@task_success.connect
def task_success_handler(sender=None, **kwargs):
    """Handle successful task completion"""
    TASK_COUNTER.labels(status='success').inc()
    logger.info(f"Task {sender.name} completed successfully")

@task_failure.connect
def task_failure_handler(sender=None, **kwargs):
    """Handle task failure"""
    TASK_COUNTER.labels(status='failure').inc()
    logger.error(f"Task {sender.name} failed: {kwargs.get('exception')}")

@celery_app.task(name='scraper_crawl_task', queue='scraper_queue', base=BaseTaskWithRetry)
def scraper_crawl_task(urls: list, timeout: int = 20, task_id: str = None) -> Dict[str, Any]:
    """
    Task thực hiện crawl dữ liệu từ danh sách URL
    """
    start_time = time.time()
    try:
        logger.info(f"[SCRAPER_TASK] Received scraper_crawl_task with ID: {task_id} for URLs: {urls}, timeout: {timeout}")
        
        # Validate input
        if not urls:
            logger.error("[SCRAPER_TASK] URLs list cannot be empty. Aborting task.")
            raise ValueError("URLs list cannot be empty")
        if not isinstance(urls, list):
            logger.error(f"[SCRAPER_TASK] Invalid URLs type: {type(urls)}. URLs must be a list. Aborting task.")
            raise ValueError("URLs must be a list")
        if not all(isinstance(url, str) for url in urls):
            logger.error("[SCRAPER_TASK] Not all URLs are strings. Aborting task.")
            raise ValueError("All URLs must be strings")
        
        # Chuyển đổi danh sách URL thành chuỗi
        urls_str = ','.join(urls)
        logger.info(f"[SCRAPER_TASK] URLs string for Scrapy: {urls_str}")
        
        # Lấy đường dẫn hiện tại
        current_dir = '/app'
        logger.info(f"[SCRAPER_TASK] Current directory for Scrapy: {current_dir}")
        
        # Kiểm tra xem scrapy có sẵn không
        scrapy_path = 'scrapy'
        logger.info(f"[SCRAPER_TASK] Using scrapy path: {scrapy_path}")
        
        # Tạo lệnh crawl
        cmd = [
            scrapy_path,
            'crawl',
            'news_spider',
            '-a', f'urls={urls_str}',
            '-a', f'timeout={timeout}',
            '-a', f'task_id={task_id}',
            '--logfile=/dev/stdout',
            '--loglevel=INFO' # Changed to INFO to reduce verbose output unless needed
        ]
        
        full_command = ' '.join(cmd)
        logger.info(f"[SCRAPER_TASK] Executing Scrapy command: {full_command}")
        
        # Thực thi lệnh crawl
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=current_dir,
            env=os.environ.copy()
        )
        
        stdout, stderr = process.communicate()
        
        duration = time.time() - start_time
        
        if process.returncode == 0:
            logger.info(f"[SCRAPER_TASK] Scrapy crawl successful for task ID: {task_id}")
            logger.info(f"[SCRAPER_TASK] Scrapy STDOUT: {stdout}")
            TASK_DURATION.labels(status='success').observe(duration)
            TASK_COUNTER.labels(status='success').inc()
            return {
                "status": "success",
                "message": "Crawl thành công",
                "task_id": task_id,
                "output": stdout,
                "duration": duration
            }
        else:
            error_msg = stderr if stderr else "Lỗi không xác định từ Scrapy"
            logger.error(f"[SCRAPER_TASK] Scrapy crawl failed for task ID: {task_id}. Return code: {process.returncode}")
            logger.error(f"[SCRAPER_TASK] Scrapy STDERR: {stderr}")
            logger.error(f"[SCRAPER_TASK] Scrapy STDOUT (on error): {stdout}") # Log stdout even on error for more context
            TASK_DURATION.labels(status='failure').observe(duration)
            TASK_COUNTER.labels(status='failure').inc()
            raise Exception(f"Crawl failed: {error_msg}")
            
    except ValueError as e:
        duration = time.time() - start_time
        TASK_DURATION.labels(status='failure').observe(duration)
        TASK_COUNTER.labels(status='failure').inc()
        logger.error(f"[SCRAPER_TASK] Validation error in scraper_crawl_task for task ID: {task_id}. Error: {str(e)}")
        raise # Re-raise the exception to Celery
    except Exception as e:
        duration = time.time() - start_time
        TASK_DURATION.labels(status='failure').observe(duration)
        TASK_COUNTER.labels(status='failure').inc()
        error_msg = f"Unexpected error in scraper_crawl_task for task ID: {task_id}. Error: {str(e)}\n{traceback.format_exc()}"
        logger.error(f"[SCRAPER_TASK] {error_msg}")
        raise # Re-raise the exception to Celery

@celery_app.task(name='batch_crawl_task', base=BaseTaskWithRetry)
def batch_crawl_task(urls: list, interval: int, timeout: int = 60, max_pages: Optional[int] = None, follow_links: bool = True) -> Dict[str, Any]:
    """
    Task để crawl định kỳ theo khoảng thời gian
    """
    logger.info(f"Starting batch crawl task for {len(urls)} URLs with interval {interval}s")
    
    while True:
        try:
            # Gửi task crawl cho batch hiện tại
            result = scraper_crawl_task.delay(
                urls=urls,
                timeout=timeout,
                max_pages=max_pages,
                follow_links=follow_links
            )
            
            logger.info(f"Batch crawl task sent with ID: {result.id}")
            
            # Đợi đến lần crawl tiếp theo
            time.sleep(interval)
            
        except Exception as e:
            logger.error(f"Error in batch crawl task: {str(e)}")
            # Đợi một khoảng thời gian ngắn trước khi thử lại
            time.sleep(60)

def check_health() -> Dict[str, Any]:
    """
    Kiểm tra sức khỏe của worker
    """
    try:
        # Kiểm tra kết nối RabbitMQ
        celery_app.control.inspect().active()
        
        # Kiểm tra kết nối MongoDB
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://root:example@mongodb:27017')
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        
        return {
            "status": "healthy",
            "message": "All systems operational",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        if 'client' in locals():
            client.close() 