from celery import Celery
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get RabbitMQ URL from environment variable
broker_url = os.getenv('CELERY_BROKER_URL', 'amqp://admin:admin123@rabbitmq:5672')
result_backend = os.getenv('CELERY_RESULT_BACKEND', 'rpc://')
logger.info(f"Using RabbitMQ URL: {broker_url}")

# Create Celery app
celery_app = Celery(
    'crawler',
    broker=broker_url,
    backend=result_backend,
    include=['app.tasks']
)

# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour
    task_soft_time_limit=3000,  # 50 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_queue='delegate_queue',
    task_queues={
        'delegate_queue': {
            'exchange': 'delegate_queue',
            'routing_key': 'delegate_queue',
        },
        'scraper_queue': {
            'exchange': 'scraper_queue',
            'routing_key': 'scraper_queue',
        }
    },
    task_routes={
        'app.tasks.*': {'queue': 'delegate_queue'},
    },
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    broker_connection_timeout=120,
    broker_connection_retry_delay=1,
    broker_heartbeat=30,
    broker_pool_limit=50,
    broker_transport_options={
        'confirm_publish': True,
        'max_retries': 10,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 10,
    }
)

# Optional: Set a flag if the Celery app is ready for use
# This can be used in your main.py to only import/use celery_app when needed
# from celery.signals import worker_ready
# @worker_ready.connect
# def at_start(sender, **kwargs):
#     logger.info("Celery app is ready in backend/celery_app.py") 