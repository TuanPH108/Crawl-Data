from celery import Celery
import os

# Get RabbitMQ URL from environment variable
broker_url = os.getenv('CELERY_BROKER_URL', 'amqp://admin:admin123@rabbitmq:5672/')
result_backend = os.getenv('CELERY_RESULT_BACKEND', 'rpc://')

# Create Celery app
celery_app = Celery(
    'scraper',
    broker=broker_url,
    backend=result_backend,
    include=['tasks']
)

# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3000,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_queue='scraper_queue',
    task_queues={
        'scraper_queue': {
            'exchange': 'scraper_queue',
            'routing_key': 'scraper_queue',
        },
    },
    task_routes={
        'tasks.*': {'queue': 'scraper_queue'},
    }
) 