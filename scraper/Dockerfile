FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r celery && useradd -r -g celery celery

# Set working directory
WORKDIR /app

# Copy application files
COPY . .

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory and set permissions
RUN mkdir -p /app/logs && \
    chown -R celery:celery /app

# Make wait-for-rabbitmq.sh executable
RUN chmod +x wait-for-rabbitmq.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV RABBITMQ_URL=amqp://admin:admin123@rabbitmq:5672/
ENV C_FORCE_ROOT=1

# Switch to non-root user
USER celery

# Default command
CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "-Q", "scraper_queue", "-P", "eventlet", "--concurrency=8"]