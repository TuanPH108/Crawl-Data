#!/bin/bash
# wait-for-rabbitmq.sh

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

MAX_RETRIES=60  # Tăng số lần retry
RETRY_INTERVAL=1  # Giảm interval xuống 1s
TCP_TIMEOUT=5
AMQP_TIMEOUT=10

echo "Waiting for RabbitMQ to be ready..."

# Function to check TCP connection
check_tcp() {
    nc -z -w $TCP_TIMEOUT "$host" "$port"
    return $?
}

# Function to check AMQP connection
check_amqp() {
    python3 -c "
import pika
import sys
import time

def try_connect(max_attempts=3):
    for attempt in range(max_attempts):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='$host',
                    port=$port,
                    credentials=pika.PlainCredentials('admin', 'admin123'),
                    heartbeat=30,
                    connection_attempts=3,
                    retry_delay=1,
                    socket_timeout=$AMQP_TIMEOUT,
                    stack_timeout=$AMQP_TIMEOUT
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue='scraper_queue', durable=True)
            connection.close()
            return True
        except Exception as e:
            print(f'Attempt {attempt + 1} failed: {e}')
            if attempt < max_attempts - 1:
                time.sleep(1)
    return False

if try_connect():
    sys.exit(0)
else:
    sys.exit(1)
"
    return $?
}

# Wait for TCP connection
retries=0
until check_tcp; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "Failed to connect to RabbitMQ TCP port after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "RabbitMQ TCP port is not ready - sleeping"
    sleep $RETRY_INTERVAL
done

echo "TCP connection established, checking AMQP..."

# Wait for AMQP connection
retries=0
until check_amqp; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "Failed to establish AMQP connection after $MAX_RETRIES attempts"
        exit 1
    fi
    echo "RabbitMQ AMQP is not ready - sleeping"
    sleep $RETRY_INTERVAL
done

echo "RabbitMQ is up and ready - executing command"
exec $cmd 