#!/bin/bash



# Function to cleanup background processes on exit
cleanup() {
    echo "Stopping all background processes..."
    kill 0
}

# Trap SIGINT and SIGTERM to run cleanup
trap cleanup EXIT

mkdir -p logs

# Start docker-compose
# This will start all the services defined in docker-compose.yml
docker-compose up -d

# Fix Redpanda topic configuration for large messages
# This is required because the default topic creation does not inherit the broker-level max message bytes
# for internal changelog topics created by Quix Streams.

TOPIC_NAME="changelog__aircraft-tumbling-window-v6--aircraft_states_raw--groupby--global_window--tumbling_window_180000_reduce"
MAX_BYTES=104857600

echo "Updating config for topic: $TOPIC_NAME"
docker exec opensky-0 rpk topic alter-config "$TOPIC_NAME" --set max.message.bytes=$MAX_BYTES

echo "Done."

echo "Starting producer..."
python src/ingest/producer_opensky.py > logs/producer.log 2>&1 &
echo "Producer started with PID $!"

sleep 3

echo "Starting consumer..."
python src/streaming/consumer_tumbling_window.py > logs/consumer.log 2>&1 &
echo "Consumer started with PID $!"

sleep 3

echo "Starting Prefect server..."
prefect server start > logs/prefect_server.log 2>&1 &
echo "Prefect server started with PID $!"

sleep 5

echo "Starting flows..."
python src/orchestration/flows.py > logs/flows.log 2>&1 &
echo "Flows started with PID $!"

echo "All services started. Logs are in logs/ directory."
echo "Press Ctrl+C to stop everything."

wait