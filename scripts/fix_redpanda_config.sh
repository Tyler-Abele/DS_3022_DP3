#!/bin/bash

# Fix Redpanda topic configuration for large messages
# This is required because the default topic creation does not inherit the broker-level max message bytes
# for internal changelog topics created by Quix Streams.

TOPIC_NAME="changelog__aircraft-tumbling-window-v6--aircraft_states_raw--groupby--global_window--tumbling_window_180000_reduce"
MAX_BYTES=104857600

echo "Updating config for topic: $TOPIC_NAME"
docker exec opensky-0 rpk topic alter-config "$TOPIC_NAME" --set max.message.bytes=$MAX_BYTES

echo "Done."
