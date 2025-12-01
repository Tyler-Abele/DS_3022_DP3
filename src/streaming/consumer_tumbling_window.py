from quixstreams import Application
from datetime import timedelta, datetime
import logging
import sys
import time
import boto3
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Configuration: Maximum age of data to process (in minutes)
MAX_DATA_AGE_MINUTES = 9

S3_BUCKET = "xxe9ff-dp3"
S3_PREFIX = "processed"

s3 = boto3.client("s3", region_name="us-east-1")

# Create the Quix Application (connects to Kafka/Redpanda)
app = Application(
    broker_address='127.0.0.1:19092',
    consumer_group='aircraft-tumbling-window-v5',
    auto_offset_reset='earliest',
)

# Define the input topic
aircraft_topic = app.topic('aircraft_states_raw', value_deserializer='json')

# Create a streaming dataframe
sdf = app.dataframe(aircraft_topic)

# Filter out events older than MAX_DATA_AGE_MINUTES
# Uses snapshot_ts if available, otherwise current time
def is_recent_enough(event):
    """Check if event is recent enough to process."""
    current_time = int(time.time())
    
    # Use snapshot_ts if available (set by producer), otherwise use current time
    event_time = event.get('snapshot_ts', current_time)
    
    # Calculate age in seconds
    age_seconds = current_time - event_time
    age_minutes = age_seconds / 60
    
    # Only process if data is less than MAX_DATA_AGE_MINUTES old
    is_recent = age_minutes < MAX_DATA_AGE_MINUTES
    
    # if not is_recent:
    #     logger.warning(f"Filtering out old event: {age_minutes:.2f} minutes old")
    
    return is_recent

sdf = sdf.filter(is_recent_enough)

# Heartbeat logging
# processed_count = 0
# def log_heartbeat(row):
#     global processed_count
#     processed_count += 1
#     if processed_count % 10 == 0:
#         logger.info(f"Heartbeat: Processed {processed_count} valid messages waiting for window...")

# sdf = sdf.update(log_heartbeat)

# Global window - group by constant key to create one window for all events
# This creates one window that aggregates all aircraft states regardless of country
# Using a constant key ensures all messages are processed in the same window

def initializer(event):
    return []

def reducer(aggregated, event):
    # Handle legacy state (dict) from previous version
    if isinstance(aggregated, dict):
        logger.warning("Found legacy state (dict), resetting to list for raw events.")
        return [event]
        
    aggregated.append(event)
    return aggregated

# Group by constant key to create a single global window
# grace_ms controls how long to keep window state after it closes (for late-arriving data)
# Set to MAX_DATA_AGE_MINUTES to ensure old windows are cleaned up
sdf = (
    sdf.group_by(lambda event: "global", name="global_window")
    .tumbling_window(
        duration_ms=timedelta(minutes=3),
        grace_ms=timedelta(minutes=MAX_DATA_AGE_MINUTES)  # Clean up windows older than this
    )
    .reduce(initializer=initializer, reducer=reducer)
    .final()
)


def make_s3_key(window_end_ms: int) -> str:
    """Generate S3 key based on window end time."""
    end_dt = datetime.fromtimestamp(window_end_ms / 1000)
    date_path = end_dt.strftime("%Y/%m/%d")
    timestamp = end_dt.strftime("%Y%m%dT%H%M%S")
    return f"{S3_PREFIX}/date={date_path}/window_raw_{timestamp}.parquet"

def write_window_to_s3(result):
    """Write window result to S3 as parquet."""
    try:
        events = result['value']
        start_ms = result['start']
        end_ms = result['end']
        
        # Prepare data for DataFrame
        # Convert list of dicts (events) to DataFrame
        df = pd.DataFrame(events)
        
        # Add window metadata
        df['window_start'] = datetime.fromtimestamp(start_ms / 1000)
        df['window_end'] = datetime.fromtimestamp(end_ms / 1000)
        
        s3_key = make_s3_key(end_ms)
        tmp_path = f"/tmp/{os.path.basename(s3_key)}"
        
        logger.info(f"Writing {len(df)} events to {tmp_path}...")
        df.to_parquet(tmp_path, index=False)
        
        logger.info(f"Uploading {tmp_path} to s3://{S3_BUCKET}/{s3_key}...")
        s3.upload_file(tmp_path, S3_BUCKET, s3_key)
        logger.info(f"Uploaded window result to s3://{S3_BUCKET}/{s3_key}")
        
        # Clean up
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            
    except Exception as e:
        logger.error(f"Failed to write window to S3: {e}")

# Print results when windows close
def print_window_result(result):
    """Print the aggregated results when a window closes."""
    start_time = datetime.fromtimestamp(result['start'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.fromtimestamp(result['end'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    
    events = result['value']
    count = len(events)
    unique_aircraft = len(set(e.get('icao24') for e in events if e.get('icao24')))
    
    print(f"{'='*60}")
    print(f"Window:           {start_time} to {end_time}")
    print(f"Total Observations: {count}")
    print(f"Unique Aircraft:  {unique_aircraft}")
    print(f"{'='*60}\n")
    
    logger.info(f"Window closed: {count} observations, {unique_aircraft} unique aircraft")
    
    # Write to S3
    write_window_to_s3(result)

sdf.update(print_window_result)

if __name__ == '__main__':
    logger.info("Starting aircraft state counter with 3-minute tumbling windows...")
    logger.info(f"Filtering out data older than {MAX_DATA_AGE_MINUTES} minutes")
    logger.info("Press Ctrl+C to stop\n")
    app.run()