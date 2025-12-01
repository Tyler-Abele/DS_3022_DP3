from quixstreams import Application
from datetime import timedelta, datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Create the Quix Application (connects to Kafka/Redpanda)
app = Application(
    broker_address='127.0.0.1:19092',
    consumer_group='aircraft-tumbling-window-v1',
    auto_offset_reset='earliest',
)

# Define the input topic
aircraft_topic = app.topic('aircraft_states_raw', value_deserializer='json')

# Create a streaming dataframe
sdf = app.dataframe(aircraft_topic)

# OPTION 1: Global window - group by constant key to create one window for all events
# This creates one window that aggregates all aircraft states regardless of country
# Using a constant key ensures all messages are processed in the same window

def initializer(event):
    return {
        'count': 0,
        'unique_aircraft': {}  # Use dict instead of set for JSON serialization
    }

def reducer(aggregated, event):
    aggregated['count'] += 1
    # Track unique aircraft by icao24 (using dict keys for uniqueness)
    icao24 = event.get('icao24')
    if icao24:
        aggregated['unique_aircraft'][icao24] = True
    return aggregated

# Group by constant key to create a single global window
sdf = (
    sdf.group_by(lambda event: "global", name="global_window")
    .tumbling_window(duration_ms=timedelta(minutes=3))
    .reduce(initializer=initializer, reducer=reducer)
    .final()
)

# OPTION 2: With grouping - separate windows per country (commented out)
# Uncomment this and comment out the above if you want separate windows per country
# sdf = (
#     sdf.group_by(lambda event: event.get('origin_country', 'unknown'), name='origin_country')
#     .tumbling_window(duration_ms=timedelta(minutes=3))
#     .reduce(initializer=initializer, reducer=reducer)
#     .final()
# )

# Print results when windows close
def print_window_result(result):
    """Print the aggregated results when a window closes."""
    start_time = datetime.fromtimestamp(result['start'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.fromtimestamp(result['end'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    
    value = result['value']
    unique_count = len(value['unique_aircraft'])  # Count dict keys
    
    print(f"{'='*60}")
    print(f"Window:           {start_time} to {end_time}")
    print(f"Total Observations: {value['count']}")
    print(f"Unique Aircraft:  {unique_count}")
    print(f"{'='*60}\n")
    
    logger.info(f"Window closed: {value['count']} observations, {unique_count} unique aircraft")

sdf.update(print_window_result)

if __name__ == '__main__':
    logger.info("Starting aircraft state counter with 3-minute tumbling windows...")
    logger.info("Press Ctrl+C to stop\n")
    app.run()

