import json
import time
import kafka
from kafka import KafkaProducer
import logging
import sys
from opensky_client import OpenSkyClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout)
logger = logging.getLogger(__name__)


# Use the externally exposed Redpanda listener ports from docker-compose.
# Each broker exposes a different port on localhost.
producer = KafkaProducer(
    bootstrap_servers=[
        "localhost:19092",
        "localhost:29092",
        "localhost:39092",
    ],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def main():
    client = OpenSkyClient()
    while True:
        states = client.get_states_dict()
        logger.info(f"Fetched {len(states)} states from OpenSky API")
        snapshot_ts = int(time.time())
        for state in states:
            state['snapshot_ts'] = snapshot_ts
            producer.send("aircraft_states_raw", value=state)
            logger.info(f"Sent state: {state}")
        time.sleep(10)

if __name__ == "__main__":
    main()