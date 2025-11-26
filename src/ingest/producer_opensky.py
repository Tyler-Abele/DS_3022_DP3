from pyopensky.rest import REST
import json
import time
import kafka
from kafka import KafkaProducer
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout)
logger = logging.getLogger(__name__)

rest = REST()

def get_current_states():
    states = rest.states()
    return states

def get_coordinates(states):
    return states[["callsign","latitude", "longitude"]].to_dict("records")

def main():
    while True:
        states = get_current_states()
        logger.info(f"Current states: {states}")
        coords = get_coordinates(states)
        logger.info(f"Coordinates: {coords}")
        time.sleep(10)

if __name__ == "__main__":
    main()