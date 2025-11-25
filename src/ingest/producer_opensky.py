from pyopensky.rest import REST
import json
import time
import kafka
from kafka import KafkaProducer
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

rest = REST()

# get all states
# states = rest.states()

# get all states in the last 10 minutes ## Example
## states_last_10 = rest.states(last=10)

def get_current_states():
    states = rest.states()
    return states

def get_states_last_10_minutes():
    states_last_10 = rest.states(last=10)
    return states_last_10

