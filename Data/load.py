import duckdb
import logging
import requests

BUCKET = "xxe9ff-dp3"
PREFIX = "/raw"
DB_FILE = "events.duckdb"
LOG_FILE = "load.log"


# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_FILE,
)

logger = logging.getLogger(__name__)

