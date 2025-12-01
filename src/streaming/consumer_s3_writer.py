from math import log
import os
import json
import time
from datetime import datetime
from typing import List, Dict
import logging
import sys
import boto3
import pandas as pd
from kafka import KafkaConsumer

S3_BUCKET = "xxe9ff-dp3"
S3_PREFIX = "processed"

BATCH_SIZE = 5000
BATCH_TIMEOUT = 50


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name="us-east-1")


def make_s3_key(now: datetime) -> str:
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    return f"{S3_PREFIX}/date={date_path}/aircraft_states_{timestamp}.parquet"


def write_batch_to_s3(batch: List[Dict]):
    if not batch:
        return

    df = pd.DataFrame(batch)
    now = datetime.now()
    s3_key = make_s3_key(now)

    tmp_path = f"/tmp/{os.path.basename(s3_key)}"
    logger.info(f"Writing {len(batch)} records to {tmp_path}...")
    df.to_parquet(tmp_path, index=False)

    logger.info(f"Uploading {tmp_path} to s3://{S3_BUCKET}/{s3_key}...")
    s3.upload_file(tmp_path, S3_BUCKET, s3_key)
    logging.info(f"Uploaded {len(batch)} records to s3://{S3_BUCKET}/{s3_key}")


def main():
    logging.info("Starting S3 consumer...")
    consumer = KafkaConsumer(
        "aircraft_states_raw",
        bootstrap_servers=[
            "localhost:19092",
            "localhost:29092",
            "localhost:39092",
        ],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    batch = []
    last_flush_time = time.time()

    for message in consumer:
        record = message.value
        batch.append(record)

        if len(batch) % 1000 == 0:
            logger.info(f"Current batch size: {len(batch)}")

        current_time = time.time()
        if (
            len(batch) >= BATCH_SIZE
            or (current_time - last_flush_time) >= BATCH_TIMEOUT
        ):
            write_batch_to_s3(batch)
            batch.clear()
            last_flush_time = current_time

    logging.info("S3 consumer reived all messages, shutting down.")


if __name__ == "__main__":
    main()
