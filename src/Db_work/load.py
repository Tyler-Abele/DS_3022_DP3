import duckdb
import os
import logging
import sys
import boto3
from pathlib import Path

# Get absolute paths based on script location
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
SRC_DIR = SCRIPT_DIR.parent

S3_BUCKET = "xxe9ff-dp3"
S3_PREFIX_DATA = "processed"
DB_file = str(PROJECT_ROOT / "air_ops.duckdb")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def main():
    try:
        with duckdb.connect(DB_file, read_only=False) as con:

            logger.info("connected to duckdb")

            # Enable HTTP/HTTPS access so that we can use s3
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")

            # set s3 region
            con.execute(
                """
                SET s3_region='us-east-1';
                """
            )

            # load in the files from s3
            # Find the most recent parquet file using boto3
            s3 = boto3.client('s3', region_name='us-east-1')
            
            # List objects to find the latest one
            # Note: This assumes a reasonable number of files or that the latest is returned in the first batch
            # For production with many files, we might need to be more specific with the prefix (e.g. today's date)
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX_DATA)
            
            if 'Contents' not in response:
                logger.warning("No files found in S3 bucket")
                return

            # Sort by Key (which contains timestamp) to get the latest
            # Format: processed/date=YYYY/MM/DD/window_raw_YYYYMMDDTHHMMSS.parquet
            latest_file = sorted(response['Contents'], key=lambda x: x['Key'], reverse=True)[0]
            latest_key = latest_file['Key']
            
            logger.info(f"Loading most recent file: {latest_key}")
            
            parquet_file = f"s3://{S3_BUCKET}/{latest_key}"

            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_states AS
                SELECT * FROM read_parquet(?);
                """,
                [parquet_file],
            )
            
            logger.info("created aircraft_states table")
            
            # load in the airframe data files
            
            data_file1 = f"s3://{S3_BUCKET}/data/aircraftDatabase.csv"
            
            con.execute(
                """
                CREATE OR REPLACE TABLE airframes AS
                SELECT * FROM read_csv_auto(?, header=True);
                """,
                [data_file1],
            )
            
            logger.info("created airframes table")

            # load in the reference data files
            data_file2 = f"s3://{S3_BUCKET}/data/doc8643AircraftTypes.csv"
            
            con.execute(
                """
                CREATE OR REPLACE TABLE model_database AS
                SELECT * FROM read_csv_auto(?, header=True);
                """,
                [data_file2],
            )

            logger.info("created aircraft_model_database table")
            
            

            logger.info("load complete")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("load Failed")


if __name__ == "__main__":
    main()
