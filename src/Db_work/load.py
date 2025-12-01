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

            # # Grab AWS creds from boto3
            # session = boto3.Session()
            # creds = session.get_credentials()
            # if creds is None:
            #     raise RuntimeError("No AWS credentials found for boto3/duckdb")

            # frozen = creds.get_frozen_credentials()

            # Configure DuckDB S3 access
            # con.execute("SET s3_region='us-east-1';")
            # con.execute("SET s3_access_key_id=?", [frozen.access_key])
            # con.execute("SET s3_secret_access_key=?", [frozen.secret_key])
            # if frozen.token:
            #     con.execute("SET s3_session_token=?", [frozen.token])

            # set s3 region
            con.execute(
                """
                SET s3_region='us-east-1';
                """
            )

            # load in the files from s3
            parquet_glob = f"s3://{S3_BUCKET}/{S3_PREFIX_DATA}/date=*/*/*/aircraft_states_*.parquet"

            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_states AS
                SELECT * FROM read_parquet(?);
                """,
                [parquet_glob],
            )
            
            logger.info("created aircraft_states table")
            
            # load in the airframe data files
            
            data_file1 = f"s3://{S3_BUCKET}/data/aircraftDatabase.csv"
            
            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_airframes AS
                SELECT * FROM read_csv_auto(?, header=True);
                """,
                [data_file1],
            )
            
            logger.info("created airframe_database table")

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
