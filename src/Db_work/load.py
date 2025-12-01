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
            parquet_glob = f"s3://{S3_BUCKET}/{S3_PREFIX_DATA}/date=*/*/*/aircraft_states_*.parquet"

            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_states AS
                SELECT * FROM read_parquet(?, union_by_name=true);
                """,
                [parquet_glob],
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
                SELECT * FROM read_csv_auto(?, delim=',', quote='''', header=True);
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
