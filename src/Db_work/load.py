import duckdb
import os
import logging
import sys
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


            # 1) Find the most recent parquet file on S3 using DuckDB metadata
            pattern = (
                f"s3://{S3_BUCKET}/{S3_PREFIX_DATA}/date=*/*/*/window_raw_*.parquet"
            )

            # 2) Get the metadata for the most recent parquet file
            result = con.execute(
                """
                SELECT file_name
                FROM parquet_metadata(?)
                ORDER BY file_name DESC
                LIMIT 1;
                """,
                [pattern],
            ).fetchone()

            # 3) Load the most recent parquet file into DuckDB
            if result is None:
                logger.warning("No parquet files found matching pattern on S3")
                return

            latest_path = result[0]
            logger.info(f"Loading most recent parquet file: {latest_path}")

            # 4) Load the most recent parquet file into DuckDB
            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_states AS
                SELECT * FROM read_parquet(?);
                """,
                [latest_path],
            )
            logger.info("created aircraft_states table")
            
            # 5) load in the airframe data files
            
            data_file1 = f"s3://{S3_BUCKET}/data/aircraftDatabase.csv"
            # 6) load the airframe data files into duckdb
            con.execute(
                """
                CREATE OR REPLACE TABLE airframes AS
                SELECT * FROM read_csv_auto(?, header=True);
                """,
                [data_file1],
            )
            
            logger.info("created airframes table")

            # 7) load in the reference data files
            data_file2 = f"s3://{S3_BUCKET}/data/doc8643AircraftTypes.csv"
            # 8) load the reference data files into duckdb
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
