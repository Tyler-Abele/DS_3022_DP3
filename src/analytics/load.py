import duckdb
import os
import logging
import sys

S3_BUCKET = "xxe9ff-dp3"
S3_PREFIX_DATA = "processed"
DB_file = "air_ops.duckdb"
AIRFRAME_DATABASE = "../Reference/aircraftDatabase.csv"
MODEL_DATABASE = "../Reference/doc8643AircraftTypes.csv"


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
            parquet_glob = (
                f"s3://{S3_BUCKET}/{S3_PREFIX_DATA}/date=*/aircraft_states_*.parquet"
            )

            con.execute(
                """
                CREATE OR REPLACE TABLE aircraft_states AS
                SELECT * FROM read_parquet($parquet_glob);
                """,
                {"parquet_glob": parquet_glob},
            )
            logger.info("created aircraft_states table")

            # load in the reference data files
            con.execute(
                """
            CREATE OR REPLACE TABLE aircraft_model_database AS
            SELECT * FROM read_csv_auto('{MODEL_DATABASE}', header=True);
    
            """
            )

            logger.info("created aircraft_model_database table")

            # load in the airframe data files
            con.execute(
                """
            CREATE OR REPLACE TABLE airframe_database AS
            SELECT * FROM read_csv_auto('{AIRFRAME_DATABASE}', header=True);
    
            """
            )

            logger.info("created airframe_database table")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("load Failed")


if __name__ == "__main__":
    main()
