import duckdb
import os
import logging

S3_BUCKET = "xxe9ff-dp3"
S3_PREFIX = "processed"

logger = logging.getLogger(__name__)

def main():
    try:
        with duckdb.connect("air_ops.duckdb") as con:
            logger.info("connected to duckdb")


            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")
            
            # load in the files from s3
            
            
            
            
    
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception("load Failed")
        
