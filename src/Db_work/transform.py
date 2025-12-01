import duckdb
import logging
import sys
from pathlib import Path

# Get absolute paths based on script location
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DB_FILE = str(PROJECT_ROOT / "air_ops.duckdb")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def create_enriched_aircraft_table(con: duckdb.DuckDBPyConnection):
    """
    create table with all relevant information by joining the tables 
    """
    logger.info("Creating enriched aircraft states table...")
    
    con.execute("""
        CREATE OR REPLACE TABLE enriched_aircraft_states AS
        SELECT 
            -- Operational data from aircraft_states
            s.icao24,
            s.origin_country,
            s.time_position,
            s.last_contact,
            s.longitude,
            s.latitude,
            s.baro_altitude,
            s.geo_altitude,
            s.on_ground,
            s.velocity,
            s.true_track,
            s.vertical_rate,
            s.squawk,
            s.spi,
            s.snapshot_ts,
            s.date,
            
            -- Aircraft metadata from airframes
            af.registration,
            af.model,
            af.icaoaircrafttype,
            af.operator,
            af.operatoricao,
            
            -- Aircraft type details from model_database
            md.ModelFullName,
            md.AircraftDescription,
            md.Description,
            md.WTC,
            
            -- Derived fields for anomaly detection
            CASE 
                WHEN s.velocity > 0 AND s.baro_altitude > 0 THEN 
                    ABS(s.vertical_rate) / NULLIF(s.velocity, 0) 
                ELSE NULL 
            END AS climb_rate_ratio,
            
            CASE 
                WHEN s.baro_altitude IS NOT NULL AND s.baro_altitude > 0 THEN 
                    s.baro_altitude / 1000.0  -- Convert to km for easier analysis
                ELSE NULL 
            END AS altitude_km
            
        FROM aircraft_states s
        LEFT JOIN airframes af ON s.icao24 = af.icao24
        LEFT JOIN model_database md ON (
            af.typecode = md.Designator 
            OR af.icaoaircrafttype = md.Designator
        )
    """)
    
    result = con.execute("SELECT COUNT(*) FROM enriched_aircraft_states").fetchone()
    logger.info(f"Created enriched_aircraft_states table with {result[0]:,} rows")
    
    # Log join statistics
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT enriched_aircraft_states.icao24) as unique_aircraft,
            COUNT(af.icao24) as rows_with_airframe_data,
            COUNT(md.Designator) as rows_with_model_data
        FROM enriched_aircraft_states
        LEFT JOIN airframes af ON enriched_aircraft_states.icao24 = af.icao24
        LEFT JOIN model_database md ON (
            af.typecode = md.Designator 
            OR af.icaoaircrafttype = md.Designator
        )
    """).fetchone()
    
    logger.info(f"Join statistics:")
    logger.info(f"  - Total rows: {stats[0]:,}")
    logger.info(f"  - Unique aircraft: {stats[1]:,}")
    logger.info(f"  - Rows with airframe data: {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
    logger.info(f"  - Rows with model data: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")



def main():
    """
    Main function to create enriched tables.
    """
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")
            
            # Step 1: Create enriched table with all joins
            create_enriched_aircraft_table(con)
            
            logger.info("Transform complete!")
            
    except Exception as e:
        logger.exception(f"Transform failed: {e}")
        raise


if __name__ == "__main__":
    main()
