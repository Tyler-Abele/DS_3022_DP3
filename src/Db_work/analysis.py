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


def create_anomaly_features_table(con: duckdb.DuckDBPyConnection):
    """
    Create a table with calculated deltas between consecutive states.
    This uses window functions to compare each state with the previous one
    for the same aircraft.
    """
    logger.info("Creating anomaly features table (calculating deltas)...")
    
    con.execute("""
        CREATE OR REPLACE TABLE anomaly_features AS
        WITH ordered_states AS (
            SELECT 
                *,
                -- Get previous state values for the same aircraft
                LAG(velocity) OVER (PARTITION BY icao24 ORDER BY time_position) as prev_velocity,
                LAG(baro_altitude) OVER (PARTITION BY icao24 ORDER BY time_position) as prev_altitude,
                LAG(time_position) OVER (PARTITION BY icao24 ORDER BY time_position) as prev_time,
                LAG(vertical_rate) OVER (PARTITION BY icao24 ORDER BY time_position) as prev_vertical_rate
            FROM enriched_aircraft_states
            WHERE time_position IS NOT NULL
        )


        SELECT 
            *,
            -- Calculate deltas
            (velocity - prev_velocity) as velocity_delta,
            (baro_altitude - prev_altitude) as altitude_delta,
            (epoch(CAST(time_position AS TIMESTAMP)) - epoch(CAST(prev_time AS TIMESTAMP))) as time_delta,
            
            -- Calculate derived rates
            CASE 
                WHEN (epoch(CAST(time_position AS TIMESTAMP)) - epoch(CAST(prev_time AS TIMESTAMP))) > 0 THEN
                    (baro_altitude - prev_altitude) / (epoch(CAST(time_position AS TIMESTAMP)) - epoch(CAST(prev_time AS TIMESTAMP)))
                ELSE NULL
            END as calculated_climb_rate
            
        FROM ordered_states
        WHERE prev_time IS NOT NULL -- Remove first observation per aircraft (no delta possible)
    """)
    
    result = con.execute("SELECT COUNT(*) FROM anomaly_features").fetchone()
    logger.info(f"Created anomaly_features table with {result[0]:,} rows")


def flag_anomalies(con: duckdb.DuckDBPyConnection):
    """
    Create a table with flagged anomalies based on physical constraints
    and sudden changes (deltas).
    """
    logger.info("Flagging anomalies...")
    
    con.execute("""
        CREATE OR REPLACE TABLE flagged_anomalies AS
        SELECT 
            *,
            -- Anomaly Flags based on Deltas
            
            -- 1. Sudden Velocity Change (Acceleration)
            -- Threshold: > 50 m/s change in < 20 seconds (approx > 2.5 g acceleration is suspicious for civil aviation)
            CASE 
                WHEN ABS(velocity_delta) > 50 AND time_delta < 20 THEN TRUE
                ELSE FALSE 
            END as sudden_velocity_anomaly,
            
            -- 2. Unrealistic Vertical Rate
            -- Threshold: > 50 m/s (approx 10,000 ft/min) is very high for commercial aircraft
            CASE 
                WHEN ABS(calculated_climb_rate) > 50 THEN TRUE
                ELSE FALSE
            END as unrealistic_climb_anomaly,
            
            -- 3. Altitude Jump
            -- Threshold: > 1000m change in < 10 seconds without corresponding high vertical rate
            CASE 
                WHEN ABS(altitude_delta) > 1000 AND time_delta < 10 THEN TRUE
                ELSE FALSE
            END as altitude_jump_anomaly
            
        FROM anomaly_features
    """)
    
    result = con.execute("SELECT COUNT(*) FROM flagged_anomalies").fetchone()
    logger.info(f"Created flagged_anomalies table with {result[0]:,} rows")
    
    # Count anomalies
    anomaly_counts = con.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN sudden_velocity_anomaly THEN 1 ELSE 0 END) as velocity_anomalies,
            SUM(CASE WHEN unrealistic_climb_anomaly THEN 1 ELSE 0 END) as climb_anomalies,
            SUM(CASE WHEN altitude_jump_anomaly THEN 1 ELSE 0 END) as altitude_anomalies
        FROM flagged_anomalies
    """).fetchone()
    
    logger.info(f"Anomaly counts:")
    logger.info(f"  - Sudden Velocity Anomalies: {anomaly_counts[1]:,}")
    logger.info(f"  - Unrealistic Climb Anomalies: {anomaly_counts[2]:,}")
    logger.info(f"  - Altitude Jump Anomalies: {anomaly_counts[3]:,}")


def main():
    """
    Main function to run anomaly detection analysis.
    """
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")
            
            # Step 1: Create statistical features for anomaly detection
            create_anomaly_features_table(con)
            
            # Step 2: Flag anomalies
            flag_anomalies(con)
            
            logger.info("Analysis complete!")
            
    except Exception as e:
        logger.exception(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main()
