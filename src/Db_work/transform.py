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
    Create an enriched aircraft states table by joining:
    - aircraft_states (operational data)
    - airframes (aircraft metadata)
    - model_database (aircraft type details)
    
    This creates a single table with all relevant information for anomaly detection.
    """
    logger.info("Creating enriched aircraft states table...")
    
    con.execute("""
        CREATE OR REPLACE TABLE enriched_aircraft_states AS
        SELECT 
            -- Operational data from aircraft_states
            s.icao24,
            s.callsign,
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
            s.sensors,
            s.squawk,
            s.spi,
            s.position_source,
            s.snapshot_ts,
            s.date,
            
            -- Aircraft metadata from airframes
            af.registration,
            af.manufacturericao,
            af.manufacturername,
            af.model,
            af.typecode,
            af.icaoaircrafttype,
            af.operator,
            af.operatorcallsign,
            af.operatoricao,
            af.operatoriata,
            af.owner,
            af.status,
            af.built,
            af.engines,
            af.modes,
            af.adsb,
            af.acars,
            af.categoryDescription,
            
            -- Aircraft type details from model_database
            md.ModelFullName,
            md.AircraftDescription,
            md.Description,
            md.EngineCount,
            md.EngineType,
            md.ManufacturerCode,
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
            COUNT(DISTINCT icao24) as unique_aircraft,
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


def create_anomaly_features_table(con: duckdb.DuckDBPyConnection):
    """
    Create a table with pre-computed features for anomaly detection.
    This includes statistical aggregations by aircraft type that can be used
    to identify outliers.
    """
    logger.info("Creating anomaly features table...")
    
    con.execute("""
        CREATE OR REPLACE TABLE anomaly_features AS
        WITH type_stats AS (
            SELECT 
                typecode,
                icaoaircrafttype,
                ModelFullName,
                COUNT(*) as observation_count,
                AVG(velocity) as avg_velocity,
                STDDEV(velocity) as stddev_velocity,
                AVG(baro_altitude) as avg_altitude,
                STDDEV(baro_altitude) as stddev_altitude,
                AVG(ABS(vertical_rate)) as avg_vertical_rate,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY velocity) as p95_velocity,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY baro_altitude) as p95_altitude
            FROM enriched_aircraft_states
            WHERE typecode IS NOT NULL 
                AND velocity IS NOT NULL 
                AND baro_altitude IS NOT NULL
            GROUP BY typecode, icaoaircrafttype, ModelFullName
            HAVING COUNT(*) >= 10  -- Only include types with sufficient data
        )
        SELECT * FROM type_stats
        ORDER BY observation_count DESC
    """)
    
    result = con.execute("SELECT COUNT(*) FROM anomaly_features").fetchone()
    logger.info(f"Created anomaly_features table with {result[0]} aircraft types")


def flag_anomalies(con: duckdb.DuckDBPyConnection):
    """
    Create a table with flagged anomalies based on statistical outliers.
    This identifies aircraft states that deviate significantly from expected
    behavior for their aircraft type.
    """
    logger.info("Flagging anomalies...")
    
    con.execute("""
        CREATE OR REPLACE TABLE flagged_anomalies AS
        SELECT 
            e.*,
            af.avg_velocity,
            af.stddev_velocity,
            af.avg_altitude,
            af.stddev_altitude,
            af.p95_velocity,
            af.p95_altitude,
            
            -- Anomaly flags
            CASE 
                WHEN e.velocity > (af.avg_velocity + 3 * af.stddev_velocity) 
                    AND af.stddev_velocity > 0 
                THEN TRUE 
                ELSE FALSE 
            END AS high_speed_anomaly,
            
            CASE 
                WHEN e.velocity < (af.avg_velocity - 3 * af.stddev_velocity) 
                    AND af.stddev_velocity > 0 
                THEN TRUE 
                ELSE FALSE 
            END AS low_speed_anomaly,
            
            CASE 
                WHEN e.baro_altitude > (af.avg_altitude + 3 * af.stddev_altitude) 
                    AND af.stddev_altitude > 0 
                THEN TRUE 
                ELSE FALSE 
            END AS high_altitude_anomaly,
            
            CASE 
                WHEN e.baro_altitude < (af.avg_altitude - 3 * af.stddev_altitude) 
                    AND af.stddev_altitude > 0 
                    AND e.on_ground = FALSE
                THEN TRUE 
                ELSE FALSE 
            END AS low_altitude_anomaly,
            
            CASE 
                WHEN e.velocity > af.p95_velocity * 1.2 
                THEN TRUE 
                ELSE FALSE 
            END AS extreme_speed_anomaly,
            
            CASE 
                WHEN e.baro_altitude > af.p95_altitude * 1.2 
                THEN TRUE 
                ELSE FALSE 
            END AS extreme_altitude_anomaly,
            
            -- Unusual vertical rate
            CASE 
                WHEN ABS(e.vertical_rate) > 50 
                    AND e.velocity < 100  -- Slow but climbing/descending fast
                THEN TRUE 
                ELSE FALSE 
            END AS unusual_vertical_rate
            
        FROM enriched_aircraft_states e
        LEFT JOIN anomaly_features af ON (
            e.typecode = af.typecode 
            OR e.icaoaircrafttype = af.icaoaircrafttype
        )
        WHERE af.typecode IS NOT NULL  -- Only flag when we have type statistics
    """)
    
    result = con.execute("SELECT COUNT(*) FROM flagged_anomalies").fetchone()
    logger.info(f"Created flagged_anomalies table with {result[0]:,} rows")
    
    # Count anomalies by type
    anomaly_counts = con.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN high_speed_anomaly THEN 1 ELSE 0 END) as high_speed,
            SUM(CASE WHEN low_speed_anomaly THEN 1 ELSE 0 END) as low_speed,
            SUM(CASE WHEN high_altitude_anomaly THEN 1 ELSE 0 END) as high_altitude,
            SUM(CASE WHEN low_altitude_anomaly THEN 1 ELSE 0 END) as low_altitude,
            SUM(CASE WHEN extreme_speed_anomaly THEN 1 ELSE 0 END) as extreme_speed,
            SUM(CASE WHEN extreme_altitude_anomaly THEN 1 ELSE 0 END) as extreme_altitude,
            SUM(CASE WHEN unusual_vertical_rate THEN 1 ELSE 0 END) as unusual_vertical
        FROM flagged_anomalies
    """).fetchone()
    
    logger.info(f"Anomaly counts:")
    logger.info(f"  - High speed: {anomaly_counts[1]:,}")
    logger.info(f"  - Low speed: {anomaly_counts[2]:,}")
    logger.info(f"  - High altitude: {anomaly_counts[3]:,}")
    logger.info(f"  - Low altitude: {anomaly_counts[4]:,}")
    logger.info(f"  - Extreme speed: {anomaly_counts[5]:,}")
    logger.info(f"  - Extreme altitude: {anomaly_counts[6]:,}")
    logger.info(f"  - Unusual vertical rate: {anomaly_counts[7]:,}")


def main():
    """
    Main function to create enriched tables and flag anomalies.
    """
    try:
        with duckdb.connect(DB_FILE, read_only=False) as con:
            logger.info("Connected to DuckDB")
            
            # Step 1: Create enriched table with all joins
            create_enriched_aircraft_table(con)
            
            # Step 2: Create statistical features for anomaly detection
            create_anomaly_features_table(con)
            
            # Step 3: Flag anomalies
            flag_anomalies(con)
            
            logger.info("Transform complete!")
            
    except Exception as e:
        logger.exception(f"Transform failed: {e}")
        raise


if __name__ == "__main__":
    main()
