import duckdb
import logging
import sys
from pathlib import Path
import pandas as pd
from sklearn.ensemble import IsolationForest
import plotly.express as px


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

# 1) Get the latest window

def get_latest_window(con):
    result = con.execute(
        "SELECT MAX(window_end) FROM aircraft_states"
    ).fetchone()
    return result[0] if result else None

# 2) Create a DataFrame for a specific window
def create_df_for_window(con, window_end) -> pd.DataFrame:
    logger.info(f"Fetching data for window_end: {window_end}")
    df = con.execute(
        """
        SELECT 
            s.*,
            md.Description
        FROM aircraft_states s
        LEFT JOIN airframes af ON s.icao24 = af.icao24
        LEFT JOIN model_database md ON (
            af.typecode = md.Designator 
            OR af.icaoaircrafttype = md.Designator
        )
        WHERE s.window_end = ?
        """,
        [window_end],
    ).fetchdf()
    return df

# 3) Train Isolation Forest
def train_isolation_forest(df: pd.DataFrame):
    features = ["latitude", "longitude", "baro_altitude", "velocity", "vertical_rate"]
    
    # Prepare data
    df_features = df[features].dropna()
    
    if df_features.empty:
        logger.warning("No data available for training after dropping NaNs.")
        return

    logger.info(f"Training Isolation Forest on {len(df_features)} records...")
    
    # Train Isolation Forest
    model = IsolationForest(contamination=0.01, random_state=42)
    df_features["anomaly"] = model.fit_predict(df_features)
    
    # Filter anomalies (-1 = anomaly, 1 = normal)
    anomalies = df_features[df_features["anomaly"] == -1]
    
    logger.info(f"Found {len(anomalies)} anomalies out of {len(df_features)} flights")
    
    if not anomalies.empty:
        logger.info("Anomalies detected:")
        # Join with original df to get callsign using the index
        anomalies_with_callsign = df.loc[anomalies.index, ["callsign"]]
        anomalies_with_callsign["anomaly"] = -1
        logger.info(f"\n{anomalies_with_callsign.to_string()}")
        
    # Visualize with anomalies highlighted
    logger.info("Generating anomaly plot...")
    df_features["is_anomaly"] = df_features["anomaly"].map({1: "Normal", -1: "Anomaly"})
    fig = px.scatter_geo(df_features, lat="latitude", lon="longitude", 
                        color="is_anomaly", 
                        color_discrete_map={"Normal": "blue", "Anomaly": "red"},
                        projection="natural earth",
                        title="Flight Anomaly Detection")
    
    output_file = "images/anomaly_detection.png"
    fig.write_image(output_file)
    logger.info(f"Plot saved to {output_file}")

def main():
    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            latest_window = get_latest_window(con)
            
            if latest_window is None:
                logger.warning("No data found in aircraft_states table.")
                return
                
            df = create_df_for_window(con, latest_window)
            
            if df.empty:
                logger.warning(f"No data found for window {latest_window}")
                return
                
            train_isolation_forest(df)
            
    except Exception as e:
        logger.exception(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
