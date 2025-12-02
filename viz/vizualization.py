import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import sys
import os
import duckdb
from pathlib import Path

# Setup paths and imports
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

from src.Db_work.analysis import create_df_for_window, get_latest_window

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

DB_FILE = str(PROJECT_ROOT / "air_ops.duckdb")
IMAGES_DIR = PROJECT_ROOT / "images"

def load_data():
    logger.info(f"Connecting to database: {DB_FILE}")
    try:
        with duckdb.connect(DB_FILE, read_only=True) as con:
            latest_window = get_latest_window(con)
            
            if latest_window is None:
                logger.warning("No data found in aircraft_states table.")
                return pd.DataFrame()
                
            df = create_df_for_window(con, latest_window)
            return df
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return pd.DataFrame()

def generate_scatter_plot(df):
    """
    Altitude vs. Velocity Scatter Plot     
    Shows the performance profile of different aircraft categories.
    """
    logger.info("Generating Altitude vs. Velocity Scatter Plot...")
    
    # Filter for the top 4 most common descriptions to keep the plot clean
    if 'Description' not in df.columns:
        logger.warning("[Skipping] 'Description' column not found for scatter plot grouping. Ensure column names are correct.")
        return

    top_n_desc = df['Description'].value_counts().nlargest(4).index.tolist()
    plot_df = df[df['Description'].isin(top_n_desc)].copy() # Use a copy for plotting
    
    if plot_df.empty:
        logger.warning("Not enough data with filtered descriptions to create the scatter plot.")
        return

    plt.figure(figsize=(10, 7))
    
    # Use seaborn scatterplot for better aesthetics and automatic legend
    sns.scatterplot(
        data=plot_df, 
        x='velocity', 
        y='geo_altitude', 
        hue='Description', 
        palette='viridis', 
        s=50, # size of points
        alpha=0.6 # transparency
    )

    plt.title('Aircraft Performance Envelope: Altitude vs. Velocity', fontsize=16)
    plt.xlabel('Ground Velocity (knots)', fontsize=12)
    plt.ylabel('Geometric Altitude (meters)', fontsize=12)
    
    # Add context lines
    plt.axhline(10000, color='gray', linestyle='--', alpha=0.5, label='Typical Jet Cruise Alt')
    plt.axvline(250, color='gray', linestyle=':', alpha=0.5, label='Regional Cruise Speed')

    # Improve legend placement
    plt.legend(title='Aircraft Type', bbox_to_anchor=(1.05, 1), loc=2)
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    
    # Save plot
    os.makedirs(IMAGES_DIR, exist_ok=True)
    output_path = IMAGES_DIR / "altitude_velocity_scatter.png"
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")
    plt.close()

if __name__ == "__main__":
    df = load_data()
    if not df.empty:
        generate_scatter_plot(df)
s