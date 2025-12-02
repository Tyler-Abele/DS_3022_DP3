import os
import sys
from pathlib import Path
from prefect import flow, task

# Add project root to sys.path to allow importing from src
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import functions from other modules
from src.Db_work.load import main as load_main
from src.Db_work.analysis import main as analysis_main
from viz.vizualization import main as visualize_main

# Load Data from S3
@task(name="Load Data from S3")
def load_data_task():
    print("Starting data load...")
    load_main()
    print("Data load complete.")

# Analyze Data
@task(name="Analyze Data")
def analyze_data_task():
    print("Starting analysis...")
    analysis_main()
    print("Analysis complete.")

# Visualize Data
@task(name="Visualize Data")
def visualize_data_task():
    print("Starting visualization...")
    visualize_main()
    print("Visualization complete.")

# Air Ops Pipeline
@flow(name="Air Ops Pipeline")
def air_ops_pipeline():
    # Run tasks
    load_data_task()
    analyze_data_task()
    visualize_data_task()

if __name__ == "__main__":
    # Serve the flow with a schedule
    air_ops_pipeline.serve(
        name="air-ops-deployment",
        cron="*/3 * * * *", # Run every 3 minutes
        tags=["air-ops", "etl"],
        description="Pipeline to load aircraft data and detect anomalies."
    )
