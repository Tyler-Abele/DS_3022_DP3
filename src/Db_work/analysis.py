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

def create_df_for_window(window_end_ms: int) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT *
        FROM aircraft_states
        WHERE window_end_ms = 
        """,
        [window_end_ms],
    ).fetchdf()
    return df


if __name__ == "__main__":
    main()
