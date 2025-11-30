import logging
from typing import Dict, List, Optional

import pandas as pd
from pyopensky.rest import REST

logger = logging.getLogger(__name__)


def _sanitize(value):
    """Convert pandas/NumPy missing values to None so JSON serialization works."""
    try:
        if pd.isna(value):
            return None
        # Convert pandas Timestamp to ISO format string
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return value
    except TypeError:
        # pd.isna raises on non-scalar containers; keep the original value.
        return value


class OpenSkyClient:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):

        self.client = REST()

    def fetch_states(self):
        try:
            return self.client.states()
        except Exception as e:
            logger.error(f"Error fetching states: {e}")
            return None

    def get_states_dict(self) -> List[Dict]:
        df = self.fetch_states()
        if df is None:
            return []
        # normalize rows into dicts
        records = []
        for _, row in df.iterrows():
            rec = self._row_to_dict(row)
            records.append(rec)
        return records

    def _row_to_dict(self, row) -> Dict:
        return {
            "icao24": _sanitize(row.get("icao24")),
            "callsign": _sanitize(row.get("callsign")),
            "origin_country": _sanitize(row.get("origin_country")),
            "time_position": _sanitize(row.get("timestamp")),
            "last_contact": _sanitize(row.get("last_position")),
            "longitude": _sanitize(row.get("longitude")),
            "latitude": _sanitize(row.get("latitude")),
            "baro_altitude": _sanitize(row.get("altitude")),
            "on_ground": _sanitize(row.get("onground")),
            "velocity": _sanitize(row.get("groundspeed")),
            "true_track": _sanitize(row.get("track")),  
            "vertical_rate": _sanitize(row.get("vertical_rate")),
            "sensors": _sanitize(row.get("sensors")),
            "geo_altitude": _sanitize(row.get("geoaltitude")),
            "squawk": _sanitize(row.get("squawk")),
            "spi": _sanitize(row.get("spi")),
            "position_source": _sanitize(row.get("position_source")),
        }
