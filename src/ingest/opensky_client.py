import logging
from typing import Dict, List, Optional

import pandas as pd
from pyopensky.rest import REST

logger = logging.getLogger(__name__)


def _sanitize(value):
    """Convert pandas/NumPy missing values to None so JSON serialization works."""
    try:
        return None if pd.isna(value) else value
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
            "time_position": _sanitize(row.get("time_position")),
            "last_contact": _sanitize(row.get("last_contact")),
            "longitude": _sanitize(row.get("longitude")),
            "latitude": _sanitize(row.get("latitude")),
            "baro_altitude": _sanitize(row.get("baro_altitude")),
            "on_ground": _sanitize(row.get("on_ground")),
            "velocity": _sanitize(row.get("velocity")),
            "true_track": _sanitize(row.get("true_track")),
            "vertical_rate": _sanitize(row.get("vertical_rate")),
            "sensors": _sanitize(row.get("sensors")),
            "geo_altitude": _sanitize(row.get("geo_altitude")),
            "squawk": _sanitize(row.get("squawk")),
            "spi": _sanitize(row.get("spi")),
            "position_source": _sanitize(row.get("position_source")),
        }
