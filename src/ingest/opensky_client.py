import logging 
from typing import List, Dict, Optional

from pyopensky.rest import REST
from pyopensky.exceptions import OpenSkyAPiError

logger = logging.getLogger(__name__)

class OpenSkyClient:
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        
        self.client = REST(username=username, password=password)

    def fetch_states(self):
        try:
            df = self.rest.states()
            return df
        except OpenSkyAPiError as e:
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
            "icao24": row.get("icao24"),
            "callsign": row.get("callsign"),
            "origin_country": row.get("origin_country"),
            "time_position": row.get("time_position"),
            "last_contact": row.get("last_contact"),
            "longitude": row.get("longitude"),
            "latitude": row.get("latitude"),
            "baro_altitude": row.get("baro_altitude"),
            "on_ground": row.get("on_ground"),
            "velocity": row.get("velocity"),
            "true_track": row.get("true_track"),
            "vertical_rate": row.get("vertical_rate"),
            "sensors": row.get("sensors"),
            "geo_altitude": row.get("geo_altitude"),
            "squawk": row.get("squawk"),
            "spi": row.get("spi"),
            "position_source": row.get("position_source"),
        }
    
    
        