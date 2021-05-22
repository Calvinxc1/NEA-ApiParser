from tqdm.auto import tqdm

from nea_schema.maria.sde.map import Station, Planet, System, Constellation
from nea_schema.maria.esi.uni import Structure
from nea_schema.maria.sde.dogma import DogmaAttribute, DogmaTypeAttribute

from ....Base import Transformer
from .....tools import maria_connect

class TransformerMarketsOrdersStations(Transformer):
    def transform(self, responses):
        location_lookup = self._extract_loc_lookup()
        station_ids = self._extract_station_ids()
        
        active_responses = list(filter(lambda x: x.status_code != 304, responses))
        t = tqdm(active_responses, desc='Transformer', leave=False)\
            if self.verbose else active_responses
        record_items = [
            row
            for response in t
            for row in self.schema.esi_parse(response, location_lookup)
            if row.location_id in station_ids
        ]
        self.logger.info('Transform process complete, %s records', len(record_items))
        self._refresh_etags(active_responses)
        return record_items
        
    def _extract_loc_lookup(self):
        conn = maria_connect(self.sql_params)
        
        station_query = conn.query(Station.station_id, System.system_id, Constellation.region_id)\
            .join(Planet, Station.planet_id == Planet.planet_id)\
            .join(System, Planet.system_id == System.system_id)\
            .join(Constellation, System.constellation_id == Constellation.constellation_id)
        location_lookup = {
            station[0]:{'system_id': station[1], 'region_id': station[2]}
            for station in station_query
        }
        
        conn.close()
        return location_lookup
    
    def _extract_station_ids(self):
        conn = maria_connect(self.sql_params)
        station_ids = [station.station_id for station in conn.query(Station)]
        conn.close()
        return station_ids
