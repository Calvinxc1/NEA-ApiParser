from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.sde.map import Station

from ...Base import Extractor
from ....tools import maria_connect

class ExtractorUniverseStructures(Extractor):
    secondary_endpoint_path = '/universe/structures/{structure_id}'
    
    def extract(self, data=None):
        self.logger.info('Extract process started')
        structure_ids = self._get_structure_ids()
        self._prime_structure_requests(structure_ids)
        self._run_threads(self.verbose)
        return self.responses
    
    def _get_structure_ids(self):
        self._prime_requests()
        self._run_threads(verbose=False)
        
        structure_ids = [
            structure_id for resp in self.responses
            for structure_id in resp.json()
        ]
        
        conn = maria_connect(self.sql_params)
        asset_items = conn.query(CorpAsset)\
            .outerjoin(Station, CorpAsset.location_id == Station.station_id)\
            .filter(CorpAsset.type_id == 27, Station.station_id == None)
        structure_ids.extend([
            asset_item.location_id
            for asset_item in asset_items
        ])
        conn.close()
        
        structure_ids = list(set(structure_ids))
        return structure_ids
    
    def _prime_structure_requests(self, structure_ids, data=None):
        requests = [self.Requester(
            self.root_url + self.secondary_endpoint_path,
            self.method, data, {'structure_id': structure_id},
            self.query_params, self.headers, self.Session, self,
        ) for structure_id in structure_ids]
        self._load_requests(requests)
