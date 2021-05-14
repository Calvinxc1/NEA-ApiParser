from nea_schema.maria.sde.map import Region

from ...Base import Extractor
from ....tools import maria_connect

class ExtractorMarketsHistory(Extractor):
    secondary_endpoint_path = '/markets/{region_id}/types'
    
    def extract(self):
        region_ids = self._get_region_ids()
        region_types = self._get_region_types(region_ids)
        
        self.logger.info('Extracting Market History from %s region/types', len(region_types))
        self._prime_requests(region_types)
        self._run_threads(self.verbose)
        self.logger.info('Extract process complete, %s responses', len(self.responses))
        return self.responses
    
    def _get_region_ids(self):
        conn = maria_connect(self.sql_params)
        region_items = conn.query(Region)
        region_ids = [region_item.region_id for region_item in region_items]
        conn.close()
        self.logger.info('Retrieved %s Regions from database', len(region_ids))
        return region_ids
    
    def _get_region_types(self, region_ids):
        self.logger.info('Extracting Region Types from %s regions.', len(region_ids))
        self._prime_region_types_requests(region_ids)
        self._run_threads(self.verbose)
        
        region_types = {}
        for response in self.responses:
            region_id = response.url.split('/')[5]
            region_types[region_id] = region_types.get(region_id, [])
            region_types[region_id].extend(response.json())
            
        region_types = [
            {'region_id': region_id, 'type_ids': type_ids}
            for region_id, type_ids in region_types.items()
        ]
            
        return region_types
        
    def _prime_region_types_requests(self, region_ids):
        requests = [self.Requester(
            self.root_url + self.secondary_endpoint_path,
            self.method, None, {'region_id': region_id},
            self.query_params, self.headers, self.Session, self,
        ) for region_id in region_ids]
        self._load_requests(requests)
    
    def _prime_requests(self, region_types):
        request_queue = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {'region_id': region['region_id']}, {**self.query_params, 'type_id': type_id},
            self.headers, self.Session, self,
        ) for region in region_types
            for type_id in region['type_ids']
        ]
        self._load_requests(request_queue)
