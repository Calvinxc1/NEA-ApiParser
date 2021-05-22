from nea_schema.maria.sde.map import Region
from nea_schema.maria.esi import Etag

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorMarketsOrdersStations(Extractor):
    def _prime_requests(self):
        region_ids = self._get_region_ids()
        etags = self._get_etags(region_ids)
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {**self.path_params, 'region_id': region_id},
            self.query_params, self.headers, etags.get(region_id),
            self.Session,
        ) for region_id in region_ids]
        self._load_requests(requests)
        
    def _get_region_ids(self):
        conn = maria_connect(self.sql_params)
        region_items = conn.query(Region)
        region_ids = [region_item.region_id for region_item in region_items]
        conn.close()
        return region_ids
    
    def _get_etags(self, region_ids):
        conn = maria_connect(self.sql_params)
        full_paths = [
            self.root_url + self.endpoint_path.format(**{**self.path_params, 'region_id': region_id})
            for region_id in region_ids
        ]
        etag_items = conn.query(Etag).filter(Etag.path.in_(full_paths))
        etags = {
            int(etag_item.path.split('/')[-2]):etag_item.etag
            for etag_item in etag_items
        }
        conn.close()
        return etags
