from nea_schema.maria.sde.map import Region

from ...Base import Extractor
from ....tools import maria_connect

class ExtractorMarketsOrders(Extractor):
    def _prime_requests(self):
        region_ids = self._get_region_ids()
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {**self.path_params, 'region_id': region_id},
            self.query_params, self.headers, self.Session,
        ) for region_id in region_ids]
        self._load_requests(requests)
        
    def _get_region_ids(self):
        conn = maria_connect(self.sql_params)
        region_items = conn.query(Region)
        region_ids = [region_item.region_id for region_item in region_items]
        conn.close()
        return region_ids
