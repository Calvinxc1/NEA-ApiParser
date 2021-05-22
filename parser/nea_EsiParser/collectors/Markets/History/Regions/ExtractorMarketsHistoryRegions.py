from nea_schema.maria.esi import Etag

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorMarketsHistoryRegions(Extractor):
    def extract(self, region_id, type_ids):
        self.region_id = region_id
        self.type_ids = type_ids
        return super().extract()
    
    def _prime_requests(self):
        etag = self._get_etag(self.region_id)
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {**self.path_params, 'region_id': self.region_id},
            {**self.query_params, 'type_id': type_id},
            self.headers, etag, self.Session,
        ) for type_id in self.type_ids
        ]
        self._load_requests(requests)
    
    def _get_etag(self, region_id):
        conn = maria_connect(self.sql_params)
        full_path = self.root_url + self.endpoint_path.format(**{**self.path_params, 'region_id': region_id})
        etag_item = conn.query(Etag).filter_by(path=full_path).one_or_none()
        etag = None if not etag_item else etag_item.etag
        conn.close()
        return etag
