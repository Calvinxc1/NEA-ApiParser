from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.esi import Etag
from nea_schema.maria.sde.map import Station

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorUniverseStructuresDetail(Extractor):
    def extract(self, structure_ids):
        self.structure_ids = structure_ids
        return super().extract()
    
    def _prime_requests(self):
        etags = self._get_etags(self.structure_ids)
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {**self.path_params, 'structure_id': structure_id},
            self.query_params, self.headers, etags.get(structure_id),
            self.Session, self,
        ) for structure_id in self.structure_ids]
        self._load_requests(requests)

    def _get_etags(self, structure_ids):
        conn = maria_connect(self.sql_params)
        full_paths = [
            self.root_url + self.endpoint_path.format(structure_id=structure_id)
            for structure_id in structure_ids
        ]
        etag_items = conn.query(Etag).filter(Etag.path.in_(full_paths))
        etags = {
            int(etag_item.path.split('/')[-1]):etag_item.etag
            for etag_item in etag_items
        }
        conn.close()
        return etags
