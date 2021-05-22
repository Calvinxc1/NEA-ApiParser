from nea_schema.maria.esi.uni import Structure
from nea_schema.maria.esi import Etag
from nea_schema.maria.sde.dogma import DogmaTypeAttribute, DogmaAttribute

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorMarketsOrdersStructures(Extractor):
    market_hub_type_id = 35892
    can_fit_data_type = 11
    
    def _prime_requests(self):
        structure_ids = self._get_structure_ids()
        etags = self._get_etags(structure_ids)
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            {**self.path_params, 'structure_id': structure_id},
            self.query_params, self.headers, etags.get(structure_id),
            self.Session,
        ) for structure_id in structure_ids]
        self._load_requests(requests)
        
    def _get_structure_ids(self):
        conn = maria_connect(self.sql_params)
        market_hub_type_ids = self._get_market_hub_type_ids(conn)
        structure_items = conn.query(Structure).filter(Structure.type_id.in_(market_hub_type_ids))
        structure_ids = [structure_item.structure_id for structure_item in structure_items]
        conn.close()
        return structure_ids
    
    def _get_market_hub_type_ids(self, conn):
        type_attr_items = conn.query(DogmaTypeAttribute).join(DogmaAttribute)\
            .filter(DogmaTypeAttribute.type_id == 35892, DogmaAttribute.data_type == 11)
        market_hub_type_ids = [int(type_attr_item.value) for type_attr_item in type_attr_items]
        return market_hub_type_ids
    
    def _get_etags(self, structure_ids):
        conn = maria_connect(self.sql_params)
        full_paths = [
            self.root_url + self.endpoint_path.format(**{**self.path_params, 'structure_id': structure_id})
            for structure_id in structure_ids
        ]
        etag_items = conn.query(Etag).filter(Etag.path.in_(full_paths))
        etags = {
            int(etag_item.path.split('/')[-1]):etag_item.etag
            for etag_item in etag_items
        }
        conn.close()
        return etags
