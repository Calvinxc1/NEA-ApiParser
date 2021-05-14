from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.sde.inv import Type, Group, Category

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorCorpAssetsNames(Extractor):
    path_params = {'corporation_id': 98479140}
    method = 'POST'
    max_item_ids = 1000
    
    def _prime_requests(self, data=None):
        item_ids = self._get_item_ids()
        item_id_sets = [
            item_ids[i*self.max_item_ids:(i+1)*self.max_item_ids]
            for i in range((len(item_ids) // self.max_item_ids) + 1)
        ]
        requests = [
            self.Requester(
                self.root_url + self.endpoint_path, self.method,
                item_id_set, self.path_params, self.query_params,
                self.headers, self.Session,
            ) for item_id_set in item_id_sets
        ]
        self._load_requests(requests)
        
    def _get_item_ids(self):
        conn = maria_connect(self.sql_params)
        asset_items = conn.query(CorpAsset) \
            .join(Type).join(Group).join(Category) \
            .filter(
                Category.category_id.in_([2, 6, 65]),
                CorpAsset.is_singleton == True,
            )
        item_ids = [asset_item.item_id for asset_item in asset_items]
        conn.close()
        return item_ids
