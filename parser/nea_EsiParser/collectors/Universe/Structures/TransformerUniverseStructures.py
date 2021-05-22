from tqdm.auto import tqdm

from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.sde.map import Station

from ...Base import Transformer
from ....tools import maria_connect

class TransformerUniverseStructures(Transformer):
    def transform(self, responses):
        active_responses = list(filter(lambda x: x.status_code != 304, responses))
        t = tqdm(active_responses, desc='Transformer', leave=False)\
            if self.verbose else active_responses
        record_items = [
            structure_id
            for response in t
            for structure_id in response.json()
        ]
        
        conn = maria_connect(self.sql_params)
        asset_items = conn.query(CorpAsset)\
            .outerjoin(Station, CorpAsset.location_id == Station.station_id)\
            .filter(CorpAsset.type_id == 27, Station.station_id == None)
        record_items.extend([
            asset_item.location_id
            for asset_item in asset_items
        ])
        record_items = set(record_items)
        
        self.logger.info('Transform process complete, %s records', len(record_items))
        self._refresh_etags(active_responses)
        return record_items
