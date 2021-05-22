from tqdm.auto import tqdm

from nea_schema.maria.sde.map import Station, Planet, System, Constellation
from nea_schema.maria.esi.uni import Structure
from nea_schema.maria.sde.dogma import DogmaAttribute, DogmaTypeAttribute

from ....Base import Transformer
from .....tools import maria_connect

class TransformerMarketsOrdersStructures(Transformer):
    def transform(self, responses):
        location_lookup = self._extract_loc_lookup()
        
        active_responses = list(filter(lambda x: x.status_code != 304, responses))
        t = tqdm(active_responses, desc='Transformer', leave=False)\
            if self.verbose else active_responses
        record_items = [
            row
            for response in t
            for row in self.schema.esi_parse(response, location_lookup)
        ]
        self.logger.info('Transform process complete, %s records', len(record_items))
        self._refresh_etags(active_responses)
        return record_items
        
    def _extract_loc_lookup(self):
        conn = maria_connect(self.sql_params)
        
        type_attr_items = conn.query(DogmaTypeAttribute).join(DogmaAttribute)\
            .filter(DogmaTypeAttribute.type_id == 35892, DogmaAttribute.data_type == 11)
        market_hub_type_ids = [int(type_attr_item.value) for type_attr_item in type_attr_items]
        
        structure_query = conn.query(Structure.structure_id, System.system_id, Constellation.region_id)\
            .join(System, Structure.system_id == System.system_id)\
            .join(Constellation, System.constellation_id == Constellation.constellation_id)\
            .filter(Structure.type_id.in_(market_hub_type_ids))
        
        location_lookup = {
            structure[0]:{'system_id': structure[1], 'region_id': structure[2]}
            for structure in structure_query
        }
        
        conn.close()
        return location_lookup
