from multiprocessing.dummy import Pool
from time import sleep

from .BaseColl import BaseColl
from nea_esiParser.schema import MarketHist, Region, Type

class MarketHistColl(BaseColl):
    endpoint_path = 'markets/{region_id}/history'
    defaults = {
        'query_params': {
            'datasource': 'tranquility',
        },
    }
    schema = MarketHist
    
    def build_responses(self):
        Session, conn = self.build_session(self.engine)
        region_ids = self._get_regions(conn)
        type_ids = self._get_types(conn)
        conn.close()
        
        region_types_params = [
            {
                'path': self.full_path,
                'path_params': {'region_id':region_id},
                'query_params': {'type_id':type_id},
            } for region_id in region_ids for type_id in type_ids
        ]
        
        response_sets = self.async_process(self.get_responses, region_types_params, self.pool_workers)
        
        cache_expire = response_sets[0][1]
        responses = [
            response
            for response_set in response_sets
            for response in response_set[0]
        ]
        
        return responses, cache_expire
        
    def _get_regions(self, conn):
        region_ids = [
            region_id[0] for region_id
            in conn.query(Region.region_id)
        ]
        return region_ids
    
    def _get_types(self, conn):
        type_ids = [
            type_id[0] for type_id
            in conn.query(Type.type_id).filter(Type.market_group_id != None).filter(Type.published == True)
        ]
        return type_ids