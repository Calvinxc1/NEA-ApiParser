from multiprocessing.dummy import Pool

from .BaseColl import BaseColl
from nea_schema.esi.mkt import Order
from nea_schema.sde.map import Region

class OrderColl(BaseColl):
    endpoint_path = 'markets/{region_id}/orders'
    defaults = {
        'query_params': {
            'datasource': 'tranquility',
            'order_type': 'all',
        },
    }
    schema = Order
    
    def build_responses(self):
        Session, conn = self._build_session(self.engine)
        params = [
            {
                'path': self.full_path,
                'path_params': {'region_id': region_id[0]},
            }
            for region_id in conn.query(Region.region_id)
        ]
        
        response_sets = self._process(self._get_responses, params)
        
        cache_expire = response_sets[0][1]
        responses = [
            response
            for response_set in response_sets
            for response in response_set[0]
        ]
        
        return responses, cache_expire