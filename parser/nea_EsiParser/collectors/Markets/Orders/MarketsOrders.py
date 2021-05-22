from datetime import timedelta as td

from nea_schema.maria.esi.mkt import Order

from ...Base import Base
from .ExtractorMarketsOrders import ExtractorMarketsOrders
from .LoaderMarketsOrders import LoaderMarketsOrders

class MarketsOrders(Base):
    endpoint_path = '/markets/{region_id}/orders'
    defaults = {
        'query_params': {
            'datasource': 'tranquility',
            'order_type': 'all',
        },
    }
    schema = Order
    purge = True
    Extractor = ExtractorMarketsOrders
    Loader = LoaderMarketsOrders
    refresh_time_shift = td(minutes=15)
    
    def load(self, record_items):
        self.Loader.load(record_items, self.responses)
