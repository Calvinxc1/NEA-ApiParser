from datetime import timedelta as td

from nea_schema.maria.esi.mkt import Order

from ....Base import Base
from .ExtractorMarketsOrdersStations import ExtractorMarketsOrdersStations
from .TransformerMarketsOrdersStations import TransformerMarketsOrdersStations
from .LoaderMarketsOrdersStations import LoaderMarketsOrdersStations

class MarketsOrdersStations(Base):
    endpoint_path = '/markets/{region_id}/orders'
    defaults = {
        'query_params': {
            'datasource': 'tranquility',
            'order_type': 'all',
        },
    }
    schema = Order
    purge = True
    Extractor = ExtractorMarketsOrdersStations
    Transformer = TransformerMarketsOrdersStations
    Loader = LoaderMarketsOrdersStations
    refresh_time_shift = td(minutes=25)
    
    def load(self, record_items):
        self.Loader.load(record_items, self.responses)
