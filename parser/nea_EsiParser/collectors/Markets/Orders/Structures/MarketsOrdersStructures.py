from datetime import timedelta as td

from nea_schema.maria.esi.mkt import Order

from .ExtractorMarketsOrdersStructures import ExtractorMarketsOrdersStructures
from .TransformerMarketsOrdersStructures import TransformerMarketsOrdersStructures
from .LoaderMarketsOrdersStructures import LoaderMarketsOrdersStructures
from ....Base import Base

class MarketsOrdersStructures(Base):
    endpoint_path = '/markets/structures/{structure_id}'
    defaults = {
        'query_params': {'datasource': 'tranquility'},
    }
    schema = Order
    purge = True
    Extractor = ExtractorMarketsOrdersStructures
    Transformer = TransformerMarketsOrdersStructures
    Loader = LoaderMarketsOrdersStructures
    
    def load(self, record_items):
        self.Loader.load(record_items, self.responses)
