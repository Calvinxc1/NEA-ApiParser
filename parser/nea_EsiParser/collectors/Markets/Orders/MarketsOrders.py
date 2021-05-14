from datetime import timedelta as td

from nea_schema.maria.esi.mkt import Order

from ...Base import Base
from .ExtractorMarketsOrders import ExtractorMarketsOrders

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
    refresh_time_shift = td(minutes=20)
