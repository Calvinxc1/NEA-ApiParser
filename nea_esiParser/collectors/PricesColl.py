from .BaseColl import BaseColl
from nea_schema.esi.mkt import Prices

class PricesColl(BaseColl):
    endpoint_path = 'markets/prices'
    schema = Prices