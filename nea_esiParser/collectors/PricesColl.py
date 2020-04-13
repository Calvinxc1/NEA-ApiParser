from .BaseColl import BaseColl
from nea_esiParser.schema import Prices

class PricesColl(BaseColl):
    endpoint_path = 'markets/prices'
    schema = Prices