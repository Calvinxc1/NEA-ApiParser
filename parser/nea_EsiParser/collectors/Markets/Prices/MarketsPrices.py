from nea_schema.maria.esi.mkt import Prices

from ...Base import Base

class MarketsPrices(Base):
    endpoint_path = '/markets/prices'
    schema = Prices
