from nea_schema.maria.esi.mkt import MarketHist

from .ExtractorMarketsHistory import ExtractorMarketsHistory
from ...Base import Base

class MarketsHistory(Base):
    endpoint_path = '/markets/{region_id}/history'
    schema = MarketHist
    Extractor = ExtractorMarketsHistory
