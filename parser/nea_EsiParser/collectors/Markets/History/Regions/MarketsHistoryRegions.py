from nea_schema.maria.esi.mkt import MarketHistory

from .ExtractorMarketsHistoryRegions import ExtractorMarketsHistoryRegions
from ....Base import Base

class MarketsHistoryRegions(Base):
    endpoint_path = '/markets/{region_id}/history'
    schema = MarketHistory
    Extractor = ExtractorMarketsHistoryRegions
    
    def pull_and_load(self, region_id, type_ids):
        self.region_id = region_id
        self.type_ids = type_ids
        return super().pull_and_load()
        
    def extract(self):
        responses = self.Extractor.extract(self.region_id, self.type_ids)
        return responses, None
