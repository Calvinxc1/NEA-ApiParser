from datetime import datetime as dt
from nea_schema.maria.esi.uni import Structure

from .ExtractorUniverseStructuresDetail import ExtractorUniverseStructuresDetail
from ....Base import Base

class UniverseStructuresDetail(Base):
    endpoint_path = '/universe/structures/{structure_id}'
    schema = Structure
    Extractor = ExtractorUniverseStructuresDetail
    
    def pull_and_load(self, structure_ids):
        self.structure_ids = structure_ids
        return super().pull_and_load()
    
    def extract(self):
        responses = self.Extractor.extract(self.structure_ids)
        if not responses: return responses, None
        
        cache_expire = max([
            dt.strptime(response.headers.get('expires'), '%a, %d %b %Y %H:%M:%S %Z')
            for response in responses
        ])
        return responses, cache_expire
