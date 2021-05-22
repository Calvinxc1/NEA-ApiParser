from datetime import datetime as dt

from nea_schema.maria.esi.corp import CorpAsset

from .ExtractorCorpAssetsNames import ExtractorCorpAssetsNames
from .TransformerCorpAssetsNames import TransformerCorpAssetsNames
from .LoaderCorpAssetsNames import LoaderCorpAssetsNames
from ....Base import Base

class CorpAssetsNames(Base):
    endpoint_path = '/corporations/{corporation_id}/assets/names'
    schema = CorpAsset
    Extractor = ExtractorCorpAssetsNames
    Transformer = TransformerCorpAssetsNames
    Loader = LoaderCorpAssetsNames
    
    def extract(self):
        responses = self.Extractor.extract()
        return responses, None
