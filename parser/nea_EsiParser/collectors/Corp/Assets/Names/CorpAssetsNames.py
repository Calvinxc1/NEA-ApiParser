from datetime import datetime as dt

from nea_schema.maria.esi.corp import CorpAsset

from .ExtractorCorpAssetsNames import ExtractorCorpAssetsNames
from .TransformerCorpAssetsNames import TransformerCorpAssetsNames
from ....Base import Base

class CorpAssetsNames(Base):
    endpoint_path = '/corporations/{corporation_id}/assets/names'
    schema = CorpAsset
    Extractor = ExtractorCorpAssetsNames
    Transformer = TransformerCorpAssetsNames
    
    def pull_and_load(self):
        self.logger.info('Began ETL process')
        start = dt.now()
        self.responses = self.Extractor.extract()
        self.record_items = self.Transformer.transform(self.responses)
        self.Loader.load(self.record_items)
        self.subprocess()
        time = dt.now() - start
        self.logger.info('ETL complete, elapsed time %s', time)
