from datetime import datetime as dt

from nea_schema.maria.esi.corp import CorpAsset

from .ExtractorCorpAssetsStations import ExtractorCorpAssetsStations
from ....Base import Base

class CorpAssetsStations(Base):
    endpoint_path = '/corporations/{corporation_id}/assets/names'
    schema = CorpAsset
    Extractor = ExtractorCorpAssetsStations
    
    def pull_and_load(self):
        self.logger.info('Began ETL process')
        start = dt.now()
        self.record_items = self.Extractor.extract()
        self.Loader.load(self.record_items)
        self.subprocess()
        time = dt.now() - start
        self.logger.info('ETL complete, elapsed time %s', time)
