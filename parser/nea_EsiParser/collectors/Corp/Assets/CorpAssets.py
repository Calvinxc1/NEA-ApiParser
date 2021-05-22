from nea_schema.maria.esi.corp import CorpAsset

from ..ExtractorCorp import ExtractorCorp
from .Names import CorpAssetsNames
from .Stations import CorpAssetsStations
from ...Base import Base

class CorpAssets(Base):
    endpoint_path = '/corporations/{corporation_id}/assets'
    schema = CorpAsset
    purge = True
    Extractor = ExtractorCorp
    
    def run_subprocesses(self):
        if self.record_items:
            self.subprocesses = [
                CorpAssetsStations(*self.init_params, parent=self),
                CorpAssetsNames(*self.init_params, parent=self),
            ]
            for subprocess in self.subprocesses: subprocess.pull_and_load()
