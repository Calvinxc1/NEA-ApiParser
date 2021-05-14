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
    subprocesses = [CorpAssetsNames, CorpAssetsStations]
