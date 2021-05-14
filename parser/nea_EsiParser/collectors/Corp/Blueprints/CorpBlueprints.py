from nea_schema.maria.esi.corp import CorpBlueprint

from ..ExtractorCorp import ExtractorCorp
from ...Base import Base

class CorpBlueprints(Base):
    endpoint_path = '/corporations/{corporation_id}/blueprints'
    schema = CorpBlueprint
    purge = True
    Extractor = ExtractorCorp
