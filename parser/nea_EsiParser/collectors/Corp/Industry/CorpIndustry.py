from nea_schema.maria.esi.corp import CorpIndustry

from .ExtractorCorpIndustry import ExtractorCorpIndustry
from ...Base import Base

class CorpIndustry(Base):
    endpoint_path = '/corporations/{corporation_id}/industry/jobs'
    schema = CorpIndustry
    Extractor = ExtractorCorpIndustry
