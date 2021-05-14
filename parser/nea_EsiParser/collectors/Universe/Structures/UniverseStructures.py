from nea_schema.maria.esi.uni import Structure

from .ExtractorUniverseStructures import ExtractorUniverseStructures
from ...Base import Base

class UniverseStructures(Base):
    endpoint_path = '/universe/structures'
    schema = Structure
    Extractor = ExtractorUniverseStructures
    