from .BaseColl import BaseColl
from nea_schema.maria.esi.corp import CorpBlueprint

class CorpBlueprintColl(BaseColl):
    endpoint_path = 'corporations/{corporation_id}/blueprints'.format(corporation_id=98479140)
    schema = CorpBlueprint