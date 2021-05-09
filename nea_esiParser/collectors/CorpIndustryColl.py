from .BaseColl import BaseColl
from nea_schema.maria.esi.corp import CorpIndustry

class CorpIndustryColl(BaseColl):
    endpoint_path = 'corporations/{corporation_id}/industry/jobs'.format(corporation_id=98479140)
    schema = CorpIndustry
    defaults = {'query_params': {
        'datasource': 'tranquility',
        'include_completed': 'true',
    }}
