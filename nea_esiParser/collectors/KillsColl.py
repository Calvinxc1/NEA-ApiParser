from .BaseColl import BaseColl
from nea_schema.esi.uni import Kills

class KillsColl(BaseColl):
    endpoint_path = 'universe/system_kills'
    schema = Kills