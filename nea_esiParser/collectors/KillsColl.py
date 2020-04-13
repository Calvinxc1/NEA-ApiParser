from .BaseColl import BaseColl
from nea_esiParser.schema import Kills

class KillsColl(BaseColl):
    endpoint_path = 'universe/system_kills'
    schema = Kills