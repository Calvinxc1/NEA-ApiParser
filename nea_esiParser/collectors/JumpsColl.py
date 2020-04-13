from .BaseColl import BaseColl
from nea_schema.esi.uni import Jumps

class JumpsColl(BaseColl):
    endpoint_path = 'universe/system_jumps'
    schema = Jumps