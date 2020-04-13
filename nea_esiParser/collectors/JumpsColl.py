from .BaseColl import BaseColl
from nea_esiParser.schema import Jumps

class JumpsColl(BaseColl):
    endpoint_path = 'universe/system_jumps'
    schema = Jumps