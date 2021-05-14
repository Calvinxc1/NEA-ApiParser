from nea_schema.maria.esi.uni import Jumps

from ...Base import Base

class UniverseJumps(Base):
    endpoint_path = '/universe/system_jumps'
    schema = Jumps
