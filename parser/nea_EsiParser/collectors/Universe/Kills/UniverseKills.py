from nea_schema.maria.esi.uni import Kills

from ...Base import Base

class UniverseKills(Base):
    endpoint_path = '/universe/system_kills'
    schema = Kills
