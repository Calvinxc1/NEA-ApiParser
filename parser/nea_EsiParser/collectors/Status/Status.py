from nea_schema.maria.esi.srv import Status

from ..Base import Base

class Status(Base):
    endpoint_path = '/status'
    schema = Status
