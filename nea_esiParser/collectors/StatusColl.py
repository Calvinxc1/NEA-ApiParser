from .BaseColl import BaseColl
from nea_schema.esi.srv import Status

class StatusColl(BaseColl):
    endpoint_path = 'status'
    schema = Status