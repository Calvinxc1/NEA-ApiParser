from nea_schema.maria.esi.corp import CorpOrder

from ..ExtractorCorp import ExtractorCorp
from ...Base import Base

class CorpOrders(Base):
    endpoint_path = '/corporations/{corporation_id}/orders'
    schema = CorpOrder
    Extractor = ExtractorCorp
