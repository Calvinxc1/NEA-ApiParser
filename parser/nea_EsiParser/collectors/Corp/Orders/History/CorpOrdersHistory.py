from nea_schema.maria.esi.corp import CorpOrder

from ..CorpOrders import CorpOrders

class CorpOrdersHistory(CorpOrders):
    endpoint_path = '/corporations/{corporation_id}/orders/history'
