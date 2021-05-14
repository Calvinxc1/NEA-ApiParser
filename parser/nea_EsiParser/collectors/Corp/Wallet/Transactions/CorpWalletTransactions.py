from nea_schema.maria.esi.corp import CorpWalletTransaction

from ..ExtractorCorpWallet import ExtractorCorpWallet
from ....Base import Base

class CorpWalletTransactions(Base):
    endpoint_path = '/corporations/{corporation_id}/wallets/{division}/transactions'
    schema = CorpWalletTransaction
    Extractor = ExtractorCorpWallet
