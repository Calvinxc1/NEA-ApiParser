from nea_schema.maria.esi.corp import CorpWalletJournal

from ..ExtractorCorpWallet import ExtractorCorpWallet
from ....Base import Base

class CorpWalletJournal(Base):
    endpoint_path = '/corporations/{corporation_id}/wallets/{division}/journal'
    schema = CorpWalletJournal
    Extractor = ExtractorCorpWallet
