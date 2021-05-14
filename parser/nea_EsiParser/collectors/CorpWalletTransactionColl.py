from .BaseColl import BaseColl
from nea_schema.maria.esi.corp import CorpWalletTransaction

class CorpWalletTransactionColl(BaseColl):
    endpoint_path = 'corporations/{corporation_id}/wallets/{division}/transactions'.format(
        corporation_id=98479140, division='{division}',
    )
    schema = CorpWalletTransaction
    defaults = {'query_params': {
        'datasource': 'tranquility',
    }}
    divisions = range(1,8)
    
    def pull_and_load(self):
        for division in self.divisions:
            self.division = division
            responses, cache_expire = self.build_responses()
            rows = self.alchemy_responses(responses)
            self.merge_rows(rows)
            
    def build_responses(self):
        responses, cache_expire = self._get_responses(self.full_path.format(division=self.division))
        responses = [response for response in responses if response is not None]
        return responses, cache_expire
    
    def alchemy_responses(self, responses):
        alchemy_data = [
            row
            for response in responses
            for row in self.schema.esi_parse(response, self.division)
        ]            
        return alchemy_data
