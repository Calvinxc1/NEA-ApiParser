from ..ExtractorCorp import ExtractorCorp
from ....tools import maria_connect

class ExtractorCorpWallet(ExtractorCorp):
    divisions = range(1,8)
    
    def _prime_requests(self):
        requests = [
            self.Requester(
                self.root_url + self.endpoint_path, self.method, None,
                {**self.path_params, 'division': division},
                self.query_params, self.headers, self.Session,
            ) for division in self.divisions
        ]
        self._load_requests(requests)
