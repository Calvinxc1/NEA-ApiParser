from copy import deepcopy as copy

from nea_schema.maria.esi import Etag

from ..ExtractorCorp import ExtractorCorp
from ....tools import maria_connect

class ExtractorCorpWallet(ExtractorCorp):
    divisions = range(1,8)
    
    def _prime_requests(self):
        etags = self._get_etags()
        requests = [
            copy(self.Requester(
                self.root_url + self.endpoint_path, self.method, None,
                {**self.path_params, 'division': division},
                self.query_params, self.headers, etags.get(division),
                self.Session,
            )) for division in self.divisions
        ]
        self._load_requests(requests)

    def _get_etags(self):
        conn = maria_connect(self.sql_params)
        full_paths = [
            self.root_url + self.endpoint_path.format(**{**self.path_params, 'division': division})
            for division in self.divisions
        ]
        etag_items = conn.query(Etag).filter(Etag.path.in_(full_paths))
        etags = {
            int(etag_item.path.split('/')[-2]):etag_item.etag
            for etag_item in etag_items
        }
        conn.close()
        return etags
