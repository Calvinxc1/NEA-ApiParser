from copy import copy
import logging
from tqdm.auto import tqdm

from nea_schema.maria.esi import Etag

from ...tools import LoggingBase, maria_connect

class Transformer(LoggingBase):
    def __init__(self, schema, sql_params=None, verbose=False, parent=None):
        self._init_logging(parent)
        self.verbose = verbose
        self.schema = schema
        self.sql_params = sql_params
        
    def transform(self, responses):
        active_responses = list(filter(lambda x: x.status_code != 304, responses))
        t = tqdm(active_responses, desc='Transformer', leave=False)\
            if self.verbose else active_responses
        record_items = [
            row
            for response in t
            for row in self.schema.esi_parse(response)
        ]
        self.logger.info('Transform process complete, %s records', len(record_items))
        self._refresh_etags(active_responses)
        return record_items
    
    def _refresh_etags(self, responses):
        etags = {}
        for response in responses:
            url = response.url.split('?')[0]
            etags[url] = etags.get(url, Etag.esi_parse(response))
                
        conn = maria_connect(self.sql_params)
        for row in etags.values(): conn.merge(row)
        self.logger.info('Etag refresh complete.')
        conn.commit()
        conn.close()
