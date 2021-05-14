import logging
from tqdm.auto import tqdm

from ...tools import LoggingBase

class Transformer(LoggingBase):
    def __init__(self, schema, sql_params=None, verbose=False, parent=None):
        self._init_logging(parent)
        self.verbose = verbose
        self.schema = schema
        self.sql_params = sql_params
        
    def transform(self, responses):
        t = tqdm(responses, desc='Transformer', leave=False) if self.verbose else responses
        record_items = [
            row for response in t
            for row in self.schema.esi_parse(response, orm=False)
        ]
        self.logger.info('Transform process complete, %s records', len(record_items))
        return record_items
