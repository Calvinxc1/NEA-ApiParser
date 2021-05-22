import logging
from tqdm.auto import tqdm

from ...tools import LoggingBase, maria_connect

class Loader(LoggingBase):
    def __init__(self, sql_params, schema, purge, verbose=False, parent=None):
        self._init_logging(parent)
        self.purge = purge
        self.schema = schema
        self.sql_params = sql_params
        self.verbose = verbose
        
    def load(self, record_items):
        self.logger.info('Performing Load on %s records', len(record_items))
        conn = maria_connect(self.sql_params)
        t = tqdm(record_items, desc='Loader', leave=False) if self.verbose else record_items
        for row in t: conn.merge(row)
        conn.commit()
        if self.purge:
            record_times = [record_item.record_time for record_item in record_items]
            delete_items = conn.query(self.schema).filter(self.schema.record_time.notin_(record_times))
            self.logger.info('Performing Purge on %s records', delete_items.count())
        conn.commit()
        conn.close()
        self.logger.info('Loading complete')
