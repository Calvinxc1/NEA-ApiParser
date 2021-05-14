import logging

from ...tools import LoggingBase, maria_connect

class Loader(LoggingBase):
    def __init__(self, sql_params, schema, purge, verbose=False, parent=None):
        self._init_logging(parent)
        self.purge = purge
        self.schema = schema
        self.sql_params = sql_params
        self.verbose = verbose
        
    def load(self, record_items):
        self.conn = maria_connect(self.sql_params)
        
        if self.purge:
            self.logger.info('Performing a purge load')
            self.conn.query(self.schema).delete()
            insert_records = record_items
            update_records = []
        else:
            self.logger.info('Performing an upsert load')
            insert_records = filter(self._is_insert_record, record_items)
            update_records = filter(self._is_update_record, record_items)
            
        self.conn.bulk_insert_mappings(self.schema, insert_records)
        self.conn.bulk_update_mappings(self.schema, update_records)
        
        self.conn.commit()
        self.conn.close()
        self.logger.info('Load process complete.')
        
    def _is_insert_record(self, record_item):
        existing_record = self._get_existing_record(record_item)
        insert_record = False if existing_record else True
        return insert_record
    
    def _is_update_record(self, record_item):
        existing_record = self._get_existing_record(record_item)
        update_record = True if existing_record else False
        return update_record
    
    def _get_existing_record(self, record_item):
        primary_keys = [col.name for col in self.schema.__mapper__.primary_key]
        try:
            query = {key:record_item[key] for key in primary_keys}
        except Exception as E:
            print(primary_keys, record_item)
            raise E
        existing_record = self.conn.get(self.schema, query)
        return existing_record
            