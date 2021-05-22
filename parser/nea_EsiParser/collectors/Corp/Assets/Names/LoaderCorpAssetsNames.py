from ....Base import Loader
from .....tools import maria_connect

class LoaderCorpAssetsNames(Loader):
    def load(self, name_lookup):
        conn = maria_connect(self.sql_params)
        record_items = conn.query(self.schema).filter(self.schema.item_id.in_(name_lookup.keys()))
        for record_item in record_items:
            record_item.item_name = name_lookup[record_item.item_id]
        conn.commit()
        conn.close()
