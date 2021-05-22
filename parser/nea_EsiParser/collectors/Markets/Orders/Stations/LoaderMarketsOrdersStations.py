from nea_schema.maria.sde.map import Station

from ....Base import Loader
from .....tools import maria_connect

class LoaderMarketsOrdersStations(Loader):
    def load(self, record_items, responses):
        conn = maria_connect(self.sql_params)
        if self.purge:
            self.logger.info('Performing Purge Load on %s records', len(record_items))
            
            region_ids = set([int(response.url.split('/')[-2]) for response in responses])
            station_ids = [station.station_id for station in conn.query(Station)]
            delete_items = conn.query(self.schema).filter(
                self.schema.region_id.in_(region_ids),
                self.schema.location_id.in_(station_ids),
            )
            delete_items.delete()
            conn.commit()
            
            try:
                conn.bulk_save_objects(record_items)
                conn.commit()
            except Exception as E:
                self.logger.error(E)
        else:
            t = tqdm(record_items, desc='Loader', leave=False) if self.verbose else record_items
            for row in t: conn.merge(row)
            conn.commit()
        conn.close()
        self.logger.info('Loading complete')