from datetime import datetime as dt

from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.esi.uni import Structure
from nea_schema.maria.sde.map import Station

from .....tools import LoggingBase, maria_connect

class CorpAssetsStations(LoggingBase):
    def __init__(self, sql_params, Session=None, esi_auth=None, verbose=False, subprocess=False, parent=None):
        self._init_logging(parent)
        self.verbose = verbose
        self.subprocess = subprocess
        self.sql_params = sql_params
        self.params = (sql_params, Session, esi_auth, verbose)
    
    def pull_and_load(self):
        self.logger.info('Updating Station ID on assets')
        conn = maria_connect(self.sql_params)
        station_assets = [
            *conn.query(CorpAsset).join(Station, CorpAsset.location_id == Station.station_id),
            *conn.query(CorpAsset).join(Structure, CorpAsset.location_id == Structure.structure_id),
        ]
        stationed_assets = [
            asset_item for station_asset in station_assets
            for asset_item in self._update_station(station_asset, station_asset.location_id)
        ]
        conn.commit()
        conn.close()
        self.logger.info('Station ID updated on assets.')
    
    def _update_station(self, asset, station_id):
        asset.station_id = station_id
        sub_assets = [
            asset_item for sub_asset in asset.child
            for asset_item in self._update_station(sub_asset, station_id)
        ]
        assets = [asset, *sub_assets]
        return assets
