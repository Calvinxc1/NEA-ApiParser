from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.esi.uni import Structure
from nea_schema.maria.sde.map import Station

from ....Base import Extractor
from .....tools import maria_connect

class ExtractorCorpAssetsStations(Extractor):
    def extract(self):
        conn = maria_connect(self.sql_params)
        station_assets = [
            *conn.query(CorpAsset).join(Station, CorpAsset.location_id == Station.station_id),
            *conn.query(CorpAsset).join(Structure, CorpAsset.location_id == Structure.structure_id),
        ]
        stationed_assets = [
            asset
            for station_asset in station_assets
            for asset in self._update_station(station_asset, station_asset.location_id)
        ]
        conn.close()
        return stationed_assets
    
    def _update_station(self, asset, station_id):
        assets = [{
            'item_id': asset.item_id,
            'station_id': station_id,
        }]
        assets.extend([
            asset
            for sub_asset in asset.child
            for asset in self._update_station(sub_asset, station_id)
        ])
        return assets