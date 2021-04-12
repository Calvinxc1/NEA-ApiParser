from .BaseColl import BaseColl
from nea_schema.maria.esi.corp import CorpAsset
from nea_schema.maria.sde.inv import Type, Group, Category
from sqlalchemy import and_

class CorpAssetColl(BaseColl):
    endpoint_path = 'corporations/{corporation_id}/assets'.format(corporation_id=98479140)
    schema = CorpAsset
    delete_before_merge = True
    
    def pull_and_load(self):
        responses, cache_expire = self.build_responses()
        rows = self.alchemy_responses(responses)
        self.merge_rows(rows)
        self.process_names()
        return cache_expire
    
    def process_names(self):
        name_ids = self._get_name_ids()
        names = self._request_names(name_ids)
        self._update_names(names)
    
    def _get_name_ids(self):
        Session, conn = self._build_session(self.engine)
        query = conn.query(CorpAsset.item_id) \
            .join(Type).join(Group).join(Category) \
            .filter(and_(
                Category.category_id.in_([2, 65]),
                CorpAsset.is_singleton == True,
            ))
        name_ids = [row.item_id for row in query]
        return name_ids
    
    def _request_names(self, name_ids):
        path = self.full_path + '/names'
        resp = self._request(path, json_body=name_ids, method='POST')
        names = {name['item_id']:name['name'] for name in resp.json()}
        return names
    
    def _update_names(self, names):
        Session, conn = self._build_session(self.engine)
        query = conn.query(CorpAsset).filter(CorpAsset.item_id.in_(list(names.keys())))
        items = [row for row in query]
        for row in items:
            row.item_name = names[row.item_id]
        conn.commit()
        conn.close()