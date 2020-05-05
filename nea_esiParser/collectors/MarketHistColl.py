from multiprocessing.dummy import Process, Queue
from time import sleep
from tqdm.notebook import tqdm

from .BaseColl import BaseColl
from nea_schema.maria.esi.mkt import MarketHist
from nea_schema.maria.sde.map import Region
from nea_schema.maria.sde.inv import Type

class MarketHistColl(BaseColl):
    endpoint_path = 'markets/{region_id}/history'
    schema = MarketHist
    days_back = 7
    buffer = 100
    
    def pull_and_load(self):
        queue_in = Queue()
        queue_out = Queue()
        
        params = self._get_region_types()
        for _ in range(self.buffer): queue_in.put(params.pop(0))
        
        threads = [
            Process(target=self._thread_process, args=(queue_in, queue_out))
            for _ in range(self.pool_workers)
        ]
        for thread in threads: thread.start()
        
        cache_expire = self._process_cycle(params, queue_in, queue_out, self.buffer)
            
        for _ in range(len(threads)): queue_in.put(False)
        for thread in threads: thread.join()
            
    def _get_region_types(self):
        Session, conn = self._build_session(self.engine)
        region_ids = self._get_regions(conn)
        type_ids = self._get_types(conn)
        conn.close()
        
        region_types_params = [
            {
                'path': self.full_path,
                'path_params': {'region_id':region_id},
                'query_params': {'type_id':type_id},
            } for region_id in region_ids for type_id in type_ids
        ]
        
        return region_types_params
        
    def _get_regions(self, conn):
        region_ids = [
            region_id[0] for region_id
            in conn.query(Region.region_id)
        ]
        return region_ids
    
    def _get_types(self, conn):
        type_ids = [
            type_id[0] for type_id
            in conn.query(Type.type_id).filter(Type.market_group_id != None).filter(Type.published == True)
        ]
        return type_ids
        
    def _thread_process(self, queue_in, queue_out):
        while True:
            try:
                params = queue_in.get_nowait()
            except:
                sleep(0.1)
                continue

            if params is False: break

            responses, cache_expire = self._get_responses(**params)
            alchemy_data = self.alchemy_responses(responses) if len(responses) > 0 else []
            queue_out.put((alchemy_data, cache_expire, params))
            
    def _process_cycle(self, params, queue_in, queue_out, buffer):
        target = len(params) + buffer
        complete = 0
        cache_expire = None
        
        if self.verbose: t = tqdm(total=target)
        while complete < target:
            try:
                alchemy_data, new_cache_expire, finished_param = queue_out.get_nowait()
            except:
                sleep(0.1)
                continue

            if cache_expire is None:
                cache_expire = new_cache_expire
            elif new_cache_expire is None:
                pass
            elif cache_expire > new_cache_expire:
                cache_expire = new_cache_expire
                
            if len(alchemy_data) > 0:
                self.merge_rows(alchemy_data)
            
            complete += 1
            if len(params) > 0: queue_in.put(params.pop(0))
            
            if self.verbose:
                t.update()
                t.set_postfix({
                    'region_id': str(finished_param['path_params']['region_id']),
                    'type_id': str(finished_param['query_params']['type_id']),
                })
                
        return cache_expire
    