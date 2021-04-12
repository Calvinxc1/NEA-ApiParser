from multiprocessing.dummy import Pool, Process, Queue
from time import sleep
from tqdm.notebook import tqdm

from .BaseColl import BaseColl
from nea_schema.maria.esi.mkt import MarketHist
from nea_schema.maria.sde.map import Region

class MarketHistColl(BaseColl):
    endpoint_path = 'markets/{region_id}/history'
    endpoint_region_types = 'markets/{region_id}/types'
    schema = MarketHist
    days_back = None
    buffer = 100
    
    def pull_and_load(self):
        queue_in = Queue()
        queue_out = Queue()
        
        regions = self._get_regions()
        market_params = self._get_region_types(regions)
        for _ in range(self.buffer): queue_in.put(market_params.pop(0))
        
        threads = [
            Process(target=self._thread_market_data, args=(queue_in, queue_out))
            for _ in range(self.pool_workers)
        ]
        for thread in threads: thread.start()
        
        cache_expire = self._process_cycle(market_params, queue_in, queue_out, self.buffer)
            
        for _ in range(len(threads)): queue_in.put(False)
        for thread in threads: thread.join()
        
    def _get_regions(self):
        Session, conn = self._build_session(self.engine)
        region_ids = [
            region_id[0] for region_id
            in conn.query(Region.region_id)
        ]
        conn.close()
        return region_ids
    
    def _get_region_types(self, region_ids):
        region_types_path = '{}/{}'.format(self.root_url, self.endpoint_region_types)
        req_params = [
            {'path': region_types_path, 'path_params': {'region_id': region_id}}
            for region_id in region_ids
        ]   
        
        with Pool(self.pool_workers) as P:
            if self.verbose:
                region_types = list(tqdm(P.imap_unordered(
                    self._thread_region_types,
                    req_params,
                ), total=len(req_params)))
            else:
                region_types = P.map(self._thread_region_types,req_params)
            
        region_types = {
            region_id:[
                type_id
                for resp in results
                for type_id in resp.json()
            ]
            for region_id, results in region_types
        }
        
        market_history_path = '{}/{}'.format(self.root_url, self.endpoint_path)
        market_params = []
        for region_id, type_ids in region_types.items():
            for type_id in type_ids:
                market_params.append({
                    'path': market_history_path,
                    'path_params': {'region_id': region_id},
                    'query_params': {'type_id': type_id},
                })
                
        return market_params
        
    def _thread_region_types(self, url_params):
        responses, cache_expire = self._get_responses(**url_params)
        return url_params['path_params']['region_id'], responses

    def _thread_market_data(self, queue_in, queue_out):
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
            
    def alchemy_responses(self, responses):
        alchemy_data = [
            row
            for response in responses
            for row in self.schema.esi_parse(response, days_back=self.days_back)
        ]            
        return alchemy_data
            
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
