from multiprocessing.dummy import Process, Queue
from tqdm.notebook import tqdm
from time import sleep
from datetime import datetime as dt

from .BaseColl import BaseColl
from nea_schema.maria.esi.uni import Structure

class StructureColl(BaseColl):
    endpoint_path = 'universe/structures/{structure_id}'
    schema = Structure
    buffer = 100
    
    def pull_and_load(self):
        queue_in = Queue()
        queue_out = Queue()
        
        params = self._get_structure_ids()
        for _ in range(self.buffer): queue_in.put(params.pop(0))
        
        threads = [
            Process(target=self._thread_process, args=(queue_in, queue_out))
            for _ in range(self.pool_workers)
        ]
        for thread in threads: thread.start()
            
        cache_expire = self._process_cycle(params, queue_in, queue_out, self.buffer)
        
        for _ in range(len(threads)): queue_in.put(False)
        for thread in threads: thread.join()
    
    def _get_structure_ids(self):
        path = '/'.join(self.full_path.split('/')[:-1])
        resps = self._get_responses(path)[0]
        structure_ids = [{
            'path': self.full_path,
            'path_params': {'structure_id':structure_id},
        } for resp in resps for structure_id in resp.json()]
        return structure_ids
    
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
                    'structure_id': str(finished_param['path_params']['structure_id']),
                })
                
        return cache_expire