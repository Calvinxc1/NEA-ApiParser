from datetime import datetime as dt, timedelta as td
from multiprocessing import Process, SimpleQueue
from time import sleep

from nea_esiParser.collectors import \
    StatusColl, JumpsColl, KillsColl, \
    PricesColl, OrderColl, MarketHistColl, \
    CorpBlueprintColl, StructureColl, \
    CorpAssetColl, CorpIndustryColl

class Spawner:
    collectors = [
        StatusColl, JumpsColl, KillsColl, PricesColl,
        MarketHistColl, CorpBlueprintColl, StructureColl,
        CorpAssetColl, CorpIndustryColl
    ]
    
    def __init__(self, sql_params, mongo_params, auth_char_id, sleep_interval=1, verbose=False):
        self.sql_params = sql_params
        self.mongo_params = mongo_params
        self.auth_char_id = auth_char_id
        self.sleep_interval = sleep_interval
        self.verbose = verbose
        
        self.enabled = True
        self.expires = {coll:dt.utcnow() for coll in self.collectors}
        self.processes = {}
        self.queue = SimpleQueue()
    
    def run(self):
        while self.enabled:
            active_procs = self._build_active_procs()
            self._spawn_next_procs(active_procs)
            self._purge_old_proc()
            self._cycle_queue()
            sleep(self.sleep_interval)
        
    def _build_active_procs(self):
        curr_time = dt.utcnow()
        active_procs = []
        for proc, expire in list(self.expires.items()):
            if (curr_time - expire).total_seconds() > 0:
                active_procs.append(proc)
                del self.expires[proc]
        
        return active_procs
            
    def _spawn_next_procs(self, active_procs):
        for coll in active_procs:
            process = Process(target=self._run_coll, args=(coll,))
            process.start()
            self.processes[process.pid] = process            
            
    def _run_coll(self, coll):
        cache_expire = coll(self.sql_params, self.mongo_params, self.auth_char_id, self.verbose).pull_and_load()
        if cache_expire is None: cache_expire = dt.utcnow() + td(minutes=1)
        self.queue.put({coll: cache_expire})
    
    def _purge_old_proc(self):
        for pid in list(self.processes.keys()):
            process = self.processes[pid]
            if not process.is_alive():
                self.processes[pid].join()
                del self.processes[pid]
                
    def _cycle_queue(self):
        if not self.queue.empty():
            new_expires = self.queue.get()
            self.expires = {**self.expires, **new_expires}