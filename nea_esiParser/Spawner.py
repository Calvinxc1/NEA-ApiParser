from datetime import datetime as dt
from multiprocessing import Process, SimpleQueue
from multiprocessing.dummy import Pool
from time import sleep

from nea_esiParser.collectors import StatusColl, JumpsColl

class Spawner:
    collectors = [
        StatusColl, JumpsColl
    ]
    
    def __init__(self, sql_params, max_threads=12, sleep_interval=1, verbose=False):
        self.sql_params = sql_params
        self.max_threads = max_threads
        self.sleep_interval = sleep_interval
        self.verbose = verbose
        
        self.enabled = True
        self.expires = {coll:dt.now() for coll in self.collectors}
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
        curr_time = dt.now()
        active_procs = []
        for proc, expire in list(self.expires.items()):
            if (curr_time - expire).total_seconds() > 0:
                active_procs.append(proc)
                del self.expires[proc]
        
        return active_procs
            
    def _spawn_next_procs(self, active_procs):
        new_process = Process(target=self._run_active_procs, args=(active_procs,), daemon=True)
        new_process.start()
        self.processes[new_process.pid] = new_process
        
    def _run_active_procs(self, active_procs):
        with Pool(self.max_threads) as P:
            expires = {
                coll: P.apply(self._run_proc, (coll, self.sql_params, self.verbose))
                for coll in active_procs
            }
            
        self.queue.put(expires)
        
    @staticmethod
    def _run_proc(coll, sql_params, verbose):
        cache_expire = coll(sql_params, verbose).pull_and_load()
        return cache_expire
    
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