from datetime import datetime as dt, timedelta as td
from multiprocessing.dummy import Process
from queue import SimpleQueue as Queue
from time import sleep

from .tools import LimitedSession, LoggingBase

from .collectors import \
    CorpAssets, CorpBlueprints, CorpIndustry, CorpOrders, \
    CorpOrdersHistory, CorpWalletJournal, CorpWalletTransactions, \
    MarketsHistory, MarketsOrders, MarketsPrices, \
    Status, \
    UniverseJumps, UniverseKills, UniverseStructures

class Spawner(LoggingBase):
    collectors = [
        CorpAssets, CorpBlueprints, CorpIndustry, CorpOrders,
        CorpOrdersHistory, CorpWalletJournal, CorpWalletTransactions,
        
        MarketsHistory, MarketsOrders, MarketsPrices,
        
        Status,
        
        UniverseJumps, UniverseKills, UniverseStructures
    ]
    
    def __init__(self, sql_params, esi_auth, sleep_interval=1, verbose=False):
        self._init_logging()
        self.sql_params = sql_params
        self.esi_auth = esi_auth
        self.sleep_interval = sleep_interval
        self.verbose = verbose
        
        self.enabled = True
        self.expires = {coll:dt.utcnow() for coll in self.collectors}
        self.processes = {}
        self.queue = Queue()
        self.Session = LimitedSession(parent=self)
    
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
            self.logger.info('Starting process %s.', coll.__name__)
            process = Process(target=self._run_coll, args=(coll,))
            process.start()
            self.processes[process.name] = process            
            
    def _run_coll(self, coll):
        cache_expire = coll(self.sql_params, self.Session, self.esi_auth, self.verbose).pull_and_load()
        if cache_expire is None: cache_expire = dt.utcnow() + td(minutes=1)
        self.logger.info('Process %s complete, requeued for %s.', coll.__name__, cache_expire)
        self.queue.put({coll: cache_expire})
    
    def _purge_old_proc(self):
        for name in list(self.processes.keys()):
            process = self.processes[name]
            if not process.is_alive():
                self.processes[name].join()
                del self.processes[name]
                
    def _cycle_queue(self):
        if not self.queue.empty():
            new_expires = self.queue.get()
            self.expires = {**self.expires, **new_expires}
