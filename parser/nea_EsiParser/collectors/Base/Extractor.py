import logging
from multiprocessing.dummy import Process
from queue import Queue, SimpleQueue, Empty
from numpy import sqrt, clip
from time import sleep
from tqdm.auto import tqdm

from nea_schema.mongo.EveSsoAuth import ActiveAuth
from nea_schema.maria.esi import Etag

from .Requester import Requester
from ...tools import LoggingBase, LimitedSession, maria_connect, mongo_init

class Extractor(LoggingBase):
    ## Can be updated in child classes, but will inherit from the Base class parameter keys
    Requester = Requester
    root_url = 'https://esi.evetech.net/latest'
    method = 'GET'
    headers = {}
    path_params = {}
    query_params = {'datasource': 'tranquility'}
    control_sleep = 0.1
    thread_sleep = 1
    max_threads = 16
    
    ## Should not be defined in child classes, as doing so would later core functionality
    _dict_merge_attrs = ['headers', 'path_params', 'query_params']
    
    def __init__(self, endpoint_path, Session=None, sql_params=None, esi_auth=None, verbose=False, parent=None):
        self._init_logging(parent)
        self.verbose = verbose
        self.endpoint_path = endpoint_path
        self.sql_params = sql_params
        self._merge_attrs()
        
        if esi_auth: self._add_auth_token(esi_auth)
        self.Session = Session if Session else LimitedSession(parent=self)
        self.responses = []
        
    def _merge_attrs(self):
        for attr in self._dict_merge_attrs:
            setattr(self, attr, {
                **getattr(Extractor, attr),
                **getattr(self, attr),
            })
    
    def _add_auth_token(self, esi_auth):
        mongo_init(esi_auth['session_name'], esi_auth['mongo_params'])
        auth = ActiveAuth.query.get(_id=esi_auth['char_id'])
        self.headers = {
            **self.headers,
            'Authorization': '{token_type} {access_token}'.format(
                token_type=auth.token_type,
                access_token=auth.access_token,
            ),
        }
    
    def extract(self):
        self._prime_requests()
        self._run_threads(self.verbose)
        self.logger.info('Extract process complete, %s responses', len(self.responses))
        return self.responses
        
    def _prime_requests(self):
        etag = self._get_etag()
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            self.path_params, self.query_params, self.headers,
            etag, self.Session, self,
        )]
        self._load_requests(requests)
        
    def _get_etag(self):
        conn = maria_connect(self.sql_params)
        full_path = self.root_url + self.endpoint_path.format(**self.path_params)
        etag_item = conn.query(Etag).filter_by(path=full_path).one_or_none()
        etag = None if not etag_item else etag_item.etag
        conn.close()
        return etag
        
    def _load_requests(self, requests):
        self.request_queue = Queue()
        for request in requests: self.request_queue.put(request)
        self.request_size = len(requests)
        self.response_queue = SimpleQueue()
    
    def _run_threads(self, verbose=False):
        if verbose: self.tbar = tqdm(desc='Extractor', total=self.request_queue.qsize(), leave=False)
        self._prime_threads(verbose)
        self._start_threads()
        self._process_threads()
        if verbose:
            self.tbar.set_postfix()
            self.tbar.close()
            
    def _prime_threads(self, verbose=False):
        self.threads = {
            thread_id:self._create_thread(self._thread_process, thread_id, verbose)
            for thread_id in range(self.target_threads)
        }
        if verbose: self.tbar.set_postfix(threads=len(self.threads))
            
    @property
    def target_threads(self):
        #target_threads = clip(int(sqrt(self.request_queue.qsize())), 1, self.max_threads)
        target_threads = self.max_threads
        return target_threads
        
    @staticmethod
    def _create_thread(target, *args, daemon=True, **kwargs):
        thread = {
            'thread': Process(target=target, args=args, kwargs=kwargs),
            'status': 'created',
        }
        thread['thread'].daemon = daemon
        return thread
            
    def _start_threads(self):
        for thread in self.threads.values():
            thread['thread'].start()
            
    def _process_threads(self):
        self.request_queue.join()
        
        while not self.response_queue.empty():
            self.responses.append(self.response_queue.get())
            
        for _ in range(len(self.threads)): self.request_queue.put('terminate')
        for thread in self.threads.values(): thread['thread'].join()
    
    def _thread_process(self, thread_id, verbose=False):
        while True:
            try:
                request = self.request_queue.get_nowait()
            except Empty:
                sleep(self.thread_sleep)
                continue
                
            if request == 'terminate': break
            
            resp, followups = request.call()
            if resp:
                self.request_size += len(followups)
                if verbose: self.tbar.total = self.request_size
                for followup in followups: self.request_queue.put(followup)
                self.response_queue.put(resp)
            
            if self.verbose: self.tbar.update()
                
            self.request_queue.task_done()
