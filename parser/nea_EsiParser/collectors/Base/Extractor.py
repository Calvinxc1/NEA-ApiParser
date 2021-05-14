import logging
from multiprocessing.dummy import Process
from queue import SimpleQueue as Queue, Empty
from time import sleep
from tqdm.auto import tqdm

from nea_schema.mongo.EveSsoAuth import ActiveAuth

from .Requester import Requester
from ...tools import LoggingBase, LimitedSession, mongo_init

class Extractor(LoggingBase):
    ## Can be updated in child classes, but will inherit from the Base class parameter keys
    Requester = Requester
    root_url = 'https://esi.evetech.net/latest'
    method = 'GET'
    headers = {}
    path_params = {}
    query_params = {'datasource': 'tranquility'}
    thread_sleep = 1
    
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
        requests = [self.Requester(
            self.root_url + self.endpoint_path, self.method, None,
            self.path_params, self.query_params, self.headers,
            self.Session, self,
        )]
        self._load_requests(requests)
        
    def _load_requests(self, requests):
        self.request_queue = Queue()
        for request in requests: self.request_queue.put(request)
        self.responses = Queue()
    
    def _run_threads(self, verbose=False):
        if verbose: self.tbar = tqdm(desc='Extractor', total=self.request_queue.qsize(), leave=False)
        self._prime_threads(verbose)
        self._start_threads()
        self._close_threads()
        if verbose:
            self.tbar.set_postfix()
            self.tbar.close()
            
    def _prime_threads(self, verbose=False):
        self.threads = {
            thread_id:self._create_thread(self._thread_process, thread_id, verbose)
            for thread_id in range(int(self.Session.rate_limit))
        }
        
    @staticmethod
    def _create_thread(target, *args, daemon=True, **kwargs):
        thread = {
            'thread': Process(target=target, args=args, kwargs=kwargs),
            'status': 'created'
        }
        thread['thread'].daemon = daemon
        return thread
            
    def _start_threads(self):
        for thread in self.threads.values():
            sleep(1 / self.Session.rate_limit)
            thread['thread'].start()
            
    def _close_threads(self):
        while not self.request_queue.empty():
            sleep(0.1)
            
        for thread in self.threads.values():
            thread['status'] = 'completed'
            thread['thread'].join()
            
        responses = []
        while not self.responses.empty():
            responses.append(self.responses.get())
        self.responses = responses
    
    def _thread_process(self, thread_id, verbose=False):
        self.threads[thread_id]['status'] = 'started'
        
        active = True
        while active:
            sleep(self.thread_sleep)
            
            try:
                request = self.request_queue.get_nowait()
            except Empty as e:
                if self.threads[thread_id]['status'] == 'completed': active = False
                else: self.threads[thread_id]['status'] = 'holding'
                continue
            
            self.threads[thread_id]['status'] = 'processing'
            resp, followups = request.call()
            
            if resp:
                for followup in followups:
                    self.request_queue.put(followup)
                self.responses.put(resp)
            
            if verbose:
                if followups: self.tbar.total += len(followups)
                self.tbar.refresh()
                self.tbar.update()
