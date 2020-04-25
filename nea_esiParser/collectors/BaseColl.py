from datetime import datetime as dt
from multiprocessing.dummy import Pool
import requests as rq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from time import sleep
from tqdm.notebook import tqdm

class BaseColl:
    root_url = 'https://esi.evetech.net/latest'
    endpoint_path = ''
    defaults = {'query_params': {'datasource': 'tranquility'}}
    schema = None
    max_requests = 10
    pool_workers = 12
    
    def __init__(self, sql_params, verbose=False):
        self.full_path = '{root}/{path}'.format(root=self.root_url, path=self.endpoint_path)
        self.path_params = {**self.defaults.get('path_params', {})}
        self.query_params = {**self.defaults.get('query_params', {})}
        self.session = rq.Session()
        self.sql_params = sql_params
        self.verbose = verbose
        
        self._load_engine()
        
    def pull_and_load(self):
        responses, cache_expire = self.build_responses()
        rows = self.alchemy_responses(responses)
        self.merge_rows(rows)
        return cache_expire
    
    def _load_engine(self):
        self.engine = create_engine('{engine}://{user}:{passwd}@{host}/{db}'.format(**self.sql_params))
    
    def _build_session(self, engine):
        while True:
            try:
                Session = sessionmaker(bind=engine)
                conn = Session()
                conn.execute('SET SESSION foreign_key_checks=0;')
                return Session, conn
            except Exception as e:
                self._load_engine()
    
    def _process(self, func, kwargs):
        proc_inputs = self._build_proc_inputs(func, kwargs)
        
        with Pool(self.pool_workers) as P:
            output = P.imap_unordered(self._proc_mapper, proc_inputs)
            if self.verbose: output = tqdm(output, total=len(proc_inputs), leave=False)
            output = list(output)
        
        return output
    
    @staticmethod
    def _build_proc_inputs(func, kwargs):
        proc_inputs = [
            {
                'func': func,
                'kwargs': kwarg_set,
            } for kwarg_set in kwargs
        ]
        return proc_inputs
    
    @staticmethod
    def _proc_mapper(inputs):
        return inputs['func'](**inputs['kwargs'])
    
    def build_responses(self):
        responses, cache_expire = self._get_responses(self.full_path)
        responses = [response for response in responses if response is not None]
        return responses, cache_expire
        
    def _get_responses(self, path, path_params={}, query_params={}):
        responses = [self._request(path, path_params, query_params)]
        if responses[0] is None:
            return [], None
        elif responses[0].status_code != 200:
            return [], None
            
        cache_expire = dt.strptime(responses[0].headers.get('expires'), '%a, %d %b %Y %H:%M:%S %Z')
        
        page_count = int(responses[0].headers.get('X-Pages', 1))
        if page_count > 1:
            params = [
                {
                    'path': path,
                    'path_params': {**path_params},
                    'query_params': {**query_params, 'page': page}
                }
                for page in range(2, page_count+1)
            ]
            
            response_items = self._process(self._request, params)
            responses.extend(response_items)
            
        return responses, cache_expire
    
    def _request(self, path, path_params={}, query_params={}):
        sleep(0.1)
        i = 0
        try:
            while i < self.max_requests:
                response = self.session.get(
                    path.format(**{**self.path_params, **path_params}),
                    params={**self.query_params, **query_params},
                )
                if response.status_code == 200: break
                i += 1
                sleep(0.1)
        except Exception as e:
            response = None
            
        return response
    
    def alchemy_responses(self, responses):
        alchemy_data = [
            row
            for response in responses
            for row in self.schema.esi_parse(response)
        ]            
        return alchemy_data
    
    def merge_rows(self, rows):
        Session, conn = self._build_session(self.engine)
        for row in rows:
            conn.merge(row)
        conn.commit()
        conn.close()