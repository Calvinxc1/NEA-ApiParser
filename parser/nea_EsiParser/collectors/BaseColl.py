from bson.objectid import ObjectId
from datetime import datetime as dt
from multiprocessing.dummy import Pool
import requests as rq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from time import sleep
from tqdm.notebook import tqdm
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError

from nea_schema.mongo import load_datastore, session

class BaseColl:
    root_url = 'https://esi.evetech.net/latest'
    endpoint_path = ''
    defaults = {'query_params': {'datasource': 'tranquility'}}
    schema = None
    max_request_tries = 100
    retry_delay = 5
    pool_workers = 12
    mongo_path = {
        'database': 'EveSsoAuth',
        'collection': 'ActiveAuths',
    }
    delete_before_merge = False
    exception_repeats = (RemoteDisconnected, ConnectionError)
    
    def __init__(self, sql_params, mongo_params, auth_char_id=None, verbose=False):
        self.full_path = '{root}/{path}'.format(root=self.root_url, path=self.endpoint_path)
        self.path_params = {**self.defaults.get('path_params', {})}
        self.query_params = {**self.defaults.get('query_params', {})}
        self.session = rq.Session()
        self.sql_params = sql_params
        self.verbose = verbose
        if auth_char_id:
            self.get_auth_token(auth_char_id, mongo_params)
        else:
            self.headers = {}
        
        self._load_engine()
        self.tbar = None
        
    def get_auth_token(self, auth_char_id, mongo_params):
        load_datastore(mongo_params)
        from nea_schema.mongo.EveSsoAuth import ActiveAuth
        
        auth = ActiveAuth.query.get(_id=auth_char_id)
        self.headers = {
            'Authorization': '{token_type} {access_token}'.format(
                token_type=auth.token_type,
                access_token=auth.access_token
            ),
        }
        
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
                session = sessionmaker(bind=engine)
                conn = session()
                conn.execute('SET SESSION foreign_key_checks=0;')
                return session, conn
            except Exception as e:
                self._load_engine()
    
    def _process(self, func, kwargs):
        proc_inputs = self._build_proc_inputs(func, kwargs)
        
        with Pool(self.pool_workers) as P:
            output = P.imap_unordered(self._proc_mapper, proc_inputs)
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
        
    def _get_responses(self, path, path_params={}, query_params={}, json_body=None, method='GET'):
        responses = [self._request(path, path_params, query_params, json_body, method)]
        if responses[0] is None: return [], None
        elif responses[0].status_code != 200: return [], None
            
        cache_expire = dt.strptime(responses[0].headers.get('expires'), '%a, %d %b %Y %H:%M:%S %Z')
        
        page_count = int(responses[0].headers.get('X-Pages', 1))
        for page in range(2, page_count+1):
            page_query_params = {**query_params, 'page': page}
            response = self._request(path, path_params, page_query_params, json_body, method)
            responses.append(response)
            
        if self.tbar and self.verbose: self.tbar.update()
            
        return responses, cache_expire
    
    def _request(self, path, path_params={}, query_params={}, json_body=None, method='GET'):
        sleep(0.1)
        
        response = None
        for attempt in self.max_request_tries:
            try:
                resp = self.session.request(
                    method,
                    path.format(**{**self.path_params, **path_params}),
                    params={**self.query_params, **query_params},
                    headers=self.headers,
                    json=json_body,
                )
            except self.exception_repeats as e:
                if self.verbose:
                    print('{}, sleeping for {} seconds and trying again ({}/{})'\
                        .format(e, self.retry_delay, i, self.max_request_retries))
                    self._print_request(path_params, query_params, json_body, resp)
                    print()
                sleep(self.retry_delay)
                continue
                
            if 500 <= resp.status_code < 600:
                if self.verbose:
                    print('Server error, sleeping for {} seconds and trying again ({}/{})'\
                        .format(self.retry_delay, i, self.max_request_retries))
                    self._print_request(path_params, query_params, json_body, resp)
                    print()
                sleep(self.retry_delay)
                continue
            elif resp.status_code != 200:
                if self.verbose:
                    print('Request error, canceling request.')
                    self._print_request(path_params, query_params, json_body, resp)
                    print()
                break
            else:
                response = resp
                break
                
        if response is None and self.verbose:
            self._print_request(path_params, query_params, json_body)
            print()
            
        return response
    
    def _print_request(self, path_params, query_params, json_body, resp=None):
        print('path:', self.endpoint_path)
        print('path params:', path_params)
        print('query params:', query_params)
        print('request body:', json_body)
        if resp: print('response code:', resp.status_code)
        if resp: print('response body:', resp.text)
    
    def alchemy_responses(self, responses):
        t = tqdm(responses) if self.verbose else responses
        alchemy_data = [
            row
            for response in t
            for row in self.schema.esi_parse(response)
        ]            
        return alchemy_data
    
    def merge_rows(self, rows):
        if self.delete_before_merge: self._purge_rows()
        
        Session, conn = self._build_session(self.engine)
        t = tqdm(rows) if self.verbose else rows
        for row in t:
            conn.merge(row)
        conn.commit()
        conn.close()
        
    def _purge_rows(self):
        Session, conn = self._build_session(self.engine)
        conn.query(self.schema).delete()
        conn.commit()
        conn.close()
