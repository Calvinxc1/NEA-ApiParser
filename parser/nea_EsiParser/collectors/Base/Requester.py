from http.client import RemoteDisconnected
from logging import getLogger
import requests as rq
from requests.exceptions import ConnectionError

from ...tools import LoggingBase, LimitedSession

class Requester(LoggingBase):
    exception_repeats = (RemoteDisconnected, ConnectionError)
    
    def __init__(self, path, method='GET', data=None, path_params={}, query_params={}, headers={}, Session=None, parent=None):
        self._init_logging(parent)
        self.Session = Session if Session else LimitedSession(parent=self)
        self.method = method
        self.path = path
        self.data = data
        self.path_params = path_params
        self.query_params = query_params
        self.headers = headers
        
        self.followup = self.query_params.get('page', 1) == 1
        
    def call(self):
        path = self.path.format(**self.path_params)
        
        resp = None
        while resp is None:
            self.logger.debug('Making %s request to %s', self.method, path)
            try:
                resp = self.Session.request(self.method, path, self.query_params, headers=self.headers, json=self.data)  
                resp, followups = self.handle_response(resp)
            except self.exception_repeats as E:
                self.logger.warn('Encountered repeatable exception, continuing request\n%s', repr(E))
                continue
            
        return resp, followups
    
    def handle_response(self, resp):
        if 200 <= resp.status_code < 300:
            pages = int(resp.headers.get('X-Pages', 1))
            followups = [
                self.build_followup(page)
                for page in range(2, pages+1)
            ] if self.followup else []
            self.logger.debug('Request successful. Endpoint has %s pages. %s followups generated.', pages, len(followups))
            return resp, followups
        
        elif resp.status_code == 404:
            self.logger.warn('Request 404, no data returned.\nResponse: %s\n%s %s\nPath: %s\nQuery: %s',
                resp.text, self.method, self.path, self.path_params, self.query_params
            )
            return False, False
        
        elif 500 <= resp.status_code < 600:
            self.logger.debug('Server-side error encountered - %s. Repeating request.', resp.status_code)
            return None, None
        
        else:
            self.logger.error('Unknown response %s, no data returned.\nResponse: %s\n%s %s\nPath: %s\nQuery: %s',
                resp.status_code, resp.text, self.method, self.path, self.path_params, self.query_params
            )
            return False, False
            
    def build_followup(self, page):
        followup = Requester(
            self.path, self.method, self.data, self.path_params,
            {**self.query_params, 'page': page}, self.headers,
            self.Session, self,
        )
        return followup
    
    
