from datetime import timedelta as td
from http.client import RemoteDisconnected
from logging import getLogger
import requests as rq
from requests.exceptions import ConnectionError
from time import sleep

from ...tools import LoggingBase, LimitedSession

class Requester(LoggingBase):
    exception_repeats = (RemoteDisconnected, ConnectionError)
    repeat_delay = 1
    
    def __init__(self, path, method='GET', data=None, path_params={}, query_params={}, headers={}, etag=None, Session=None, parent=None):
        self._init_logging(parent)
        self.Session = Session if Session else LimitedSession(parent=self)
        self.method = method
        self.path = path
        self.data = data
        self.path_params = path_params
        self.query_params = query_params
        self.headers = headers
        if etag: self.headers = {**self.headers, 'If-None-Match': etag}
        
        self.followup = self.query_params.get('page', 1) == 1
        
    def call(self):
        path = self.path.format(**self.path_params)
        resp = None
        while resp is None:
            sleep(self.repeat_delay)
            self.logger.debug('Making %s request to %s', self.method, path)
            try:
                resp = self.Session.request(self.method, path, self.query_params, headers=self.headers, json=self.data)  
                resp, followups = self.handle_response(resp)
            except self.exception_repeats as E:
                self.logger.info('Encountered repeatable exception, continuing request\n%s', repr(E))
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
        
        elif resp.status_code == 304:
            self.logger.debug('Response 304, no new data available.')
            return False, False
        
        elif resp.status_code == 404:
            self.logger.info('Response 404, no data returned.\nResponse: %s\n%s %s\nPath: %s\nQuery: %s',
                resp.text, self.method, self.path, self.path_params, self.query_params
            )
            return False, False
        
        elif resp.status_code == 420:
            error_limit_time = float(resp.headers.get('X-ESI-Error-Limit-Remain'))
            self.logger.info(
                'Response 420, waiting %s for error limit to resolve',
                td(seconds=error_limit_time),
            )
            sleep(error_limit_time)
            return None, None
        
        elif 500 <= resp.status_code < 600:
            self.logger.debug('Server-side error encountered - %s. Repeating request.', resp.status_code)
            return None, None
        
        else:
            self.logger.error('Unknown response %s, no data returned.\nResponse: %s\nResponse Headers: %s\n%s %s\nPath: %s\nQuery: %s',
                resp.status_code, resp.text, resp.headers, self.method, self.path, self.path_params, self.query_params
            )
            return False, False
            
    def build_followup(self, page):
        followup = Requester(
            self.path, self.method, self.data, self.path_params,
            {**self.query_params, 'page': page}, self.headers,
            None, self.Session, self,
        )
        return followup
    
    
