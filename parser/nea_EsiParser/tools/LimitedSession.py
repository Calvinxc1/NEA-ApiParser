from datetime import datetime as dt
import logging
import requests as rq
from time import sleep

from .LoggingBase import LoggingBase

class LimitedSession(LoggingBase):
    def __init__(self, rate_limit=10, parent=None):
        self._init_logging(parent)
        self.rate_limit = rate_limit
        self.session = rq.Session()
        self.last_usage = dt.now()
        
    def request(self, *args, **kwargs):
        time_since_last = (dt.now() - self.last_usage).total_seconds()
        delay = (1/self.rate_limit) - time_since_last
        if delay > 0: sleep(delay)
        self.last_usage = dt.now()
        self.logger.debug('%s request made to %s', *args[0:2])
        resp = self.session.request(*args, **kwargs)
        return resp
