from datetime import datetime as dt, timedelta as td
from logging import LoggerAdapter, getLogger

from .Extractor import Extractor
from .Transformer import Transformer
from .Loader import Loader
from ...tools import LoggingBase, LimitedSession

class Base(LoggingBase):
    ## Optionally definable in the child class, but will inherit the parent if not.
    Extractor = Extractor
    Transformer = Transformer
    Loader = Loader
    purge = False
    refresh_time_shift = td(seconds=0)
    subprocesses = []
    
    ## *Must* be defined in the child class for it to function properly
    endpoint_path = ''
    schema = None
    
    def __init__(self, sql_params, Session=None, esi_auth=None, verbose=False, parent=None):
        self._init_logging(parent)
        self.verbose = verbose
        
        self.Extractor = self.Extractor(self.endpoint_path, Session, sql_params, esi_auth, verbose, parent=self)
        self.Transformer = self.Transformer(self.schema, sql_params, verbose, parent=self)
        self.Loader = self.Loader(sql_params, self.schema, self.purge, verbose, parent=self)
        self.params = (sql_params, Session, esi_auth, verbose)
        
    def pull_and_load(self):
        self.logger.info('Began ETL process')
        start = dt.now()
        self.responses, cache_expire = self.extract()
        self.record_items = self.Transformer.transform(self.responses)
        self.Loader.load(self.record_items)
        self.subprocess()
        time = dt.now() - start
        self.logger.info('ETL complete, elapsed time %s', time)
        cache_expire += self.refresh_time_shift
        return cache_expire
    
    def extract(self):
        responses = self.Extractor.extract()
        cache_expire = max([
            dt.strptime(response.headers.get('expires'), '%a, %d %b %Y %H:%M:%S %Z')
            for response in responses
        ])
        return responses, cache_expire
        
    def subprocess(self):
        for i, subprocess in enumerate(self.subprocesses):
            self.logger.info('Starting subprocess %s', subprocess.__name__)
            subprocess = subprocess(*self.params, parent=self)
            subprocess.pull_and_load()
            self.subprocesses[i] = subprocess
