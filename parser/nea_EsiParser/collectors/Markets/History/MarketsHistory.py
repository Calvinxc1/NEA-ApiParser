from multiprocessing.dummy import Pool
from tqdm.contrib.concurrent import thread_map

from nea_schema.maria.esi.mkt import MarketHistory

from .ExtractorMarketsHistory import ExtractorMarketsHistory
from .TransformerMarketsHistory import TransformerMarketsHistory
from .Regions import MarketsHistoryRegions 
from ...Base import Base

class MarketsHistory(Base):
    endpoint_path = '/markets/{region_id}/types'
    schema = MarketHistory
    Extractor = ExtractorMarketsHistory
    Transformer = TransformerMarketsHistory
    Loader = None
    
    def run_subprocesses(self):
        if self.verbose:
            self.subprocesses = thread_map(
                self._subprocess_thread,
                self.record_items.items(),
                max_workers=self.max_subprocess_threads,
            )
        else:
            with Pool(self.max_subprocess_threads) as P:
                self.subprocesses = P.imap_unordered(
                    self._subprocess_thread,
                    self.record_items.items(),
                )
        
    def _subprocess_thread(self, items):
        subprocess = MarketsHistoryRegions(*self.init_params, self)
        subprocess.pull_and_load(*items)
        return subprocess
