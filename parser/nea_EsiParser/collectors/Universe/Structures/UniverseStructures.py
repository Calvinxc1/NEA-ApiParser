from datetime import datetime as dt
from nea_schema.maria.esi.uni import Structure

from .Detail import UniverseStructuresDetail
from .TransformerUniverseStructures import TransformerUniverseStructures
from ...Base import Base

class UniverseStructures(Base):
    endpoint_path = '/universe/structures'
    schema = Structure
    Transformer = TransformerUniverseStructures
    Loader = None
    
    def run_subprocesses(self):
        if self.record_items:
            self.subprocesses = [
                UniverseStructuresDetail(*self.init_params, self)
            ]
            for subprocess in self.subprocesses: subprocess.pull_and_load(self.record_items)
