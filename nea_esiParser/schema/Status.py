from datetime import datetime as dt
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import DATETIME as DateTime, INTEGER as Integer, TINYTEXT as TinyText, BOOLEAN as Boolean

from . import _Base

class Status(_Base):
    __tablename__ = 'srv_ServerStatus'
    
    ## Columns
    record_time = Column(DateTime, primary_key=True, autoincrement=False)
    players = Column(Integer)
    server_version = Column(TinyText)
    start_time = Column(DateTime)
    vip = Column(Boolean, default=False)

    @classmethod
    def esi_parse(cls, esi_return):
        data = esi_return.json()
        data = {
            **data,
            'start_time': dt.strptime(data['start_time'], '%Y-%m-%dT%H:%M:%SZ'),
            'record_time': dt.strptime(esi_return.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z'),
        }
        
        class_obj = cls(**data)
        return class_obj