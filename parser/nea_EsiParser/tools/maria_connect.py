from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def maria_connect(sql_params):
    engine = create_engine('{engine}://{user}:{passwd}@{host}/{db}'.format(**sql_params))
    session = sessionmaker(bind=engine)
    conn = session()
    conn.execute('SET SESSION foreign_key_checks=0;')
    return conn
