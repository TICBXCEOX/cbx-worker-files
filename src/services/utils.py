from sqlalchemy import create_engine
from configs import *
from psycopg2 import connect
from psycopg2.pool import SimpleConnectionPool

def get_db_connection():
    connection = connect(
        dbname=PG_DATABASE,
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return connection

def get_pool():
    pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=PG_DATABASE,
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return pool

def get_engine():
    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    return engine