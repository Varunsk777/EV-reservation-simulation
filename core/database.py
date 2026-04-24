import psycopg2
from config.settings import POSTGRES_CONFIG

def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)  # type: ignore