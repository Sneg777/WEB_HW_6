from psycopg2 import OperationalError, connect
from contextlib import contextmanager\

@contextmanager
def create_connection():
    try:
        conn = connect(host="localhost", database="HW6", user="postgres", password="567234")
        yield conn
        conn.close()
    except OperationalError as e:
        raise OperationalError(f'Failed to connect database connection {e}')