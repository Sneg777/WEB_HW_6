import logging
from psycopg2 import DatabaseError

from connection import create_connection


def create_table(conn, sql_expression: str):

    c = conn.cursor()
    try:
        c.execute(sql_expression)
        conn.commit()
    except DatabaseError as e:
        logging.error(e)
        conn.rollback()
    finally:
        c.close()


if __name__ == '__main__':

    with open('create_table.sql', 'r', encoding='utf-8') as file:
        sql_create_users_table = file.read()

    try:
        with create_connection() as conn:
                create_table(conn, sql_create_users_table)
    except RuntimeError as err:
        logging.error(err)
