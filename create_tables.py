import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries




def drop_tables(cur, conn):
    """
    DROP TABLES
    Arguments:
        cur: cursor object
        conn: connection object to redshift

    Return: 
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    CREATE TABLES
    Arguments:
        cur: the cursor object
        conn: connection object to redshift

    Return: 
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establish connection to redshift and create tables, drop tables, and close connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()