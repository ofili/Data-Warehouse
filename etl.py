import configparser
import psycopg2
from sql_queries import *


def load_staging_tables(cur, conn):
    """
    Loads staging tables from S3 bucket to  redshift cluster.
    """
    [cur.execute(query) for query in copy_table_queries] # execute all queries in list
    conn.commit() # commit the changes to the table

def insert_tables(cur, conn):
    """
    Inserts data from staging tables to fact and dimension tables.
    """
    [cur.execute(query) for query in insert_table_queries] # execute all queries in list
    conn.commit() # commit the changes to the table



def count_staging(cur, conn):
    """
    Counts the number of rows in staging tables.
    """
    for query in count_staging_queries:
        cur.execute(query)
        conn.commit()
        print(cur.fetchall())


def dim_query_count(cur, conn):
    """
    Counts the number of rows in dimension tables.
    """
    for query in count_fact_dim_queries:
        cur.execute(query)
        conn.commit()
        print(cur.fetchall())


''' def main():
    """
    Establishes connection to redshift cluster and loads staging tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connection parameters
    host = config.get('CLUSTER', 'HOST')
    dbname = config.get('CLUSTER', 'DB_NAME')
    user = config.get('CLUSTER', 'DB_USER')
    password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.get('CLUSTER', 'DB_PORT')

    print('-'*50 + ' establishing connection' + '-'*50)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(f'connected to ' + host)
    print('-'*50 + ' connection established ' + '-'*50)
    cur = conn.cursor()

    # Load staging tables
    print('-'*50 + ' loading staging tables' + '-'*50)
    load_staging_tables(cur, conn)
    print('-'*50 + ' loaded staging tables' + '-'*50)
    count_staging(cur, conn)
    print('-'*50 + ' inserting into tables' + '-'*50)
    insert_tables(cur, conn)
    print('-'*50 + ' insert completed' + '-'*50)
    dim_query_count(cur, conn)

    conn.close()


if __name__ == "__main__":
    main() '''
