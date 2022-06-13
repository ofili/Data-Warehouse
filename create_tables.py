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
    [cur.execute(query) for query in drop_table_queries] # execute all queries in list
    conn.commit() # commit the changes to the database


def create_tables(cur, conn):
    """
    CREATE TABLES
    Arguments:
        cur: the cursor object
        conn: connection object to redshift

    Return: 
        None
    """
    [cur.execute(query) for query in create_table_queries] # execute all queries in list
    conn.commit() # commit the changes to the database


''' def main():
    """
    Establish connection to redshift and create tables, drop tables, and close connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connection parameters
    host = config.get('CLUSTER', 'HOST')
    dbname = config.get('CLUSTER', 'DB_NAME')
    user = config.get('CLUSTER', 'DB_USER')
    password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.get('CLUSTER', 'DB_PORT')

    # Establish the database connection
    print('-'*50 + ' establishing connection' + '-'*50)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)
    print(f'connected to redshift cluster'.format(host))
    print('-'*50 + ' connection established ' + '-'*50)
    cur = conn.cursor()

    # Drop tables
    print(f'dropping tables'.format(host))
    drop_tables(cur, conn)
    print('-'*50 + ' tables dropped ' + '-'*50)

    # Create tables
    print(f'creating tables'.format(host))
    create_tables(cur, conn)
    print('-'*50 + ' tables created ' + '-'*50)

    conn.close()


if __name__ == "__main__":
    main()
'''