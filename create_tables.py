import logging.config
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Setting up logger
logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


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

# Run all function
def main():
    """
    - Establish connection to redshift, 
    - calls the drop tables function,
    - calls the create tables function
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
    logger.info("Establishing connection to redshift cluster")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.debug(f"Response: {conn}")
    logger.info(f"Connected to redshift cluster at {host}")

    # Drop tables
    logger.info(f"Dropping tables")
    drop_tables(cur, conn)
    logger.info(f"Dropped tables ")
    logger.info(msg="-"*50)

    # Create tables
    logger.info(f"Creating tables")
    create_tables(cur, conn)
    logger.info(f"Created tables ")
    logger.info(msg="-"*50)


if __name__ == '__main__':
    main()
