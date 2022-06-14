import logging.config
import configparser
import psycopg2
import logging.config
from sql_queries import *

# Setting up logger
logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


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


# Run all function
def main():
    """
    - Establish connection to redshift, 
    - calls the load staging tables function,
    - calls the insert tables function,
    - calls the count staging function,
    - calls the dim query count function,
    - closes the connection. 
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

    # Load data
    logger.info(f"Loading data into staging tables")
    load_staging_tables(cur, conn)
    logger.info(f"Loaded data into staging tables ")
    logger.info(msg="-"*50)

    # Count copy to staging tables
    count_staging(cur, conn)

    # Insert data
    logger.info(f"Inserting data into tables")
    insert_tables(cur, conn)
    logger.info(f"Inserted data into tables ")
    logger.info(msg="-"*50)

    # Count data insert
    dim_query_count(cur, conn)

    # Close connection
    logger.info(f"Closing connection")
    conn.close()
    logger.info(f"Connection closed")


if __name__ == '__main__':
    main()
    logger.info(f"Script completed")
