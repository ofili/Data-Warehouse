''' import logging.config
from create_tables import *
from etl import *

# Setting up logger
logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

# Run all function
def main():
    """
    - Establish connection to redshift, 
    - calls the drop tables function,
    - calls the create tables function,
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
 '''