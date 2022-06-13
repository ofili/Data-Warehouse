from create_tables import *
from etl import *

# Run all function
def main():
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
    print('establishing connection' + '-'*50)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(f'connected to redshift cluster'.format(host))
    print('connection established ' + '-'*50)

    # Drop tables
    print('dropping tables' + '-'*50)
    drop_tables(cur, conn)
    print('tables dropped ' + '-'*50)

    # Create tables
    print('creating tables' + '-'*50)
    create_tables(cur, conn)
    print('tables created ' + '-'*50)

    # Load data
    print('loading data' + '-'*50)
    load_staging_tables(cur, conn)
    print('data loaded ' + '-'*50)

    # Count copy to staging tables
    count_staging(cur, conn)

    # Insert data
    print('inserting data' + '-'*50)
    insert_tables(cur, conn)
    print('data inserted ' + '-'*50)

    # Count data insert
    dim_query_count(cur, conn)

    # Close connection
    print('closing connection' + '-'*50)
    conn.close()
    print('connection closed ' + '-'*50)

if __name__ == '__main__':
    main()
    print('*'*25 + ' all processes completed ' + '*'*25)