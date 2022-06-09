import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_staging_order, count_staging_queries, insert_table_order, count_fact_dim_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables from S3 bucket to  redshift cluster.
    """
    for idx, query in enumerate(copy_table_queries):
        cur.execute(query)
        conn.commit()
        row = cur.execute(count_staging_queries[idx]).fetchone()
        print(f"{copy_staging_order[idx]} has {row[0]} rows")


    #for query in copy_table_queries:
    #    cur.execute(query)
    #    conn.commit()
    #    print(f"{query} has been loaded")
        


def insert_tables(cur, conn):
    """
    Inserts data from staging tables to fact and dimension tables.
    """
    for idx, query in enumerate(insert_table_queries):
        cur.execute(query)
        conn.commit()
        row = cur.execute(count_fact_dim_queries[idx]).fetchone()
        print(f"{insert_table_order[idx]} has {row[0]} rows")

    #for query in insert_table_queries:
    #    cur.execute(query)
    #    conn.commit()


def main():
    """
    Establishes connection to redshift cluster and loads staging tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()