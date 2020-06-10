import configparser
import psycopg2
from logging import getLogger
from sql_queries import copy_table_queries, insert_table_queries

log = getLogger(__name__)

def load_staging_tables(cur, conn):
    """Loading the data from S3 buckets into the staging tables of Redshift

    Keyword arguments:
    cur  -- the curser of the database
    conn -- the connection to the database
    """
    
    log.info("Loading staging tables...")
    print("Loading staging tables...\n")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """inserting the data into the facts and dimensional tables

    Keyword arguments:
    cur  -- the curser of the database
    conn -- the connection to the database
    """
    
    log.info("inserting into dimensional and facts tables...")
    print("inserting into dimensional and facts tables...\n")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    log.info("Connection established")
    print("Connection established\n")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()