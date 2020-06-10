import configparser
import psycopg2
from logging import getLogger
from sql_queries import create_table_queries, drop_table_queries

log = getLogger(__name__)

def drop_tables(cur, conn):
    """Dropping tables if they exists in the Redshift database
    
    Keyword arguments:
    cur  -- the curser of the database
    conn -- the connection to the database
    """
    
    log.info("Dropping tables...")
    print("Dropping tables...\n")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """Creating the facts and dimensional tables
    
    Keyword arguments:
    cur  -- the curser of the database
    conn -- the connection to the database
    """
    
    log.info("Creating tables...")
    print("Creating tables...\n")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    log.info("Connection established")
    print("Connection established\n")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()