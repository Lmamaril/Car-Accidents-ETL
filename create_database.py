import psycopg2
from sql_queries import drop_table_queries, create_table_queries

"""
ETL 

A script to create tables and load data into the database.
"""

def connect_database():
    """ connect to the database
    args:
        :: None ::
    returns:
        :: conn :: psycopg2 connect object
        :: cur :: psycopg2 cursor
    """
    conn =  psycopg2.connect("host=127.0.0.1 dbname=postgres user=user password=password")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    return conn, cur

def create_database(cur):
    cur.execute("DROP DATABASE IF EXISTS CarAccidentsDB")
    cur.execute("""
        CREATE DATABASE CarAccidentsDB 
        WITH ENCODING 'utf8' TEMPLATE template0
    """)

def drop_tables(cur):
    """ drop tables from the database
    args:
        :: cur :: psycopg2 cursor
    returns:
        :: None ::
    """
    for table_query in drop_table_queries:
        cur.execute(table_query)

def create_tables(cur): 
    """ create tables for the database
    args:
        :: cur :: psycopg2 cursor
    returns:
        :: None ::
    """
    for table_query in create_table_queries:
        cur.execute(table_query)

def main():
    """ main entry point for dropping and creating tables """
    
    # create the postgres database
    conn, cur = connect_database()
    create_database(cur)
    conn.close()
    
    # drop and create tables
    conn, cur = connect_database()    
    drop_tables(cur)
    create_tables(cur)
    conn.close()
    
if __name__ == "__main__":
    main()
    
    
    
    
    