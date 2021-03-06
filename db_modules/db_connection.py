import psycopg2

"""
    This module is the accesspoint for db connection 
    throughout the project.
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

