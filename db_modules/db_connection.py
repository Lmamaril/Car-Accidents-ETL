import psycopg2

"""
    This module is the accesspoint for db connection 
    throughout the project.
"""

hostname = "127.0.0.1"
username = "user"
pw = "password"

def connect_database():
    """ connect to the database
    args:
        :: None ::
    returns:
        :: conn :: psycopg2 connect object
        :: cur :: psycopg2 cursor
    """
    conn =  psycopg2.connect(
        "host={} dbname=postgres user={} password={}"
        .format(hostname, username, pw))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    return conn, cur

