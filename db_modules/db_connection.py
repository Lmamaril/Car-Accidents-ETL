import psycopg2
import config
"""
    This module is the accesspoint for db connection 
    throughout the project
"""

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
        .format(config.hostname, config.username, config.pw))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    return conn, cur

def test_access():
    return "access granted"

