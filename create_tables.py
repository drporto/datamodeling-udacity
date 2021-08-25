import psycopg2
from sql_queries import drop_table_queries, create_table_queries
from sql_connection import *

def create_database():
    """
    Description: This function is responsible for creating and connecting to the
    sparkifydb.

    Arguments:
        None
        
    Returns:
        cur: the cursor object attached to the sparkifydb database
        conn: connection to the database sparkifydb.
    """   
    # connect to default database
    conn = psycopg2.connect(host = dbhost, dbname = dbnameDefault, user = userDefault, password = passwordDefault)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute('DROP DATABASE IF EXISTS {}'.format(dbnameProject))
    cur.execute('CREATE DATABASE {} WITH ENCODING \'utf8\' TEMPLATE template0'.format(dbnameProject))

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(host = dbhost, dbname = dbnameProject, user = userProject, password = passwordProject)
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Description: This function is responsible for droping each
    table using the queries in 'drop_table_queries' list inside
    of 'sql_queries.py'

    Arguments:
        None
        
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function is responsible for creating each 
    table using the queries in 'create_table_quereis' list inside
    of 'sql_queries.py'

    Arguments:
        None
        
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Description: This function is responsible for wrapping the
    other function related to:
    - Connection with the database
    - Drop and creation of the necessary database
    - Drop and creation of the necessaires tables
    - Terminating the connection
    This is achieved using the the queries in 'sql_queries.py'
    
    Arguments:
        None
    
    Returns:
        None
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

    
if __name__ == "__main__":
    main()