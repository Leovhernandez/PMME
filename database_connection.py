import os
import psycopg2
import configparser
from psycopg2 import pool

# Load connection parameters prioritizing environment variables first then 'config.ini' being the fallback
# ***Ensure that your 'config.ini' file is added to .gitignore so it is NOT tracked by GIT
config = configparser.ConfigParser()
config.read('config.ini')
params = {
    'dbname': os.getenv('DB_NAME', config.get('database', 'dbname')),
    'user': os.getenv('DB_USER', config.get('database', 'user')),
    'password': os.getenv('DB_PASSWORD', config.get('database', 'password')),
    'host': os.getenv('DB_HOST', config.get('database', 'host')),
    'port': os.getenv('DB_PORT', config.get('database', 'port')) # Default PostgreSQL port
}

# Setting up connection pool which is more efficient for repeated database interactions instead of direct connection
# SimpleConnectionPool(1, 10, **params) opens the connection pool with a min of 1 connection and max of 10 connections
db_conn_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **params)

try:
    # Try to get a connection from the pool
    conn = db_conn_pool.getconn()
    
    # if conn is true (successful) 
    if conn:
        print("Connection to the database successful")
    
        # Create a cursor object
        cursor = conn.cursor()
    
        # Execute a query
        cursor.execute("SELECT version();")
    
        # Fetch result
        record = cursor.fetchone()
        print(f"You are connected to - {record[0]}")
        
        # Close the cursor and the connection
        cursor.close()
        conn.close() # Actually returns the connection to the pool
        db_conn_pool.putconn(conn) # Explicitly return the connection to the pool

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # Close the connection pool
    if db_conn_pool:
        db_conn_pool.closeall()