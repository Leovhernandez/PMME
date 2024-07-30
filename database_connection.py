import os
import psycopg2
import configparser
from psycopg2 import pool
import pandas as pd

# parsing manufacturing data from csv file
# must use absolute file path if file being parsed is not in same project directory as python file
# data = pd.read_csv('/mnt/c/Users/ninih/Documents/Programming/PMME/PM-Data.csv')
# can just use file name in this case because PM-Data.csv is in the same project directory as database_connection.py
data = pd.read_csv('PM-Data.csv')

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

def load_data_to_db(data):
    try:
        # Try to get a connection from the pool
        conn = db_conn_pool.getconn()
        
        # if conn is true (successful) 
        if conn:
            print("Connection to the database successful")
        
            # Create a cursor object
            cursor = conn.cursor()
        
            # This query will not execute because manufacturing_data relation was 
            # created before this code was written
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS manufacturing_data(
                udi INT,
                product_id VARCHAR,
                type VARCHAR,
                air_temperature_k NUMERIC(4,1),
                process_temperature_k NUMERIC(4,1),
                rotational_speed_rpm INT,
                torque_nm NUMERIC(3,1),
                tool_wear_min INT,
                machine_failure BOOLEAN,
                twf BOOLEAN,
                hdf BOOLEAN,
                pwf BOOLEAN,
                osf BOOLEAN,
                rnf BOOLEAN
            )
            """)
            
            # Convert integer values to boolean since manufacturing_data relation was
            # created in SQL (not in the python file) with the below  variables as integers
            data['machine_failure'] = data['machine_failure'].astype(bool)
            data['twf'] = data['twf'].astype(bool)
            data['hdf'] = data['hdf'].astype(bool)
            data['pwf'] = data['pwf'].astype(bool)
            data['osf'] = data['osf'].astype(bool)
            data['rnf'] = data['rnf'].astype(bool)
            
            for i, row in data.iterrows():
                cursor.execute("""
                    INSERT INTO manufacturing_data (
                        udi, product_id, type, air_temperature_k, process_temperature_k, rotational_speed_rpm, 
                        torque_nm, tool_wear_min, machine_failure, twf, hdf, pwf, osf, rnf)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", tuple(row))
        
            # Fetch result code only uncomment if you want to display first 10 records from the table in terminal output
            # not necessary as I can do SQL command in terminal and query it manually
            # cursor.execute("SELECT * FROM manufacturing_data LIMIT 10")
            # record = cursor.fetchall()
            # for record in records:
            #   print(record)
            
            # committing modifications to the db
            conn.commit()
            
            # Close the cursor and the connection
            cursor.close()
            conn.close()
            # Returns the connection to the pool
            db_conn_pool.putconn(conn)

    except (Exception, psycopg2.DatabaseError) as error:
        # print("Error while connecting to PostgreSQL", error)
        # rollback changes in case of connection error
        conn.rollback()
    finally:
        # Close the connection pool
        if db_conn_pool:
            db_conn_pool.closeall()
            
load_data_to_db(data)