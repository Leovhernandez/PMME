import os
import psycopg2
import configparser


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

# Try establishing the connection using the unpacking arguments operator **
# Will return the conn connection object if successful
try:
    conn = psycopg2.connect(**params)
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
    conn.close()

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while connecting to PostgreSQL", error)
    