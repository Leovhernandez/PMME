import os # provides functions to interact with the operating system
import psycopg2 # database adapter for python to communicate with sql db
import configparser #parses config files
from psycopg2 import pool # pool is used to manage a pool of db connections
import pandas as pd # data analysis module  
import matplotlib.pyplot as plt # matplotlib is a plotting library
import seaborn as sns #data visualization library

# Create a dictionary for saving plots if it doesn't exist
output_dir = 'plots'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# parsing manufacturing data from csv file
# must use absolute file path if file being parsed is not in same project directory as python file
# data = pd.read_csv('/mnt/c/Users/ninih/Documents/Programming/PMME/PM-Data.csv')
# can just use file name in this case because PM-Data.csv is in the same project directory as database_connection.py
data = pd.read_csv('PM-Data.csv')

# summarizing of data...describe() generates descriptive statistics that summarize the central tendency(mean, median, mode),
# dispersion(e.g. range, variance, and standard deviation), and shape of a dataset's distribution, excluding NaN values
summary_stats = data.describe()
print(summary_stats)

# Check for missing values and print out the total number of missing values for each column
missing_values = data.isnull().sum()
print(missing_values)

# Histograms for numerical features
# list of specified columns in the dataset
numerical_features = ['air_temperature_k', 'process_temperature_k', 'rotational_speed_rpm', 'torque_nm', 'tool_wear_min']
# selects the columns specified in the numerical_features list in the dataframe 'data'
# then creates a histogram of each of numerical_features columns specified with 15 bins each and figure size of 15x10 
data[numerical_features].hist(bins=15, figsize=(15, 10))
# displays histograms generated in previous line
plt.savefig(os.path.join(output_dir, 'histograms.png'))
plt.close()

# Scatter plots to examine relationships
# creates a martix of scatterplots for each pair of numerical_features
sns.pairplot(data, vars=numerical_features, hue='machine_failure')
plt.savefig(os.path.join(output_dir, 'scatter_plot.png'))
plt.close()

# ensure only numeric columns are used for correlation
numeric_cols = data.select_dtypes(include=['number']).columns

# Heatmap for correlation
# creates a new figure with size 10inx8in
plt.figure(figsize=(10, 8))
# computes the correlation matrix for the dataframe 'data'...
# correlation matrix shows the correlation coefficients between the pairs of numerical 
# features which indicate how strongly they are related to each other
correlation_matrix = data[numeric_cols].corr() 
# creates the heatmap of the correlation matrix
# annot=True adds correlation coefficients as annotations on the heatmap
# cmap='coolwarm' sets the colormap to a color gradient of cool to warm colors
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.savefig(os.path.join(output_dir, 'heatmap.png'))
plt.close()

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
# only need to call the below function when updating the psql db          
# load_data_to_db(data)