from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import psycopg2
import pymysql
from geopy.distance import geodesic
from datetime import datetime, timedelta


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')




# Write the solution here


# PostgreSQL connection details
pg_dbname = 'devices_db'
pg_host = 'localhost'
pg_port = '5432'
pg_user = 'postgres'
pg_password = 'password'

# MySQL connection details
mysql_dbname = 'aggregated_data_db'
mysql_host = 'localhost'
mysql_port = 3306
mysql_user = 'root'
mysql_password = 'password'

# Connect to PostgreSQL
pg_conn = psycopg2.connect(dbname=pg_dbname, host=pg_host, port=pg_port, user=pg_user, password=pg_password)
pg_cursor = pg_conn.cursor()

# Extract data from PostgreSQL
pg_cursor.execute("SELECT device_id, temperature, location, time FROM devices;")
rows = pg_cursor.fetchall()

# Connect to MySQL
mysql_conn = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_dbname)
mysql_cursor = mysql_conn.cursor()

# Create a new table to store the transformed data
mysql_cursor.execute("CREATE TABLE IF NOT EXISTS aggregated_data ("
                     "device_id VARCHAR(36) NOT NULL,"
                     "hour_start TIMESTAMP NOT NULL,"
                     "max_temperature INTEGER NOT NULL,"
                     "data_points INTEGER NOT NULL,"
                     "total_distance FLOAT NOT NULL,"
                     "PRIMARY KEY (device_id, hour_start)"
                     ")")

# Initialize variables to store the transformed data
current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
hour_data = {}
agg_data = []

# Loop through the data and perform the required transformations
for row in rows:
    device_id = row[0]
    temperature = row[1]
    location = row[2]
    time = row[3]
    time_utc = datetime.utcfromtimestamp(time)
    
    # If the current hour has changed, calculate the aggregated data for the previous hour
    if time_utc >= current_hour + timedelta(hours=1):
        for device_hour, device_data in hour_data.items():
            max_temp = max(device_data['temperatures'])
            data_points = len(device_data['temperatures'])
            total_distance = sum(device_data['distances'])
            agg_data.append((device_hour[0], device_hour[1], max_temp, data_points, total_distance))
        hour_data = {}
        current_hour = current_hour + timedelta(hours=1)
    
    # Calculate the distance between the current location and the previous location (if available)
    if device_id in hour_data:
        prev_location = hour_data[device_id]['location']
        distance = geodesic(prev_location, location).kilometers
        hour_data[device_id]['distances'].append(distance)
    else:
        distance = 0.0
    
    # Add the data point to the hour_data dictionary
    device_hour = (device_id, current_hour)
    if device_hour not in hour_data:
        hour_data[device_hour] = {'temperatures': [], 'distances': [], 'location': location}
    hour_data[device_hour]['temperatures'].append(temperature)
    hour_data[device_hour]['location'] = location

# Calculate the aggregated data for the final hour
for device_hour, device_data in hour_data.items():
    max_temp = max(device_data['temperatures'])
    data_points = len(device_data['temperatures'])
    total_distance = sum(device_data['distances'])
    agg_data.append((device_hour[0], device_hour[1], max_temp, data_points, total_distance))

# Store the aggregated data into MySQL
mysql_cursor.executemany("INSERT INTO aggregated_data (device_id, hour_start, max_temperature, data_points, total_distance) VALUES (%s, %s, %s, %s, %s)", agg_data)
mysql_conn.commit()

# Close the database connections
pg_cursor.close()
pg_conn.close()
mysql_cursor.close()
mysql_conn.close()
