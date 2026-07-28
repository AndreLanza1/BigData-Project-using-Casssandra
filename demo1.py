from cassandra.cluster import Cluster
from datetime import datetime
import time


RESET = '\033[0m'
BOLD = '\033[1m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RED = '\033[91m'
MAGENTA = '\033[95m'

print(f"{MAGENTA}{BOLD}Connecting to cluster...{RESET}")
node_names = ['cassandra-node1', 'cassandra-node2', 'cassandra-node3']
cluster = Cluster(node_names, port=9042)
session = cluster.connect()
session.default_timeout = 60.0
print(f"{GREEN}Connected{RESET}\n")



# Create Keyspace 
print(f"{YELLOW}{BOLD}--- SETUP: Create keyspace ---{RESET}")
print("Goal: create the database environment, which Cassandra calls a Keyspace with replication strategy SimpleStrategy and replication factor 2. ")
cql_keyspace = """CREATE KEYSPACE IF NOT EXISTS iot_data 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"""

print(f"\n{CYAN}CQL:\n{cql_keyspace}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

session.execute("DROP KEYSPACE IF EXISTS iot_data;")

session.execute(cql_keyspace)
session.set_keyspace('iot_data')

print(f"{GREEN}-> Keyspace 'iot_data' created.{RESET}\n")

# Create Initial Table
print(f"{YELLOW}{BOLD}--- SETUP: Create sensors table ---{RESET}")
print("Goal: Create a table where the coumpound primary key is (sensor_id, timestamp)")
print("So the partition key is 'sensor_id' and the clustering column is 'timestamp' ( chronologically sorts the data inside that specific aggregate).")
cql_table = """CREATE TABLE sensors (
    sensor_id int,
    timestamp timestamp,
    temperature float,
    PRIMARY KEY (sensor_id, timestamp)
);"""

print(f"\n{CYAN}CQL:\n{cql_table}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

session.execute(cql_table)
print(f"{GREEN}-> Table 'sensors' created.{RESET}\n")

# Insert Initial Data
print(f"{YELLOW}{BOLD}--- SETUP: Insert sensor readings ---{RESET}")
print("Goal: Populate the table.")

print(f"\n{CYAN}CQL:\nINSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (1, <current_time>, 22.5);{RESET}")
print(f"{CYAN}CQL:\nINSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (2, <current_time>, 19.5);{RESET}")

input(f"\n{BOLD}[Press Enter to insert data]{RESET}")

# Pre-populating some data
data = [
    (1, datetime.now(), 22.5),
    (2, datetime.now(), 19.5)
]
for s_id, tstamp, temp in data:
    session.execute("INSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (%s, %s, %s)", (s_id, tstamp, temp))

print(f"{GREEN}-> Setup complete.{RESET}\n")

print(f"{MAGENTA}{BOLD}--- STARTING MAIN DEMO ---{RESET}\n")
print(f"{MAGENTA}{BOLD} Explore Cassandra's Query-Driven Modeling {RESET}\n")

# STEP 1: Valid Query 
print(f"{YELLOW}{BOLD}--- STEP 1: Query by partition key (sensor_id) ---{RESET}")
print("Goal: Retrieve readings for Sensor #2.")
print("Expected: Success. It works, because the sensor_id is our Partition Key.")
cql_query1 = "SELECT * FROM sensors WHERE sensor_id = 2;"
print(f"\n{CYAN}CQL: {cql_query1}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

rows = session.execute(cql_query1)
for row in rows:
    print(f"{YELLOW} -> [Partition: {row.sensor_id}]{RESET} {GREEN}-> (Time: {row.timestamp} | Temp: {row.temperature}°C){RESET}")


# STEP 2: The Error 
print(f"\n{YELLOW}{BOLD}--- STEP 2: Query by Temperature ---{RESET}")
print("Goal: Try to find all sensors that recorded temperature: 19.5 degrees")
print("Expected: Cassandra intentionally blocks this query returning an error.")
cql_query2 = "SELECT * FROM sensors WHERE temperature = 19.5;"
print(f"\n{CYAN}CQL: {cql_query2}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

try:
    session.execute(cql_query2)
except Exception as e:
    print(f"{RED}{BOLD}-> ERROR!{RESET}")
    print(f"{RED}-> Details: {e}{RESET}\n")


# STEP 3: Query-Driven Modeling 
print(f"{YELLOW}{BOLD}--- STEP 3: Solving with a new table ---{RESET}")
print("Goal: Follow the Query-Driven Modeling so we create a table where 'temperature' is the Partition Key.")
cql_new_table = """CREATE TABLE sensors_by_temp (
    temperature float,
    sensor_id int,
    timestamp timestamp,
    PRIMARY KEY (temperature, sensor_id, timestamp)
);"""
print(f"\n{CYAN}CQL:\n{cql_new_table}{RESET}")
session.execute(cql_new_table)

print(f"{MAGENTA}\nMigrating data from 'sensors' to 'sensors_by_temp'...{RESET}\n")

legacy_rows = session.execute("SELECT * FROM sensors;")
for row in legacy_rows:
    session.execute(
        "INSERT INTO sensors_by_temp (temperature, sensor_id, timestamp) VALUES (%s, %s, %s)",
        (row.temperature, row.sensor_id, row.timestamp)
    )
    time.sleep(0.1)

print(f"{BOLD}sensors_by_temp:{RESET}")
print("-" * 55)

migrated_data = session.execute("SELECT * FROM sensors_by_temp;")
for row in migrated_data:
    # Evidenziamo che la temperatura è la partizione, e il resto sono le colonne di clustering
    print(f"{YELLOW}[Partition: {row.temperature}°C]{RESET} {GREEN}-> (Sensor ID: {row.sensor_id}, Time: {str(row.timestamp)}){RESET}")

print(f"{GREEN}-> New table 'sensors_by_temp' created and initial data inserted.{RESET}\n")

# STEP 4: Final Result
print(f"{YELLOW}{BOLD}--- STEP 4: Re-running the temperature query ---{RESET}")
cql_query3 = "SELECT * FROM sensors_by_temp WHERE temperature = 19.5;"
print(f"\n{CYAN}CQL: {cql_query3}{RESET}")
input(f"\n{BOLD}[Press Enter to execute the query]{RESET}")

rows = session.execute(cql_query3)
for row in rows:
    print(f"{GREEN}-> Temp: {row.temperature}°C | Sensor: {row.sensor_id} | Time: {row.timestamp}{RESET}\n")

print(f"{MAGENTA}{BOLD}\n --- DEMO COMPLETED ---{RESET}")
