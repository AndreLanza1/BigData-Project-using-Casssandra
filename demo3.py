from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import socket
import time


RESET = '\033[0m'
BOLD = '\033[1m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RED = '\033[91m'
MAGENTA = '\033[95m'
ORANGE = '\033[38;5;202m'


print(f"{MAGENTA}{BOLD}Connecting to cluster...{RESET}")
node_names = ['cassandra-node1', 'cassandra-node2', 'cassandra-node3']
cluster = Cluster(node_names, port=9042)
session = cluster.connect('iot_data')
session.default_timeout = 60.0

# Map Docker IPs to Container Names for readable output
ip_to_name = {}
for name in node_names:
    try:
        ip = socket.gethostbyname(name) # Chiede alla rete interna di Docker qual è l'IP del container con quel nome
        ip_to_name[ip] = name
    except Exception:
        pass

def format_node(host_obj):
    """Safely extracts IP from the Host object and maps it to the container name"""
    if not host_obj:
        return "Unknown-Node"
    raw_address = getattr(host_obj, 'address', str(host_obj))
    clean_ip = raw_address.split(':')[0] 
    name = ip_to_name.get(clean_ip, clean_ip)
    return f"{name} ({clean_ip})"

print(f"{GREEN}Connected{RESET}\n")

print(f"{MAGENTA}{BOLD}We will now show how Cassandra achieves fault tolerance even when a node storing the data fails.")
print(f"{RESET}")


print(f"{YELLOW}{BOLD}--- STEP 1: Write (Replication Factor = 2) ---{RESET}")
print("Goal: Insert sensor data and observe which nodes physically store the replicas.")
cql_insert = "INSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (?, toTimestamp(now()), ?);"

print(f"\n{CYAN}CQL: {cql_insert}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

insert_query = session.prepare(cql_insert)
sensor_data = [(1, 22.5), (2, 19.8), (3, 25.1), (4, 18.0), (5, 21.3), (5, 23.7)]
node_to_kill = ""

for s_id, temp in sensor_data:
    bound = insert_query.bind((s_id, temp))
    session.execute(bound)
    
    # Usando il consistent hashing di Cassandra per identificare in quali nodi i dati di sensor 99 sarebbero scritti
    replicas = cluster.metadata.get_replicas('iot_data', bound.routing_key)
    replica_strings = [format_node(r) for r in replicas]
    
    # Save a specific node name to stop during Phase 3
    if s_id == 3:
        clean_ip = getattr(replicas[0], 'address', str(replicas[0])).split(':')[0]
        node_to_kill = ip_to_name.get(clean_ip, clean_ip)
        target_replica_strings = replica_strings 
    
    print(f"{GREEN}-> Inserted Sensor {s_id}: {temp}°C | Saved on: {replica_strings}{RESET}")
    time.sleep(0.5)

print(f"\n With a replication factor of 2, each piece of data is stored on two separate nodes. ")

print(f"\n{YELLOW}{BOLD}--- STEP 2: Read ---{RESET}")
print("Goal: With all nodes up and running, we query the latest temperature from Sensor 3.")

target_sensor = 3
cql_select = f"SELECT sensor_id, temperature FROM sensors WHERE sensor_id = {target_sensor} ORDER BY timestamp DESC LIMIT 1"

print(f"\n{CYAN}CQL: {cql_select}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

read_query = session.prepare("SELECT sensor_id, temperature FROM sensors WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 1")
read_query.consistency_level = ConsistencyLevel.ONE 

future = session.execute_async(read_query, [target_sensor])
row = future.result().one()

if row:
    print(f"{GREEN} -> Fetching data... Sensor ID: {row.sensor_id}, Temperature: {row.temperature}°C{RESET}")


print(f"\n{YELLOW}{BOLD}--- STEP 3: Simulate Failure ---{RESET}")
print("Goal: Simulate a hardware failure (stop one of the replica nodes) to test fault tolerance.")
print(f"Expected: The read should still succeed by fetching data from the surviving replica.")
print(f"The data for sensor {target_sensor} is currently stored on {target_replica_strings[0]} and {target_replica_strings[1]}.")
print(f"{BOLD}\n -> ACTION REQUIRED:{RESET} Open a new terminal and run: {ORANGE}docker stop {node_to_kill}{RESET}")


print(f"\n{YELLOW}{BOLD}--- STEP 4: Read data while a replica is down (Availability) ---{RESET}")
print("Goal: Set consistency level to ONE (just need one surviving replica to answer) and perform the read. We are prioritizing availability here.")
print(f"\n{CYAN}CQL: CONSISTENCY ONE")
print(f"CQL: {cql_select}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

time.sleep(1)

try:
    # Uses the read_query prepared in Phase 2 which has CL = ONE
    future_down = session.execute_async(read_query, [target_sensor])
    row_down = future_down.result().one()

    if row_down:
        surviving_replica = [node for node in target_replica_strings if node_to_kill not in node][0]

        print(f"{GREEN} -> Fetching data... Sensor ID: {row_down.sensor_id}, Temperature: {row_down.temperature}°C{RESET}")
        print(f"{GREEN} -> Fault tolerance achieved. Data retrieved from surviving replica: {surviving_replica}{RESET}")
        
except Exception as e:
    print(f"{RED}{BOLD} -> ERROR!{RESET}")
    print(f"{RED} -> Details: {e}{RESET}\n")



print(f"\n{YELLOW}{BOLD}--- STEP 5: Re-run query forcing Consistency ---{RESET}")
print("Goal: Show Cassandra's Tunable Consistency. We explicitly set ConsistencyLevel to ALL (demand a response from ALL replicas) and perform the same query.")
print("Expected: FAILURE. Since one replica is dead, it will fail to respond. We sacrificed Availability for strict Consistency.")

print(f"\n{CYAN}CQL: CONSISTENCY ALL")
print(f"CQL: {cql_select}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

time.sleep(1)

# Prepare the query again, this time with ConsistencyLevel.ALL
read_query_all = session.prepare("SELECT sensor_id, temperature FROM sensors WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 1")
read_query_all.consistency_level = ConsistencyLevel.ALL

try:
    future_all = session.execute_async(read_query_all, [target_sensor])
    row_all = future_all.result().one()
    
    if row_all:
        print(f"{GREEN} -> Fetching data... Sensor ID: {row_all.sensor_id}, Temperature: {row_all.temperature}°C{RESET}")
        
except Exception as e:
    print(f"{RED}{BOLD} -> ERROR {RESET}")
    print(f"{RED} -> Details: {e}{RESET}\n")


print(f"{MAGENTA}{BOLD}\n --- DEMO COMPLETED ---{RESET}")
print(f" -> Don't forget to restart the node: {ORANGE}docker start {node_to_kill}{RESET}\n")
cluster.shutdown()