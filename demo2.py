from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
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

ip_to_name_cache = {}

def get_container_name(ip):
    global ip_to_name_cache
    for name in node_names:
        try:
            current_ip = socket.gethostbyname(name) # Chiede alla rete interna di Docker qual è l'IP del container con quel nome
            ip_to_name_cache[current_ip] = name
        except Exception:
            pass
    return ip_to_name_cache.get(ip, "Unknown")

def format_node(host_obj):
    if not host_obj: return "Unknown-Node"
    raw_address = getattr(host_obj, 'address', str(host_obj))
    clean_ip = raw_address.split(':')[0] 
    name = get_container_name(clean_ip)
    return f"{name} ({clean_ip})"


def inspect_hints_folder(container_name, phase="create"):
    print(f"\n{MAGENTA}[🔍] Let's look inside the Coordinator's disk{RESET}")
    print(f"Open terminal and paste this command:")
    print(f"{ORANGE}docker exec {container_name} ls -lh /var/lib/cassandra/hints{RESET}")
    
    if phase == "create":
        print("We see a '.hints' file waiting to be delivered.")
        input(f"\n{BOLD}[Press Enter ONLY AFTER checking the folder]{RESET}")
    elif phase == "delete":
        print("Expected: The folder should now be empty, the hint was delivered.")
        input(f"\n{BOLD}[Press Enter to continue]{RESET}")

print(f"{GREEN}Connected{RESET}\n")

print(f"{MAGENTA}{BOLD}Cassandra's Eventual Consistency and the use of Hinted Handoff.{RESET}")
print(f"{MAGENTA}{BOLD}Goal is to see what happens when a node fails during a write. {RESET}\n")

print(f"{GREEN}Connected{RESET}\n")

print(f"{YELLOW}{BOLD}--- STEP 1: Target Selection ---{RESET}")
print("Goal: Identify the two physical nodes responsible for our test data.")
print("Using consistent hashing, the data from sensor 99 are written in those nodes")

target_sensor = 99
test_temp = 42.0

dummy_query = session.prepare("INSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (?, toTimestamp(now()), ?)")
bound_dummy = dummy_query.bind([target_sensor, test_temp])

# Usando il consistent hashing di Cassandra per identificare in quali nodi i dati di sensor 99 sarebbero scritti
replicas = cluster.metadata.get_replicas('iot_data', bound_dummy.routing_key)

primary_host = replicas[0]
target_host = replicas[1]

primary_ip = primary_host.address.split(':')[0]
target_ip = target_host.address.split(':')[0]

primary_name = get_container_name(primary_ip)
target_name = get_container_name(target_ip)

print(f"{GREEN} -> Sensor {target_sensor} maps (with RF=2) to: {format_node(primary_host)} and {format_node(target_host)}{RESET}")


print(f"\n{YELLOW}{BOLD}--- STEP 2: Simulate Failure ---{RESET}")
print("Goal: Stop one of the nodes that contains the data of sensor 99 to introduce a failure.")
print(f"{BOLD}\n -> ACTION REQUIRED{RESET}: Open a terminal and run {ORANGE}{BOLD} docker stop {target_name}{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER stopping {target_name}]{RESET}")


print(f"\n{YELLOW}{BOLD}--- STEP 3: Write Data (Triggering Hint) ---{RESET}")
print("Goal: Write data while a replica is down to force the Coordinator to store a Hint.")
cql_insert = f"INSERT INTO sensors (sensor_id, timestamp, temperature) VALUES ({target_sensor}, toTimestamp(now()), {test_temp});"

print("Expected: We are performing a write operation with consistency level ONE. The surviving replica will save the data, and the Coordinator will store a Hint for the failed node.")
print(f"\n{CYAN}CQL:\nCONSISTENCY ONE;\n{cql_insert}{RESET}")
input(f"\n{BOLD}[Press Enter to execute the write]{RESET}")

try:
    # la traformo per tracciarla e analizzarla dopo
    stmt_one = SimpleStatement(cql_insert, consistency_level=ConsistencyLevel.ONE)
    # Execute the write with tracing enabled to analyze the path
    future = session.execute_async(stmt_one, trace=True)
    future.result()
    
    # Dice chi e' il coordinator che ha ricevuto la query
    coord_ip_clean = getattr(future.coordinator_host, 'address', str(future.coordinator_host)).split(':')[0]
    coord_raw_name = get_container_name(coord_ip_clean)
    
    print(f"{GREEN}-> SUCCESS! The surviving replica ({primary_name}) accepted the write.{RESET}")
    print(f"\n{MAGENTA}   [🔍] Query Trace Analysis...{RESET}")
   
    #recuperare i dati di trace
    trace_write = future.get_query_trace()
    primary_node_involved = False
    
    # primary ip e' il nodo superstite
    # stampa tutti gli eventi con source l'ip del nodo superstite
    for event in trace_write.events:
        if event.source == primary_ip:
            primary_node_involved = True
            if "Mutation" in event.description or "Appending" in event.description or "Memtable" in event.description:
                print(f"      [WRITE TRACE] {primary_name}:{YELLOW} {event.description}{RESET}")
            
    if primary_node_involved:
        print(f"   -> The trace proves that data was written physically on {primary_name}'s disk")
    else:
        print(f"{RED} -> [WARNING] Could not confirm write on the surviving node from trace.{RESET}")

    
    print(f"\n The Request is orchestrated by Coordinator node: {format_node(future.coordinator_host)}")
    print(f" {target_name} is down, so the coordinator stored an 'Hint' on its local disk.")
    
    
except Exception as e:
    print(f"{RED}{BOLD} -> ERROR!{RESET}")
    print(f"{RED} -> Details: {e}{RESET}")

inspect_hints_folder(coord_raw_name, phase="create")

print(f"\n{YELLOW}{BOLD}--- STEP 4: Hinted Handoff  ---{RESET}")
print("Goal: Restart the dead node and let it sync missing data via Hinted Handoff.")
print(f"{BOLD} -> ACTION REQUIRED 1.{RESET} Restart {target_name}:{ORANGE} docker start {target_name}{RESET}")
print(f"{BOLD} -> ACTION REQUIRED 2.{RESET} Wait ~20 seconds for Gossip and Handoff.{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER starting the node AND waiting 20 seconds]{RESET}")

inspect_hints_folder(coord_raw_name, phase="delete")

print("Finally to prove that Node that was down during the write actually received the data")

print(f"\n{BOLD} -> ACTION REQUIRED:{RESET} kill {primary_name} (the ONLY node that originally saved the data){ORANGE} docker stop {primary_name}{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER stopping {primary_name}]{RESET}")


print(f"\n{YELLOW}{BOLD}--- STEP 5: Read Tracing ---{RESET}")
print("Goal: Read data from sensor 99, it must come from the newly restarted node.")
cql_select = f"SELECT sensor_id, temperature FROM sensors WHERE sensor_id = {target_sensor} ORDER BY timestamp DESC LIMIT 1;"

print(f"\n{CYAN}CQL:\n{cql_select}{RESET}")
input(f"\n{BOLD}[Press Enter to execute]{RESET}")

read_stmt = SimpleStatement(cql_select, consistency_level=ConsistencyLevel.ONE)

try:
    future_read = session.execute_async(read_stmt, trace=True)
    row = future_read.result().one()

    if row:
        print(f"{GREEN} -> Fetching data... Sensor ID: {row.sensor_id}, Temperature: {row.temperature}°C{RESET}")
        print(f" -> Request orchestrated by Coordinator Node: {format_node(future_read.coordinator_host)}{RESET}")
        
        print(f"\n{MAGENTA}[🔍] Query Trace Analysis...{RESET}")
        trace_read = future_read.get_query_trace()
        target_node_involved = False
        
        for event in trace_read.events:
            if event.source == target_ip:
                target_node_involved = True
                print(f"   [READ TRACE] {target_name} ({target_ip}): {event.description}")
                
        if target_node_involved:
            print(f"{GREEN} -> Trace clearly proves data was read from the restarted node ({target_name}){RESET}\n")
        else:
            print(f"{RED} -> [WARNING] {target_name}'s IP not found in trace. Something went wrong.{RESET}\n")

    else:
        print(f"{RED} -> [ERROR] No data found.{RESET}\n")
except Exception as e:
    print(f"{RED}{BOLD} -> ERROR!{RESET}")
    print(f"{RED} -> Details: {e}{RESET}\n")

print(f"{MAGENTA}{BOLD}\n --- DEMO COMPLETED ---{RESET}")
print(f" -> Remember to restart the downed node: {ORANGE} docker start {primary_name}{RESET}\n")
cluster.shutdown()