from cassandra.cluster import Cluster
from cassandra.policies import HostStateListener
import threading
import socket
import time
import subprocess


RESET = '\033[0m'
BOLD = '\033[1m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RED = '\033[91m'
MAGENTA = '\033[95m'
ORANGE = '\033[38;5;202m'

# Event to notify the main thread when a new node joins
node_added_event = threading.Event()


print(f"{MAGENTA}{BOLD}Connecting to cluster...{RESET}")
node_names = ['cassandra-node1', 'cassandra-node2', 'cassandra-node3']

ip_to_name_cache = {}
for name in node_names: 
    try:
        ip = socket.gethostbyname(name) # Chiede alla rete interna di Docker qual è l'IP del container con quel nome
        ip_to_name_cache[ip] = name # Costruisce una mappa IP -> Nome del container per identificare facilmente i nodi nei log
    except Exception:
        pass

def get_container_name(ip):
    if ip in ip_to_name_cache: return ip_to_name_cache[ip]
    try:
        name, _, _ = socket.gethostbyaddr(ip) # Chiede alla rete interna di Docker qual è il nome del container associato a quell'IP 
        return name.split('.')[0]
    except Exception:
        return "cassandra-node4" # Fallback for the new arrival

def format_node(host_obj):
    if not host_obj: return "Unknown-Node"
    raw_address = getattr(host_obj, 'address', str(host_obj))
    clean_ip = raw_address.split(':')[0] 
    name = get_container_name(clean_ip)
    return f"{name} ({clean_ip})"

# LISTENER 
class ScalingListener(HostStateListener):
    def on_add(self, host):
        pass
    # Quando un nuovo nodo viene rilevato come "up" dal driver, aggiorna la mappa IP->Nome e notifica il main thread
    def on_up(self, host):
        clean_ip = getattr(host, 'address', str(host)).split(':')[0]
        ip_to_name_cache[clean_ip] = "cassandra-node4" 
        
        print(f"{GREEN}{BOLD}-> Node: cassandra-node4 ({clean_ip}) joined the ring{RESET}\n")
        
        node_added_event.set()

    def on_down(self, host): pass
    def on_remove(self, host): pass
    def on_suspect(self, host): pass

cluster = Cluster(node_names, port=9042)
session = cluster.connect('iot_data')
session.default_timeout = 60.0

listener = ScalingListener()
cluster.register_listener(listener) # Registra il listener per monitorare i cambiamenti nella topologia del cluster

print(f"{GREEN}Connected{RESET}\n")

dummy_query = session.prepare("INSERT INTO sensors (sensor_id, timestamp, temperature) VALUES (?, toTimestamp(now()), 0.0)")
test_sensors = [1, 2, 3, 4, 5, 6]

print(f"{MAGENTA}{BOLD} This last demo shows horizontal scaling in Cassandra.{RESET}\n")

print(f"{YELLOW}{BOLD}--- STEP 1: Data distribution (3 Nodes) ---{RESET}")
print("We currently have a cluster with 3 nodes, and we can see exactly in which nodes each sensor data is stored.")

for s_id in test_sensors:
    bound = dummy_query.bind([s_id])
    # Usando il consistent hashing di Cassandra per identificare in quali nodi i dati di sensor 99 sarebbero scritti
    replicas = cluster.metadata.get_replicas('iot_data', bound.routing_key)
    storage_nodes = [format_node(r) for r in replicas]
    print(f" -> Sensor ID: {s_id} | Stored on: {storage_nodes}{RESET}")


input(f"\n{BOLD}[Press Enter to execute]{RESET}")

print(f"\n{YELLOW}{BOLD}--- STEP 2: Horizontal Scaling ---{RESET}")

print("Goal: Add a fourth node to the cluster by creating a new Docker container.")
print(f"{BOLD}-> ACTION REQUIRED:{RESET} Open a terminal and run {ORANGE}{BOLD}docker-compose up -d cassandra-node4{RESET}")

print(f"\n While waiting for the new node to synchronize with the cluster using the Gossip Protocol,")
print(f" we open another terminal and run a query to retrieve data for sensor 2. ")
print(f" It should be able to execute successfully even if the new node is entering the cluster.")
print(f"   {ORANGE}{BOLD}docker exec -it cassandra-node1 cqlsh{RESET}")
print(f"   {ORANGE}{BOLD}SELECT * FROM iot_data.sensors WHERE sensor_id = 2;{RESET}\n")
while True:
    # Wait for the node_added_event to be set by the listener when the new node is detected as up
    if node_added_event.wait(timeout=6.0): 
        break
    
    current_hosts = cluster.metadata.all_hosts()
    if len(current_hosts) >= 4:
        print(f"\n{GREEN}{BOLD} -> [SCALING EVENT] New node detected{RESET}")
        print(f"{GREEN} -> The cluster now has {len(current_hosts)} active nodes{RESET}\n")
        break
    
    print(f" -> ...still waiting for topology update...{RESET}")



print(f"{YELLOW}{BOLD}--- STEP 3: Data Rebalancing (Auto-Balancing) ---{RESET}")
print("Goal: Observe how the cluster auto balances once node 4 joins.")

data_moved = False
for s_id in test_sensors:
    bound = dummy_query.bind([s_id])
    # Usando il consistent hashing di Cassandra per identificare in quali nodi i dati di sensor 99 sarebbero scritti

    replicas = cluster.metadata.get_replicas('iot_data', bound.routing_key)
    storage_nodes = [format_node(r) for r in replicas]
    
    if any("node4" in node for node in storage_nodes):
        data_moved = True
        print(f"{CYAN} - MOVED Sensor ID: {s_id} | Now on: {storage_nodes} {RESET}")
    else:
        print(f"{YELLOW} - STATIC Sensor ID: {s_id} | Remained on: {storage_nodes}{RESET}")

if data_moved:
    print("Thanks to Consistent Hashing, Cassandra only moves the exact partitions whose tokens are now assigned to the new node, ")
    print("which is why some data remained static while other partitions moved. ")
else:
    print(f"\n{RED}-> The new node tokens didn't cover these 6 sensors in this test run.{RESET}")
    print("This can happen due to the randomness of token assignment and the small sample size. ")


print(f"{MAGENTA}{BOLD}--- DEMO COMPLETED ---{RESET}")
cluster.shutdown()