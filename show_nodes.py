from cassandra.cluster import Cluster
from colorama import Fore, Style, init
import socket

init(autoreset=True)

print(Fore.CYAN + "[SYSTEM] Retrieving cluster topology...\n")

# --- CONNESSIONE ---
node_names = ['cassandra-node1', 'cassandra-node2', 'cassandra-node3']
cluster = Cluster(node_names, port=9042)
session = cluster.connect() # Non serve specificare il keyspace per leggere i metadati

# --- MAPPATURA NOMI ---
ip_to_name_cache = {}
for name in node_names:
    try:
        current_ip = socket.gethostbyname(name)
        ip_to_name_cache[current_ip] = name
    except Exception:
        pass

def get_container_name(ip):
    # 1. Controlla se l'IP è già nella nostra cache
    if ip in ip_to_name_cache:
        return ip_to_name_cache[ip]
    
    # 2. Se non c'è, interroga direttamente il server DNS di Docker (Reverse Lookup)
    try:
        name, _, _ = socket.gethostbyaddr(ip)
        # Pulisce il nome (es. toglie i suffissi di rete di Docker)
        return name.split('.')[0] 
    except Exception:
        return ip # Se proprio fallisce tutto, stampa l'IP

# --- STAMPA TABELLA ---
print(Fore.YELLOW + "="*60)
print(Fore.YELLOW + f" {'CONTAINER NAME':<20} | {'HOST ID (UUID)':<36}")
print(Fore.YELLOW + "="*60)

# Chiediamo al driver la lista di tutti i nodi fisici
hosts = cluster.metadata.all_hosts()

for host in hosts:
    # Estraiamo IP pulito e UUID
    clean_ip = getattr(host, 'address', str(host)).split(':')[0]
    name = get_container_name(clean_ip)
    host_id = str(getattr(host, 'host_id', 'NO-ID'))
    
    print(Fore.WHITE + f" {name:<20} | {Fore.LIGHTGREEN_EX}{host_id}")

print(Fore.YELLOW + "="*60 + "\n")

cluster.shutdown()