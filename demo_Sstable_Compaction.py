from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time

RESET = '\033[0m'
BOLD = '\033[1m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RED = '\033[91m'
MAGENTA = '\033[95m'
ORANGE = '\033[38;5;202m'


print(f"{MAGENTA}{BOLD} Connecting to cluster...{RESET}")
cluster = Cluster(['cassandra-node1', 'cassandra-node2', 'cassandra-node3'], port=9042)
session = cluster.connect('iot_data')
session.default_timeout = 60.0
print(f"{GREEN} Connected{RESET}\n")

print(f"{CYAN}\n Preparing a clean table for the SSTable Demo...{RESET}")

try:
    session.execute("DROP TABLE IF EXISTS iot_data.sstable_demo")
    session.execute("CREATE TABLE iot_data.sstable_demo (id int PRIMARY KEY, payload text)")
    print(f"{GREEN} -> Clean table 'sstable_demo' created{RESET}\n")
except Exception as e:
    print(f"{RED}{BOLD}-> [ERROR] Unable to create table: {e}{RESET}")
    exit(1)

def inspect_engine(phase, container_name="cassandra-node1"):
    print(f"\n{MAGENTA}[🔍] Engine analysis on {container_name}{RESET}")

    # Comandi protetti contro i problemi di parsing
    cmd_stats = f"docker exec {container_name} bash -c \"nodetool tablestats iot_data.sstable_demo | grep -e 'Memtable data size' -e 'Space used (live)' -e 'SSTable count'\""
    cmd_disk = f"docker exec {container_name} bash -c \"ls -lh /var/lib/cassandra/data/iot_data/sstable_demo-*/\""
    
    print(f"\n1. Check RAM and counters: ")
    print(f"{ORANGE}{BOLD}{cmd_stats}{RESET}")
    
    print(f"\n2. Check physical files: ")
    print(f"{ORANGE}{BOLD}{cmd_disk}{RESET}")
    
    if phase == "memtable":
        print(f"\nExpected: Data is only in 'Memtable data size'. 'Space used' is 0 and 'SSTable count' is 0.")
    elif phase == "sstable1":
        print(f"\nExpected: Memtable resets to 0. 'Space used' goes up and 'SSTable count' becomes 1 (file me-1).")
    elif phase == "fragmented":
        print(f"\nExpected: 'SSTable count' becomes 2. On disk, we can see multiple files (me-1 and me-2).")
    elif phase == "compacted":
        print(f"\nExpected: 'SSTable count' returns to 1. Old files vanish and a single merged file appears (me-3).")
        
    input(f"\n{BOLD}[Press Enter ONLY AFTER analyzing the output in the terminal]{RESET}")

def insert_data(session, start_id, end_id):
    """Inserts heavy rows to visibly grow bytes in RAM and on Disk."""
    insert_query = session.prepare("INSERT INTO sstable_demo (id, payload) VALUES (?, 'Very long text to deliberately take up a lot of disk space and make the growth of KB in Cassandra evident during the tests... ')")
    for i in range(start_id, end_id):
        session.execute_async(insert_query, [i])
    time.sleep(2)

print(f"{MAGENTA}{BOLD}--- STARTING MAIN DEMO ---{RESET}\n")


print(f"{YELLOW}{BOLD}--- STEP 1: Memtable ---{RESET}")
print("Goal: Insert 1000 records and prove they live only in RAM, never touching the disk initially.")

print(f"\n{CYAN}-> Inserting the first 1000 records...{RESET}")
insert_data(session, 1, 1000)

inspect_engine("memtable")

print(f"\n{YELLOW}{BOLD}--- STEP 2: Forcing the Flush (SSTable Creation) ---{RESET}")
print("Goal: Force Cassandra to flush the Memtable (RAM) into an immutable binary file on disk (SSTable).")

print(f"{BOLD}\n -> ACTION REQUIRED:{RESET} Open a new terminal and run: {ORANGE}{BOLD}docker exec cassandra-node1 nodetool flush iot_data sstable_demo{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER the flush has finished]{RESET}")

inspect_engine("sstable1")

print(f"\n{YELLOW}{BOLD}--- STEP 3: Fragmentation (Immutability in Action) ---{RESET}")
print("Goal: Insert more data and flush. Since SSTables are immutable, Cassandra will create a new separate file.")

print(f"\n{CYAN}-> Inserting ANOTHER 1000 records...{RESET}")
insert_data(session, 1000, 2000)

print(f"{BOLD}\n -> ACTION REQUIRED:{RESET} Open a new terminal and run: {ORANGE}{BOLD}docker exec cassandra-node1 nodetool flush iot_data sstable_demo{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER the second flush has finished]{RESET}")

inspect_engine("fragmented")


print(f"\n{YELLOW}{BOLD}--- STEP 4: Compaction (The Cleanup Process) ---{RESET}")
print("Goal: Trigger a compaction to merge the fragmented SSTables into a single, optimized file.")

print(f"{BOLD}\n -> ACTION REQUIRED:{RESET} Open a new terminal and run: {ORANGE}{BOLD}docker exec cassandra-node1 nodetool compact iot_data sstable_demo{RESET}")
input(f"\n{BOLD}[Press Enter ONLY AFTER the compaction has finished]{RESET}")

inspect_engine("compacted")

print(f"{MAGENTA}{BOLD}\n --- DEMO COMPLETED ---{RESET}")
cluster.shutdown()