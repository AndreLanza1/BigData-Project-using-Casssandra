# Apache Cassandra: Distributed NoSQL DBMS

![Big Data](https://img.shields.io/badge/Focus-Big%20Data-blue)
![Apache Cassandra](https://img.shields.io/badge/Database-Cassandra%203.11-turquoise)
![Docker](https://img.shields.io/badge/Environment-Docker%20Compose-whale)
![Python](https://img.shields.io/badge/Client-Python%20Driver-yellow)

## Project Overview
This repository contains a hands-on project developed for the **Big Data** course. The primary objective is to practically demonstrate the core architectural pillars, mechanisms, and trade-offs of **Apache Cassandra** (such as the Consistent Hashing, Tunable Consistency, and Masterless scaling) through a containerized multi-node cluster environment.

### Cassandra vs. Other Big Data Solutions
Unlike traditional Relational Databases (RDBMS) or other Big Data frameworks seen in class (like HDFS or HBase), Cassandra introduces unique paradigms to handle massive workloads:
* **Query-Driven Design (Denormalization) vs. RDBMS:** Traditional RDBMS normalize data to reduce redundancy and rely on heavy `JOIN` operations. Cassandra does not support distributed joins due to network latency; instead, it requires a query-driven approach where data is denormalized into a single aggregate specifically designed around the application's read patterns.
* **Decentralized (Masterless) vs. Master-Slave (HDFS, MongoDB):** Frameworks like HDFS rely on a Master node (NameNode), creating a Single Point of Failure (SPOF). Cassandra features a fully symmetric, masterless peer-to-peer architecture where all nodes are identical, ensuring continuous availability and fault tolerance.
* **Write-Optimized Storage Engine:** Instead of using B-Trees (which cause random disk I/O and locking), Cassandra appends data sequentially to a `Commit Log` and writes it to an in-memory `Memtable`. When full, the Memtable is flushed to immutable `SSTables`. Conflicting updates across replicas are resolved automatically during reads via a *Last-Write-Wins (LWW)* policy.


## Docker Cluster Architecture
The entire environment is fully containerized using Docker, simulating a real-world distributed topology:
* **3-Node Cassandra Cluster (`cassandra-node1`, `cassandra-node2`, `cassandra-node3`):** Running Cassandra 3.11 isolated inside a custom bridge network (`bigdata_project_cassandra-net`). They communicate via the **Gossip Protocol** (`GossipingPropertyFileSnitch`) to exchange cluster topology details and use the *Phi Accrual Failure Detector* to adaptively detect node crashes. `cassandra-node1` acts as the *Seed Node* to bootstrap the cluster.
* **Python Client Container:** A separate container built with the official `cassandra-driver`, used as the orchestration driver to run the interactive experiments.


## Interactive Demos 

The repository includes 4 independent Python scripts designed to show the Cassandra principles in action. Each script focuses on a specific aspect of Cassandra's architecture and behavior, allowing you to see the underlying mechanics in real-time:

### Demo 1: Query-Driven design and Data Migration (`demo1.py`)
* **Purpose:** Demonstrates how data modeling shifts from relational tables to Cassandra's query-centric paradigm.
* **How it works:** It creates an `iot_data` Keyspace (Replication Factor = 2) and creates a table (`sensors`) with partition key `sensor_id` and clustering column `timestamp`. It then attempts a temperature query which fails because temperature isn't the partition key (The error suggests using `ALLOW FILTERING` but this can cause performance unpredictability). It then migrates the data into a newly optimized table (`sensors_by_temp`), explicitly setting the *Partition Key* and *Clustering Columns* to answer the previous query instantly without any performance bottlenecks.

### Demo 2: Eventual Consistency and Hinted Handoff (`demo2.py`)
* **Purpose:** Demonstrates Cassandra's Eventual Consistency model and how it handles temporary node failures, while inspecting how requests are routed within the cluster.
* **How it works:** The script executes an asynchronous write operation and identifies which node acts as the **Coordinator** for that request. It simulates a scenario where a replica node goes offline temporarily. During this downtime, the Coordinator stores "hints" locally. When the dead node is restarted, the Coordinator performs a **Hinted Handoff**, streaming the missed updates to the recovered node to ensure eventual consistency. By enabling and parsing the **Query Trace**, the script extracts the internal cluster logs, proving the exact physical execution path and verifying that the requested data was successfully served by the newly recovered node.

### Demo 3: Fault Tolerance and Tunable Consistency (`demo3.py`)
* **Purpose:** Experiments with Cassandra's tunable consistency levels and visually validates the trade-offs of the CAP Theorem (Consistency vs. Availability).
* **How it works:** After simulating a node failure within the cluster, the script attempts the exact same read query under two different configurations to show how developers can tune behavior per request:
  1. With `ConsistencyLevel.ONE`: The query succeeds because the Coordinator only needs a response from one surviving replica. This prioritizes **Availability** and fault tolerance.
  2. With `ConsistencyLevel.ALL`: The query fails and throws an `UnavailableException` because the Coordinator demands responses from *all* replicas, which is impossible with a downed node. This prioritizes **Strict Consistency**, sacrificing system availability.

### Demo 4: Scale Out (`demo4.py`)
* **Purpose:** Illustrates horizontal scalability and how data is distributed across a cluster ring using token ranges.
* **How it works:** The script monitors the cluster topology as a new, fourth node (`cassandra-node4`) dynamically joins the live cluster. By querying the driver's token metadata (the token ring), it demonstrates **Consistent Hashing** in action. Instead of re-shuffling the entire database, Cassandra automatically recalculates the token ranges and relocates *only* the specific partitions whose hash values fall into the newly assigned ranges of `node4`. This minimizes network overhead and allows linear scale-out without system downtime.

## How to Run the Project

Follow these instructions to spin up the cluster and execute the experimental scripts on your local machine.

1. Make sure Docker is running. Open your terminal in the root directory of the project and start the pre-configured Cassandra nodes:
    ```bash
    docker start cassandra-node1 cassandra-node2 cassandra-node3
    ```

    (Note: If running for the very first time, make sure you build the environment using `docker-compose up -d`).

    To verify that the cluster is fully up, stable, and all 3 nodes are in the UN (Up/Normal) state, execute Cassandra's native diagnostics tool:
    ```bash
    docker exec -it cassandra-node1 nodetool status
    ```
2. Once the cluster status shows all nodes are ready, execute the client container to run the interactive demos one by one:
    ```bash
    docker run -it --rm --network bigdata_project_cassandra-net python-client python demo1.py
    ```

## Repository Structure
```
├── docker-compose.yml     # Multi-node Cassandra cluster deployment configuration
├── Dockerfile             # Setup for the interactive Python client container
├── requirements.txt       # Python dependencies
├── demo1.py               
├── demo2.py               
├── demo3.py              
├── demo4.py             
├── demo_Sstable_Compaction.py   # Additional demo: Memtables, SSTables, and Compaction process
├── show_nodes.py                # Utility: Cluster topology and token ring inspector
├── Presentation.pdf       # Complete theoretical presentation
└── README.md              
```
> **Note:** Most of the architectural diagrams and graphics in the Presentation.pdf are original creations of the authors designed specifically for this project. Additional references regarding Facebook's messaging infrastructure were sourced from [ByteByteGo](https://blog.bytebytego.com/p/facebooks-database-handling-billions)

**Authors**    
Andrea Lanzarone
Claudia Cornacchia   
