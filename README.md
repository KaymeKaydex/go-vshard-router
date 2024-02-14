# Go VShard Router

go-vshard-router is a library for sending requests to a sharded tarantool cluster directly,
without using tarantool-router. go-vshard-router takes a new approach to creating your cluster

Old cluster schema
```mermaid
graph TD
    subgraph Tarantool Database Cluster
        subgraph Replicaset 1
            Master_001_1
            Replica_001_2
        end

    end

ROUTER1["Tarantool vshard-router 1_1"] --> Master_001_1
ROUTER2["Tarantool vshard-router 1_2"] --> Master_001_1
ROUTER3["Tarantool vshard-router 1_3"] --> Master_001_1
ROUTER1["Tarantool vshard-router 1_1"] --> Replica_001_2
ROUTER2["Tarantool vshard-router 1_2"] --> Replica_001_2
ROUTER3["Tarantool vshard-router 1_3"] --> Replica_001_2

GO["Golang service"]
GO --> ROUTER1
GO --> ROUTER2
GO --> ROUTER3
```
New cluster schema
```mermaid
graph TD
    subgraph Application Host
        Golang-Service
    end

    Golang-Service --> |iproto| MASTER1
    Golang-Service --> |iproto| REPLICA1
    
    MASTER1["Master 001_1"]
    REPLICA1["Replica 001_2"]
    
    subgraph Tarantool Database Cluster
        subgraph Replicaset 1
            MASTER1
            REPLICA1
        end
    end

    ROUTER1["Tarantool vshard-router(As contorol plane)"]
    ROUTER1 --> MASTER1
    ROUTER1 --> REPLICA1
```
# Getting started
