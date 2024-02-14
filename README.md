# Go VShard Router

go-vshard-router is a library for sending requests to a sharded tarantool cluster directly,
without using tarantool-router. go-vshard-router takes a new approach to creating your cluster 
```mermaid
graph TD
    subgraph Tarantool Database Cluster
        subgraph Replicaset 1
            Instance1
        end
        subgraph Replicaset 2
            Instance2
        end
    end
```

# Getting started
