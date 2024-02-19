# Go VShard Router
Translations:
- [English](https://github.com/KaymeKaydex/go-vhsard-router/blob/main/README.md)

go-vshard-router — библиотека для отправки запросов напрямую в стораджа в шардированный кластер tarantool,
без использования tarantool-router. go-vshard-router применяет новый подход к созданию кластера

Схема кластера с tarantool-proxy
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

Новая схема использования
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
## Как начать использовать?
### Предварительные условия

- **[Go](https://go.dev/)**: любая из **двух последних мажорных версий** [releases](https://go.dev/doc/devel/release).

### Установка Go-Vshard-Router

