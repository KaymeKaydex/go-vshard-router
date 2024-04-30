# Customer service
## What it is?

This example is taken directly from the vshard repository example. A go service was written on top.
Includes only 2 endpoints (see [swagger](go-service/docs/swagger.yaml)): count, record sales information.
Only a few changes have been made:

- slightly modified Makefile
- by default, the example was created for fault tolerance according to Raft (only for replicaset 1, since there are 3 instances)
- the number of buckets is set to 10k
- minor fixes to critical errors (for example, the “transaction” attempt was replaced by box.atomic)
- added 1 more instance to replicaset 1 so that raft could select a new master
## How to start?

1. Start the cluster

```sh
$ cd tarantool
$ make start
```

2. Launch the service
```sh
$ cd go-service # from the customer directory
$ make start
```