## Unreleased

BUG FIXES:

* RouterCallImpl: fix decoding the response from vshard.storage.call
* RouterCallImpl: do not return nil error when StorageCallAssertError has happened
* BucketStat: always returns non-nil err, fixed
* DiscoveryAllBuckets returns nil even if errGr.Wait() returns err, fixed
* DiscoveryHandleBuckets: misusage of atomics, fixed

FEATURES:

* Added etcd v2 topology provider implementation (#16)
* Add TopologyController mock for testing improve

REFACTOR:

* Refactored docs (add QuickStart doc) and that library base on vhsard router
* Several linters are enabled because they are usefull



## 0.0.11

BUG FIXES:

* Fix buckets discovery (it doesn't freeze now)

FEATURES:

* BucketStat has become a public method (#21)
* Add golang-ci logic

REFACTOR:

* WSL lint providers fix
* Lint refactor with spaces
* Split tests for shadow and not vshard module
* Update Makefile with cover & lint
* Add more tests for providers
* TopologyController now is an interface
* Pool struct has been replaced with the Pooler interface, which has improved coverage (#21)

## 0.0.10

BUG FIXES:

* fix empty and multiple tnt procedure responses

FEATURES:

* start write CHANGELOG file
* use TopologyProvider instead direct config topology; if us uses old schema just change it to static provider
* add go report card

REFACTOR:

* refactored place for rs method
