## Unreleased

BUG FIXES:

* Fix buckets discovery (it doesn't freeze now)

## 0.0.11

FEATURES:

* BucketStat has become a public method (#21)
* Add golang-ci logic

REFACTOR:
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
