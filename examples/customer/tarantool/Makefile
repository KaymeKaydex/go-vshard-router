all: stop clean start enter

libs: # устанавливает все либы в .rocks/lib и .rocks/share
	tarantoolctl rocks --tree .rocks install --only-deps customer-scm-1.rockspec

start: libs
	tarantoolctl start storage_1_a
	tarantoolctl start storage_1_b
	tarantoolctl start storage_2_a
	tarantoolctl start storage_2_b
	tarantoolctl start router_1
	@echo "Waiting cluster to start"
	@sleep 1
	echo "vshard.router.bootstrap()" | tarantoolctl enter router_1

stop:
	tarantoolctl stop storage_1_a
	tarantoolctl stop storage_1_b
	tarantoolctl stop storage_2_a
	tarantoolctl stop storage_2_b
	tarantoolctl stop router_1

.PHONY: enter
router:
	tarantoolctl enter router_1
storage:
	tarantoolctl enter storage_1_a

logcat:
	tail -f data/*.log

clean:
	rm -rf data/

.PHONY: console test deploy clean
