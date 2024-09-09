export NREPLICASETS:=5
export START_PORT:=33000

RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[0;33m
NC=\033[0m # No Color

default: run

all: default

run: | clean cluster-up bootstrap gotest cluster-down

# cleanup tmp working directory
clean:
	@echo "${GREEN}STAGE: CLEANUP${NC}"
	rm -rf tmp

# prepare vshard-storages, that contains ${NREPLICASETS} replicasets.
# every replicaset has one master and one follower instance.
# every replicaset runs in background mode, no logs are stored (/dev/null)
cluster-up:
	@echo "${GREEN}STAGE: CLUSTER UP${NC}"
	mkdir -p tmp
	rsid=1 ; while [[ $$rsid -le ${NREPLICASETS} ]] ; do								\
		mkdir -p tmp/$${rsid}/master;													\
		mkdir -p tmp/$${rsid}/follower;													\
		ln -sf `(pwd)`/storage.lua tmp/$${rsid}/master/storage_$${rsid}_master.lua;		\
		ln -sf `(pwd)`/cfgmaker.lua tmp/$${rsid}/master/cfgmaker.lua;					\
		ln -sf `(pwd)`/storage.lua tmp/$${rsid}/follower/storage_$${rsid}_follower.lua;	\
		ln -sf `(pwd)`/cfgmaker.lua tmp/$${rsid}/follower/cfgmaker.lua;					\
		TT_WORK_DIR=tmp/$${rsid}/master/ TT_PID_FILE=tarantool.pid TT_BACKGROUND=true TT_LOG=tarantool.log tarantool tmp/$${rsid}/master/storage_$${rsid}_master.lua;		\
		TT_WORK_DIR=tmp/$${rsid}/follower/ TT_PID_FILE=tarantool.pid TT_BACKGROUND=true TT_LOG=tarantool.log tarantool tmp/$${rsid}/follower/storage_$${rsid}_follower.lua; \
		((rsid = rsid + 1)) ; \
	done

# bootstrap vshard cluster using lua vshard.router
bootstrap:
	@echo "${GREEN}STAGE: BOOTSTRAP CLUSTER${NC}"
	mkdir -p tmp/router_work_dir
	TT_WORK_DIR=tmp/router_work_dir/ tarantool router.lua

# stop vshard storage tarantool
cluster-down:
	@echo "${GREEN}STAGE: CLUSTER DOWN${NC}"
	rsid=1 ; while [[ $$rsid -le ${NREPLICASETS} ]] ; do		\
		kill -9 `cat tmp/$${rsid}/master/tarantool.pid`;		\
		kill -9 `cat  tmp/$${rsid}/follower/tarantool.pid`;		\
		((rsid = rsid + 1)) ; \
	done

# run go tests, minus "-" signs before command allows failures, otherwise cluster-down stage won't run.
gotest:
	@echo "${GREEN}STAGE: RUN GOTESTS${NC}"
	-go test -race -parallel=20 -coverpkg="../../" -coverprofile cover.out -timeout=90s
#	go tool cover -html=cover.out
