TEST_TIMEOUT?=100s
EXTENDED_TEST_TIMEOUT=1m
GO_CMD?=go
LOCAL_BIN:=$(CURDIR)/bin
# golang-ci tag
GOLANGCI_TAG:=latest
# golang-ci bin file path
GOLANGCI_BIN:=$(GOPATH)/bin/golangci-lint

.PHONY: install-lint
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info #Downloading swaggo latest)
	$(GO_CMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_TAG)
endif

prepare-tnt:
	@$(MAKE) -C ./tests/tnt prepare

test: prepare-tnt
	export START_PORT=33000
	export NREPLICASETS=5
	$(GO_CMD) test ./... -race -parallel=10 -timeout=$(TEST_TIMEOUT) -covermode=atomic -coverprofile=coverage.out.tmp -coverpkg="./..."
	@cat coverage.out.tmp | grep -v "mock" > coverage.out
	@rm coverage.out.tmp
	@$(MAKE) -C ./tests/tnt cluster-down

cover: test
	 $(GO_CMD) tool cover -html=coverage.out

test/integration:
	@$(MAKE) -C ./tests/integration test

generate/mocks:
	mockery --name=Pool --case=underscore --output=mocks/pool --outpkg=mockpool # need fix it later
	mockery --name=TopologyController --case=underscore --output=mocks/topology --outpkg=mocktopology

.PHONY: lint
lint: install-lint
	$(GOLANGCI_BIN) run --config=.golangci.yaml ./...

testrace: BUILD_TAGS+=testonly
testrace:
	@CGO_ENABLED=1 \
	$(GO_CMD) test -tags='$(BUILD_TAGS)' -race  -timeout=$(EXTENDED_TEST_TIMEOUT) -parallel=20

test/tnt:
	@$(MAKE) -C ./tests/tnt
