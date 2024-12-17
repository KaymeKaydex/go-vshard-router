# Default timeout for tests
TEST_TIMEOUT?=100s
# Timeout for extended tests
EXTENDED_TEST_TIMEOUT=1m
# Command to invoke Go
GO_CMD?=go
# Path to local binary output directory
LOCAL_BIN:=$(CURDIR)/bin
# Version tag for golangci-lint
GOLANGCI_TAG:=latest
# Path to the golangci-lint binary
GOLANGCI_BIN:=$(GOPATH)/bin/golangci-lint


# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help

.DEFAULT_GOAL := help

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: install-lint
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info #Downloading swaggo latest)
	$(GO_CMD) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_TAG)
endif

prepare-tnt:
	@$(MAKE) -C ./tests/tnt prepare

test: prepare-tnt
	tt rocks install vshard 0.1.26
	export START_PORT=33000
	export NREPLICASETS=5
	$(GO_CMD) test ./... -race -parallel=10 -timeout=$(TEST_TIMEOUT) -covermode=atomic -coverprofile=coverage.out.tmp -coverpkg="./..."
	@cat coverage.out.tmp | grep -v "mock" > coverage.out
	@rm coverage.out.tmp
	@$(MAKE) -C ./tests/tnt cluster-down

cover: test ## Generate and open the HTML report for test coverage.
	 $(GO_CMD) tool cover -html=coverage.out

generate/mocks:
	mockery --name=Pool --case=underscore --output=mocks/pool --outpkg=mockpool # need fix it later
	mockery --name=TopologyController --case=underscore --output=mocks/topology --outpkg=mocktopology

.PHONY: lint
lint: install-lint ## Run GolangCI-Lint to check code quality and style.
	$(GOLANGCI_BIN) run --config=.golangci.yaml ./...

testrace: BUILD_TAGS+=testonly
testrace:
	@CGO_ENABLED=1 \
	$(GO_CMD) test -tags='$(BUILD_TAGS)' -race  -timeout=$(EXTENDED_TEST_TIMEOUT) -parallel=20

test/tnt:
	@$(MAKE) -C ./tests/tnt
