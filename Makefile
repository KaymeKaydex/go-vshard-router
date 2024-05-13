TEST_TIMEOUT?=20s
LOCAL_BIN:=$(CURDIR)/bin
# golang-ci tag
GOLANGCI_TAG:=latest
# golang-ci bin file path
GOLANGCI_BIN:=$(GOPATH)/bin/golangci-lint

.PHONY: install-lint
install-lint:
ifeq ($(wildcard $(GOLANGCI_BIN)),)
	$(info #Downloading swaggo latest)
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_TAG)
endif

test:
	go test ./... -parallel=10 -timeout=$(TEST_TIMEOUT) -coverprofile=coverage.out.tmp
	@cat coverage.out.tmp | grep -v "mock" > coverage.out
	@rm coverage.out.tmp

cover: test
	 go tool cover -html=coverage.out

test/integration:
	@$(MAKE) -C ./tests/integration test

generate/mocks:
	mockery --name=Pool --case=underscore --output=mocks/pool --outpkg=mockpool

.PHONY: lint
lint: install-lint
	$(GOLANGCI_BIN) run --config=.golangci.yaml ./...

