TEST_TIMEOUT?=20s

test:
	go test ./... -parallel=10 -timeout=$(TEST_TIMEOUT) -coverprofile=coverage.out.tmp
	@cat coverage.out.tmp | grep -v "mock" > coverage.out
	@rm coverage.out.tmp

test/integration:
	@$(MAKE) -C ./tests/integration test

generate/mocks:
	mockery --name=Pool --case=underscore --output=mocks/pool --outpkg=mockpool