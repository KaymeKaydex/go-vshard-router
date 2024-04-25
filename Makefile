TEST_TIMEOUT?=20s

test:
	go test $(go list ./... | grep -v /tests/) -parallel=10 -timeout=$(TEST_TIMEOUT) -coverprofile=coverage.out

test/integration:
	@$(MAKE) -C ./tests/integration test
