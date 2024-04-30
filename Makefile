TEST_TIMEOUT?=20s

test:
	go test ./... -parallel=10 -timeout=$(TEST_TIMEOUT) -coverprofile=coverage.out

test/integration:
	@$(MAKE) -C ./tests/integration test
