TTCTL := tt
TEST_TIMEOUT?=20s

clean:
	( rm -rf ./testdata/.rocks )

deps: clean
	( cd testdata; $(TTCTL) rocks install vshard 0.1.26 )

test: deps test/integration

test/integration:
	go test ./... -parallel=10 -timeout=$(TEST_TIMEOUT)
