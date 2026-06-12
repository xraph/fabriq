.PHONY: all build test test-integration test-all bench lint fmt tidy cover clean

GO     := go
MODULE := github.com/xraph/fabriq

all: build test lint

build:
	$(GO) build ./...

test:
	$(GO) test -race -count=1 ./...

# Integration tests need Docker (testcontainers: Postgres+Timescale, Redis, FalkorDB, ES).
test-integration:
	$(GO) test -race -count=1 -tags=integration -timeout 20m ./...

test-all: test test-integration

bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint not found: https://golangci-lint.run/usage/install/"; exit 1)
	golangci-lint run --timeout 10m

fmt:
	gofmt -w .
	goimports -w -local $(MODULE) .

tidy:
	$(GO) mod tidy

clean:
	rm -f coverage.out coverage.html
