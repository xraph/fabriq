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
	$(GO) test -race -count=1 -tags=integration -p 1 -timeout 30m ./...

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

# --- Container image + Helm -------------------------------------------------
IMAGE ?= ghcr.io/xraph/fabriq
TAG   ?= dev
CHART := deploy/helm/fabriq

# Vendor first so the build is hermetic despite the local grove `replace`
# directives (they point outside this tree until grove is tagged).
.PHONY: vendor docker-build docker-push helm-lint helm-template
vendor:
	$(GO) mod vendor

docker-build: vendor
	docker build -f deploy/docker/Dockerfile -t $(IMAGE):$(TAG) .

docker-push:
	docker push $(IMAGE):$(TAG)

helm-lint:
	helm lint $(CHART) --set secret.postgresDSN=postgres://u:p@pg:5432/fabriq

helm-template:
	helm template fabriq $(CHART) --set secret.postgresDSN=postgres://u:p@pg:5432/fabriq

clean:
	rm -f coverage.out coverage.html
	rm -rf vendor
