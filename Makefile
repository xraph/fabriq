.PHONY: help \
	build run dev hot install clean \
	fmt lint lint-fix vet check all \
	test test-verbose test-integration test-all bench bench-smoke \
	cover cover-html \
	tidy deps check-deps mod-download mod-verify \
	vendor docker-build docker-push helm-lint helm-template helm-package \
	docs docs-build \
	h b r t c f l lf v d i ti

# Default target
.DEFAULT_GOAL := help

# Variables
BINARY_NAME=fabriq
CMD_DIR=./cmd/fabriq
BUILD_DIR=./bin
MODULE=github.com/xraph/fabriq
GO=go

# Version is stamped into main.version (matches goreleaser's -X main.version).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS=-ldflags "-s -w -X main.version=$(VERSION)"

# Container image + Helm chart
IMAGE ?= ghcr.io/xraph/fabriq
TAG   ?= dev
CHART := deploy/helm/fabriq

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[0;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

## help: Display this help message
help:
	@echo "$(BLUE)Available targets:$(NC)"
	@echo ""
	@echo "$(GREEN)Build & Run:$(NC)"
	@echo "  make build (b)        - Compile all packages and build the binary"
	@echo "  make run (r)          - Run the worker (serve)"
	@echo "  make dev (d)          - Run with live reload (air)"
	@echo "  make install (i)      - Install the binary to GOPATH/bin"
	@echo "  make clean (c)        - Remove build artifacts"
	@echo ""
	@echo "$(GREEN)Code Quality:$(NC)"
	@echo "  make fmt (f)          - Format code with gofmt and goimports"
	@echo "  make lint (l)         - Run linter (golangci-lint)"
	@echo "  make lint-fix (lf)    - Run linter with auto-fix"
	@echo "  make vet (v)          - Run go vet"
	@echo "  make check            - Run fmt, vet, and lint"
	@echo ""
	@echo "$(GREEN)Testing:$(NC)"
	@echo "  make test (t)         - Run unit tests (race)"
	@echo "  make test-verbose     - Run unit tests with verbose output"
	@echo "  make test-integration (ti) - Run integration tests (testcontainers; needs Docker)"
	@echo "  make test-all         - Run unit + integration tests"
	@echo "  make bench            - Run benchmarks"
	@echo "  make bench-smoke      - Run benchmarks once (CI smoke check)"
	@echo "  make cover            - Generate coverage profile"
	@echo "  make cover-html       - Generate HTML coverage report"
	@echo ""
	@echo "$(GREEN)Dependencies:$(NC)"
	@echo "  make deps             - Install development dependencies"
	@echo "  make tidy             - Tidy and verify go modules"
	@echo "  make vendor           - Vendor dependencies (needed for docker-build)"
	@echo "  make mod-download     - Download go modules"
	@echo "  make mod-verify       - Verify go modules"
	@echo ""
	@echo "$(GREEN)Container & Helm:$(NC)"
	@echo "  make docker-build     - Build the container image locally ($(IMAGE):$(TAG))"
	@echo "  make docker-push      - Push the container image"
	@echo "  make helm-lint        - Lint the Helm chart"
	@echo "  make helm-template    - Render the Helm chart"
	@echo "  make helm-package     - Package the Helm chart into dist/"
	@echo ""
	@echo "$(GREEN)Documentation:$(NC)"
	@echo "  make docs             - Serve documentation locally"
	@echo "  make docs-build       - Build documentation"
	@echo ""
	@echo "$(GREEN)Other:$(NC)"
	@echo "  make all              - Run check, test, and build"
	@echo "  make help (h)         - Show this help message"

## build: Compile all packages and build the binary
build:
	@echo "$(BLUE)Building $(BINARY_NAME) ($(VERSION))...$(NC)"
	@$(GO) build ./...
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "$(GREEN)✓ Build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

## run: Run the worker
run:
	@echo "$(BLUE)Running $(BINARY_NAME) serve...$(NC)"
	$(GO) run $(CMD_DIR) serve

## dev: Run in development mode with live reload
dev:
	@echo "$(BLUE)Running in development mode...$(NC)"
	@command -v air >/dev/null 2>&1 || { echo "$(YELLOW)air not found, installing...$(NC)"; $(GO) install github.com/air-verse/air@latest; }
	@mkdir -p tmp
	air --build.cmd "$(GO) build -o ./tmp/$(BINARY_NAME) $(CMD_DIR)" --build.bin "./tmp/$(BINARY_NAME) serve"

## hot: Alias for dev
hot: dev

## install: Install binary to GOPATH/bin
install: build
	@echo "$(BLUE)Installing $(BINARY_NAME)...$(NC)"
	$(GO) install $(LDFLAGS) $(CMD_DIR)
	@echo "$(GREEN)✓ Installed to $(shell $(GO) env GOPATH)/bin/$(BINARY_NAME)$(NC)"

## clean: Remove build artifacts
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(BUILD_DIR) tmp dist
	@rm -f coverage.out coverage.html
	@$(GO) clean
	@echo "$(GREEN)✓ Clean complete$(NC)"

## fmt: Format code
fmt:
	@echo "$(BLUE)Formatting code...$(NC)"
	@gofmt -s -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local $(MODULE) . || echo "$(YELLOW)goimports not found, skipping (run: make deps)$(NC)"
	@echo "$(GREEN)✓ Formatting complete$(NC)"

## lint: Run linter
lint:
	@echo "$(BLUE)Running linter...$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)"; exit 1; }
	golangci-lint run --build-tags=integration --timeout 10m
	@echo "$(GREEN)✓ Linting complete$(NC)"

## lint-fix: Run linter with auto-fix
lint-fix:
	@echo "$(BLUE)Running linter with auto-fix...$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)"; exit 1; }
	golangci-lint run --fix --build-tags=integration --timeout 10m
	@echo "$(GREEN)✓ Linting with fixes complete$(NC)"

## vet: Run go vet
vet:
	@echo "$(BLUE)Running go vet...$(NC)"
	$(GO) vet ./...
	@echo "$(GREEN)✓ Vet complete$(NC)"

## check: Run fmt, vet, and lint
check:
	@echo "$(BLUE)Running all checks...$(NC)"
	@$(MAKE) fmt
	@$(MAKE) vet
	@$(MAKE) lint
	@echo "$(GREEN)✓ All checks passed$(NC)"

## test: Run unit tests (race)
test:
	@echo "$(BLUE)Running unit tests...$(NC)"
	$(GO) test -race -count=1 ./...
	@echo "$(GREEN)✓ Tests complete$(NC)"

## test-verbose: Run unit tests with verbose output
test-verbose:
	@echo "$(BLUE)Running unit tests (verbose)...$(NC)"
	$(GO) test -race -count=1 -v ./...

## test-integration: Run integration tests (testcontainers; needs Docker)
test-integration:
	@echo "$(BLUE)Running integration tests (testcontainers)...$(NC)"
	$(GO) test -race -count=1 -tags=integration -p 1 -timeout 30m ./...
	@echo "$(GREEN)✓ Integration tests complete$(NC)"

## test-all: Run unit + integration tests
test-all: test test-integration

## bench: Run benchmarks
bench:
	@echo "$(BLUE)Running benchmarks...$(NC)"
	$(GO) test -bench=. -benchmem -run='^$$' ./...

## bench-smoke: Run benchmarks once (CI smoke check)
bench-smoke:
	@echo "$(BLUE)Running benchmark smoke check...$(NC)"
	$(GO) test -bench=. -benchtime=10x -benchmem -run='^$$' ./...

## cover: Generate coverage profile
cover:
	@echo "$(BLUE)Generating coverage profile...$(NC)"
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "$(GREEN)✓ Coverage profile: coverage.out$(NC)"

## cover-html: Generate HTML coverage report
cover-html: cover
	@echo "$(BLUE)Generating HTML coverage report...$(NC)"
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)✓ HTML coverage report: coverage.html$(NC)"
	@command -v open >/dev/null 2>&1 && open coverage.html || echo "Open coverage.html in your browser"

## tidy: Tidy and verify modules
tidy:
	@echo "$(BLUE)Tidying modules...$(NC)"
	$(GO) mod tidy
	$(GO) mod verify
	@echo "$(GREEN)✓ Modules tidied$(NC)"

## deps: Install development dependencies
deps:
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	@echo "Installing goimports..."
	@$(GO) install golang.org/x/tools/cmd/goimports@latest
	@echo "Installing air (hot reload)..."
	@$(GO) install github.com/air-verse/air@latest
	@echo "Installing golangci-lint..."
	@command -v golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell $(GO) env GOPATH)/bin
	@echo "$(GREEN)✓ Development dependencies installed$(NC)"

## check-deps: Check if required tools are installed
check-deps:
	@echo "$(BLUE)Checking development dependencies...$(NC)"
	@command -v goimports >/dev/null 2>&1 && echo "$(GREEN)✓ goimports$(NC)" || echo "$(YELLOW)✗ goimports (run: make deps)$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 && echo "$(GREEN)✓ golangci-lint$(NC)" || echo "$(YELLOW)✗ golangci-lint (run: make deps)$(NC)"
	@command -v air >/dev/null 2>&1 && echo "$(GREEN)✓ air$(NC)" || echo "$(YELLOW)✗ air (run: make deps)$(NC)"
	@command -v helm >/dev/null 2>&1 && echo "$(GREEN)✓ helm$(NC)" || echo "$(YELLOW)✗ helm (https://helm.sh/docs/intro/install/)$(NC)"

## mod-download: Download modules
mod-download:
	@echo "$(BLUE)Downloading modules...$(NC)"
	$(GO) mod download
	@echo "$(GREEN)✓ Modules downloaded$(NC)"

## mod-verify: Verify modules
mod-verify:
	@echo "$(BLUE)Verifying modules...$(NC)"
	$(GO) mod verify
	@echo "$(GREEN)✓ Modules verified$(NC)"

# --- Container image + Helm --------------------------------------------------
# The Dockerfile builds from a vendored tree (the repo carries local grove
# `replace` directives that point outside this tree until grove is tagged), so
# docker-build vendors first.

## vendor: Vendor dependencies (required for a hermetic docker-build)
vendor:
	@echo "$(BLUE)Vendoring dependencies...$(NC)"
	$(GO) mod vendor
	@echo "$(GREEN)✓ Vendored$(NC)"

## docker-build: Build the container image locally
docker-build: vendor
	@echo "$(BLUE)Building image $(IMAGE):$(TAG)...$(NC)"
	docker build -f deploy/docker/Dockerfile -t $(IMAGE):$(TAG) .
	@echo "$(GREEN)✓ Built $(IMAGE):$(TAG)$(NC)"

## docker-push: Push the container image
docker-push:
	docker push $(IMAGE):$(TAG)

## helm-lint: Lint the Helm chart
helm-lint:
	helm lint $(CHART) --set postgres.dsn=postgres://u:p@pg:5432/fabriq --set redis.addr=redis:6379

## helm-template: Render the Helm chart
helm-template:
	helm template fabriq $(CHART) --set postgres.dsn=postgres://u:p@pg:5432/fabriq --set redis.addr=redis:6379

## helm-package: Package the Helm chart into dist/
helm-package:
	@mkdir -p dist
	helm package $(CHART) --destination dist

## docs: Serve documentation locally
docs:
	@echo "$(BLUE)Serving documentation...$(NC)"
	@cd docs && pnpm install && pnpm dev

## docs-build: Build documentation
docs-build:
	@echo "$(BLUE)Building documentation...$(NC)"
	@cd docs && pnpm install && pnpm build
	@echo "$(GREEN)✓ Documentation built$(NC)"

## all: Run check, test, and build
all: check test build
	@echo "$(GREEN)✓ All tasks complete$(NC)"

# Short aliases (forwarders — the long target carries the recipe)
h: help
b: build
r: run
t: test
c: clean
f: fmt
l: lint
lf: lint-fix
v: vet
d: dev
i: install
ti: test-integration
