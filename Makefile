# Makefile for the go-kv project

# --- Variables ---
# Unit test packages: exclude tests/ directory
UNIT_TEST_PKGS := $(shell go list ./... | grep -v 'github.com/xmh1011/go-kv/tests')
# Integration test packages
INTEGRATION_TEST_PKGS := ./tests/...

# Define the output binary names
SERVER_BINARY=kv-server
CLIENT_BINARY=kv-client
SERVER_CMD_PATH=./cmd/server
CLIENT_CMD_PATH=./cmd/client

# go import format
GO_FILES := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
GOIMPORTS_REVISER := goimports-reviser
COMPANY_PREFIXES := "github.com/xmh1011"
PROJECT_NAME := "github.com/xmh1011/go-kv"
IMPORTS_ORDER := "std,general,company,project"

# --- Targets ---

.PHONY: all deps build test integration-test bench-test perf-test cover install-mockgen mockgen clean help cluster stop-cluster proto install-protoc-gen install-go-imports-reviser format

.DEFAULT_GOAL := help

## help: Shows this help message.
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F ': ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## all: Run all tests.
all: test

## deps: Tidy and download dependencies.
deps:
	@echo " tidy and downloading dependencies..."
	@go mod tidy

## build: Build both kv-server and kv-client binaries.
build: deps
	@echo " building $(SERVER_BINARY)..."
	@go build -o $(SERVER_BINARY) $(SERVER_CMD_PATH)
	@echo " building $(CLIENT_BINARY)..."
	@go build -o $(CLIENT_BINARY) $(CLIENT_CMD_PATH)

## test: Run unit tests only (excludes tests/ directory and benchmarks).
test: deps
	@echo " running unit tests..."
	@go test -race -timeout=20m -v -cover -coverprofile=coverage.txt -coverpkg=./... $(UNIT_TEST_PKGS)

## integration-test: Run integration tests in tests/ directory only.
integration-test: deps
	@echo " running integration tests..."
	@go test -race -v -timeout=30m $(INTEGRATION_TEST_PKGS)

## perf-test: Run production performance tests and save results.
perf-test: deps
	@echo " running production performance tests (gRPC + LSM)..."
	@mkdir -p test_results
	@go test -race -v -timeout=30m -run "TestProduction" ./tests/ 2>&1 | tee test_results/perf_test.txt
	@echo " results saved to test_results/perf_test.txt"

## bench: Run all benchmark tests and save results to benchmark_results/.
bench-test: deps
	@echo " running benchmark tests..."
	@mkdir -p benchmark_results
	@go test -bench=. -benchmem -benchtime=3s -timeout=30m ./... 2>&1 | tee benchmark_results/benchmark.txt
	@echo " results saved to benchmark_results/benchmark.txt"

## run-tests: Run comprehensive tests using the test runner script.
run-tests: deps
	@echo " running comprehensive tests..."
	@./scripts/run_tests.sh

## cover: Open the HTML coverage report in your browser.
cover: test
	@echo " opening coverage report..."
	@go tool cover -html=coverage.txt

install-mockgen:
	@echo "Installing mockgen..."
	@command -v mockgen >/dev/null 2>&1 || go install github.com/golang/mock/mockgen@latest

mockgen:
	mockgen -source=pkg/storage/storage.go -destination=pkg/storage/storage_mock.go -package=storage
	mockgen -source=pkg/transport/transport.go -destination=pkg/transport/transport_mock.go -package=transport
	mockgen -source=raft/api/service.go -destination=raft/api/service_mock.go -package=api

install-protoc-gen:
	@echo "Installing protoc-gen-go and protoc-gen-go-grpc..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

## proto: Generate gRPC code from proto files.
proto: install-protoc-gen
	@echo " generating gRPC code..."
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/transport/grpc/pb/raft.proto

## cluster: Start a 3-node local cluster using generated configs.
cluster: build
	@echo " starting 3-node cluster..."
	@mkdir -p data
	@nohup ./$(SERVER_BINARY) -c conf/config-1.yaml > raft-node-1.log 2>&1 & echo $$! > raft-node-1.pid
	@nohup ./$(SERVER_BINARY) -c conf/config-2.yaml > raft-node-2.log 2>&1 & echo $$! > raft-node-2.pid
	@nohup ./$(SERVER_BINARY) -c conf/config-3.yaml > raft-node-3.log 2>&1 & echo $$! > raft-node-3.pid
	@echo " cluster started. Logs in raft-node-*.log"

## stop-cluster: Stop the local cluster.
stop-cluster:
	@echo " stopping cluster..."
	@-if [ -f raft-node-1.pid ]; then kill `cat raft-node-1.pid` && rm raft-node-1.pid; fi
	@-if [ -f raft-node-2.pid ]; then kill `cat raft-node-2.pid` && rm raft-node-2.pid; fi
	@-if [ -f raft-node-3.pid ]; then kill `cat raft-node-3.pid` && rm raft-node-3.pid; fi
	@echo " cluster stopped."

# Code style checks
install-go-imports-reviser:
	@echo "Installing go-imports-reviser..."
	@command -v goimports-reviser >/dev/null 2>&1 || go install github.com/incu6us/goimports-reviser/v3@latest

format: install-go-imports-reviser
	@echo "Fixing import order for all Go files"
	@$(GOIMPORTS_REVISER) \
		-format \
		-company-prefixes "$(COMPANY_PREFIXES)" \
		-project-name "$(PROJECT_NAME)" \
		-imports-order "$(IMPORTS_ORDER)" \
		$(GO_FILES)
	for file in $(GO_FILES); do \
		gofmt -w "$$file"; \
	done

## clean: Remove all generated files, test artifacts, and clear Go test cache.
clean:
	@echo " cleaning up..."
	@go clean -testcache
	@rm -f coverage.txt coverage.html unittest.txt $(SERVER_BINARY) $(CLIENT_BINARY) raft-node-*.log raft-node-*.pid
	@rm -rf benchmark_results test_results data
	@find . -type f -name "*.sst" -delete
	@find . -type f -name "*.wal" -delete
	@find . -type f -name "*.wf" -delete
	@find . -type f -name "*.log" -delete
	@find . -type d -name "*-level" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "test-temp-*" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "__debug_bin*" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf /tmp/go-kv-test-* 2>/dev/null || true
	@echo " cleanup complete."
