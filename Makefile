# Makefile for the go-kv project

# --- Variables ---
# Unit test packages: exclude tests/ directory
UNIT_TEST_PKGS := $(shell go list ./... | grep -v 'github.com/xmh1011/go-kv/tests')

# Define the output binary names
SERVER_BINARY=kv-server
CLIENT_BINARY=kv-client
SERVER_CMD_PATH=./cmd/server
CLIENT_CMD_PATH=./cmd/client
CLUSTER_READY_TIMEOUT ?= 30

# go import format
GO_FILES := $(shell find . -type f -name '*.go' -not -path "./vendor/*")
GOIMPORTS_REVISER := goimports-reviser
COMPANY_PREFIXES := "github.com/xmh1011"
PROJECT_NAME := "github.com/xmh1011/go-kv"
IMPORTS_ORDER := "std,general,company,project"

# --- Targets ---

.PHONY: all deps build test integration-test e2e-test bench-test long-test cover install-mockgen mockgen clean help cluster wait-cluster cluster-smoke stop-cluster proto install-protoc-gen install-go-imports-reviser format

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

# ==================== Test Targets ====================

## test: Run unit tests (excludes tests/ directory).
test: deps
	@echo " running unit tests..."
	@go test -race -timeout=20m -v -cover -coverprofile=coverage.txt -coverpkg=./... $(UNIT_TEST_PKGS)

## integration-test: Run integration tests (tests/integration_test.go).
integration-test: deps
	@echo " running integration tests..."
	@go test -race -v -timeout=30m ./tests/integration_test.go

## e2e-test: Run end-to-end performance tests (tests/e2e_perf_test.go).
e2e-test: deps
	@echo " running end-to-end tests..."
	@go test -race -v -timeout=30m ./tests/e2e_perf_test.go

## bench-test: Run benchmark tests and save results.
bench-test: deps
	@echo " running benchmark tests..."
	@mkdir -p benchmark_results
	@go test -run='^$$' -bench=. -benchmem -benchtime=3s -timeout=30m ./... 2>&1 | tee benchmark_results/benchmark.txt
	@echo " results saved to benchmark_results/benchmark.txt"

## long-test: Run long-running tests (10+ minutes each, for CI/nightly).
long-test: deps
	@echo " running long-running tests..."
	@GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$$' -count=1

## cover: Open the HTML coverage report in your browser.
cover: test
	@echo " opening coverage report..."
	@go tool cover -html=coverage.txt

# ==================== Code Generation ====================

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

# ==================== Cluster Management ====================

## cluster: Start a 3-node local cluster using generated configs.
cluster: build
	@echo " starting 3-node cluster..."
	@$(MAKE) --no-print-directory stop-cluster >/dev/null
	@mkdir -p data
	@rm -f raft-node-*.pid raft-node-*.log raft-node-*.out
	@GO_KV_LOG_FILENAME=raft-node-1.log GO_KV_LOG_CONSOLE=false nohup ./$(SERVER_BINARY) -c conf/config-1.yaml > raft-node-1.out 2>&1 & echo $$! > raft-node-1.pid
	@GO_KV_LOG_FILENAME=raft-node-2.log GO_KV_LOG_CONSOLE=false nohup ./$(SERVER_BINARY) -c conf/config-2.yaml > raft-node-2.out 2>&1 & echo $$! > raft-node-2.pid
	@GO_KV_LOG_FILENAME=raft-node-3.log GO_KV_LOG_CONSOLE=false nohup ./$(SERVER_BINARY) -c conf/config-3.yaml > raft-node-3.out 2>&1 & echo $$! > raft-node-3.pid
	@$(MAKE) --no-print-directory wait-cluster
	@echo " cluster started. Logs in raft-node-*.log"

wait-cluster:
	@if ! command -v nc >/dev/null 2>&1; then \
		echo " nc is required to check cluster readiness."; \
		$(MAKE) --no-print-directory stop-cluster >/dev/null; \
		exit 1; \
	fi
	@echo " waiting for cluster readiness..."
	@for node_port in 1:8001 2:8002 3:8003; do \
		node=$${node_port%:*}; \
		port=$${node_port#*:}; \
		pid_file=raft-node-$$node.pid; \
		deadline=$$(( $$(date +%s) + $(CLUSTER_READY_TIMEOUT) )); \
		while ! nc -z 127.0.0.1 $$port >/dev/null 2>&1; do \
			if [ -f $$pid_file ]; then \
				pid=$$(cat $$pid_file); \
				if ! kill -0 $$pid >/dev/null 2>&1; then \
					echo " node $$node exited before 127.0.0.1:$$port became ready."; \
					echo " tail of raft-node-$$node.out:"; \
					tail -n 40 raft-node-$$node.out 2>/dev/null || true; \
					echo " tail of raft-node-$$node.log:"; \
					tail -n 40 raft-node-$$node.log 2>/dev/null || true; \
					$(MAKE) --no-print-directory stop-cluster >/dev/null; \
					exit 1; \
				fi; \
			else \
				echo " missing $$pid_file while waiting for 127.0.0.1:$$port."; \
				$(MAKE) --no-print-directory stop-cluster >/dev/null; \
				exit 1; \
			fi; \
			if [ $$(date +%s) -ge $$deadline ]; then \
				echo " timed out waiting for node $$node on 127.0.0.1:$$port."; \
				echo " tail of raft-node-$$node.out:"; \
				tail -n 40 raft-node-$$node.out 2>/dev/null || true; \
				echo " tail of raft-node-$$node.log:"; \
				tail -n 40 raft-node-$$node.log 2>/dev/null || true; \
				$(MAKE) --no-print-directory stop-cluster >/dev/null; \
				exit 1; \
			fi; \
			sleep 1; \
		done; \
		pid=$$(cat $$pid_file); \
		if ! kill -0 $$pid >/dev/null 2>&1; then \
			echo " node $$node exited while 127.0.0.1:$$port appeared reachable."; \
			echo " tail of raft-node-$$node.out:"; \
			tail -n 40 raft-node-$$node.out 2>/dev/null || true; \
			echo " tail of raft-node-$$node.log:"; \
			tail -n 40 raft-node-$$node.log 2>/dev/null || true; \
			$(MAKE) --no-print-directory stop-cluster >/dev/null; \
			exit 1; \
		fi; \
		echo " node $$node is listening on 127.0.0.1:$$port"; \
	done

## cluster-smoke: Start a local cluster, run set/get/delete, then stop it.
cluster-smoke:
	@set -e; \
	$(MAKE) --no-print-directory cluster; \
	trap '$(MAKE) --no-print-directory stop-cluster >/dev/null' EXIT; \
	key="make_cluster_smoke"; \
	value="ok"; \
	echo " running cluster smoke test..."; \
	./$(CLIENT_BINARY) set $$key $$value; \
	output=$$(./$(CLIENT_BINARY) get $$key); \
	echo "$$output"; \
	echo "$$output" | grep -q "Value: $$value"; \
	./$(CLIENT_BINARY) delete $$key; \
	echo " cluster smoke test passed."

## stop-cluster: Stop the local cluster.
stop-cluster:
	@echo " stopping cluster..."
	@for node in 1 2 3; do \
		pid_file=raft-node-$$node.pid; \
		if [ -f $$pid_file ]; then \
			pid=$$(cat $$pid_file); \
			if kill -0 $$pid >/dev/null 2>&1; then \
				kill $$pid >/dev/null 2>&1 || true; \
				for _ in 1 2 3 4 5; do \
					if ! kill -0 $$pid >/dev/null 2>&1; then \
						break; \
					fi; \
					sleep 1; \
				done; \
				if kill -0 $$pid >/dev/null 2>&1; then \
					echo " force stopping node $$node (pid $$pid)"; \
					kill -9 $$pid >/dev/null 2>&1 || true; \
				fi; \
			fi; \
			rm -f $$pid_file; \
		fi; \
	done
	@echo " cluster stopped."

# ==================== Code Style ====================

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

# ==================== Cleanup ====================

## clean: Remove all generated files, test artifacts, and clear Go test cache.
clean:
	@echo " cleaning up..."
	@go clean -testcache
	@rm -f coverage.txt coverage.html unittest.txt $(SERVER_BINARY) $(CLIENT_BINARY) raft-node-*.log raft-node-*.out raft-node-*.pid
	@rm -rf benchmark_results data
	@find . -type f -name "*.sst" -delete
	@find . -type f -name "*.wal" -delete
	@find . -type f -name "*.wf" -delete
	@find . -type f -name "*.log" -delete
	@find . -type d -name "*-level" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "test-temp-*" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "__debug_bin*" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf /tmp/go-kv-test-* 2>/dev/null || true
	@echo " cleanup complete."
