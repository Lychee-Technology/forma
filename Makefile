.PHONY: test test-unit lint coverage build build-tools build-benchmark benchmark-smoke benchmark-regression benchmark-heavy benchmark-heavy-live build-all build-lambda clean all create-build-dir link validate-schema-consistency

# Binary names
BINARY_SERVER=server
BINARY_TOOLS=tools
BINARY_SAMPLE=sample
BINARY_LAMBDA=bootstrap

# Build directory
BUILD_DIR=build

# Coverage output
COVERAGE_PROFILE=$(BUILD_DIR)/coverage.out
COVERAGE_HTML=$(BUILD_DIR)/coverage.html

# Entry points
MAIN_SERVER=./cmd/server
MAIN_TOOLS=./cmd/tools
MAIN_SAMPLE=./cmd/sample
MAIN_LAMBDA=./cmd/lambda

# Keep the Go build cache inside the workspace to avoid permission errors in restricted environments.
GOCACHE?=$(CURDIR)/.gocache
# Disable VCS stamping by default to prevent stat-cache permission warnings in sandboxed builds.
GOFLAGS?=-buildvcs=false
GOENV=GOCACHE=$(GOCACHE) GOFLAGS="$(GOFLAGS)"

GOOS=$(shell $(GOENV) go env GOOS)
GOARCH=$(shell $(GOENV) go env GOARCH)
TEST_PKGS=.
TEST_PKGS+= ./cdc
TEST_PKGS+= ./cmd/...
TEST_PKGS+= ./factory
TEST_PKGS+= ./internal/...

# Default target
all: test build-all

# Run unit tests
test: test-unit

test-unit:
	@echo "Running unit tests..."
	@$(GOENV) go test $(TEST_PKGS)

# Run linter (same as CI lint job)
lint:
	@echo "Installing golangci-lint..."
	@$(GOENV) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
	@echo "Running golangci-lint..."
	@PATH="$$($(GOENV) go env GOPATH)/bin:$$PATH" golangci-lint run --timeout=5m

# Run unit tests with coverage report
coverage: create-build-dir
	@echo "Running unit tests with coverage..."
	@$(GOENV) go test $(TEST_PKGS) -coverprofile=$(COVERAGE_PROFILE)
	@$(GOENV) go tool cover -func=$(COVERAGE_PROFILE)
	@$(GOENV) go tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	@echo "Coverage report written to $(COVERAGE_HTML)"

benchmark-smoke: create-build-dir
	@echo "Running benchmark smoke validation..."
	@mkdir -p .artifacts/benchmark/smoke
	@$(GOENV) go run ./cmd/benchmark baseline -preset ci-smoke -output-dir .artifacts/benchmark/smoke \
		-channel ci \
		-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
		-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

benchmark-regression: create-build-dir
	@echo "Running benchmark regression live subset..."
	@mkdir -p .artifacts/benchmark/regression
	@$(GOENV) go run ./cmd/benchmark baseline -preset small-live \
		-distribution uniform \
		-output-dir .artifacts/benchmark/regression \
		-channel manual \
		-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
		-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

benchmark-trend: create-build-dir
	@echo "Running benchmark trend analysis..."
	@$(GOENV) go run ./cmd/benchmark trend -history-dir .artifacts/benchmark

# Concurrency evidence sweep (#104): full small-live at C=1/2/4/8 plus the
# aggregated report. Each level is a complete live run (~35min+ on an idle
# machine, hours under load) — operator-initiated only, never CI.
benchmark-concurrency: create-build-dir
	@echo "Running concurrency evidence sweep (C=1,2,4,8; several hours)..."
	@mkdir -p .artifacts/benchmark/concurrency
	@for c in 1 2 4 8; do \
		$(GOENV) go run ./cmd/benchmark baseline -preset small-live \
			-distribution uniform \
			-concurrency $$c \
			-output-dir .artifacts/benchmark/concurrency \
			-channel manual -label concurrency-sweep-c$$c \
			-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
			-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "") || exit 1; \
	done
	@$(GOENV) go run ./cmd/benchmark concurrency-report \
		-input-dir .artifacts/benchmark/concurrency \
		-md-out .artifacts/benchmark/concurrency/concurrency-report.md \
		-json-out .artifacts/benchmark/concurrency/concurrency-report.json
	@echo "Concurrency report written to .artifacts/benchmark/concurrency/concurrency-report.md"

benchmark-heavy: create-build-dir
	@echo "Running benchmark heavy planning set..."
	@mkdir -p .artifacts/benchmark/heavy
	@$(GOENV) go run ./cmd/benchmark baseline -preset heavy-plan \
		-distribution uniform \
		-output-dir .artifacts/benchmark/heavy \
		-channel manual \
		-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
		-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

# Full live workload matrix at large scale (10M trades). Hours of wall clock
# and heavy RAM/disk — operator-initiated on an idle machine only, never CI.
# Truth-pass oracles are spot-check sampled (see the baseline runbook).
benchmark-heavy-live: create-build-dir
	@echo "Running benchmark heavy live set (hours; idle machine only)..."
	@mkdir -p .artifacts/benchmark/heavy-live
	@$(GOENV) go run ./cmd/benchmark baseline -preset heavy-live \
		-distribution hotspot-overlap \
		-output-dir .artifacts/benchmark/heavy-live \
		-channel manual \
		-git-sha $$(git rev-parse HEAD 2>/dev/null || echo "") \
		-git-ref $$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")

# Build server for current platform
build-server: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH)"
	@CGO_ENABLED=1 $(GOENV) go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH) $(MAIN_SERVER)
	@echo "Server build complete."

# Build tools for current platform
build-tools: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH)"
	@$(GOENV) go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH) $(MAIN_TOOLS)
	@echo "Tools build complete."

validate-schema-consistency: build-tools link
	@echo "Running schema consistency validator..."
	@./$(BUILD_DIR)/$(BINARY_TOOLS) validate-schema-consistency \
		--db-host $${DB_HOST:-localhost} \
		--db-port $${DB_PORT:-5432} \
		--db-user $${DB_USER:-postgres} \
		--db-password $${DB_PASSWORD:-postgres} \
		--db-name $${DB_NAME:-forma} \
		--db-ssl-mode $${DB_SSL_MODE:-disable} \
		--schema-registry-table $${SCHEMA_TABLE:-schema_registry_dev} \
		--schema-dir $${SCHEMA_DIR:-cmd/server/schemas} \
		--eav-table $${EAV_TABLE:-eav_data_dev}

# Build sample for current platform
build-sample: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH)"
	@$(GOENV) go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH) $(MAIN_SAMPLE)
	@echo "Sample build complete."

# Build Lambda function for AWS Lambda (ARM64, Amazon Linux 2023)
# Note: This target is for local testing. For CI/CD, use the GitHub Actions workflow
# which builds in an Amazon Linux 2023 ARM container for CGO compatibility.
build-lambda: create-build-dir
	@echo "Building Lambda function for linux-arm64 -> $(BUILD_DIR)/$(BINARY_LAMBDA)"
	@CGO_ENABLED=1 GOOS=linux GOARCH=arm64 $(GOENV) go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_LAMBDA) $(MAIN_LAMBDA)
	@echo "Lambda build complete."

# Build Lambda ZIP package (requires linux-arm64 build)
build-lambda-zip: build-lambda
	@echo "Creating Lambda deployment package..."
	@cd $(BUILD_DIR) && zip -j lambda.zip $(BINARY_LAMBDA)
	@echo "Lambda ZIP package created: $(BUILD_DIR)/lambda.zip"

# Build all binaries (server, tools, sample)
build-all: build-server build-tools build-sample link
	@echo "All builds complete. Binaries in $(BUILD_DIR)/"

# Create symlinks for current platform
link:
	@echo "Creating symlinks for current platform..."
	@CURRENT_OS=$$($(GOENV) go env GOOS); CURRENT_ARCH=$$($(GOENV) go env GOARCH); PLATFORM=$$CURRENT_OS-$$CURRENT_ARCH; \
	echo "Detected platform: $$PLATFORM"; \
	cd $(BUILD_DIR) || exit 1; \
	if [ -f "$(BINARY_SERVER)-$$PLATFORM" ]; then rm -f $(BINARY_SERVER); ln -s $(BINARY_SERVER)-$$PLATFORM $(BINARY_SERVER); echo "Linked $(BINARY_SERVER)-$$PLATFORM -> $(BINARY_SERVER)"; fi; \
	if [ -f "$(BINARY_TOOLS)-$$PLATFORM" ]; then rm -f $(BINARY_TOOLS); ln -s $(BINARY_TOOLS)-$$PLATFORM $(BINARY_TOOLS); echo "Linked $(BINARY_TOOLS)-$$PLATFORM -> $(BINARY_TOOLS)"; fi; \
	if [ -f "$(BINARY_SAMPLE)-$$PLATFORM" ]; then rm -f $(BINARY_SAMPLE); ln -s $(BINARY_SAMPLE)-$$PLATFORM $(BINARY_SAMPLE); echo "Linked $(BINARY_SAMPLE)-$$PLATFORM -> $(BINARY_SAMPLE)"; fi
	@echo "Symlinks created."

# Create build directory
create-build-dir:
	@mkdir -p $(BUILD_DIR) $(GOCACHE)

# Clean build directory
clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete."
