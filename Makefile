.PHONY: test test-unit coverage build build-tools build-benchmark build-all build-lambda clean all create-build-dir link

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
GOENV=GOCACHE=$(GOCACHE)

GOOS=$(shell $(GOENV) go env GOOS)
GOARCH=$(shell $(GOENV) go env GOARCH)

# Default target
all: test build-all

# Run unit tests
test: test-unit

test-unit:
	@echo "Running unit tests..."
	@$(GOENV) go test ./...

# Run unit tests with coverage report
coverage: create-build-dir
	@echo "Running unit tests with coverage..."
	@$(GOENV) go test ./... -coverprofile=$(COVERAGE_PROFILE)
	@$(GOENV) go tool cover -func=$(COVERAGE_PROFILE)
	@$(GOENV) go tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	@echo "Coverage report written to $(COVERAGE_HTML)"

# Build server for current platform
build-server: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH)"
	@CGO_ENABLED=1 $(GOENV) go build -buildvcs=false -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH) $(MAIN_SERVER)
	@echo "Server build complete."

# Build tools for current platform
build-tools: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH)"
	@$(GOENV) go build -buildvcs=false -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH) $(MAIN_TOOLS)
	@echo "Tools build complete."

# Build sample for current platform
build-sample: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH)"
	@$(GOENV) go build -buildvcs=false -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH) $(MAIN_SAMPLE)
	@echo "Sample build complete."

# Build Lambda function for AWS Lambda (ARM64, Amazon Linux 2023)
# Note: This target is for local testing. For CI/CD, use the GitHub Actions workflow
# which builds in an Amazon Linux 2023 ARM container for CGO compatibility.
build-lambda: create-build-dir
	@echo "Building Lambda function for linux-arm64 -> $(BUILD_DIR)/$(BINARY_LAMBDA)"
	@CGO_ENABLED=1 GOOS=linux GOARCH=arm64 $(GOENV) go build -buildvcs=false -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_LAMBDA) $(MAIN_LAMBDA)
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
