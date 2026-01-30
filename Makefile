.PHONY: test test-unit coverage build build-tools build-benchmark build-all clean all create-build-dir link

# Binary names
BINARY_SERVER=server
BINARY_TOOLS=tools
BINARY_SAMPLE=sample

# Build directory
BUILD_DIR=build

# Coverage output
COVERAGE_PROFILE=$(BUILD_DIR)/coverage.out
COVERAGE_HTML=$(BUILD_DIR)/coverage.html

# Entry points
MAIN_SERVER=./cmd/server
MAIN_TOOLS=./cmd/tools
MAIN_SAMPLE=./cmd/sample

GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)

# Default target
all: test build-all

# Run unit tests
test: test-unit

test-unit:
	@echo "Running unit tests..."
	@go test ./...

# Run unit tests with coverage report
coverage: create-build-dir
	@echo "Running unit tests with coverage..."
	@go test ./... -coverprofile=$(COVERAGE_PROFILE)
	@go tool cover -func=$(COVERAGE_PROFILE)
	@go tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	@echo "Coverage report written to $(COVERAGE_HTML)"

# Build server for current platform
build-server: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH)"
	@CGO_ENABLED=1 go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SERVER)-$(GOOS)-$(GOARCH) $(MAIN_SERVER)
	@echo "Server build complete."

# Build tools for current platform
build-tools: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH)"
	@go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_TOOLS)-$(GOOS)-$(GOARCH) $(MAIN_TOOLS)
	@echo "Tools build complete."

# Build sample for current platform
build-sample: create-build-dir
	@echo "Building $(GOOS)-$(GOARCH) -> $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH)"
	@go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_SAMPLE)-$(GOOS)-$(GOARCH) $(MAIN_SAMPLE)
	@echo "Sample build complete."

# Build all binaries (server, tools, sample)
build-all: build-server build-tools build-sample link
	@echo "All builds complete. Binaries in $(BUILD_DIR)/"

# Create symlinks for current platform
link:
	@echo "Creating symlinks for current platform..."
	@CURRENT_OS=$$(go env GOOS); CURRENT_ARCH=$$(go env GOARCH); PLATFORM=$$CURRENT_OS-$$CURRENT_ARCH; \
	echo "Detected platform: $$PLATFORM"; \
	cd $(BUILD_DIR) || exit 1; \
	if [ -f "$(BINARY_SERVER)-$$PLATFORM" ]; then rm -f $(BINARY_SERVER); ln -s $(BINARY_SERVER)-$$PLATFORM $(BINARY_SERVER); echo "Linked $(BINARY_SERVER)-$$PLATFORM -> $(BINARY_SERVER)"; fi; \
	if [ -f "$(BINARY_TOOLS)-$$PLATFORM" ]; then rm -f $(BINARY_TOOLS); ln -s $(BINARY_TOOLS)-$$PLATFORM $(BINARY_TOOLS); echo "Linked $(BINARY_TOOLS)-$$PLATFORM -> $(BINARY_TOOLS)"; fi; \
	if [ -f "$(BINARY_SAMPLE)-$$PLATFORM" ]; then rm -f $(BINARY_SAMPLE); ln -s $(BINARY_SAMPLE)-$$PLATFORM $(BINARY_SAMPLE); echo "Linked $(BINARY_SAMPLE)-$$PLATFORM -> $(BINARY_SAMPLE)"; fi
	@echo "Symlinks created."

# Create build directory
create-build-dir:
	@mkdir -p $(BUILD_DIR)

# Clean build directory
clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete."
