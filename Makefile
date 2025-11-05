.PHONY: test build clean all

# Binary name
BINARY_NAME=server

# Build directory
BUILD_DIR=build

# Platforms and architectures
PLATFORMS=darwin-amd64 darwin-arm64 linux-amd64 linux-arm64

# Entry point
MAIN_PATH=./cmd/server

# Default target
all: test build

# Run tests
test:
	@echo "Running tests..."
	@go test ./...

# Build all platform variants
build: clean create-build-dir
	@echo "Building for all platforms..."
	@for platform in $(PLATFORMS); do \
		GOOS=$$(echo $$platform | cut -d- -f1); \
		GOARCH=$$(echo $$platform | cut -d- -f2); \
		OUTPUT=$(BUILD_DIR)/$(BINARY_NAME)-$$platform; \
		echo "Building $$platform -> $$OUTPUT"; \
		GOOS=$$GOOS GOARCH=$$GOARCH go build -ldflags="-s -w" -o $$OUTPUT $(MAIN_PATH); \
	done
	@echo "Build complete. Binaries in $(BUILD_DIR)/"

# Create build directory
create-build-dir:
	@mkdir -p $(BUILD_DIR)

# Clean build directory
clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete."
