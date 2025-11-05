#!/usr/bin/env bash

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Cleanup function to stop containers on exit
cleanup() {
    print_warning "Shutting down..."
    print_info "Stopping PostgreSQL container..."
    cd "$PROJECT_DIR" && docker compose -f deploy/docker-compose.yml down --remove-orphans 2>/dev/null || true
    exit 0
}

# Set up trap to catch SIGINT and SIGTERM
trap cleanup SIGINT SIGTERM

# Change to project directory
cd "$PROJECT_DIR"

# ============================================================================
# 1. Set environment variables
# ============================================================================
print_info "Setting environment variables..."
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="forma"
export DB_USER="postgres"
export DB_PASSWORD="postgres"
export DB_SSL_MODE="disable"
export PORT="8080"
print_success "Environment variables configured"

# ============================================================================
# 2. Start PostgreSQL via Docker Compose
# ============================================================================
docker compose -f deploy/docker-compose.yml down --remove-orphans || true
print_info "Starting PostgreSQL container..."
docker compose -f deploy/docker-compose.yml up -d

# Wait for PostgreSQL to be ready
print_info "Waiting for PostgreSQL to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec forma-postgres pg_isready -U postgres >/dev/null 2>&1; then
        print_success "PostgreSQL is ready"
        break
    fi
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        print_error "PostgreSQL failed to start after $max_attempts attempts"
        docker compose -f deploy/docker-compose.yml logs
        exit 1
    fi
    sleep 1
done

# ============================================================================
# 3. Detect OS and architecture, then compile Go code
# ============================================================================
print_info "Detecting OS and architecture..."
GOOS=$(go env GOOS)
GOARCH=$(go env GOARCH)
print_info "Building for: $GOOS/$GOARCH"

# Create build directory
mkdir -p build/schemas

# Compile Go code
print_info "Compiling Go code..."
GOOS=$GOOS GOARCH=$GOARCH go build -ldflags="-s -w" -o build/server ./cmd/server
print_success "Go code compiled successfully: build/server"

# ============================================================================
# 4. Copy JSON schema files to build directory
# ============================================================================
print_info "Copying JSON schema files..."
cp cmd/server/schemas/*.json build/schemas/ 2>/dev/null || true
print_success "Schema files copied to build/schemas/"

# ============================================================================
# 5. Start server process
# ============================================================================
print_success "All preparations complete!"
print_info "Starting server on port $PORT..."
print_info "Database: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo ""

pushd "$PROJECT_DIR/build"
./server
popd