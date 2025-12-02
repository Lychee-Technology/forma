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
SCHEMA_DIR="$PROJECT_DIR/cmd/server/schemas"

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
    docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" down --remove-orphans 2>/dev/null || true  
    exit 0
}

# Set up trap to catch SIGINT and SIGTERM
trap cleanup SIGINT SIGTERM

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
docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" down --remove-orphans || true
print_info "Starting PostgreSQL container..."
docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" up -d

# ============================================================================
# 3. Build all components
# ============================================================================
# Compile Go code
print_info "Compiling Go code..."

pushd "$PROJECT_DIR"
make clean
make build-all
popd

# ===========================================================================
# 4. Wait for PostgreSQL to be ready
# ===========================================================================
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
        docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml"  logs
        exit 1
    fi
    sleep 1
done

# ============================================================================
# 5. Initialize database
# ============================================================================
./build/tools init-db \
  --db-host "$DB_HOST" \
  --db-port "$DB_PORT" \
  --db-name "$DB_NAME" \
  --db-user "$DB_USER" \
  --db-password "$DB_PASSWORD" \
  --db-ssl-mode "$DB_SSL_MODE" \
  --schema-table "schema_registry_dev" \
  --eav-table "eav_data_dev" \
  --entity-main-table "entity_main_dev" \
  --schema-dir "$SCHEMA_DIR"

# ============================================================================
# 7. Start server process
# ============================================================================
print_success "All preparations complete!"
print_info "Starting server on port $PORT..."
print_info "Database: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo ""

SCHEMA_DIR="$SCHEMA_DIR" "$PROJECT_DIR/build/server"