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
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="forma"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_SSL_MODE="disable"
SCHEMA_DIR="$PROJECT_DIR/cmd/sample/schemas"
print_success "Environment variables configured"

make build-all

# ============================================================================
# 2. Start PostgreSQL via Docker Compose
# ============================================================================
pushd deploy
docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" down --remove-orphans || true
print_info "Starting PostgreSQL container..."
docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" up -d

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
        docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" logs
        exit 1
    fi
    sleep 1
done
popd

./build/tools init-db \
  --db-host "$DB_HOST" \
  --db-port "$DB_PORT" \
  --db-name "$DB_NAME" \
  --db-user "$DB_USER" \
  --db-password "$DB_PASSWORD" \
  --db-ssl-mode "$DB_SSL_MODE" \
  --schema-table "schema_registry_sample" \
  --eav-table "eav_data_sample" \
  --entity-main-table "entity_main_sample" \
  --schema-dir "$SCHEMA_DIR"

print_success "Database initialized with sample schemas"

# ============================================================================
# 3. Run the sample application
# ============================================================================
# Cleanup function to stop containers on exit
cleanup() {
    print_warning "Shutting down..."
    print_info "Stopping PostgreSQL container..."
    docker compose -f "$PROJECT_DIR/deploy/docker-compose.yml" down --remove-orphans 2>/dev/null || true
    exit 0
}

# Set up trap to catch SIGINT and SIGTERM
trap cleanup SIGINT SIGTERM

print_info "Running sample ..."

"$PROJECT_DIR/build/sample" -csv "$PROJECT_DIR/cmd/sample/testdata/watches-sample.csv" -db "postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME" -schema-dir "$SCHEMA_DIR"
