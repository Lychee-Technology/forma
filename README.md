# Forma

Forma is a general-purpose data management system built on PostgreSQL. It uses JSON Schema for data definition and a dual storage model (Hot Fields Table + EAV Table) to handle highly dynamic data structures without schema migrations.

## Prerequisites

- **Go** 1.26+
- **Docker or Podman**, plus Docker Compose compatibility (for local PostgreSQL and S3-compatible storage)
- **Bun** (for E2E test scripts)
- **k6** (for load testing; Docker fallback available)

## Quick Start

```bash
# Clone and enter the project
git clone https://github.com/lychee-technology/forma.git
cd forma

# Start PostgreSQL via Docker Compose
docker compose -f deploy/docker-compose.yml up -d

# Build all binaries
make build-all

# Initialize database tables (required once)
./build/tools init-db \
  --db-host localhost \
  --db-port 5432 \
  --db-name forma \
  --db-user postgres \
  --db-password postgres \
  --db-ssl-mode disable \
  --schema-dir cmd/server/schemas

# Start the server
SCHEMA_DIR=cmd/server/schemas ./build/server
```

Or use the convenience script that does all of the above:

```bash
./scripts/local_server.sh
```

The server listens on port `8080` by default. Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `forma` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `` | Database password |
| `DB_SSL_MODE` | `disable` | SSL mode |
| `SCHEMA_DIR` | `` | Directory containing schema JSON files |
| `PORT` | `8080` | HTTP listen port |

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/{schema}` | Create records (single object or array) |
| `GET` | `/api/v1/{schema}/{row_id}` | Get a single record |
| `GET` | `/api/v1/{schema}` | Query records with pagination (`?page=&items_per_page=&sort_by=&sort_order=&attrs=`) |
| `PUT` | `/api/v1/{schema}/{row_id}` | Update a record |
| `DELETE` | `/api/v1/{schema}` | Batch delete (JSON body: array of row_id strings) |
| `GET` | `/api/v1/search` | Cross-schema search (`?schemas=&q=&page=&items_per_page=`) |
| `POST` | `/api/v1/advanced_query` | Advanced query with condition DSL (JSON body) |

## Testing

### Unit & Integration Tests

```bash
# Run all unit and integration tests
make test

# Run with coverage report
make coverage

# Run linter
make lint
```

### Go E2E Harness (container-based)

Uses Docker or Podman through testcontainers. Validates the three-tier federated query architecture (Postgres Hot + S3 Delta/Base → DuckDB merge-on-read).

```bash
# Auto-detect Docker or Podman, configure testcontainers, and run make test.
./scripts/test_with_container_runtime.sh

# Smoke test: verify infrastructure starts
go test -v ./internal/e2e_harness/... -timeout=5m

# Full federated suite (functional + consistency + failure modes)
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m

# Performance tests only (longer timeout)
go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m
```

The runtime helper honors `DOCKER_HOST`. With rootless Podman it starts the user
socket at `$XDG_RUNTIME_DIR/podman/podman.sock`, exports the Docker-compatible
endpoint, disables the Ryuk reaper, and runs `make test`.

### Bun E2E (black-box API validation)

Requires a running Forma server and PostgreSQL.

```bash
cd tests/e2e
cp .env.example .env
bun install

# Default pipeline: register schemas → generate data → CDC flush → federated check
bun run test

# Individual steps
bun run register-schemas
bun run gen-data -- --schema all --count 10000
bun run cdc-flush
bun run federated-check

# Extended steps
bun run cdc-init          # Backfill base parquet (add -- --replace-delta to re-init over existing delta files)
bun run compactor -- --all # Merge delta into base
```

### k6 Load Testing

```bash
cd tests/e2e
bun run build-k6

bun run k6-smoke   # 5 VUs, 30s
bun run k6-full    # 30 VUs, 2m
bun run k6-perf    # 100 VUs, 5m
```

### Benchmarks

```bash
make benchmark-smoke       # CI smoke validation
make benchmark-regression  # Small live subset
make benchmark-heavy       # Heavy planning set
```

## Documentation

- [Documentation Index](docs/index.md)
- [Error Handling](docs/error-handling.md)
- [Schema Consistency Migration Guide](docs/schema-consistency-migration.md)
- [E2E Test Matrix](docs/e2e-tests-en.md)
- [Integration Test Cases](docs/integ-tests-en.md)
- [Go E2E Harness README](internal/e2e_harness/README.md)
- [Bun E2E README](tests/e2e/README.md)

## Why Forma?

Forma targets the gap between rigid RDBMS schemas and schema-less NoSQL stores:

- **Zero-Downtime Schema Evolution** — Add or modify fields by updating JSON Schema metadata; no `ALTER TABLE` required.
- **ACID on PostgreSQL** — Inherits full transactional guarantees from Postgres.
- **Smart SQL Generation** — CTE + JSON_AGG eliminates N+1 queries in EAV models.
- **Federated Query (Lakehouse)** — PostgreSQL for OLTP, DuckDB + Parquet on S3 for OLAP. Anti-Join + Dirty Set ensures consistency across tiers.
