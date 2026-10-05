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
| `METRICS_STDOUT` | unset (off) | `true` writes every emitted metric as a JSON line on stdout (`docs/telemetry.md`) |
| `MAX_ENTITY_SIZE_BYTES` | `1048576` | Cap on every HTTP request body, trailing bytes included; a larger body answers `413` before it is decoded |
| `MAX_BATCH_SIZE` | `1000` | Cap on operations per batch create/update/delete; a larger batch answers `400` |
| `MAX_QUERY_ROWS` | `10000` | How deep offset pagination reaches: a query or search page whose window (`page` × `items_per_page`) ends past it answers `400`, so walk further by filtering the query (`0` disables) |
| `QUERY_TIMEOUT_SECONDS` | `30` | Budget for a get, query or search; an exceeded budget answers `504` (`0` disables) |
| `TRANSACTION_TIMEOUT_SECONDS` | `30` | Budget for one write transaction (create, update, delete, atomic batch); `0` disables |
| `DUCKDB_QUERY_TIMEOUT_SECONDS` | `30` | Budget for all the DuckDB work of one federated request, inside the query budget; `0` disables |
| `HTTP_READ_HEADER_TIMEOUT_SECONDS` | `10` | `http.Server` ReadHeaderTimeout |
| `HTTP_READ_TIMEOUT_SECONDS` | `30` | `http.Server` ReadTimeout (whole request, body included); a body still arriving when it expires answers `408` |
| `HTTP_WRITE_TIMEOUT_SECONDS` | `90` | `http.Server` WriteTimeout, armed before the body is read; must exceed `HTTP_READ_TIMEOUT_SECONDS` plus the largest budget (checked at boot) |
| `HTTP_IDLE_TIMEOUT_SECONDS` | `120` | `http.Server` IdleTimeout for keep-alive connections |
| `HTTP_MAX_HEADER_BYTES` | `1048576` | `http.Server` MaxHeaderBytes |

The limits and timeouts are validated before the server opens its database
connection: a negative value, a zero size or batch cap, a non-zero
`MAX_QUERY_ROWS` below the 100-row page cap, or a bounded
`HTTP_WRITE_TIMEOUT_SECONDS` that does not exceed `HTTP_READ_TIMEOUT_SECONDS`
plus the largest bounded budget (the query budget, or the DuckDB budget when
the query budget is `0`, or the transaction budget, whichever is longest)
fails startup with a message naming the field. The write deadline starts
when the headers have been read, so it has to cover the body upload, the
budget, and the response; a bounded write timeout therefore also needs a
bounded read timeout. An unparsable value keeps the default, as for every
other integer variable above.

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

Pagination is offset-based: `items_per_page` is capped at 100, and a page
whose window ends past `MAX_QUERY_ROWS` rows (10000 by default) answers `400`
naming the limit instead of scanning past every earlier row. To read further,
narrow the result with an `advanced_query` condition, for example on the sort
attribute past the last row already read.

A `date` or `datetime` attribute is returned as an RFC3339 string when its year
is 0000 to 9999, and otherwise as a string of its exact epoch milliseconds
(e.g. `"9223372036854775807"`), which a create or update accepts back unchanged
([docs/error-handling.md](docs/error-handling.md), #591).

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
