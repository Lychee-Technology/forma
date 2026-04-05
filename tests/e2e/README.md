# Forma E2E Tests

End-to-end tests for validating Forma's CDC (Change Data Capture) and Federated Query functionality.

For the repo-wide E2E inventory across both Go harness tests and Bun/k6 workflows, see [../../docs/e2e-tests-cn.md](../../docs/e2e-tests-cn.md) and [../../docs/e2e-tests-en.md](../../docs/e2e-tests-en.md).

## Prerequisites

- [Bun](https://bun.sh/) runtime
- [k6](https://k6.io/) for load testing, or Docker for the built-in fallback runner
- Docker & Docker Compose (for local infrastructure)
- Go toolchain (for building CDC tools)

## Quick Start

```bash
# 1. Start the Forma server
./scripts/local_server.sh &

# 2. Setup E2E tests
cd tests/e2e
cp .env.example .env
bun install

# 3. Run the default Bun E2E flow
bun run test
```

`bun run test` only runs `register-schemas -> gen-data -> cdc-flush -> federated-check`. `cdc-init`, `compactor`, and k6 are available as manual follow-up steps.

For a one-shot local performance run that starts Forma, waits for `/health`, runs `k6-full`, and then cleans up, use:

```bash
./scripts/run_local_k6_full.sh
```

## Project Structure

```
tests/e2e/
├── lib/
│   ├── env.ts          # Environment configuration loader
│   └── http.ts         # HTTP client with retry logic
├── scripts/
│   ├── register-schemas.ts  # Register lead/visit/log schemas
│   ├── gen-data.ts          # Generate test data via API
│   ├── cdc-init.ts          # Backfill base parquet from existing data
│   ├── cdc-flush.ts         # Trigger CDC flush to S3
│   ├── compactor.ts         # Merge delta parquet files into base
│   └── federated-check.ts   # Validate federated query consistency
├── k6/
│   ├── scenarios.ts    # k6 load test scenarios
│   └── dist/           # Compiled k6 bundle
├── reports/            # JSON test reports
├── .env.example        # Environment template
├── package.json
└── tsconfig.json
```

## Scripts

### Default Flow vs Extended Steps

- Default flow: `bun run test`
- Included by default: `register-schemas`, `gen-data`, `cdc-flush`, `federated-check`
- Manual extensions: `cdc-init`, `compactor`, `build-k6`, `k6-smoke`, `k6-full`, `k6-perf`

### Register Schemas

Registers the lead, visit, and log schemas with Forma:

```bash
bun run register-schemas
```

### Generate Test Data

Generates test data via the Forma API:

```bash
# Generate 10,000 leads
bun run gen-data -- --schema lead --count 10000 --batch-size 500

# Generate all schemas
bun run gen-data -- --schema all --count 5000

# Use defaults from .env
bun run gen-data
```

### CDC Init

Initializes base parquet files from existing `entity_main` + `eav_data`. This is mainly used for backfilling an existing deployment before incremental CDC takes over.

```bash
# Initialize all schemas
bun run cdc-init

# Dry run
bun run cdc-init -- --dry-run

# Single schema with custom target file size
bun run cdc-init -- --schema-id 101 --target-file-size-mb 256
```

### CDC Flush

Triggers the CDC flush process to export change_log data to S3 as Parquet files:

```bash
# Run CDC flush
bun run cdc-flush

# Dry run (shows what would be flushed)
bun run cdc-flush -- --dry-run
```

Notes:
- Schema registry is required: set `SCHEMA_TABLE` and `SCHEMA_DIR` in `.env` so the CDC tools can generate schema-aware parquet.
- Optional: tune `CDC_EST_ROW_BYTES` and `CDC_MAX_BATCH_BYTES` to split batches by estimated file size (targets 10–50MB).

### Compactor

Runs compaction to merge delta parquet files into the base tier.

```bash
# Compact the default schema
bun run compactor

# Compact all known schemas
bun run compactor -- --all

# Compact one schema
bun run compactor -- --schema-id 101
```

### Federated Check

Validates data consistency between Forma API and direct Postgres queries:

```bash
# Check all schemas with default sample size
bun run federated-check

# Check specific schema
bun run federated-check -- --schema lead --sample-size 200

# Full scan (compare all records)
bun run federated-check -- --full-scan
```

## Load Testing with k6

### Build k6 Bundle

```bash
bun run build-k6
```

### Run Scenarios

```bash
# Smoke test (5 VUs, 30s)
bun run k6-smoke

# Full test (30 VUs, 2m)
bun run k6-full

# Performance test (100 VUs, 5m)
bun run k6-perf

# Custom run with environment variables
k6 run -e BASE_URL=http://localhost:8080 -e SCENARIO=full k6/dist/bundle.js
```

The `k6-*` scripts prefer a local `k6` binary. If `k6` is not installed, they automatically fall back to `docker run grafana/k6` when Docker is available.

### k6 Thresholds

| Metric | Threshold | Description |
|--------|-----------|-------------|
| `forma_federated_query_duration` | p95 < 200ms | Primary SLA for federated queries |
| `forma_query_duration` | p95 < 100ms | General query latency |
| `forma_query_success` | rate > 99% | Query success rate |
| `http_req_failed` | rate < 1% | HTTP error rate |

## Environment Variables

See `.env.example` for all available configuration options:

| Variable | Default | Description |
|----------|---------|-------------|
| `BASE_URL` | `http://localhost:8080` | Forma API base URL |
| `PG_HOST` | `localhost` | Postgres host |
| `PG_PORT` | `5432` | Postgres port |
| `PG_USER` | `postgres` | Postgres user |
| `PG_PASSWORD` | `postgres` | Postgres password |
| `PG_DB` | `forma` | Postgres database |
| `PG_SSL_MODE` | `disable` | Postgres sslmode (disable, require, verify-full) |
| `SCHEMA_TABLE` | `schema_registry_dev` | Schema registry table for CDC/schema-aware export |
| `SCHEMA_DIR` | `../../cmd/server/schemas` | Directory containing schema JSON files |
| `S3_ENDPOINT` | `http://localhost:19000` | S3/RustFS endpoint |
| `S3_BUCKET` | `forma-cdc` | S3 bucket for CDC data |
| `DATASET_SIZE` | `10000` | Default dataset size |
| `BATCH_SIZE` | `500` | Default batch size |
| `CDC_EST_ROW_BYTES` | `0` | Estimated bytes per row (0 to auto) for CDC batch sizing |
| `CDC_MAX_BATCH_BYTES` | `0` | Max bytes per CDC batch (0 to auto) |

## Reports

All scripts generate JSON reports in the `reports/` directory:

- `schema-registration.json` - Schema registration results
- `data-gen.json` - Data generation results
- `cdc-init.json` - CDC base initialization results
- `cdc-flush.json` - CDC flush results
- `compactor.json` - Compaction results
- `federated-check.json` - Federated query validation results
- `k6-*.json` - k6 load test results

## Full E2E Flow

The default Bun E2E flow validates:

1. **Schema Registration** - Registers schemas via API
2. **Data Generation** - Creates test records in Postgres (hot tier)
3. **CDC Flush** - Exports data to S3 as Parquet (cold tier)
4. **Federated Check** - Validates query consistency across tiers

```bash
# Run the default flow
bun run test
```

Extended validation is available as manual follow-up:

1. **CDC Init** - Backfills base parquet for existing data
2. **Compaction** - Merges delta parquet files into base
3. **Load Testing** - Validates p95 latency under load

```bash
# Optional backfill
bun run cdc-init

# Default flow steps
bun run register-schemas
bun run gen-data -- --schema all --count 10000
bun run cdc-flush
bun run federated-check

# Optional compaction and load
bun run compactor -- --all
bun run build-k6
bun run k6-full
```

## Troubleshooting

### CDC tools not found

Ensure you've built the CDC tools:

```bash
make build-tools
```

### Connection refused

Ensure Docker infrastructure is running:

```bash
cd deploy && docker compose up -d
```

### Schema not found

Register schemas first:

```bash
bun run register-schemas
```

### k6 not found

Either install k6 locally, or rely on the Docker fallback used by `bun run k6-smoke|k6-full|k6-perf`.

Install k6 locally:

```bash
# macOS
brew install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```
