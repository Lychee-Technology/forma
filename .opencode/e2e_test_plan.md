# E2E Test Plan: CDC + Federated Query

## Overview
End-to-end testing suite for validating CDC (Change Data Capture) flush to S3 and federated query correctness/performance across Postgres hot data and DuckDB/S3 cold data.

## Target SLAs
- Federated query p95 latency: < 200ms
- Data volume: up to 100M rows (configurable; default local runs use 50k-100k)

## Technology Stack
- **Runtime**: Bun (TypeScript)
- **Load Testing**: k6
- **Infrastructure**: Docker Compose (Postgres 16 + RustFS/S3-compatible)
- **Reporting**: JSON artifacts

## Directory Structure
```
tests/e2e/
├── .env.example          # Environment configuration template
├── package.json          # Bun project config
├── tsconfig.json         # TypeScript config
├── README.md             # Local run instructions
├── lib/
│   ├── env.ts            # Environment loader
│   └── http.ts           # HTTP client wrapper
├── scripts/
│   ├── register-schemas.ts   # Schema registration
│   ├── gen-data.ts           # Data generation via API
│   ├── cdc-flush.ts          # CDC flush trigger
│   └── federated-check.ts    # Correctness validation
├── k6/
│   ├── scenarios.ts      # k6 test scenarios (smoke/full/perf)
│   └── dist/             # Compiled k6 bundle
└── reports/              # JSON output artifacts
```

## Test Schemas
Using existing Forma schemas:
- `lead.json` - CRM lead records (complex nested objects)
- `visit.json` - Property visit records (references lead)
- `log.json` - Activity logs (references lead/visit)

## Implementation Steps

### 1. Project Scaffold
- Create `tests/e2e` directory structure
- Add `package.json` with bun config
- Add `tsconfig.json` targeting ES2020
- Create `.env.example` with all required variables

### 2. HTTP Client (`lib/http.ts`)
- Thin wrapper over fetch with base URL
- JSON encoding/decoding
- Retry logic (3x on 429/5xx)
- Optional auth header support

### 3. Environment Config (`lib/env.ts`)
- Load `.env` variables
- Export typed config object
- Validate required variables

### 4. Schema Registration (`scripts/register-schemas.ts`)
- Read schemas from `cmd/server/schemas/`
- POST to Forma schema registry endpoint
- Idempotent (skip if exists)
- Output: `reports/schema-registration.json`

### 5. Data Generation (`scripts/gen-data.ts`)
- CLI args: `--schema`, `--count`, `--batch-size`
- Generate payloads matching schema requirements:
  - Lead: id, tenantId, ownerUserId, pipeline, stage, status, timestamps, contact, source
  - Visit: id, leadId, userId, propertyId, scheduledStartAt, status
  - Log: id, ownerId, type, createdAt, optional leadId/visitId
- Include `trace_id` + `seq` for auditing
- Batch POSTs to Forma API
- Output: `reports/data-gen.json`

### 6. CDC Flush (`scripts/cdc-flush.ts`)
- Spawn `./build/tools cdc-flush` with env-configured args
- Capture stdout/stderr and exit code
- Output: `reports/cdc-flush.json`

### 7. Federated Check (`scripts/federated-check.ts`)
- Query Forma federated endpoint with test filter
- Query Postgres directly for comparison
- Compare: row counts, ID sets, optional checksums
- Output: `reports/federated-check.json`

### 8. k6 Scenarios (`k6/scenarios.ts`)
- **smoke**: 5 VUs, 2 min, basic search
- **full**: 30 VUs, 5 min, mixed read/write
- **perf**: 100 VUs, 5 min, indexed queries, p95<200ms threshold
- Build with esbuild for k6 compatibility
- Output: `reports/k6-summary.json`

## Local Execution Flow
```bash
# 1. Start infrastructure
docker compose up -d

# 2. Setup test environment
cd tests/e2e
cp .env.example .env
# Edit .env with local values
bun install

# 3. Register schemas
bun run scripts/register-schemas.ts

# 4. Generate test data
bun run scripts/gen-data.ts --schema lead --count 10000 --batch-size 500
bun run scripts/gen-data.ts --schema visit --count 5000 --batch-size 500
bun run scripts/gen-data.ts --schema log --count 5000 --batch-size 500

# 5. Trigger CDC flush
bun run scripts/cdc-flush.ts

# 6. Validate federated query correctness
bun run scripts/federated-check.ts

# 7. Run performance tests
bunx esbuild k6/scenarios.ts --bundle --format=esm --target=es2020 \
  --platform=neutral --external:k6 --external:'k6/*' --outfile=k6/dist/bundle.js
k6 run --out json=reports/k6-summary.json k6/dist/bundle.js
```

## Environment Variables (.env)
```
# Forma API
BASE_URL=http://localhost:8080
AUTH_TOKEN=                    # Optional bearer token

# Postgres (for CDC flush)
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=postgres
PG_DB=forma

# S3/RustFS
S3_ENDPOINT=http://localhost:19000
S3_BUCKET=forma-cdc
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio_password

# Test parameters
DATASET_SIZE=10000
BATCH_SIZE=500
```

## Assumptions
- Schema registration: POST `/api/v1/schema` with `{name, schema}` body
- Entity creation: POST `/api/v1/{schemaName}` with entity body
- Federated search: GET/POST `/api/v1/search` or `/api/v1/advanced_query`
- CDC flush CLI at `./build/tools cdc-flush`

## Success Criteria
- All schemas registered successfully
- Data generation completes without errors
- CDC flush marks all change_log rows as flushed
- Federated query returns correct row counts matching Postgres
- k6 perf scenario: p95 < 200ms under load
