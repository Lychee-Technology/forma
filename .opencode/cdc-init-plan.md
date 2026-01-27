# CDC Init Tool Implementation Plan

## Purpose
Initialize S3 parquet files from existing `entity_main` + `eav_data` for users enabling CDC on an existing Forma deployment.

## Design Decisions
1. **Output Type**: Base files (~256MB) - better for query performance
2. **Change Log**: Skip change_log population - simpler approach, assumes CDC will only track future changes
3. **Batch Size**: 50,000 rows per batch for faster initial export

## Implementation Tasks

### Task 1: Modify `cmd/tools/main.go`
- [ ] Add `cdc-init` case to switch statement
- [ ] Add help text for cdc-init command

### Task 2: Add `BuildBasePath` helper to `internal/cdc/manifest.go`
- [ ] Add function to generate base file path: `<prefix>/<schema_id>/<min_row_id>_<max_row_id>.parquet`

### Task 3: Create `internal/cdc/init_exporter.go`
- [ ] Create `buildBaseExportSQL()` - SQL builder for base file export
- [ ] Differs from delta export: no change_log, direct entity_main + eav_data query
- [ ] Uses `ltbase_created_at` as `time_slot` for base files

### Task 4: Create `cmd/tools/cdc_init.go`
- [ ] CLI argument parsing (reuse patterns from cdc_flush.go)
- [ ] Main orchestration logic:
  - Get all schema IDs from schema_registry
  - For each schema: count rows, paginate, export batches
- [ ] Helper functions:
  - `selectEntityMainBatch()` - paginate through entity_main
  - `getEntityMainCount()` - count rows per schema
  - `initSchema()` - orchestrate single schema export

### Task 5: Testing
- [ ] Add integration test in e2e suite
- [ ] Test with existing data scenario

## CLI Interface

```bash
forma-tools cdc-init [options]

Options:
  # Database connection (same as cdc-flush)
  --pg-host, --pg-port, --pg-user, --pg-password, --pg-db, --pg-ssl-mode
  
  # Table names
  --entity-main-table    Entity main table name (default: entity_main)
  --eav-table            EAV data table name (default: eav_data)
  
  # S3 settings (same as cdc-flush)
  --s3-bucket, --s3-prefix, --s3-endpoint, --s3-region, --s3-use-ssl, --s3-use-path
  
  # Schema registry
  --schema-registry-table  Schema registry table name (required)
  --schema-dir             Directory with *_attributes.json files (required)
  
  # Init-specific options
  --batch-size            Rows per batch (default: 50000)
  --schema-id             Specific schema ID to init (optional, default: all schemas)
  --dry-run               Preview without writing files
  
  # DuckDB settings
  --duckdb-path, --duck-threads, --duck-mem-limit, --query-timeout
  
  # Compression
  --parquet-compression, --parquet-compression-level
```

## Data Flow

```
1. Get all schema IDs from schema_registry
   SELECT schema_id FROM schema_registry

2. For each schema_id:
   a. Count rows: SELECT COUNT(*) FROM entity_main
      WHERE ltbase_schema_id = ? AND ltbase_deleted_at IS NULL
   b. Calculate batch count = ceil(total_rows / batch_size)

3. For each batch (paginate by ltbase_row_id):
   a. SELECT row_ids: ORDER BY ltbase_row_id LIMIT batch_size OFFSET ...
   b. Export via DuckDB to s3://bucket/base/<schema_id>/<min>_<max>.parquet

4. Report: total rows exported, parquet files created per schema
```

## S3 Path Convention

- **Base files**: `s3://<bucket>/base/<schema_id>/<min_row_id>_<max_row_id>.parquet`
- **Delta files**: `s3://<bucket>/delta/<schema_id>/<uuid_v7>.parquet` (existing)
