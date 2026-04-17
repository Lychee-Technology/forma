# Target: `internal/cdc/export_sql_builder.go`

## Why This Target

`export_sql_builder.go` assembles the SQL that drives CDC exports, including option defaults, main-table vs EAV projection splitting, and aggregation assembly.
It is high-value CDC logic with a large pure-string-builder surface that can be exercised locally without external services.

## Primary Test Files

- `internal/cdc/export_sql_shared_test.go`
- `internal/cdc/init_exporter_sql_test.go`
- `internal/cdc/duckdb_exporter_sql_test.go`

## Functions In Scope

- `resolveExportSQLOptions`
- `buildParquetCopyOptions`
- `resolveMainAndEAVTableNames`
- `buildMainEntityQuery`
- `buildEAVQuery`
- `buildSchemaDrivenProjection`
- `buildEAVAggregationSQL`

## Priority Scenarios

1. Given CDC config omits parquet settings, when export SQL options are resolved, then default compression, level, and parquet version are used.
2. Given CDC config omits the DuckDB memory limit, when export SQL options are resolved, then the supplied default memory limit is used.
3. Given custom table names are empty or unsafe, when table names are resolved, then sanitized names are used and empty values fall back to defaults.
4. Given `activeOnly` is true, when the main-entity query is built, then the deleted-row guard is included.
5. Given `activeOnly` is false, when the main-entity query is built, then the deleted-row guard is omitted.
6. Given `eavAttrIDs` is empty, when the EAV query is built, then no `attr_id IN (...)` filter is added.
7. Given schema-driven projection mixes bound and unbound attributes, when the projection is built, then main projections, EAV aggregates, and selected attr ids are split correctly.
8. Given multiple attributes share the same bound main column, when schema-driven projection is built, then the main column list does not duplicate the underlying column.
9. Given no EAV aggregates exist, when EAV aggregation SQL is built, then the query still groups by `row_id` correctly.
10. Given bool, numeric, and text attributes participate in schema-driven projection, when projections are built, then casts and aliases reflect the metadata.

## Keep Bias

Prefer candidates that harden defaulting behavior, table-name resolution, and schema-driven projection splits.
Do not spend iterations re-checking only the top-level `buildBaseExportSQL` and `buildExportSQL` happy paths that already have broad coverage.

## Constraints

- Prefer extending `internal/cdc/export_sql_shared_test.go` for reusable semantics and helper-level coverage.
- Use the other nearby exporter SQL test files only when the scenario naturally belongs to delta/base-specific assembly.
- Favor semantic substring assertions over full SQL snapshots.
- Keep all tests local and deterministic; do not require Docker, S3, or live database setup for this target.
