Plan for OLAP/DuckDB exporter alignment

Done
- Defaults raised to spec thresholds (20k rows / 1h) with coverage.
- Export now batch-scoped, path aligned, mem limit honored, and basic SQL tests added.
- Schema-aware projection path added (uses SchemaRegistry cache when provided) with type casting and attr_id filtering; byte-cap truncation and mark-flushed-by-ids wired.

Next steps
1) Enforce schema-aware export: require schema registry in CDC flush, validate cache presence before export; add env/flags already to E2E; fail fast if missing.
2) Batch sizing: replace truncate with multi-chunk batching using estimated row bytes and target window 10–50 MB; log planned vs actual.
3) Manifest/layout: write per-schema manifest (Base/Delta entries with size_bytes, min/max row_id/time_slot); use canonical `<project>/<schema_id>/<uuid>.parquet` and tmp swap.
4) Compaction: daily worker for new vs historical updates, dirty-ratio decision, base rewrite/atomic publish, present_<schema>.parquet roll-up when >256MB.
5) Merge-on-read SQL template: Base + Delta + PG buffer, ROW_NUMBER dedupe, soft-delete filter; expose for Advanced Search.
6) Tests/fixtures: DuckDB integration (non-empty parquet, typed projections, delete/dedupe), manifest/compaction decision tests.
7) Observability: metrics/logs for batch rows/bytes, file sizes, manifest writes, compaction actions.
