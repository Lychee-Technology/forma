Plan for OLAP/DuckDB exporter alignment

Done
- Defaults raised to spec thresholds (20k rows / 1h) with coverage.
- Export now batch-scoped, path aligned, mem limit honored, and basic SQL tests added.
- Schema-aware projection path added (uses SchemaRegistry cache when provided) with type casting and attr_id filtering; byte-cap truncation and mark-flushed-by-ids wired.

Next steps
1) Wire SchemaRegistry into RunOnce caller(s) so schema-aware projection is active in production; handle cache errors gracefully.
2) Improve batch sizing: estimate bytes from schema and data stats; optionally split batches instead of truncate, and record batch_id/snapshot_ts.
3) S3 layout & manifests: integrate manifest JSON write/read and temp publish flow; include min/max row_id/time_slot for pruning.
4) Compaction/base maintenance: implement daily worker for new vs historical updates, dirty-ratio decision, atomic rewrite/publish, and rename present_<schema>.parquet when surpassing 256MB.
5) Query template: provide merge-on-read SQL (Base + Delta + Postgres buffer) with dedupe (ROW_NUMBER) and soft-delete filtering; ensure columns align with exporter.
6) Testing & observability: DuckDB fixture tests for typed projections, dedupe/deletes, batch byte caps; manifest/compaction decision tests; metrics/logs for batch sizing and manifest writes.
