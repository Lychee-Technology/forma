# Implementation Plan: Federated Query Engine

## Overview

This implementation plan builds upon the existing PostgreSQL EAV query infrastructure to add federated query capabilities with DuckDB. The approach extends the current `PostgresPersistentRecordRepository` with a new federated query engine that can seamlessly query both PostgreSQL (hot data) and DuckDB/S3 (warm/cold data) simultaneously.

## Tasks

- [ ] 1. Dual-path translation and pushdown
  - Implement metadata-aware translator that emits both PostgreSQL pushdown clauses (entity_main) and DuckDB logical fragments, covering operators and type casts (UUID, numeric, date/time).
  - Integrate translation into BuildDuckDBQuery and Postgres paths so postgres_scan receives physical column predicates; add unit/property tests for translation and pushdown separation.
  - _Requirements: 2.1, 2.2, 4.1_

- [ ] 2. Federated DuckDB execution pipeline (S3 + PG)
  - Extend DuckDB SQL template to include dirty_ids CTE, s3_source via read_parquet (path templating), pg_source via postgres_scan with pushdown, anti-join, UNION ALL, QUALIFY ROW_NUMBER() LWW, soft-delete filter, and logical filters.
  - Align PG/DuckDB types with explicit CASTs to match Parquet schema; add tests for anti-join, deduplication, and pagination correctness.
  - _Requirements: 1.4, 3.1, 3.2, 7.4_

- [ ] 3. Streaming result processing
  - Add streaming iterator for DuckDB results (no full slice) and reuse Postgres streaming semantics; ensure pagination uses deduped order and PersistentRecord format remains consistent.
  - _Requirements: 4.2, 8.1, 8.4_

- [ ] 4. Routing, degraded modes, and resilience
  - Enhance routing thresholds and PreferHot/MaxRows handling; respect AllowS3Fallback semantics and include tier selection in plans.
  - Implement circuit breaker (e.g., 5 failures/30s) and degraded modes: PG-only fallback when S3/DuckDB unavailable; S3-only with eventual consistency flag; emit warnings/metadata.
  - _Requirements: 1.1, 1.2, 5.1, 5.2, 5.3_

- [ ] 5. Observability and execution plans
  - Emit opentelemetry metrics: fed_query_latency_histogram (translation/execution/streaming), fed_query_row_count (PG vs S3/DuckDB), fed_query_pushdown_efficiency.
  - Expand execution plan reporting with row estimates, pushdown status, degraded-mode notes, and per-source timings.
  - _Requirements: 6.2, 6.3, 6.5_

- [ ] 6. Integration tests and health checks
  - Add end-to-end federated query tests covering anti-join, routing decisions, degraded paths, streaming, and pagination; include S3/parquet path rendering.
  - Strengthen DuckDB/PostgreSQL/S3 health checks and configuration validation.
  - _Requirements: 1.1, 6.1, 6.4_

- [ ] 7. Final checkpoint - core federated query functionality complete
  - Ensure all tests pass; confirm API compatibility and documentation updates.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The implementation builds incrementally on existing PostgreSQL query infrastructure
- DuckDB integration is designed to be non-intrusive to existing OLTP operations
