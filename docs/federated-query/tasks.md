# Implementation Plan: Federated Query Engine

## Overview

This plan decomposes the federated-query E2E testing方案 into smaller, independently runnable tasks. Each task should leave the codebase compiling (e.g., `go test ./...` succeeds) even if other tasks are incomplete.

## Tasks

- [ ] 1. E2E test harness & fixtures
  - Add a minimal harness to boot DuckDB with postgres_scanner/httpfs and connect to test PostgreSQL/MinIO (or in-memory mocks if available).
  - Create reusable fixtures: base/delta Parquet files plus PG change_log/entity_main/eav_data seeds for row_id 1–5 as described in design.md.
  - Deliverable compiles with harness + fixture loaders, even if no scenarios assert yet.

- [ ] 2. Mixed read, anti-join & LWW correctness
  - Implement an E2E test that issues the canonical filter (age>18, name prefix, tag=developer) through the real Search API path.
  - Assert returned row_ids {1,2,4,5} with row_id 3 excluded; verify QUALIFY ROW_NUMBER() and deleted_ts handling.
  - Leaves harness + single scenario test green.

- [ ] 3. Predicate pushdown coverage
  - Add an E2E that inspects PG scan row counts/plan (e.g., metrics/log capture) to prove entity_main predicates are pushed down.
  - Include a control case without pushdown to compare fed_query_pushdown_efficiency.

- [ ] 4. EAV filter + type casting alignment
  - Add E2E ensuring tag (attr_id=205) filtering happens in DuckDB and matches EAV data; include UUID/text/integer cast alignment with Parquet schema.
  - Validate no precision/overflow regressions.

- [ ] 5. Sorting & pagination stability
  - Add E2E asserting created_at DESC order with LIMIT/OFFSET after deduplication; ensure deterministic pagination across runs.

- [ ] 6. Streaming large result sets
  - Build a high-row-count fixture and assert the HTTP/DB iterator streams rows without loading all results; confirm latency histogram “streaming” bucket emits.

- [ ] 7. Degraded modes
  - Add E2Es for S3 unavailable (PG-only with partial_result=true) and PG unavailable (S3-only when consistency=eventual else 503).
  - Assert warning metadata/log entries are emitted.

- [ ] 8. Circuit breaker & fallback routing
  - Simulate 5 consecutive failures (timeout/OOM) to trip the breaker; assert storage=['olap'] is rejected and optional storage=['oltp'] fallback is used.

- [ ] 9. Security/escaping checks
  - Add filters with special characters/LIKE wildcards and assert both $PG_WHERE_CLAUSE and $LOGICAL_WHERE_CLAUSE are safely escaped with correct results.

- [ ] 10. Observability assertions
  - Add E2E hooks to collect and assert fed_query_latency_histogram, fed_query_row_count (S3 vs PG), and pushdown efficiency metrics per scenario.

## Notes

- Tasks can be executed in any order; each should maintain a compiling codebase even if other scenarios are pending.
- Prefer reuse of harness/fixtures to minimize duplication across E2Es.
