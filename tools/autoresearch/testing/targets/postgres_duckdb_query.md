# Target: `internal/postgres_duckdb_query.go`

## Why This Target

This file bridges federated query planning, dirty-id exclusion, DuckDB execution, and row streaming.
It is performance-sensitive and correctness-sensitive, and it already has partial integration coverage.

## Primary Test Files

- `internal/postgres_duckdb_federated_integration_test.go`
- nearby DuckDB-focused test files in `internal/`

## Functions In Scope

- `StreamDuckDBFederatedQuery`
- `fetchAndRecordDirtyIDs`
- `buildDuckDBQueryWithPlan`
- `streamDuckDBRows`
- `finalizeDuckDBExecutionPlan`

## Priority Scenarios

1. Given the caller passes a nil query, when DuckDB execution starts, then a validation error is returned.
2. Given the DuckDB client is unavailable, when federated execution is requested, then the request fails fast.
3. Given dirty-id fetching fails, when the query is prepared, then the error is propagated and execution stops.
4. Given query rendering fails, when DuckDB SQL is built, then no execution happens.
5. Given a row cannot be scanned, when streaming rows, then the stream stops with an error.
6. Given `rows.Err()` becomes non-nil, when streaming completes, then the error is returned.
7. Given `rowHandler` returns an error, when streaming rows, then iteration stops and the error is propagated.
8. Given execution-plan capture is disabled, when the query finishes, then no plan metadata is attached.
9. Given execution-plan capture is enabled, when the query finishes, then source metadata is attached when available.

## Constraints

- Prefer existing test files.
- Keep tests deterministic and local.
- Do not broaden into full-system E2E unless function-level coverage is blocked.
