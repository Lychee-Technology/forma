# Target: `internal/postgres_persistent_repository_query.go`

## Why This Target

This file contains input normalization, optimized query streaming, and hybrid condition building across main-table and EAV storage.

## Primary Test Files

- `internal/postgres_persistent_repository_repo_test.go`
- related repository query tests in `internal/`

## Functions In Scope

- `StreamOptimizedQuery`
- `runOptimizedQuery`
- `buildHybridConditions`
- `hybridConditionBuilder.buildComposite`
- `hybridConditionBuilder.buildKv`

## Priority Scenarios

1. Given an invalid clause or schema id, when `StreamOptimizedQuery` starts, then validation fails immediately.
2. Given invalid limit or offset values, when the query is normalized, then safe defaults are applied.
3. Given `rowHandler` returns an error, when rows are streamed, then iteration stops and the error is propagated.
4. Given a composite condition is empty, when hybrid conditions are built, then it is handled consistently.
5. Given nested AND and OR filters, when hybrid conditions are built, then the semantic grouping is preserved.
6. Given an attribute maps to a main-table column, when conditions are built, then the main-table path is used.
7. Given no main-table column binding exists, when conditions are built, then the EAV fallback path is used.

## Constraints

- Prefer unit or focused integration tests over E2E.
- Avoid coupling tests to exact SQL formatting when semantic assertions are sufficient.
