# Target: `internal/entity_query_service.go`

## Why This Target

This service layer owns pagination normalization, schema validation, sorting rules, cross-schema aggregation, and result shaping.
It is a good source of real behavioral tests and regression guards.

## Primary Test Files

- `internal/entity_manager_test.go`
- prefer adding or extending a focused `internal/entity_query_service_test.go` when service-level behavior is hard to express cleanly in `entity_manager_test.go`

## Functions In Scope

- `Query`
- `CrossSchemaSearch`
- `validateCrossSchemaRequest`
- `buildSchemaContexts`

## Priority Scenarios

1. Given the caller sends a nil request, when `Query` starts, then validation fails immediately.
2. Given invalid page or page-size values, when `Query` runs, then normalized pagination is used.
3. Given the caller sorts by an unknown attribute, when `Query` runs, then a clear error is returned.
4. Given schema lookup fails, when `Query` or `CrossSchemaSearch` runs, then the error is propagated.
5. Given the repository omits total-pages metadata, when results are returned, then the service backfills it correctly.
6. Given no schema returns records, when `CrossSchemaSearch` runs, then the result is an empty, well-formed page.
7. Given schema contexts cannot be built, when cross-schema search starts, then no partial aggregation is returned.
8. Given counting or fetching fails for one schema, when cross-schema search runs, then error propagation matches the service contract.

## Constraints

- Prefer behavior-level assertions over implementation-level mocking.
- Keep changes in existing tests when possible.
- This target is intentionally service-layer friendly: prefer small stub implementations over database-heavy setups.
- Favor scenarios around request normalization, schema lookup, pagination metadata, and cross-schema aggregation semantics.
