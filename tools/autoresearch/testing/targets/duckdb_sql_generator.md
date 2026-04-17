# Target: `internal/duckdb_sql_generator.go`

## Why This Target

`duckdb_sql_generator.go` owns DuckDB-specific predicate rendering, LIST semantics, type inference, path templating, and dirty-row exclusion assembly.
It already has a useful test base, but the behavior surface is still wide enough to justify a full target rather than only a small helper cleanup pass.

## Primary Test Files

- `internal/duckdb_sql_generator_test.go`
- `internal/duckdb_render_and_dirtyids_test.go`
- nearby DuckDB-focused helper tests in `internal/`

## Functions In Scope

- `BuildListPredicate`
- `ValidateOrderByForListTypes`
- `ValidateOrderByAttributesForListTypes`
- `RenderS3ParquetPath`
- `GenerateDuckDBWhereClause`
- `generateDuckDBCondition`
- `generateDuckDBCompositeCondition`
- `generateDuckDBKvCondition`
- `duckDBSQLOperator`
- `detectDuckDBValueType`
- `parseDuckDBParamValue`
- `AppendDirtyExclusion`
- `GenerateDuckDBWhereClauseWithExclusions`

## Priority Scenarios

1. Given LIST predicates over text and numeric element types, when DuckDB predicates are built, then casts match the element type.
2. Given an unsupported LIST operator, when the predicate is built, then a clear validation error is returned.
3. Given a nil query or empty composite condition, when DuckDB WHERE generation runs, then it returns `1=1` without args.
4. Given nested AND and OR composites, when DuckDB clauses are generated, then grouping and argument ordering are preserved.
5. Given `starts_with` and `contains` operators, when DuckDB SQL operators are mapped, then wildcard rewriting is correct.
6. Given bool, UUID, RFC3339 datetime, and epoch-millis literals, when value types are inferred and parameters are parsed, then the resulting typed values are correct.
7. Given dirty row ids and an existing WHERE clause, when exclusions are appended, then the base args remain first and exclusion args follow in order.
8. Given ORDER BY includes a LIST attribute, when validation runs, then it fails with an actionable error containing the attribute reference.
9. Given ORDER BY includes only non-LIST or unknown attributes, when validation runs, then it does not fail spuriously.
10. Given malformed or underspecified parquet path templates, when rendering runs, then parse and render behavior is explicit and deterministic.

## Keep Bias

Prefer candidates that cover composite-condition semantics, parameter typing, and clause/arg composition over repeating basic one-function happy paths already present.
Choose tests that connect multiple helpers together when they still stay local and deterministic.

## Constraints

- Prefer extending `internal/duckdb_sql_generator_test.go`.
- Use `internal/duckdb_render_and_dirtyids_test.go` when the scenario is specifically about template rendering or dirty-id exclusion wiring.
- Avoid broad integration tests; this target should stay helper-focused and local.
- Do not add repetitive variants of scenarios already covered by straightforward happy-path tests unless the new case changes semantics.
- For SQL assertions, prefer checking operators, casts, wildcards, grouping, and arg ordering instead of entire formatted strings.
