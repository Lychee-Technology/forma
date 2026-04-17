# Target: `internal/dualpath_sql_generator.go`

## Why This Target

`dualpath_sql_generator.go` translates one condition tree into parallel Postgres and DuckDB predicates.
It is branch-heavy, correctness-sensitive, and mostly pure logic, which makes it a strong local autoresearch target.

## Primary Test Files

- `internal/dualpath_sql_generator_test.go`
- `internal/dualpath_sql_generator_health_test.go`

## Functions In Scope

- `ToDualClauses`
- `classifyPredicate`
- `buildPgMainClause`
- `buildPgMainCompositeClause`
- `buildPgMainKvClause`
- `buildDuckClause`
- `buildDuckCompositeClause`
- `buildDuckKvClause`

## Priority Scenarios

1. Given a nil condition, when dual clauses are built, then Postgres pushdown is empty and DuckDB returns `1=1`.
2. Given an empty composite condition, when main and DuckDB clauses are built, then both sides use their no-op behavior consistently.
3. Given nested AND and OR children, when dual clauses are built, then grouping and argument ordering are preserved across both outputs.
4. Given a known attribute without a column binding, when Postgres main pushdown is built, then the predicate is skipped without error.
5. Given an unknown attribute, when Postgres main pushdown is built, then it is ignored rather than treated as a hard failure.
6. Given text, numeric, date, and bool attributes with supported operators, when predicates are classified, then only pushdown-safe combinations are accepted.
7. Given a bound attribute with an unsupported operator, when Postgres main pushdown is built, then a clear error is returned.
8. Given a DuckDB predicate without metadata, when the clause is built, then value-type fallback inference still produces the expected cast and parameter value.
9. Given a LIKE-style operator such as `starts_with` or `contains`, when a DuckDB clause is built, then the wildcard rewrite is correct and no extra cast is introduced.
10. Given bool and datetime encodings on main-table columns, when Postgres main pushdown is built, then the converted argument matches the column encoding.

## Keep Bias

Prefer candidates that strengthen composite-condition semantics, pushdown classification boundaries, and parameter ordering.
Do not spend iterations on duplicating the existing smoke coverage for simple happy-path text comparisons.

## Constraints

- Prefer extending `internal/dualpath_sql_generator_test.go`.
- Use `internal/dualpath_sql_generator_health_test.go` only when a small focused helper case fits naturally there.
- Favor semantic assertions over whole-query exact string snapshots when possible.
- When SQL text must be checked, assert the operator, grouping, column name, and placeholder order rather than unrelated formatting.
- Keep tests local and deterministic; do not add infrastructure-backed coverage for this target.
