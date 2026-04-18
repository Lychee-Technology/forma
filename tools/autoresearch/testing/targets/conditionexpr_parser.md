# Target: `internal/conditionexpr/parser.go`

## Why This Target

`parser.go` is a shared condition-expression helper that feeds multiple higher-level query builders.
It is pure logic, has a compact surface area, and a small mistake here can fan out into Postgres, DuckDB, and query-normalizer behavior.

## Primary Test Files

- `internal/conditionexpr/parser_test.go`

## Functions In Scope

- `ParseOperatorValueLenient`
- `ParseOperatorValueStrict`
- `ParseOperatorValueEqualsOnEmptyOperator`
- `CanonicalOperator`
- `ToSQLOperator`
- `ParseNumeric`
- `ParseRFC3339OrUnixMs`

## Priority Scenarios

1. Given an RFC3339 literal that includes multiple `:` characters, when lenient parsing runs, then it is treated as `equals` rather than split into a fake operator.
2. Given malformed `op:value` pairs with missing operator or missing value, when strict parsing runs, then a clear validation error is returned.
3. Given `:value` input, when equals-on-empty parsing runs, then it normalizes to `equals` while preserving the value.
4. Given operator aliases and mixed casing/whitespace, when canonicalization runs, then aliases normalize to canonical names and unknown operators are lowercased/trimmed only.
5. Given LIKE-style operators, when SQL operator mapping runs, then wildcard normalization is correct and the canonical SQL operator is returned.
6. Given unsupported operators, when SQL operator mapping runs, then a clear error is returned.
7. Given integer, floating-point, and invalid numeric literals, when numeric parsing runs, then the parsed type or error matches expectations.
8. Given RFC3339, unix-millis, and invalid datetime literals, when datetime parsing runs, then the parsed time or error matches expectations.
9. Given a raw string with multiple colons that is not a datetime, when lenient parsing runs, then only the first colon is treated as the operator separator.
10. Given empty raw strings, whitespace-only operator names, or bare operators, when parsing helpers run, then behavior stays explicit and deterministic.

## Keep Bias

Prefer tests that lock down normalization boundaries and malformed-input behavior over repeating simple happy paths.
Choose scenarios that protect higher-level callers from ambiguous parsing, especially around datetimes, aliases, and colon-heavy literals.

## Constraints

- Extend `internal/conditionexpr/parser_test.go`.
- Keep tests local and deterministic.
- Favor table-driven helper tests when a scenario naturally fits.
- Do not broaden into higher-level query builder tests for this target.
- Prefer exact operator/value assertions and concrete error content over broad non-empty checks.
