# Condition DSL Reference

The condition DSL is the filter language of `POST /api/v1/advanced_query`,
of `forma.QueryRequest.Condition`, and of the federated query path. A
condition is a tree of two node kinds.

A **composite** node combines children:

```json
{ "l": "and", "c": [ { "a": "status", "v": "active" }, { "a": "age", "v": "gte:18" } ] }
```

- `l`: `"and"` or `"or"`.
- `c`: the child conditions, composite or leaf.

A **leaf** (`forma.KvCondition`) filters one attribute:

```json
{ "a": "price", "v": "gt:100" }
```

- `a`: the attribute name from the JSON Schema, or a raw `ltbase_*` column.
- `v`: `operator:operand`. Without an operator the leaf means `equals`, and a
  bare ISO 8601 timestamp or unix-millisecond value is always an `equals`
  operand (its colons are not an operator separator). Operators are
  case-insensitive.

## Operators

| Operator | Alias | Operand | Meaning |
| --- | --- | --- | --- |
| `equals` | `eq` | one value | equal (default) |
| `not_equals` | `neq` | one value | has a value, and it differs |
| `gt`, `gte`, `lt`, `lte` | | one value | ordered comparison |
| `starts_with` | | text | prefix match, text attributes only; the operand is literal text |
| `contains` | | text | substring match, text attributes only; the operand is literal text |
| `in` | | JSON array | equals one of the values |
| `not_in` | | JSON array | has a value, and it equals none of the values |
| `is_null` | | `true` | the attribute has no value |
| `is_not_null` | | `true` | the attribute has a value |

Operand rules shared by every operator:

- Values are coerced by the attribute's value type: unix milliseconds or
  RFC 3339 for `date` and `datetime`, a number for the numeric types,
  `true`/`false`/`1`/`0` for `bool`, a UUID for `uuid`. A `uuid` operand is
  compared in canonical lowercase form, so upper- and lowercase spellings of
  one UUID match the same rows wherever the attribute is stored.
- An operand the value type cannot take, or an operator it does not take
  (`starts_with` on a `uuid`), is `ErrInvalidInput` naming the attribute,
  refused before the query runs.
- A numeric operand is a finite number. `NaN` and `Inf` are
  `ErrInvalidInput` on every numeric type, refused before the query runs:
  they are not values the types hold, and PostgreSQL orders `NaN` above
  every number, so `lt:NaN` used to match every value stored in `eav_data`
  or a `double` column.
- A numeric operand compares exactly against whole-number storage: a
  `smallint`, `integer` or `bigint` column of `entity_main`, and a `bigint`
  attribute on the DuckDB tiers. On an `integer` attribute holding 7,
  `equals:7.9` and `gt:7.5` do not match and `lt:7.5` and `not_equals:7.9`
  do; an operand beyond the column's width compares too, so `lt:2147483648`
  matches every value of an `integer` column. Such a comparison binds one
  `bigint` parameter, which keeps it on the column's btree index. A literal
  that is not an integer in int64 range is its float64 image, so
  `9007199254740993.5` denotes 9007199254740994.
- A `starts_with` or `contains` operand matches as literal text: `%`, `_`
  and `\` in it match themselves (`starts_with:a_c` does not match `abc`).
- A comparison never matches a row where the attribute has no value. In
  particular `not_equals` and `not_in` do not match absent values.

## `in` and `not_in`

```json
{ "a": "status", "v": "in:[\"active\", \"pending\"]" }
{ "a": "age",    "v": "not_in:[18, 21]" }
```

The operand is a JSON array of **1 to 1,000** elements.

- Each element is a JSON string, number or boolean. An element is coerced
  with the rules `equals` applies to the attribute's value type, so
  `in:[1704164645000, "2024-01-02T03:04:05Z"]` on a `datetime` names two
  spellings of instants, and `in:[true, 0]` on a `bool` names both values.
- Duplicates are allowed and change nothing.
- A `uuid` element is parsed and compared in canonical lowercase form, so
  upper- and lowercase spellings of one UUID match the same rows.
- A numeric element is compared at the width the attribute's value is stored
  at, the same as an `equals` operand. Whole-number storage cannot hold a
  fractional element, so the element is dropped there: on a `bigint`
  attribute, and on an `integer` or `smallint` attribute bound to an
  `entity_main` column, `in:[18, 18.5]` matches what `in:[18]` matches, and
  `not_in:[18.5]` matches every present value.
- An `integer` or `smallint` attribute stored in `eav_data` is stored as
  `DOUBLE` on every tier (#384), so a fractional element is kept. It matches
  a fractional value written before the write path began refusing them, as
  `equals:1.5` does: `in:[1.5]` matches such a value and `not_in:[1.5]`
  excludes it.
- `in` and `not_in` compare a scalar value. They are refused on a list
  (`array`) attribute; use `is_null` / `is_not_null` for list presence.
- `$now` is not accepted inside a list.

Refused operands are `ErrInvalidInput` (HTTP 400) naming the attribute and,
where one applies, the limit, the element index, and the value type:

| Operand | Published message |
| --- | --- |
| `in:[]` | `operator 'in' on attribute 'code' takes a JSON array of 1 to 1000 values; the array is empty` |
| 1,001 elements | `operator 'in' on attribute 'code' takes a JSON array of 1 to 1000 values; the array has more than 1000 values` |
| `not_in:"a"` | `operator 'not_in' on attribute 'tag' takes a JSON array of 1 to 1000 values; the value is not a JSON array` |
| `in:[1, null]` | `element at index 1 of the 'in' list for attribute 'age' is null; ...` |
| `in:[1, "x"]` on an `integer` | `element at index 1 of the 'in' list for integer attribute 'age': "x" is not a number` |

### The `not_in` NULL rule

`not_in` never matches a row where the attribute has no value, the same as
`not_equals` and as SQL `NOT IN` with a non-NULL list. To include those
rows, say so:

```json
{ "l": "or", "c": [ { "a": "status", "v": "not_in:[\"archived\"]" },
                    { "a": "status", "v": "is_null:true" } ] }
```

## `is_null` and `is_not_null`

```json
{ "a": "deleted_reason", "v": "is_null:true" }
```

- The only accepted operand is the literal `true`. `is_null:false` and
  `is_not_null:TRUE` are refused; write the other operator instead.
- They are valid on every value type, including list attributes.
- A scalar attribute has a value when its main-table column, or its
  `eav_data` value column, is not NULL.
- A list attribute has a value when it was written at all: an empty list `[]`
  is present (`is_not_null` matches it), and only an attribute that was
  never written, or was removed, is null. Parquet tiers keep the same
  distinction, an empty `LIST` versus `NULL` (#204).
- `is_null` on an attribute stored in `eav_data` also matches an entity that
  has no `eav_data` rows at all: on the Postgres route the query anchors on
  `entity_main` whenever a condition uses `is_null`.

## The `$now` operand

```json
{ "a": "locked_until", "v": "lte:$now" }
{ "a": "seen_at",      "v": "gt:$now-30s" }
{ "a": "renew_on",     "v": "lte:$now+720h" }
```

`$now` is the database server's transaction timestamp, PostgreSQL `now()`,
truncated to milliseconds. It never reads the client or application clock, so
every row in one query sees the same instant.

- An optional offset follows `$now`: a signed Go duration
  (`time.ParseDuration`, units `ms`, `s`, `m`, `h`), such as `$now-30s` or
  `$now+1h30m`. The sign is required. The offset must be a whole number of
  milliseconds, the precision dates are stored at (`$now+1us` is refused).
- It is an operand of `equals`, `not_equals`, `gt`, `gte`, `lt` and `lte`
  on `date` and `datetime` attributes only. Any other operator on those
  types is `ErrInvalidInput`.
- On every other value type `$now` is an ordinary literal: `equals:$now` on a
  text attribute matches the string `"$now"`.
- On a `datetime`, the comparison is against the shifted instant.
- On a `date`, `$now` is the **UTC calendar date** of the shifted instant:
  `equals:$now` matches today's UTC date and `equals:$now-24h` yesterday's.
- On an attribute bound to a text column with `iso8601` encoding (whole-second
  RFC 3339 images), `$now` compares at second precision without losing
  rows: `lt`/`gte` compare against the instant rounded up to the second,
  `lte`/`gt` rounded down, and `equals` matches only when the shifted
  instant falls on a whole second.
- **Federated queries refuse `$now`.** A request with
  `Federated.Enabled` is `ErrInvalidInput` whenever its condition names
  `$now` on a date or datetime attribute, whichever tier the router would
  pick, because the DuckDB-evaluated tiers cannot read the PostgreSQL
  transaction time:

  ```text
  the $now operand on date attribute 'scheduledStartAt' is not supported on a federated query: it is the PostgreSQL transaction time, which the DuckDB-evaluated tiers cannot read; send an absolute timestamp instead
  ```

## Go constructors

Package `forma` builds these leaves, so Go callers never hand-write the
operand string:

| Call | Leaf |
| --- | --- |
| `forma.In("status", "active", "pending")` | `{a: "status", v: "in:[\"active\",\"pending\"]"}` |
| `forma.NotIn("age", 18, 21)` | `{a: "age", v: "not_in:[18,21]"}` |
| `forma.IsNull("deleted_reason")` | `{a: "deleted_reason", v: "is_null:true"}` |
| `forma.IsNotNull("tags")` | `{a: "tags", v: "is_not_null:true"}` |
| `forma.Now("locked_until", forma.FilterLessEq, 0)` | `{a: "locked_until", v: "lte:$now"}` |
| `forma.Now("seen_at", forma.FilterGreaterThan, -30*time.Second)` | `{a: "seen_at", v: "gt:$now-30s"}` |

`In`/`NotIn` JSON-encode their values, so `uuid.UUID` and `time.Time` work
directly. The server validates the result like any other leaf; the
constructors do not enforce the 1-to-1,000 bound or the operator set
`Now` accepts. The operator constants are `forma.FilterIn`,
`forma.FilterNotIn`, `forma.FilterIsNull` and `forma.FilterIsNotNull`.

## Query plans and parameters

- A list binds as **one** array parameter (`= ANY($n)` on PostgreSQL,
  `= ANY(CAST(? AS T[]))` on DuckDB), and the `$now` offset binds as one
  `bigint` parameter. The rendered SQL and both plan caches (the federated
  shape cache and the Postgres render cache) are therefore independent of
  the list's length, its contents, and the offset; `$now` and an absolute
  timestamp are different statements.
- A numeric comparison against whole-number storage binds an int64 bound,
  and its operator depends on whether the operand is an integer in int64
  range, a fraction, or beyond int64: `equals:7.5` on an `integer` column
  renders `< $n::bigint` with $n the smallest int64, which no value is
  below. The federated shape cache keys on that operand class as well as
  on the operator.
- A `starts_with` / `contains` pattern escapes its operand with `\`,
  PostgreSQL's default LIKE escape; the DuckDB renders name it with
  `ESCAPE '\'`, since DuckDB has no default.
- `not_in` renders as `(lhs IS NOT NULL AND lhs <> ALL(list))`. The explicit
  `IS NOT NULL` is load-bearing: `NULL <> ALL(empty)` is true.
- On the federated route, `is_null` / `is_not_null` on a column-bound
  attribute push into the hot-tier pushdown; `in` / `not_in` are evaluated by
  DuckDB at the S3 semijoin and on the merged result.

## Backends

| Backend | Status |
| --- | --- |
| PostgreSQL | Verified: a real-PostgreSQL truth table over every storage form, with `NUMERIC` and `DOUBLE PRECISION` EAV values, and a `$now` test seeded from `SELECT now()`. Numeric and `starts_with`/`contains` operands are checked against exact arithmetic and Go string matching rather than against another route (`TestConditionOperandConformance_*`), over `smallint`, `integer`, `bigint` and `double` columns and both EAV value types. |
| DuckDB (federated) | Verified: `in`, `not_in`, `is_null` and `is_not_null` return the same rows on the DuckDB route and the Postgres route across cold, warm and hot tiers (`TestConditionDSLOperatorParityAcrossTiers`). The DuckDB renders of numeric and `starts_with`/`contains` leaves run against an in-memory DuckDB with exact expected rows (`TestNumericOperand_DuckDBConformance`, `TestLikeOperand_DuckDBConformance`); the hot-tier pushdown runs there over native tables. `TestConditionOperandSemanticsAcrossRoutes` runs the same operands through the production harness on the Postgres route and on the DuckDB route over hot rows (`postgres_scan`) and warm parquet. `$now` is refused. |
| Aurora DSQL | DSQL: specified, not verified (#633). The PostgreSQL rendering uses array binds with `= ANY` / `<> ALL`, `now()`, `extract(epoch ...)`, `floor`, `mod`, `to_timestamp` and `to_char`; none of it has run against a DSQL cluster. |

## Compatibility

The new operators and `$now` were refused before #646 (an unknown operator,
or an unparseable date), so no query that worked before changes meaning.
`$now` on a non-date attribute was, and still is, a literal.

The operand rules above corrected three answers, and each changes what an
existing query returns:

- A fractional operand against a whole-number `entity_main` column was
  truncated toward zero: `equals:7.9` matched 7, `gte:1.5` matched 1, and
  `lt:1.5` missed 1. An operand beyond the column's width failed the query
  after it started. On the DuckDB tiers a fraction against a `bigint`
  attribute was rounded: `equals:7.5` matched 8.
- `%`, `_` and `\` in a `starts_with` or `contains` operand acted as LIKE
  wildcards and escapes. A caller that relied on them as wildcards now
  matches the characters literally.
- `NaN` and `Inf` operands are refused; before, `lt:NaN` matched every value
  stored in `eav_data` or a `double` column, and failed on a whole-number
  column after the query started.
