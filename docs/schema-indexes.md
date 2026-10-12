# Declared Indexes (`x-forma-indexes`)

`init-db` gives every column of `entity_main` one index keyed
`(ltbase_schema_id, ltbase_row_id, <column>)`. Because `ltbase_row_id` comes
before the column, these indexes cannot seek on the column's value. A query
such as "pending messages whose `next_attempt_at` has passed" reads every row
of the schema. A schema can declare composite indexes on its column-bound
attributes for these access paths. `init-db` then builds them without
blocking writes.

## Declaring an index

Put `x-forma-indexes` at the top level of the schema file (`<schema>.json`):

```json
{
  "type": "object",
  "properties": {
    "status": { "type": "string" },
    "partition_key": { "type": "string" },
    "next_attempt_at": { "type": "string", "format": "date-time" },
    "locked_until": { "type": "string", "format": "date-time" },
    "enqueued_at": { "type": "string", "format": "date-time" }
  },
  "x-forma-indexes": [
    { "name": "due",   "attributes": ["status", "next_attempt_at"] },
    { "name": "lease", "attributes": ["status", "locked_until"] },
    { "name": "fifo",  "attributes": ["partition_key", "enqueued_at"] }
  ]
}
```

The value must be an array. A schema that declares no indexes leaves the key
out; `null`, like any other non-array value, fails the load.

Each entry is an object with exactly two keys:

- `name` must match `^[a-z][a-z0-9_]{0,30}$` and be unique within the schema.
  It names the declaration in errors and in `init-db` output. It does not name
  the physical index.
- `attributes` lists 1 to 4 attribute names with no repeats. Their order is the
  index key order: put the attributes a query compares for equality first, and
  the one it ranges over or sorts by last.

Every attribute must be **column-bound**: its entry in `<schema>_attributes.json`
has a `column_binding` to an `entity_main` column. The rules for an attribute are:

- An EAV attribute (no `column_binding`) is refused. The same goes for an
  attribute the attributes file does not define, and for a retired one.
- An attribute bound to `ltbase_schema_id` or `ltbase_row_id` is refused,
  because every declared index already carries both.
- A `bool` attribute is refused under either encoding, because some of its
  filters compare the column's truth value rather than the column, and no
  index on the column serves them. `in` and `not_in` do this under both
  encodings. Under `bool_text`, `equals` renders as `(<col> = '1') = $n`, which
  PostgreSQL turns into an index condition only for a `true` it sees at
  planning time: `false` becomes `<col> <> '1'`, and a cached generic plan
  keeps the expression. Admitting `bool_smallint`, whose `equals` does seek, is
  tracked in #671.
- Keys other than `name` and `attributes` are refused, so `unique`, `where` and
  expressions are rejected rather than silently ignored.

A declaration that breaks a rule fails the schema load. The server, the
Lambda entrypoint, `cdc-flush` and `cdc-init` refuse to start, and `init-db`
fails at its declared-index step. The error names the schema file, the
schema, the index, and the offending attribute, for example:

```
schema outbox_message listed in registry table schema_registry: schema file schemas/outbox_message.json: schema outbox_message: index due: attribute payload is stored in EAV (it has no column_binding); only column-bound attributes can be indexed
```

## The physical index

Each declaration becomes a plain btree index keyed
`(ltbase_schema_id, <bound columns in declared order>, ltbase_row_id)`. The
index is not partial, not unique, and has no expressions. `due` above, with
`status` bound to `text_01` and `next_attempt_at` to `bigint_01`, becomes:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS "entity_main_cx_<16 hex digits>"
    ON "entity_main" (ltbase_schema_id, "text_01", "bigint_01", ltbase_row_id)
```

The name is the table name, `_cx_`, and a digest of the column tuple. Two
declarations that bind the same column tuple share one physical index. This
holds whether the declarations are in different schemas or the same one.
Sharing is safe because `ltbase_schema_id` leads the key, so each schema reads
only its own range. `init-db` lists every declaration an index serves.

## Building: `init-db`

When `init-db` runs with `--schema-dir`, it builds declared indexes as its last
step. This happens after the transaction that creates tables and registers
schemas has committed. The step works as follows:

1. It loads the registered schemas from `--schema-dir` through the same file
   registry the server uses, so it plans exactly the declarations the server
   validates.
2. It reads the indexes already on `--entity-main-table`.
3. For each planned index that is missing, it runs one
   `CREATE INDEX CONCURRENTLY IF NOT EXISTS` statement in autocommit mode.
   Concurrent creates and updates keep running. The build waits for write
   transactions already in flight, and it cannot run inside a transaction.
4. It prints one line per declared index: created, already present, or
   unreferenced.

A re-run whose declarations are unchanged issues no index DDL.
`--skip-declared-indexes` leaves the step out, for operators who build indexes
out of band. Without `--schema-dir` there are no declarations to read, so the
step is skipped with a message.

`init-db` exits non-zero and names the index when a declared index is
unusable:

- **Invalid** (`pg_index.indisvalid = false`). An interrupted
  `CREATE INDEX CONCURRENTLY` leaves the index in this state. Interruptions
  include a cancelled `init-db`, a role- or database-level
  `statement_timeout`, and a lost connection. The remedy is to
  drop it with `DROP INDEX CONCURRENTLY "<name>"` and rerun `init-db`. If a
  build is still running in another session, `init-db` names that backend's pid
  instead. The remedy then is to wait for the build to finish and rerun.
- **Mismatched**: an index under the planned name is not exactly what
  `init-db` would build. Its key columns differ, or it uses another access
  method (BRIN, hash, …), is unique or partial, has an expression key or
  `INCLUDE` columns, or has a non-default operator class, collation or sort
  order on a key column. The error lists each difference. `init-db` never
  replaces an index, so drop it yourself and rerun.
- **Name taken**: another relation in the same PostgreSQL schema holds the
  name.

### No automatic drops

`init-db` never drops an index. When you remove a declaration, its index stays
in place, and every later run reports it:

```
Unreferenced declared index: entity_main_cx_… on entity_main (ltbase_schema_id, text_02, bigint_03, ltbase_row_id): no schema declares this key; init-db never drops indexes, so drop it yourself once nothing reads through it
```

Rebuilding a dropped index on a large table is slow. A server still running
the previous schema generation may also read through the old index, so a drop
waits for the operator's decision. Drop the index with
`DROP INDEX CONCURRENTLY` once nothing depends on it.

## What a declared index serves

Declared indexes serve queries that run on PostgreSQL, which is the hot-tier
OLTP query path. The S3 Parquet tiers never read them. The federated hot leg
currently pushes no predicate into PostgreSQL (#631).

PostgreSQL can seek a declared index on these operators:

- equality: `equals` and `in`;
- ordered comparison: `gt`, `gte`, `lt` and `lte`, including against `$now`
  (see [Condition DSL](condition-dsl.md));
- `is_null`.

These hold for every attribute a declaration admits: text, uuid, the integer
and `double` columns, and dates and date-times under `unix_ms` or `iso8601`. A
test runs each operator against a one-attribute index of each kind and checks
the plan.

A query should constrain the leading attributes with equality, and the last
attribute with a range or a sort. The tests behind #647 also check that the
hot-tier SQL Forma generates reads through the declared index for these
multi-attribute shapes:

| Shape | Condition |
| --- | --- |
| due | `status` equals or `in`, and `next_attempt_at` `lte:$now` |
| lease expired | `status` equals, and `locked_until` `lte:$now` |
| FIFO | `partition_key` equals, and `enqueued_at` ranged and sorted |

A btree key does not serve `contains` and `starts_with`, which render as
`LIKE` patterns.

## Not supported

- Indexes on EAV attributes or on `bool` attributes.
- Unique, partial and expression indexes.
- Automatic index selection, and automatic drops.

**DSQL: specified, not verified (#633).** The declaration format and the
physical shape are specified independently of PostgreSQL. No DSQL code path
builds or checks these indexes.
