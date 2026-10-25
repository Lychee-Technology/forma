# Schema Evolution and Declared Types

This page records the owner's decisions on what a declared `valueType` means
for stored data, and what Forma does when a declaration changes. It is the
canonical record for D1 (#708) and D2 (#710). Other pages link here rather than
repeating it.

Every statement below is marked as one of:

- **Policy:** owner-approved architecture. It holds whether or not the code
  enforces it yet.
- **Implemented:** what the code does, with the test that pins it.
- **Not implemented:** policy the code does not enforce yet, with the issue
  that owns it.
- **Future:** a capability outside both decisions.

The principle behind both decisions: **a declared type is an integrity
constraint on stored data, not a hint for a best-effort conversion at query
time.** Protecting stored data from silent change takes priority over
answering every query.

## D1: Invalid legacy integer images on OLTP reads (#708)

Decided 2026-10-24.

### Context

A `smallint` or `integer` may be bound to a main column wider than its declared
type:

| declared | wider columns it can be bound to |
|---|---|
| `integer` | `bigint_*`, `double_*` |
| `smallint` | `integer_*`, `bigint_*`, `double_*` |

The write funnel judges the declared type before the column
(`transform.checkStorageFit`, #459), so it writes only a whole number inside the
declared range. Anything else in such a column is history: written before the
funnel, or by direct SQL.

Before #708 the OLTP read converted such an image with Go's `int32()` /
`int16()`, so `1001.5` read as `1001` and `4294967296` as some other integer.
Go leaves an out-of-range float conversion to the platform: it was
`-2147483648` on amd64, where every measurement on this page was taken. An
update that did not name the attribute merged the converted value into the
document it wrote, the funnel admitted it, and the column was overwritten. The
stored value changed without anyone naming the attribute.

### Decision

**Policy.** The OLTP read refuses an image the declared type cannot hold: a
fraction, a value outside the declared range, NaN, or an infinity. The refusal
is a read-path consistency error, which is operator-visible and never a 4xx
(see [Error Handling](./error-handling.md)). The read never wraps, truncates,
rounds, or drops the value, and never answers it as an absent attribute.
Anything on the OLTP route that decodes the row fails the same way: a GET, a
query page holding the row, and relation enrichment that reads it as a parent.

**Implemented** (`transform.checkBoundIntegerImage`). The error names the schema
id, the row, the attribute, the bound column, the declared type and the stored
image:

```text
convert attribute 'qty': record schema=708 row=0b5f2e84-… attrID=2: stored integer image 4294967296
in bound column bigint_01 does not fit declared type integer (expected a whole number in
[-2147483648, 2147483647]); rewrite the attribute to repair the row (#708)
```

A `bigint_*` image is judged on its exact int64. An `integer_*` image (a
`smallint`'s) and a `double_*` image are judged on their float64, against
bounds that are float64 integers. Every comparison is exact,
so the declared type's own bounds are never refused
(`TestReadKeepsBoundIntegerImageInsideDeclaredType`).

### Unrelated updates and repairs

An update that does not name the attribute decodes its stored value to carry it
into the write, so over an invalid image it fails with the read's error. It
writes nothing, so the image stays exactly as stored
(`TestUnrelatedUpdateOverBoundIntegerImageFails`; on PostgreSQL,
`TestBoundIntegerWideColumnCensusReadAndRepair`).

An update that names the attribute repairs the row. The merge discards a stored
value its written row replaces without decoding it ([the update merge and the
written row](./error-handling.md#the-update-merge-and-the-written-row-590-review)),
so the update succeeds whatever the image, and keeps the attributes it does not
name. For example, with `cnt` holding `1001.5`:

```bash
curl -X PUT http://localhost:8080/api/v1/e2e_simple/<row_id> \
  -H 'Content-Type: application/json' -d '{"cnt": 1001}'
```

Choosing the repaired value (`1001`, `1002`, or something else) is the
operator's decision. Forma does not pick one.

### Census

**Implemented.** `validate-schema-consistency` reports every such row, with the
column, as a failure. A re-flush reads the column the same way, so the row is
never requeued:

```text
- smallint/integer values in wider bound columns outside the declared range or non-integral, which the OLTP read refuses,
  in entity_main_dev: schema=e2e_simple schema_id=20 attr_id=3 attribute=cnt declared=integer row_id=… column=double_02 value=1001.5
```

See [Schema Consistency Migration Guide](./schema-consistency-migration.md#eav-integer-values-past-their-declared-width-501)
for the flags and the repair procedure.

### Relationship to #590, #627 and #621

- **#590** is the precedent. A `bigint` image in `eav_data` or a `double_*`
  column that names no int64 is refused the same way. D1 applies the same rule
  to `smallint` and `integer`.
- **#627** made the federated legs read such a column at the declared type
  through `TRY_CAST(<column> AS <declared>)`, the way the CDC export does. D1
  does not change that.
- **#621** is a proposed row-scoped invalid-image representation that both
  routes could share. D1 does not implement it.

### Transitional difference between routes

**Policy, accepted by the owner as transitional:** for the same invalid image,
the routes answer differently until #621 or a later decision unifies them.

| stored image (`integer` in `double_*`) | OLTP route | federated route |
|---|---|---|
| `1001.5` | consistency error | `1002` (rounded half to even) |
| `4294967296` | consistency error | absent |
| `NaN`, `±Infinity` | consistency error | absent |
| any whole number in range | the value | the value |

Valid data reads the same on every route. The federated reading never justifies
an OLTP conversion. The census is the guard: it reads `entity_main`, which
holds the current version of every row, whether or not that row has been
flushed to parquet yet.

### Not covered: EAV-only integers (#729)

An EAV-only `smallint`/`integer` keeps #384's conversion on the OLTP read. The
same unrelated-update write-back happens there, measured on PostgreSQL:
`value_numeric` `4294967296` becomes `-2147483648` (on amd64), and `1001.5`
becomes `1001`. Whether D1's refusal extends to `eav_data` is #729's decision.

## D2: Schema type evolution fails closed (#710)

Decided 2026-10-24. **Not implemented:** nothing in Forma enforces it yet. The
sections below separate the policy from what the code does today.

### Context

A declaration is active wherever a process loads `SCHEMA_DIR`: the server, the
Lambda entrypoint, `cdc-flush`, `compactor`, `cdc-init` and the validator each
load it on their own. When an attribute's `valueType` changes, the next
process to start simply uses the new type. Nothing compares it with the old one.

The data written under the old type stays where it is. Postgres keeps the stored
values, and parquet keeps the exported column at the old DuckDB type. The
federated scan unifies the files with `union_by_name=true` and casts the
unified column to the new declared type (`sqlgen.duckDBAttrCast`).

Measured on the production e2e harness (PostgreSQL and DuckDB). Rows were
written and flushed under the first declaration, the schema was redeclared
(`Env.EvolveSchema`, which models a restart under a new directory), and one more
row was written and flushed under the second:

| redeclaration | old values | federated read after |
|---|---|---|
| `numeric → bigint`, `double_01` | `1e30`, `1000.5`, `5` | every query fails: `Conversion Error: Type DOUBLE with value 1e+30 can't be cast … INT64` |
| `numeric → bigint`, EAV-only | `1e30`, `1000.5`, `5` | every query fails, same error |
| `numeric → integer`, `double_01` | `1e10`, `1000.5`, `5` | every query fails: `… 10000000000.0 … INT32` |
| `numeric → bigint`, `double_01` | `1000.5`, `1001.5`, `5` | succeeds silently: `1000.5` reads `1000`, `1001.5` reads `1002`, and `ratio equals 1001` matches nothing |
| `numeric → integer`, EAV-only | `1e10`, `1000.5`, `5` | succeeds silently: `1e10` decodes to `-2147483648` (amd64), and `ratio equals 1000` matches nothing although a row decodes to `1000` |

So the change either takes every federated query over the schema down, or
silently changes values. Which one happens depends on the data, not on the
change.

### Decision

**Policy.**

1. A type change becomes active only if the existing data, on every tier, is
   shown to satisfy the new type. The changes fall into three classes:
   - **proven lossless:** every value the old type can hold, in the storage
     the attribute is bound to, is a value of the new type;
   - **proven incompatible:** a stored value violates the new type;
   - **unverified:** compatibility cannot currently be established.

   Only the first class may activate. The second and the third fail closed.
2. Implicit evolution is lossless. Rounding, truncation, clamping, and
   substituting NULL are not implicitly approved. They require an explicit,
   operator-authorized migration (#730).
3. A refused change leaves the previous declaration active and its data
   unmodified, and nothing partially activates. The refusal names the schema,
   the attribute, the old and new types, the evidence, and the remediation.
4. Changes that do not alter an existing attribute's type keep working. These
   are the #189 evolutions: adding and removing attributes, renaming them (as a
   remove plus an add), and promoting an EAV-only attribute to a bound column
   under the same type.

### Why query-time casting is not migration

Making the parquet legs read old values with `TRY_CAST` at the new type was the
alternative #710 named. It would stop the statement failures, but `1e30` would
read as absent and `1000.5` as `1000`. Every query would then silently answer
values that no longer exist anywhere. `TRY_CAST(...) = NULL` is a lost value,
not a migrated one. The cast also runs on every query and forever: nothing is
ever converted, so a later reader still sees the old type underneath.

### Comparison with PostgreSQL's `ALTER COLUMN TYPE`

A strongly typed OLTP database changes a column's type in one statement that
converts every stored row, and either the whole change commits or none of it
does. D2 adopts that principle: verify the stored data, then activate, or leave
the old type in place.

Forma's default is **stricter than PostgreSQL**, on purpose. PostgreSQL
`ALTER TABLE … ALTER COLUMN r TYPE bigint` over a `float8` column fails the
statement on `1e30` ("bigint out of range"). Over `5, 1000.5, 1001.5` it
succeeds and stores `5, 1000, 1002`, rounding half to even. Its `numeric`
cast rounds half away from zero (`1000.5::numeric::bigint` is `1001`). Forma
refuses the fraction instead. A rounding conversion is a migration policy an
operator chooses (#730), never an implicit consequence of editing a
declaration.

### Physical storage precision

A logical value of the new type is not necessarily one the bound column can
store exactly. A `bigint` in a `double_*` column keeps only its float64 image,
which is exact within ±2^53, and the write funnel admits exactly
`[-9007199254740992, 9007199254740992]` there (#590). So `numeric → bigint`
over a `double_*` column needs every stored value to be a whole number within
±2^53, not merely within int64. In the measurement above, `2^60` and
`2^53 + 2` read back as int64 values the funnel would refuse, and the census
reports them. A `numeric` value written as `9007199254740993` was already
stored as `9007199254740992` under `numeric` (#205), so no migration can
recover the written digits.

### What the code does today

- Nothing refuses a type change. Each process loads the new declaration at
  start.
- No persisted record of an earlier declaration exists in PostgreSQL:
  `schema_registry` maps a schema name to its id, and nothing else. The only
  persisted trace of an earlier type is the parquet column type, which
  manifests stamp per file (`FileEntry.Columns`, #256).
- `generate-attributes` rewrites an existing attribute's `valueType` to the type
  it infers from the JSON Schema. Measured: a hand-set `bigint` bound to
  `double_01` came back as `numeric`. It refuses a type change only when
  re-adding a retired attribute (#342).
- The OLTP route is protected row by row for some types: a bound or EAV
  `bigint` image that names no int64 (#590), and a bound `smallint`/`integer`
  image (D1), are refused on read. An EAV-only `smallint`/`integer` is not
  (#729).

### Why enforcement is not a narrow patch

Fail-closed enforcement needs three things the code does not have:

1. **A record of the active declaration** that every process checks before it
   serves. Today there is nothing to compare a new declaration against, and a
   check inside one tool would be bypassed by every other process.
2. **A data check across tiers.** Postgres rows can be checked with the width
   census. Parquet files need a DuckDB scan of each live file holding the
   attribute, or a proof from the old type alone. A manifest entry written
   before #256 has no column types.
3. **A write fence.** Old-generation processes keep writing old-type values
   while the check runs, and after it, until they restart.

The proposed design, its open decisions, and the rollout plan are on #710.
Until it lands, D2 is policy that operators uphold by hand: do not change an
existing attribute's `valueType` except through a widening that is lossless
for its storage.

### Existing incompatible deployments

**Identify one.** An attribute is affected when a live parquet file holds its
column at a type the current declaration does not losslessly contain. For
example, the manifest lists a `DOUBLE` column `ratio` while `ratio` is now
declared `bigint`. The manifest JSON names each file's column types (`columns`).
For entries without them, `DESCRIBE SELECT * FROM read_parquet('<file>')` in
DuckDB gives the same answer. `validate-schema-consistency` reports the Postgres
side: a bound `bigint` in a `double_*` column outside ±2^53 or non-integral, a
bound `smallint`/`integer` outside its type, and EAV-only integers outside
their width.

**Recover.** These steps were measured on the harness:

- Restoring the previous declaration, by restarting every process on the
  earlier `SCHEMA_DIR`, brings federated reads back with the old values intact.
  `1e30` and `1000.5` read as stored. This is the supported recovery.
- Rewriting every affected row through the API and re-flushing does **not**
  clear the failure. The superseded version of each row stays live in parquet,
  and the cast still reaches it. A compaction pass in the same harness promoted
  the delta files to base without merging them, so the old versions stayed. The
  harness never triggered a merging compaction, so whether one would drop them
  is unknown.

After restoring, activating the new type is a migration (#730): decide per value
what it becomes, rewrite Postgres, replace the old-typed parquet, and verify
every tier before switching.

**Do not** delete, rewrite, round, truncate, NULL, or requeue production values
or parquet objects to make a redeclaration work without an explicit decision.
Each of these is destructive or lossy. A requeue in particular re-exports
Postgres values but leaves the superseded parquet versions in place.

### Explicit migration (Future, #730)

A supported way to change a type with an operator-chosen conversion policy,
covering Postgres rows, parquet rewrite, CDC and manifest coordination,
activation, write isolation and rollback, is a separate capability (#730). D2
does not depend on it: fail-closed enforcement refuses and keeps the old
schema.
