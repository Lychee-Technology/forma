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

Decided 2026-10-24, amended 2026-10-25 ([amendment](https://github.com/Lychee-Technology/forma/issues/710#issuecomment-6406151727)).
**Partially implemented** by Phase 1 (#732), a mitigation rather than
enforcement: `generate-attributes` no longer changes an existing attribute's
type, and a federated read refuses live parquet that the current declaration
cannot read losslessly. A redeclaration is still activated by every process
that loads it; the record and check that would refuse it up front are Phase 2
(#733). #710 stays open until it lands. The sections below separate the policy
from what the code does today.

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
| `numeric → bigint`, `double_01` | `1e30`, `1000.5`, `5` | the read fails: `Conversion Error: Type DOUBLE with value 1e+30 can't be cast … INT64` |
| `numeric → bigint`, EAV-only | `1e30`, `1000.5`, `5` | the read fails, same error |
| `numeric → integer`, `double_01` | `1e10`, `1000.5`, `5` | the read fails: `… 10000000000.0 … INT32` |
| `numeric → bigint`, `double_01` | `1000.5`, `1001.5`, `5` | succeeds silently: `1000.5` reads `1000`, `1001.5` reads `1002`, and `ratio equals 1001` matches nothing |
| `numeric → integer`, EAV-only | `1e10`, `1000.5`, `5` | succeeds silently: `1e10` decodes to `-2147483648` (amd64), and `ratio equals 1000` matches nothing although a row decodes to `1000` |

The error comes from the strict cast to the declared type in the query's final
projection (`buildOuterSelect`), which DuckDB evaluates on every row the
query's filter keeps, before the page is cut. A query therefore fails whenever
its filter keeps an incompatible row, even when that row would fall outside the
returned page; every unfiltered list does. A query whose filter excludes every
incompatible row still answers.

So the change either fails every federated query that reaches an incompatible
value, or silently changes values. Which one happens depends on the data, not
on the change.

These measurements predate #732. Since then the first four rows fail every
federated read with a `parquet_type_incompatible` refusal that names the
object, and none of them is read silently. The last row is unchanged: an
EAV-only `integer` exports the same DOUBLE column as `numeric`, so the parquet
footer carries no trace of the change (see the limits below).

### Decision

**Policy.**

1. Whether a type change is allowed depends only on the old and new types and
   the storage the attribute is bound to, **never on the stored values**:

   | change | outcome |
   |---|---|
   | narrowing: `numeric → bigint`/`integer`/`smallint`, `bigint → integer`/`smallint`, `integer → smallint` | refused, even when every stored value fits; use a migration (#730) |
   | another family: text ↔ number, `bigint → date`, bool ↔ anything | refused |
   | `smallint → integer → bigint` | allowed |
   | `smallint`/`integer → numeric` | allowed; exact in every storage |
   | `bigint → numeric`, `double_*` column or EAV-only | allowed; exact, since that storage holds only ±2^53 (#590) |
   | `bigint → numeric`, `bigint_*` column | allowed with a warning: a value beyond ±2^53 reads rounded to its float64 on every route |
   | any change of an EAV-only attribute whose old type is `smallint` or `integer` | refused until #729 is fixed |

2. Implicit evolution is lossless. Rounding, truncation, clamping, and
   substituting NULL are not implicitly approved. They require an explicit,
   operator-authorized migration (#730). The one exception is `bigint →
   numeric` in a `bigint_*` column, accepted with the warning above.
3. A refused change leaves the previous declaration active and its data
   unmodified, and nothing partially activates. The refusal names the schema,
   the attribute, the old and new types, and the remediation.
4. Changes that do not alter an existing attribute's type keep working. These
   are the #189 evolutions: adding and removing attributes, renaming them (as a
   remove plus an add), and promoting an EAV-only attribute to a bound column
   under the same type.

The first version of D2 (2026-10-24) let a narrowing activate when the stored
data happened to fit, such as `numeric → bigint` over only whole values. That
proof holds only at one moment: an old-generation process can write `1000.5`
after it. Guaranteeing it needed a writer fence across every process, and on
Aurora DSQL a read does not make two transactions conflict, so a writer that
checks the generation inside its transaction can still commit after
activation. With every allowed change decided by type alone, an old writer's
values stay valid under the new type, and no fence is needed.

Old processes that meet a value the new type allows and theirs does not fail
closed: a bound `smallint`/`integer` image by D1, a `bigint` image that names
no int64 by #590. An EAV-only `smallint`/`integer` does not (#729), hence the
last row of the table.

`items_type` changes are not separately ruled.

### Generations

**Policy.**

- A generation is a per-schema monotonic number plus a content hash over the
  JSON Schema file and its attributes file. Every edit is a new generation.
  The active generation is a column on `schema_registry`.
- **Only the control plane records a generation:** `init-db` records
  generation 1 when it registers a new schema; a baseline command seeds an
  existing schema after auditing its stored data against the current
  declaration; an apply command records each later edit. Apply accepts an
  allowed change without a separate approval step, prints the warning and
  records it on the generation.
- **The data plane only reads:** the server, the Lambda entrypoint,
  `cdc-flush`, `cdc-init` and the compactor refuse to start when the schema
  has no active generation, or when an attribute they declare is missing from
  the active generation or differs from it in type, `items_type` or binding.
  Lacking an attribute the active generation has is allowed: that is an older
  process. So apply runs first, then the data plane rolls.
- A removed attribute keeps its ledger entry, with its type, permanently. A
  rename stays a removal plus an addition under a new ID.
- Rollback is a roll-forward: a new generation with the old content, judged by
  the same rules.

Accepted consequences: once a widening or `bigint → numeric` is applied, a
process still on the old `SCHEMA_DIR` refuses to start, including a Lambda
cold start or a scale-out during the rollout, and applying the old
`SCHEMA_DIR` again is refused. Rolling the code back with the new `SCHEMA_DIR`
works. `bigint → numeric` is one-way; its reverse needs #730.

An existing deployment needs the `schema_registry` column and a baseline
before its first start on a release that enforces this; #733 owns that upgrade
procedure, including DSQL, where `init-db` cannot run yet (#690).

### Why query-time casting is not migration

Making the parquet legs read old values with `TRY_CAST` at the new type was the
alternative #710 named. It would stop the statement failures, but `1e30` would
read as absent and `1000.5` as `1000`. A query whose filter keeps an
incompatible row would then silently answer values that no longer exist
anywhere, and a predicate on the attribute would match against the cast value
(the `ratio equals 1001` case above). A query whose filter excludes every
incompatible row would not be affected. `TRY_CAST(...) = NULL` is a lost value,
not a migrated one. The cast also runs on every query and forever: nothing is
ever converted, so a later reader still sees the old type underneath.

### Comparison with PostgreSQL's `ALTER COLUMN TYPE`

A strongly typed OLTP database changes a column's type in one statement that
converts every stored row, and either the whole change commits or none of it
does. D2 keeps the all-or-nothing half of that principle and drops the
conversion: a change that would need any stored value converted is refused,
and an allowed change needs none, except the rounding that `bigint → numeric`
accepts in a `bigint_*` column.

Forma's default is **stricter than PostgreSQL**, on purpose. PostgreSQL
`ALTER TABLE … ALTER COLUMN r TYPE bigint` over a `float8` column fails the
statement on `1e30` ("bigint out of range"). Over `5, 1000.5, 1001.5` it
succeeds and stores `5, 1000, 1002`, rounding half to even. Its `numeric`
cast rounds half away from zero (`1000.5::numeric::bigint` is `1001`). Forma
refuses `numeric → bigint` whatever the values. A rounding conversion is a
migration policy an operator chooses (#730), never an implicit consequence of
editing a declaration.

### Physical storage precision

A logical value of the new type is not necessarily one the bound column can
store exactly. A `bigint` in a `double_*` column keeps only its float64 image,
which is exact within ±2^53, and the write funnel admits exactly
`[-9007199254740992, 9007199254740992]` there (#590); an EAV-only `bigint` has
the same limit. That is why `bigint → numeric` is exact in those storages,
while a `bigint_*` column holds the full int64 and is not. In the measurement
above, `2^60` and `2^53 + 2` read back as int64 values the funnel would
refuse, and the census reports them. A `numeric` value written as
`9007199254740993` was already stored as `9007199254740992` under `numeric`
(#205), so no migration can recover the written digits.

### What the code does today

- Nothing refuses a type change up front. Each process loads the new
  declaration at start.
- No persisted record of an earlier declaration exists in PostgreSQL:
  `schema_registry` maps a schema name to its id, and nothing else. The only
  persisted trace of an earlier type is the parquet column type, which
  manifests stamp per file (`FileEntry.Columns`, #256).
- **Implemented (#732):** `generate-attributes` keeps an existing attribute's
  `valueType` and `items_type` while the JSON Schema still maps it to the same
  family (numeric, text/uuid, date/datetime, bool, list), so a hand-set
  `bigint` bound to `double_01` no longer comes back as `numeric`. A change of
  family is refused for live and retired attributes alike; every refused
  attribute is listed and the attributes file is left unchanged
  (`TestGenerateAttributesRefusesFamilyChange`). It cannot see a change made
  by editing the attributes file by hand.
- **Implemented (#732):** before the main scan, a federated read judges every
  live parquet object holding a column whose type the declaration does not
  export. A lossless widening (SMALLINT under `integer`) reads as before. A
  pair the values decide (DOUBLE under `bigint`, BIGINT under `integer`, text
  under a number) is proved by one bounded scan of the object for a violating
  value, using the domain of the bound column (±2^53 for `bigint` in
  `double_*`). A different family, or a list element, is refused from the
  footer alone. A violation fails the read with
  `forma.ParquetTypeIncompatibleError`, which names the schema, attribute,
  declared type, bound column, object and offending value (text values are
  not quoted). It is never degraded to a Postgres-only answer, and nothing is
  pruned or rewritten. A manifest-listed object whose footer or proof cannot
  be read fails the read closed with the usual read-failure classification,
  so a corrupt object is still excluded as a #251 partial scan. Pinned by
  `TestCheckTypesProvesValuesUnderTheNewDeclaration` and the production
  harness's `TestTypeGuard*` scenarios.
- **Not implemented:** under the amendment, a BIGINT object under a `numeric`
  declaration reads as lossless, rounding a value beyond ±2^53. The guard
  still demands a proof that every value is within ±2^53 and refuses the rest;
  #733 changes the rule.
- **Not implemented:** the guard reads only what parquet footers show. A
  redeclaration that keeps the exported type is invisible to it: `numeric →
  integer` or `numeric → smallint` on an EAV-only attribute (all DOUBLE, the
  #729 gap), and `bigint → date` (both BIGINT). It does not check Postgres rows
  under the new declaration, and it does not prove list elements, which it
  refuses instead. Closing these needs the declaration record of Phase 2
  (#733).
- The OLTP route is protected row by row for some types: a bound or EAV
  `bigint` image that names no int64 (#590), and a bound `smallint`/`integer`
  image (D1), are refused on read. An EAV-only `smallint`/`integer` is not
  (#729).

### Why enforcement is not a narrow patch

Fail-closed enforcement needs two things the code does not have:

1. **A record of the active declaration** that every process checks before it
   serves. Today there is nothing to compare a new declaration against, and a
   check inside one tool would be bypassed by every other process.
2. **A baseline for existing deployments.** The first record of a schema that
   already holds data must not bless a declaration the data contradicts, so the
   baseline audits the stored data once: Postgres with the width census,
   parquet through manifest `Columns` or the file footers. A manifest entry
   written before #256 has no column types.

The first version of D2 needed two more: a check of every tier's data on each
activation, and a write fence against old-generation processes. The amendment
removed both by deciding each change from the types alone, as the D2 decision
above explains.

The mechanism is decided on #710 and implemented by #733. Phase 1 (#732)
catches the common cases on the federated read, after the fact. Until #733
lands, operators still uphold D2 by hand: change an existing attribute's
`valueType` only as the table above allows.

### Existing incompatible deployments

**Identify one.** Since #732 the federated read names it. The HTTP response
carries only the `parquet_type_incompatible` class, the schema id and an
`error_id`; the server's log line for that `error_id` names the attribute, the
object and the offending value. More generally, an attribute is affected when
a live parquet file holds its column at a type the current declaration does
not losslessly contain. For
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
