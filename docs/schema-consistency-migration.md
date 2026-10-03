# Schema Consistency Migration Guide

This guide covers the upgrade to the schema/metadata consistency hardening introduced in PR `#121` (`709c31a`).

## What Changed

Older releases tolerated several schema and metadata inconsistencies by logging warnings or silently skipping bad records. The hardened release turns those cases into startup or request-time errors.

The checks `#121` introduced fail on:

- duplicate `schema_id` values in the schema registry table
- duplicate `attributeID` values inside a single `<schema>_attributes.json`
- duplicate hot-column bindings inside a single `<schema>_attributes.json`
- EAV rows whose `attr_id` is not present in metadata for that schema — **since
  superseded, see below**
- EAV rows whose value is stored in the wrong physical column (`value_text` vs `value_numeric`)
- writes that reference attributes not defined in schema metadata

**Superseded since `#294`.** The runtime path no longer errors on EAV rows whose
`attr_id` is absent from metadata: it skips them on read and preserves them on
write. Since `#341` the validator likewise reports the ones a `retired` ledger
entry accounts for as informational rather than as failures. Only that one
bullet changed; the rest of the list still fails as written. See
[Unknown attribute IDs in EAV](#unknown-attribute-ids-in-eav).

A later release (`#314`) adds one more startup check and one more request-time check:

- any schema registered in `schema_registry` whose `<schema>.json` cannot be loaded and resolved fails **server startup**
- create payloads that violate their entity's JSON Schema are rejected with `400`

A later release (`#342`) adds one more startup check:

- any active attribute that rebinds a **retired** attributeID, main-column
  binding, or folded parquet column fails **server startup**

All three are covered below.

## Recommended Migration Flow

1. Stop writes to the target environment.
2. Back up the database.
3. Run the checked-in SQL script for DB-level sanity checks.
4. Run the checked-in Go validator for full metadata-aware validation.
5. Fix all reported issues — never by deleting EAV rows the validator reports in
   the informational block. Those are `#294`-preserved rows; deleting them is
   irreversible. See
   [Unknown attribute IDs in EAV](#unknown-attribute-ids-in-eav).
6. Deploy the hardened release.
7. Run the validator again after deploy.

For development and staging, a 5-15 minute maintenance window is usually enough.

## Quick Commands

DB-level SQL check:

```bash
psql "$DATABASE_URL" \
  -v schema_table=schema_registry_dev \
  -v eav_table=eav_data_dev \
  -v entity_main_table=entity_main_dev \
  -f scripts/validate_schema_consistency.sql
```

Full validation:

```bash
make validate-schema-consistency \
  DB_HOST=localhost \
  DB_PORT=5432 \
  DB_USER=postgres \
  DB_PASSWORD=postgres \
  DB_NAME=forma \
  DB_SSL_MODE=disable \
  SCHEMA_TABLE=schema_registry_dev \
  SCHEMA_DIR=cmd/server/schemas \
  EAV_TABLE=eav_data_dev \
  CHANGE_LOG_TABLE=change_log_dev \
  ENTITY_MAIN_TABLE=entity_main_dev \
  WIDTH_EXPORT_CUTOVER=2026-08-29T00:00:00Z
```

`WIDTH_EXPORT_CUTOVER` is optional; see
[EAV integer values past their declared width](#eav-integer-values-past-their-declared-width-501).

Direct tool invocation:

```bash
./build/tools validate-schema-consistency \
  --db-host localhost \
  --db-port 5432 \
  --db-user postgres \
  --db-password postgres \
  --db-name forma \
  --db-ssl-mode disable \
  --schema-registry-table schema_registry_dev \
  --schema-dir cmd/server/schemas \
  --eav-table eav_data_dev \
  --change-log-table change_log_dev \
  --entity-main-table entity_main_dev \
  --width-export-cutover 2026-08-29T00:00:00Z
```

Add `--requeue-stale-width-exports` to repair the stale exports the
integer-width census reports; see
[EAV integer values past their declared width](#eav-integer-values-past-their-declared-width-501).

## What Each Checker Covers

### SQL Script

`scripts/validate_schema_consistency.sql` is useful when you want a fast database-only pass in `psql`.

It checks:

- duplicate `schema_id` values in the schema registry table
- duplicate `schema_name` rows in the schema registry table
- `eav_data.schema_id` values that do not exist in the registry
- `entity_main.ltbase_schema_id` values that do not exist in the registry
- rows with both `value_text` and `value_numeric` populated
- rows with neither `value_text` nor `value_numeric` populated
- duplicate logical primary keys in `entity_main`
- duplicate logical primary keys in `eav_data`

### Go Validator

The Go validator reuses the same metadata loading path the server uses at runtime. That means it will catch the same startup failures before you deploy.

It validates:

- schema registry rows can be loaded without duplicate schema IDs
- every referenced `<schema>_attributes.json` parses successfully
- each schema’s metadata has unique `attributeID` values
- each schema’s metadata has unique `column_binding.col_name` values
- every active `column_binding.col_name` is a column `entity_main` has
  (`#557`) — `foo`, `text_99` or `TEXT_01` are refused; the server refuses to
  start on the same shapes, so run this before deploying a build carrying the
  guard
- every active `column_binding` can round-trip its `valueType` through the
  bound column and encoding (`#459`) — e.g. `text`→`uuid_02` or `bool` with
  the default encoding are refused; the server refuses to start on the same
  shapes, so run this before deploying a build carrying the guard
- `eav_data.attr_id` values all map to known metadata IDs for the same
  `schema_id` — ids belonging to a `retired` ledger entry (`#342`) are reported
  as informational rather than as failures, because those rows are the `#294`
  preserved state (`#341`)
- numeric/date/bool values are not incorrectly stored in `value_text`
- text/uuid/list values are not incorrectly stored in `value_numeric`
- no `list` attribute still carries a scalar row (`array_indices = ''` with a
  value), left over from before the attribute became a list (`#372`)
- no unbound `date`/`datetime` attribute (or list of them) carries an
  `eav_data.value_numeric` image the upgraded read path refuses: a number
  that is not whole (a fraction, `NaN`, `±Infinity`) or one past 2^53 epoch
  milliseconds (`#592`). After the upgrade such a row, and every update of it
  that does not name the attribute, is a consistency error
- no EAV-only `smallint`/`integer`/`bigint` value (list items included) lies
  outside its declared width or is non-integral in a way that makes the tiers
  disagree (`#501`). It reads `--change-log-table` to tell exported rows from
  pending ones; pass `--change-log-table ''` on a deployment without CDC
  (or set `CHANGE_LOG_TABLE=` explicitly empty: the tool and the
  `make validate-schema-consistency` target keep an empty value instead of
  falling back to `change_log_dev`)
- no `bigint` bound to a `double_*` column holds a value outside ±2^53 or one
  that is not a whole number (`#618`). The census reads these columns from
  `--entity-main-table`, and fails when a schema declares such a binding and
  the table name is empty

Use both checks before upgrading. The SQL script gives quick database facts; the Go validator gives the final runtime-compatible answer.

## Interpreting Failures

### Duplicate schema IDs

Example validator failure:

```text
validate-schema-consistency: load schema metadata: failed to load schema registry: duplicate schema id 100 for contact and lead
```

Fix by assigning one schema a new unused ID and updating the corresponding `schema_id` foreign keys in:

- `eav_data_*`
- `entity_main_*`
- `change_log_*`

### Duplicate attribute IDs in metadata

Example startup failure:

```text
schema contact has duplicate attribute id 7 for email and phone
```

Fix the conflicting `<schema>_attributes.json` file and re-run the validator.

### Duplicate column bindings

Example startup failure:

```text
schema contact has duplicate column binding text_01 for email and phone
```

Only one attribute may own a given hot column inside a schema. Remove or change one binding.

### Unknown attribute IDs in EAV

Example validator output:

```text
- unknown attribute IDs in eav_data_dev: schema_id=100 attr_id=99 rows=12
```

That means the table contains rows that current metadata cannot decode. **Three**
different things can put an `attr_id` in this state, and they call for opposite
actions: the rows were **never legitimate** (case (i)), they are **preserved by
attribute removal** (`#294`, case (ii)), or the attribute is still legitimate and
its **metadata was lost by accident** (case (iii)). Since `#341` the validator
classifies case (ii) for you whenever the ledger carries the retired entry, so
the manual determination below is only needed for what the validator still
reports as a **failure**.

Concretely: rows whose `attr_id` matches a `retired` ledger entry are reported in
a separate informational block and do not fail the run.

```text
schema consistency checks passed for 3 schema(s), 1 informational finding(s)
informational (not a failure):
- preserved EAV rows for retired attributes in eav_data_dev: schema=visit schema_id=1 attr_id=25 attribute=contactSnapshot rows=12
```

Everything else still comes out as an `unknown attribute IDs` **failure**, and
that is what the rest of this section is for.

**Determine which case you are in before touching any row.** Version control on
the schema files is the record: check whether that `attr_id` was ever a
legitimate attribute in an earlier generation of the schema's
`<schema>_attributes.json`, whether it was later removed, and whether the entry
that should describe it is simply missing. The determination is yours whenever
the validator reports a **failure** rather than an informational finding, and
three possibilities remain at that point: the rows were never legitimate (case
(i)), the attribute was retired **before** `#342` so no `retired` entry records
it and the validator can only see an unledgered id (case (ii), *unledgered
variant* — the ledgered majority of case (ii) never reaches you as a failure at
all), or legitimate metadata was lost by accident and must be restored
(case (iii)).

**Case (i) — never legitimate.** The rows came from a bad deployment, a
mis-mapped import, or leftover test data: no schema generation ever defined that
`attr_id`, and no metadata is missing. Fix by deleting the orphaned EAV rows.

**Case (ii) — preserved by attribute removal (`#294`).** The attribute did exist and was removed from the schema. Since `#294` (tolerate-and-preserve) these rows are the **expected** state: the read path skips them and the write path preserves them untouched, so removing an attribute is non-destructive and re-adding it (same `attributeID`) restores the stored values. **Do not delete them.** Deletion is destructive and irreversible — it permanently forfeits the re-add restore path — and nothing else in the system is asking you to do it. Leave the rows in place and treat the validator finding as informational.

This case reaches you as a *failure* only when the attribute was removed before
`#342`, so the ledger carries no `retired` entry to classify it against. The
repair is to rebuild that entry, not to delete the rows — see
[Removing an attribute](#removing-an-attribute-342).

**Case (iii) — metadata lost by accident.** The attribute is still legitimate and
genuinely belongs to the schema, but its metadata is gone — a partial deploy, a
rollback that reverted the attributes file but not the data, or a ledger entry
hand-deleted before `#342`. The fix is to **restore the metadata, not to touch
the rows**: put the property back into `<schema>.json` and re-run
`generate-attributes` (or restore the attributes file from version history),
keeping the original `attributeID`. Once metadata describes the id again the
rows decode as they always did. If the attribute is not wanted back, retire it
properly instead — see
[Removing an attribute](#removing-an-attribute-342).

If you cannot establish which case applies, treat the rows as case (ii) and leave them alone; keeping undecodable rows costs storage, deleting recoverable ones costs the data.

Whichever case applies, an `attributeID` freed by removing an attribute must never be reused for a different attribute: the preserved rows would silently bind to the new attribute's name, or make the row unreadable with a storage type mismatch. Since `#342` that is enforced at startup rather than left to convention — see [Removing an attribute](#removing-an-attribute-342) below.

### Removing an attribute (`#342`)

**The blessed workflow is: delete the property from `<schema>.json`, then run
`generate-attributes`. Never hand-delete an entry from
`<schema>_attributes.json`.** The attributes file is the schema's attributeID
ledger, not just its list of active attributes. The generator keeps the dropped
attribute's entry, marks it `"retired": true`, and forces its required policy to
optional — so the `attributeID`, any `column_binding`, and the attribute's
folded parquet column all stay reserved against the EAV rows `#294` preserved.

Retired entries are ledger-only. Every metadata registration path validates the
**full** file — retired entries included — and strips them only afterwards, so a
retired attribute reads, writes, flushes, and projects exactly as if its entry
were absent.

The `retired` marker is **generator-owned**: it records that
`generate-attributes` no longer produces the entry. Hand-setting it on an
attribute the schema still declares is not a supported way to hide a live
attribute — the next `generate-attributes` run finds the property, sees the
type is unchanged, and silently clears the marker again. To retire an
attribute, remove it from `<schema>.json` and regenerate.

**Startup guard.** Handing a retired `attributeID` to a different attribute
aborts startup:

```text
schema contact reuses attribute id 7: retired attribute phone (valueType text) still owns preserved EAV rows and cannot be rebound to mobile; re-add the original name and valueType to restore it, or assign a new id
```

The main-column analogue fires when an active attribute claims a retired
attribute's hot column:

```text
schema contact reuses main column text_01: retired attribute phone (valueType text) still owns its stored values and cannot share the column with mobile; keep the binding retired or assign a new column
```

A folded-parquet-column collision with a retired attribute is rejected the same
way: already-flushed parquet files still carry that column. In every case the
fix is to give the **new** attribute an unused id/column — never to delete the
retired entry.

**Re-adding.** Putting the property back into `<schema>.json` under the same
name, the same `valueType` **and** the same `items_type`, then re-running
`generate-attributes`, clears the `retired` marker; the preserved EAV rows
become visible again — **but only for rows that have not yet been flushed to
the lakehouse.** The CDC export builds its `attr_id IN (...)` filter from the
*active* attribute cache, which no longer contains the retired entry
(`internal/cdc/export_sql_builder.go`), so a retired attribute's values are
never written to delta or base parquet. Once a row has flushed
(`change_log.flushed_at != 0`) it is served from the warm/cold tier, where that
column simply does not exist, and the preserved Postgres EAV row is unreachable
by federated reads. **On a CDC-enabled (lakehouse) deployment, treat retirement
as effectively irreversible for already-flushed rows**; un-retiring restores
values only for rows still resident in the hot tier.

The asymmetry cuts the other way too. Because the Postgres EAV row survives, an
unrelated later update makes that row hot again, and the *next* flush exports
the re-added attribute — resurrecting per row a value that read `NULL` while
the attribute was retired. Plan a retirement on the assumption that the value
disappears from reads immediately and may reappear row-by-row on subsequent
writes. This is inherited `#294` tolerate-and-preserve behavior, not something
`#342` introduced; `#342` only makes the retirement explicit in the ledger.

Re-adding under a different `valueType` or `items_type` is
a generator error naming the attribute and both the old and the new
type/items — the stored rows carry the old physical type and would be
unreadable. Renaming an attribute is *not* a re-add: it needs a fresh
`attributeID`, and the old entry stays retired. Note the boundary: this type
check fires **only** on retired entries. Changing the `valueType` of a *live*
attribute is still written straight through by `generate-attributes`, which
leaves that attribute's existing EAV rows in the wrong physical column — the
condition the next section tells you to repair. Treat a live type change as a
remove-then-add-under-a-new-name, not an edit.

**Shipped schemas already carrying the marker.** Two entries are now
`retired: true`, for different reasons — in both cases because
`generate-attributes` no longer *produces* the entry, which is what the marker
records:

- `visit_full_attributes.json` `logs` (`attributeID` 29) — the `logs` property
  was removed from `visit_full.json`. This is the ordinary removal case, and it
  is the **breaking** one of the two.
- `visit_attributes.json` `contactSnapshot` (`attributeID` 25) — the
  `contactSnapshot` property is still declared in `visit.json`, but as a `$ref`
  to `lead.json#/properties/contact`, which is an **object**. The generator now
  resolves that `$ref` and traverses into it, emitting one entry per leaf
  (`contactSnapshot.annualIncome`, `contactSnapshot.birthday`, …) and no bare
  `contactSnapshot` entry. The `attributeID` 25 entry is a leftover from when
  `$ref`s were not followed and the node was recorded as a single `text` value.

In both cases the EAV rows under those ids flip from visible to skipped-on-read,
and the rows themselves are preserved (`#294`). **The operational impact of the
two is not symmetric**, so assess them separately:

- **`logs` (29) is write-breaking, not merely read-affecting.**
  `visit_full.json` declares no `logs` property *and* sets no
  `additionalProperties`, so JSON Schema validation lets the extra key through:
  before this change **both** payload shapes reached `attributeID` 29. `logs`
  was declared an *array of strings* until it was removed, and the transformer
  recurses into an array under the *bare* attribute name — one EAV row per
  element, distinguished only by `array_indices` — while a scalar payload lands
  on the same name with empty `array_indices`. Either shape resolved through the
  attribute cache, so id 29 was written and read back. After this change the
  attribute is stripped from the active cache, so a write carrying `logs` in any
  shape is rejected with `400 attribute 'logs' is not defined for this schema`,
  and any values already stored under id 29 disappear from reads. If any client
  was writing `logs` to `visit_full`, this is a breaking API change for that
  client, and — unlike an ordinary retirement — **`attributeID` 29 cannot be
  restored under its original shape**:
  - Re-adding the original array declaration makes `generate-attributes` emit
    `valueType: list` / `items_type: text` (the shape `attendees` carries in the
    same ledger). Entry 29 is recorded `valueType: text`, so the retired-re-add
    type check refuses it and tells the operator to "restore the original type
    or use a new attribute name" — a demand the original type cannot satisfy.
  - Re-adding it as a scalar `text` property does pass that check and un-retires
    29, but it does not repair the break: since `#314`, JSON Schema validation
    rejects the array payload a legacy client sends, and the preserved rows
    carry `array_indices`, so reads materialize an array under a property the
    schema now declares a string.

  So there are two honest paths, and neither restores id 29 to service:
  - Accept the break: update clients to stop sending `logs`. Values under id 29
    stay preserved on disk but unreadable.
  - Declare a **new** property under a different name as an array of strings.
    `generate-attributes` treats an unseen name as new, assigns it a fresh
    `attributeID` above the current maximum with `valueType: list` /
    `items_type: text`, and leaves entry 29 retired with its rows untouched. The
    old values do not migrate themselves — copy them across if they matter.
- **`contactSnapshot` (25) is practically inert.** Reaching the bare
  `attributeID` 25 leaf requires the client to send `contactSnapshot` as a
  *scalar*: an object payload recurses through the map branch of the
  transformer's flattening and lands on `contactSnapshot.<leaf>` entries, never
  on the bare name. Neither shape reaches the writer today.
  `contactSnapshot` carries `x-relation`, and
  `RelationIndex.StripComputedFields` has always removed the bare root from the
  payload whatever the type of its value, so a scalar has not been written since
  the strip existed; `#318` extended the strip to the dotted descendants too.
  Both are removed before validation and before persistence, and silently rather
  than as a rejection (see `docs/error-handling.md`, "Relation subtrees are
  never caller-writable"). So rows under id 25 can only exist if some client
  wrote a scalar before that, and no supported write path can produce new ones.
  `contactSnapshot` also has no property to re-add — it would un-retire only if
  that node ever resolved to a scalar again, which is not an operator action;
  treat `attributeID` 25 as permanently reserved.

**Residual gap.** An attribute whose ledger entry was hand-deleted *before*
`#342` leaves no record at all, so the guard cannot see it and its
`attributeID` looks free. For those schemas the never-reuse rule is still
documentation-only: check the schema files' version history before assigning any
`attributeID` that no current entry claims.

**Rebuilding a lost ledger entry.** Hand-add the entry back to
`<schema>_attributes.json` under its original name, with its original
`attributeID`, its original `valueType` (and `items_type`, for a list), **and its
original `column_binding.col_name` if the attribute had a hot column**, marked
retired:

```json
"legacy_field": {
  "attributeID": 9,
  "valueType": "text",
  "column_binding": {"col_name":"text_01"},
  "retired": true
}
```

The `column_binding` matters as much as the id, and like `valueType` it has to be
recovered from the schema files' version history — nothing derives it. Omit it
and the entry reserves only the id: a later attribute can bind that main-table
column with the guard silent, and reads then serve the retired attribute's stale
values out of `text_01`. The third reserved resource, the folded parquet column,
needs no separate field — it is derived from the attribute name, which this
recipe already requires you to restore.

This is the one hand-edit the ledger sanctions, and it is not the unsupported
case described above: the schema no longer declares the property, so the next
`generate-attributes` run finds nothing to re-add and keeps the marker. Once the
entry exists, the reuse guard protects the id, the main column and the folded
parquet column, and `validate-schema-consistency` reclassifies its preserved rows
as informational (`#341`).

Two things to expect. The entry is now subject to full-ledger validation, so if
that `attributeID`, that `column_binding.col_name`, or that attribute name's
folded parquet column was already handed to a different attribute during the
pre-`#342` era, startup will fail naming both — that is pre-existing corruption
surfacing, not a regression, and the fix is to give the *newer* attribute an
unused id/column/name. And the `valueType` you record must be the one the stored
rows actually carry: get it wrong and a future re-add restores values through the
wrong physical column.

### Storage-column mismatches

Example validator output:

```text
- numeric/date/bool attributes stored in value_text: schema_id=100 attr_id=2 rows=3
```

This means the row uses the wrong physical value column for the declared `valueType`.

Fix by rewriting the bad rows into the correct column and clearing the wrong one.

### Column bindings to unknown entity_main columns (`#557`)

Example validator output:

```text
- column bindings to unknown entity_main columns: schema=contact attribute nick (valueType text) binds to unknown main column text_99: column_binding.col_name must be one of ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, ltbase_created_by, ltbase_deleted_by, ltbase_updated_by, text_01, text_02, text_03, text_04, text_05, text_06, text_07, text_08, text_09, text_10, smallint_01, smallint_02, smallint_03, integer_01, integer_02, integer_03, bigint_01, bigint_02, bigint_03, double_01, double_02, double_03, uuid_01, uuid_02
```

The `column_binding.col_name` is not an `entity_main` column. Before the
guard, the binding loaded (a name like `text_99` or `foo` classifies as a
text column by prefix) and every write to the attribute failed with
`unsupported column`, while reads never returned it. The server now refuses
to load the schema; fix the attributes file before deploying:

- rebind the attribute to a listed column of the right family (e.g.
  `text_04`), or
- drop the `column_binding` so the attribute lives in EAV.

No stored value can exist under the bad name, so no data migration is needed.

The admitted set in the message is the one `entity_main` column set: the
`init-db` DDL, the `forma.MainColumn*` constants and the runtime's column list
(`internal/model/columns.go`, which the writer, the read projection and the
CDC column order derive from) all state it, and tests pin the three together
(`#585`). Before `#585` the DDL also created `bigint_04`, `bigint_05`,
`double_04` and `double_05`, which the runtime never wrote, projected or
flushed; a database initialised by an older build still carries them as
empty columns, and a binding to one of them is refused like any other
unknown name. They can be dropped or left in place. The v0.2.0 constants
`forma.MainColumnBigint04`, `MainColumnBigint05`, `MainColumnDouble04` and
`MainColumnDouble05` still compile, marked `Deprecated`, so a dependent
build does not break on update; a binding built from one is refused at
registration with the same error. Rebind to `bigint_01..03` or
`double_01..03` before the next breaking release removes them.

### valueType/column-encoding binding mismatches (`#459`)

Example validator output:

```text
- valueType/column-encoding binding mismatches: schema=log attribute leadId (valueType text) cannot round-trip through main column uuid_02 (uuid column, encoding default): text binds only to text columns (default encoding)
```

The attribute's `valueType` and its `column_binding` disagree about the
physical encoding. Before the guard, such a write answered a redacted 500
(text into a uuid column) or silently dropped the value (text into a numeric
column). The server now refuses to load the schema; fix the attributes file
before deploying:

- rebind the attribute to a column of the right family (e.g. `text_02`), or
- change `valueType` to what the column stores (e.g. `uuid`) if every stored
  and admissible value already has that shape.

Existing rows in the old column are not moved by either change; migrate them
with SQL first if they must survive. For the shipped `log` schema, which #459
rebound `leadId` from `uuid_02` to `text_02` and `visitId` from `uuid_01` to
`text_03`:

```sql
-- shipped `log` schema: leadId uuid_02 -> text_02, visitId uuid_01 -> text_03
UPDATE entity_main
   SET text_02 = uuid_02::text
 WHERE ltbase_schema_id = <log schema id> AND uuid_02 IS NOT NULL;
UPDATE entity_main
   SET text_03 = uuid_01::text
 WHERE ltbase_schema_id = <log schema id> AND uuid_01 IS NOT NULL;
-- NULL the old columns once the new binding is live
```

Parquet tiers (delta/base) are keyed by attribute name, not by main column, so
they need no rewrite. The shipped `cmd/server/schemas/log_attributes.json`
itself was rebound in #459; a deployment still carrying the old copy must apply
this migration before upgrading, or the server refuses to load the schema.

Admitted pairs: `text`→text; `uuid`→uuid; `smallint`/`integer`/`bigint`/`numeric`
→ smallint/integer/bigint/double (a value that does not fit the column's
width is refused at write time as invalid input); `date`/`datetime` (whose
logical value is epoch milliseconds: input finer than a millisecond is
floored to the millisecond before any destination rule judges it, #589)
→bigint (`unix_ms` or default, the full int64 epoch-ms range, which the HTTP API
renders as an epoch-ms string outside years 0000-9999, #591) or text (`iso8601`, an
RFC3339 string at whole seconds within the layout's four-digit year: a value
whose epoch millis are off a whole second is refused at write time as invalid
input rather than truncated, and a value outside 0000-01-01T00:00:00Z to
9999-12-31T23:59:59Z is refused rather than stored as an image the RFC3339
reader cannot parse, #582); `bool`→smallint (`bool_smallint`) or text
(`bool_text`); `list` never binds. An unbound `date`/`datetime` keeps the
float64 image in `eav_data.value_numeric` and admits |millis| ≤ 2^53, refusing
anything past it as invalid input; the read judges a stored image by the same
rule (`#592`, see
[Date images the read path refuses](#date-images-the-read-path-refuses-592)).

Some refused pairs do store and read back losslessly on the Postgres path:
`uuid`→text, `bool`→smallint/integer/bigint/double with the default encoding,
and `date`/`datetime`→double with the default encoding. They are refused as a
matter of policy, not because stored data is at risk: the filter rendering
and the DuckDB projection key on the declared type and the encoding, so a
`bool` with the default encoding is compared as text `'1'`/`'0'` and a `uuid`
attribute is projected from a UUID-typed column. If a deployment carries one
of these shapes, no row is corrupted; rebind with the explicit encoding
(`bool_smallint`, `bool_text`, `unix_ms`) or to the column family the
valueType names, and migrate the existing column values with SQL as above.

### Scalar rows under list attributes (`#372`)

Example validator output:

```text
- scalar rows stored under list attributes in eav_data_dev: schema=lead schema_id=100 attr_id=18 attribute=tags rows=7
```

The attribute's metadata now says `valueType: list` (typically because the
schema property changed from a scalar to an array and `generate-attributes` was
re-run), but these rows still hold the value the way a scalar is stored: one row
with `array_indices = ''` and a non-NULL value column. List elements are stored
one row per element with `array_indices` set to the element index (`'0'`,
`'1'`, ...). The write path has rejected a scalar body for an array property
since `#314`, so such rows are always pre-existing data, never new writes.

The row is not lost, but the tiers disagree on its shape until it is rewritten:
the OLTP read returns the scalar (`"alpha"`), while the parquet export and the
DuckDB hot-tier pivot return a one-element list (`["alpha"]`). Before `#372`
the DuckDB tiers returned `[]` instead, silently dropping the value. The
one-element list assumes the value sits in the storage column the list's items
type uses; a legacy value in the other column (a text value under a numeric
list, or the reverse) reads as `[null]` on the DuckDB tiers and is reported by
the storage-column checks above as well as by this one, so fix the column
first and then promote the row.

The explicit empty-list marker (`#204`), which is the same key with **both**
value columns NULL, is a supported state and is never reported here.

Inspect the rows:

```sql
SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric
FROM eav_data_dev
WHERE schema_id = 100 AND attr_id = 18 AND array_indices = ''
  AND (value_text IS NOT NULL OR value_numeric IS NOT NULL)
LIMIT 50;
```

Fix by promoting each scalar row to element `'0'` of its list. The primary key
is `(schema_id, row_id, attr_id, array_indices)`, so guard against an entity
that already has an element `'0'` (a list written after the flip while the
legacy row was left behind); those rows need a decision per entity, not a
blanket update:

```sql
UPDATE eav_data_dev AS s
SET array_indices = '0'
WHERE s.schema_id = 100 AND s.attr_id = 18 AND s.array_indices = ''
  AND (s.value_text IS NOT NULL OR s.value_numeric IS NOT NULL)
  AND NOT EXISTS (
    SELECT 1 FROM eav_data_dev AS d
    WHERE d.schema_id = s.schema_id AND d.row_id = s.row_id
      AND d.attr_id = s.attr_id AND d.array_indices = '0'
  );
```

Until that per-entity decision is made, the DuckDB tiers read a mixed entity as
the real elements in index order followed by the legacy value last
(`["e0", "e1", "legacy"]`): the empty index casts to NULL, which the ordered
aggregate places after every element index. The position is deterministic but
carries no meaning, so do not read it as the entity's intended list.

After the rewrite, re-run the validator. A direct SQL rewrite does not stamp
`change_log`, so an entity that was already flushed keeps its last exported
value until its next write re-flushes it. That is harmless for exports made
after `#372`, which already carry the one-element list the rewritten row
produces; parquet written before `#372` holds `[]` for the row and serves that
from the warm/cold tiers until the entity is written again.

### EAV integer values past their declared width (`#501`)

Example validator output:

```text
- EAV integer values whose parquet copy predates the #384 storage-width export in eav_data_dev: schema=lead schema_id=100 attr_id=14 attribute=qty declared=integer row_id=6f1c… value=4294967296 last_flushed_at=1756300000000
- bigint EAV values outside ±2^53 (the float64-exact range) or non-integral in eav_data_dev: schema=lead schema_id=100 attr_id=15 attribute=big declared=bigint row_id=0b7e… value=9007199254740994
- bigint values in double columns outside ±2^53 (the float64-exact range) or non-integral in entity_main_dev: schema=lead schema_id=100 attr_id=16 attribute=ratio declared=bigint row_id=3c9a… column=double_01 value=9007199254740994
informational (not a failure):
- exported EAV integer values outside the declared width, which may predate the #384 storage-width export (pass -width-export-cutover to confirm), in eav_data_dev: schema=lead schema_id=100 attr_id=14 attribute=qty declared=integer row_id=91d2… value=1.5 last_flushed_at=1756300000000
```

`eav_data.value_numeric` is an unconstrained `NUMERIC`. Before `#384` the write
path accepted an EAV-only `smallint`/`integer`/`bigint` value that did not fit
the declared width (`4294967296` under `integer`, `1.5` under anything). Since
`#384` the write path rejects such values, but rows written earlier are still
there. For `bigint` the funnel judged the exact int64 rather than the float64
image `eav_data` stores until `#590`: a value past 2^53 was accepted and
stored rounded to its float64 image (`9007199254740993` as
`9007199254740992`; an int64 from `9223372036854775296` (2^63−512) up as
`9223372036854775808`, 2^63, which `#612` had already refused). Since `#590`
the funnel admits exactly `[-9007199254740992, 9007199254740992]`, the range
the image keeps exactly, and the census reports every stored `bigint` outside
it. The census lists every such row, one line per stored element
(`array_indices` is shown for list items), and classifies it by what the tiers
serve for it:

- **Stale export (failure).** A `smallint`/`integer` row whose last flush ran
  before `--width-export-cutover`. That flush exported the value through
  `TRY_CAST(value_numeric AS INTEGER/SMALLINT)`, so the parquet copy holds
  NULL (out of range) or a rounded value (`1.5` → `2`). The row is not in the
  dirty set, so the federated route serves that copy while the OLTP route
  reads the true value. Compaction only merges parquet, so it never repairs
  the copy.
- **Candidate (informational).** The same row shape when no cutover is given:
  the validator cannot tell whether the last flush predates `#384`. Pass the
  cutover to turn candidates into either failures or nothing.
- **bigint out of contract (failure).** A `bigint` value past ±2^53 or with
  a fraction. One past 2^53 was rounded on the write, so no route can recover
  the caller's value; every route reads the rounded image. One past int64 or
  with a fraction is projected by every DuckDB leg, the unflushed hot leg
  included, through `TRY_CAST(value_numeric AS BIGINT)` to NULL or a rounded
  value, and the OLTP route refuses the read with an error naming the
  attribute and row (before `#590` it converted through Go's `int64()`, whose
  result past int64 depends on the platform). A re-flush reproduces the same
  image, so only rewriting the value repairs it.

Rows that are pending (`change_log.flushed_at = 0`), never exported, or last
exported at or after the cutover are not reported: every tier reads them the
same way under `#384`'s storage-width (`DOUBLE`) projection, even though the
value is still outside the declared width.

**The cutover** is the time from which every `cdc-flush` run used the `#384`
export: the first flush of the first build carrying `#384`, or any later time
you are sure of. Passing a later cutover is safe. It only turns more rows into
failures, and requeuing a row whose copy was already correct only re-exports
the same values. Passing an earlier one lets stale exports through.

**Repair the smallint/integer classes** by re-running the validator with
`--requeue-stale-width-exports`, then running `cdc-flush`:

```bash
./build/tools validate-schema-consistency ... \
  --width-export-cutover 2026-08-29T00:00:00Z \
  --requeue-stale-width-exports
./build/tools cdc-flush ...
```

The requeue touches no value. For each reported row, once, it takes the row's
version lock, advances `entity_main.ltbase_updated_at` by the same
`GREATEST(now, previous + 1)` rule a write uses, and stamps `change_log` slot 0
with that version. The row enters the dirty set immediately, so the federated
route serves the Postgres value from that moment. The next flush re-exports it
at storage width with a newer `ver_ts` than the stale copy, which therefore
loses the merge on the warm and cold tiers. Requeued rows are listed in the
informational block, so a repair run exits zero unless something else fails. A
row with no `entity_main` row cannot be requeued and stays a failure; it is an
orphan to delete or restore by hand. A requeue does not need
`--width-export-cutover`: without one it requeues every candidate. It does need
non-empty `--change-log-table` and `--entity-main-table`; the validator rejects
an empty one before the census runs, so no row is requeued. After
`cdc-flush`, re-run the validator with the same cutover to confirm the report
is clean.

A direct SQL `UPDATE` of `eav_data` does **not** repair a stale export: it does
not stamp `change_log`, so the row stays out of the dirty set and the warm and
cold tiers keep serving the old copy.

**Repair the bigint class** by rewriting the value through the API as an
integral value from `-9007199254740992` to `9007199254740992`. That is the
range the write funnel accepts for an EAV-only `bigint` (`#590`): `eav_data`
keeps only the float64 image, which is exact within ±2^53 and rounded past
it. An API write stamps `change_log`, so the next flush re-exports the entity.
Decide per row whether the value was meant to be clamped, rounded, or moved to
a `numeric` attribute (or to a column-bound `bigint`, which keeps the full
int64 range). Name the attribute in the update itself, nested
(`{"contact":{"total":42}}`) or as its literal dotted key
(`{"contact.total":42}`), under any `required_policy`. An update never decodes
a stored value its written row discards, so an update naming the attribute
repairs every image the census reports and keeps the attributes it does not
name. That includes one past int64 or with a fraction, which the read itself
refuses. An update replaces a list as a whole, so repair a list item by
sending the whole list. An update
that leaves the attribute out merges into the document the OLTP route reads,
so it re-submits the stored value. A whole image past 2^53 is refused as
invalid input that names the attribute and its image. One past int64 or with a
fraction fails the read as a server error that names the attribute and the
row. Either way, the unrelated update is blocked until the attribute is
rewritten.

**A `bigint` bound to a `double_*` column** holds the same contract since
`#590`. The column keeps only the float64 image, so the funnel admits ±2^53,
an unrelated update is blocked over a stored value outside that range, and an
update naming the attribute repairs it. The census scans these columns too
(`#618`). For every attribute with `valueType` `bigint` and a
`column_binding.col_name` of `double_01`..`double_03`, it reads that column of
`--entity-main-table` and reports each row whose value is a whole number past
±2^53, a fraction, `NaN`, or an infinity. The comparison is exact, because
2^53 is a float64, so `±9007199254740992` and every whole number between them
are never reported. Each line is a failure of the bigint class and names the
schema, the attribute, the row, and the column (`column=double_01`). `value`
is the stored float64 in plain digits (`9223372036854775808` for 2^63), or in
exponent form from 1e21 up. `last_flushed_at` is shown when the row has been
exported, as for an EAV line.

The census needs `--entity-main-table` whenever a schema declares such a
binding. With an empty table name it stops with an error naming the first
such attribute instead of skipping the scan. Without such a binding the
census does not read the table.

Repair each reported row the same way as an EAV `bigint`: rewrite the value
through the API, naming the attribute. `--requeue-stale-width-exports` never
requeues these rows, because a re-flush does not change what the column
holds. What the routes serve until the repair depends on the image. A whole
number past ±2^53 inside int64 reads as the stored value on every route. Any
other image is refused by the OLTP route with an error naming the attribute
and the row. The federated route reads a fraction rounded to a whole number.
For an image past int64, `NaN`, or an infinity, the federated route fails
every query that reaches the row while the row is unflushed, and reads the
attribute as absent once it has been exported (`#627`). Repair those rows
first.

### Date images the read path refuses (`#592`)

Example validator output:

```text
- date/datetime images the read path refuses in eav_data_dev: schema=visit schema_id=100 attr_id=1 attribute=seenAt rows=2 (value_numeric must be a whole number with |value| <= 9007199254740992)
```

An unbound `date`/`datetime` (or a list of them) is stored in
`eav_data.value_numeric` as the float64 image of its epoch milliseconds,
which is exact within ±2^53 (`9007199254740992`, years -283457 to 287396).
Since `#592` the write path admits exactly that range and refuses anything
past it as invalid input. The OLTP read judges the stored digits, not a
float64 rounding of them, by the same rule: a whole number with |millis| ≤
2^53. A row that reads can therefore always be rewritten. That matters
because an update rebuilds the whole document and re-enters the write
funnel: an image the read accepted and the write refused would fail an
update that never mentioned the attribute, as the caller's invalid input.
The federated read applies the same rule to the value its projection
receives. A Parquet `BIGINT` reaches the projection with its digits intact.
A value the DuckDB Postgres scanner reads does not: until `#621` the scanner
narrows it to a float64 first, on the hot leg and in the CDC export that
writes the Parquet copies (see below).

Before `#592` no rule judged the unbound destination. A row written earlier,
or edited by hand, can hold a whole number past 2^53, a fraction, `NaN`,
`±Infinity`, or a number past int64, and the Postgres read turned these into
a nearby instant, a truncated one, a wrapped one, or an absent attribute.
After the upgrade every OLTP read of such a row (a `GET`, and a list or query
the Postgres route serves), and every update that does not name the
attribute, fails with an operator-visible error, never a 4xx:

```text
datetime value of attribute 1 in value_numeric: stored value 9007199254740993 (287396-10-12T08:59:00.993Z) is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53); rewrite it with an update that names the attribute, or bind the attribute to a bigint column (docs/schema-consistency-migration.md)
```

A fraction, `NaN` or an infinity reads as `… (not a whole number of epoch
milliseconds) names no epoch millisecond instant`. The error names the row and
the attribute, and the server never modifies the stored row. Run the
validator before upgrading. Its predicate is the read's own rule
(`transform.StoredDateImageRefusedSQL`), so it lists every attribute whose
rows the upgraded read refuses.

**The federated tiers read these rows differently until `#621`.** The DuckDB
Postgres scanner types `NUMERIC` as `DOUBLE` before any expression Forma
renders, so the federated hot leg, and the Parquet copy the CDC export writes
from the row, read such an image narrowed: 2^53+1 as 2^53, `1000.5` as
`1000`, `NaN` and the infinities as 1970-01-01T00:00:00Z, and a number past
int64 as absent. One row can therefore fail on the OLTP route and answer a
different instant on the federated one. The census is the only guard for
those tiers, so clear it before relying on either route.

Inspect the rows:

```sql
SELECT schema_id, row_id, attr_id, array_indices, value_numeric
FROM eav_data_dev
WHERE schema_id = 100 AND attr_id = 1 AND value_numeric IS NOT NULL
  AND (value_numeric <> trunc(value_numeric) OR abs(value_numeric) > 9007199254740992)
LIMIT 50;
```

Then decide per attribute:

- **The value is not a real instant.** Every date past ±2^53 lies after
  year 287396 or before year -283457, and a fraction was never a
  millisecond. Rewrite the value through the API with an update that names
  the attribute, nested (`{"visit":{"endAt":…}}`) or as its literal dotted
  key (`{"visit.endAt":…}`), under any `required_policy`. An update never
  decodes a stored value its written row discards, so it repairs every image
  the census reports and keeps the attributes it does not name. An update
  replaces a list as a whole, so repair a list item by sending the whole
  list. An API write stamps `change_log`, so the next flush re-exports the
  entity and the federated tiers follow. Rows whose image is inside the range
  (for example `9007199254740992` itself) are readable and need no change.
- **The magnitude is intended** (a scalar attribute only; `list` never
  binds). A `bigint` holds only a whole number inside int64, so only such an
  image can be an intended instant. `::bigint` rounds a fraction (`1000.5`
  becomes `1001`) and fails on `NaN`, an infinity and a number past int64.
  While the attribute is still unbound, rewrite those rows as in the
  previous item. Then bind the attribute to a `bigint_*` column with the
  `unix_ms` encoding, which keeps the full int64 range exactly. Before the
  new binding goes live, move every row of the attribute into
  `entity_main`, not only the reported ones. Stop writes to the schema
  first, because a write between the move and the new binding lands in
  `eav_data` again.

  This query lists the rows the move cannot carry exactly. Besides an image
  that is not a whole int64, it lists a value in `value_text` (a
  [storage-column mismatch](#storage-column-mismatches)), a list item, and a
  row with no `entity_main` row, which need a decision per row. It must
  return no rows:

  ```sql
  SELECT e.row_id, e.array_indices, e.value_text, e.value_numeric
  FROM eav_data_dev AS e
  WHERE e.schema_id = 100 AND e.attr_id = 1
    AND NOT (e.array_indices = '' AND e.value_text IS NULL
             AND e.value_numeric IS NOT NULL
             AND e.value_numeric = trunc(e.value_numeric)
             AND e.value_numeric >= -9223372036854775808
             AND e.value_numeric < 9223372036854775808
             AND EXISTS (SELECT 1 FROM entity_main_dev AS m
                          WHERE m.ltbase_schema_id = e.schema_id
                            AND m.ltbase_row_id = e.row_id));
  ```

  The move copies exactly the rows that query leaves out, so no cast rounds
  or fails: on the `NUMERIC` column a whole image keeps its digits. It then
  deletes every row of the attribute and raises if that removed a row the
  copy did not carry. The raise rolls back both statements, so even outside
  a transaction the move never stops halfway or loses a row:

  ```sql
  DO $$
  DECLARE copied bigint; removed bigint;
  BEGIN
    UPDATE entity_main_dev AS m
       SET bigint_01 = e.value_numeric::bigint
      FROM eav_data_dev AS e
     WHERE e.schema_id = m.ltbase_schema_id AND e.row_id = m.ltbase_row_id
       AND e.schema_id = 100 AND e.attr_id = 1 AND e.array_indices = ''
       AND e.value_text IS NULL
       AND e.value_numeric = trunc(e.value_numeric)
       AND e.value_numeric >= -9223372036854775808
       AND e.value_numeric < 9223372036854775808;
    GET DIAGNOSTICS copied = ROW_COUNT;
    DELETE FROM eav_data_dev WHERE schema_id = 100 AND attr_id = 1;
    GET DIAGNOSTICS removed = ROW_COUNT;
    IF copied <> removed THEN
      RAISE EXCEPTION 'moved % of the % eav_data rows of attr_id 1; nothing was changed', copied, removed;
    END IF;
  END $$;
  ```

  A bound attribute has no `eav_data` rows, so the census no longer reports
  it. The HTTP API renders an instant outside years 0000 to 9999 as an
  epoch-millisecond string (`#591`).

After the repair, re-run the validator. As with the other SQL repairs, a
direct rewrite does not stamp `change_log`: a flushed row keeps its last
exported image on the warm/cold tiers until its next write re-flushes it.

### Registered schema with no `<schema>.json` (`#314`)

Symptom: the server refuses to start with `failed to build the schema guards
over <SCHEMA_DIR>: failed to build schema validator: failed to load schema
"<name>" for validation: schema data not found: <name>`. When
`Entity.SchemaDirectory` is unset the leading clause reads `failed to build the
schema guards (Entity.SchemaDirectory is unset)` instead; everything after it is
the same.

The file registry deliberately tolerates a schema whose `<name>_attributes.json`
exists while `<name>.json` does not — it registers the attribute cache and
records no JSON Schema. Such a deployment runs fine on earlier releases. Since
`#314` the validator resolves **every** name `schema_registry` lists, so that
shape now aborts startup.

Preflight, and it is the whole check: **every schema name registered in
`schema_registry` has a resolvable `<name>.json` in `SCHEMA_DIR`.**

```sql
SELECT schema_name FROM schema_registry ORDER BY schema_name;
```

Compare that list against `ls $SCHEMA_DIR/*.json`. No database column carries the
schema document — `schema_registry` holds only `schema_id` and `schema_name` —
so the repair is always on disk: add the missing `<name>.json`, or
delete the `schema_registry` row if the schema is dead.

The same check covers an unparseable document and a `$ref` that points outside
`SCHEMA_DIR`; both fail startup with the offending schema named.

### Create payloads violating their JSON Schema (`#314`)

`enum`, `pattern`, `type`, `minimum`/`maximum` and the schema's own `required`
are now enforced on create, in addition to the metadata's `required_policy`.
`format` is **not** enforced. Updates only log violations unless
`VALIDATE_UPDATES_STRICT=true`. Full contract, including the shipped-schema
behaviour changes and the remaining gaps, is in
[`error-handling.md`](./error-handling.md#json-schema-enforcement-on-write).

## Suggested SQL Fix Workflow

Inspect a bad attribute:

```sql
SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric
FROM eav_data_dev
WHERE schema_id = 100 AND attr_id = 99
LIMIT 50;
```

Delete orphaned rows if you have confirmed they are invalid:

```sql
DELETE FROM eav_data_dev
WHERE schema_id = 100 AND attr_id = 99;
```

**This statement is only for case (i)** — an `attr_id` no schema generation ever
defined. It must **not** be run against rows preserved by attribute removal
(`#294`); those are the expected state, and deleting them permanently forfeits
the restore-on-re-add path. See
[Unknown attribute IDs in EAV](#unknown-attribute-ids-in-eav) before running it.

Inspect value-column mismatches:

```sql
SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric
FROM eav_data_dev
WHERE schema_id = 100 AND attr_id = 2 AND value_text IS NOT NULL
LIMIT 50;
```

## Deployment Checklist

- database backup taken
- `scripts/validate_schema_consistency.sql` returns no duplicate schema IDs
- `make validate-schema-consistency` returns success. Deployments whose schemas
  have had attributes **retired** will see those rows listed in the
  informational block — expected `#294` tolerate-and-preserve state, not a
  deployment blocker, and not part of the exit code (`#341`). With the shipped
  ledgers this covers `attr_id` 25 on `visit` and `attr_id` 29 on `visit_full`.
  A remaining `unknown attribute IDs` **failure** means one of three things: rows
  that were never legitimate (delete them), an attribute retired before `#342`
  whose ledger entry is missing (rebuild the entry), or metadata lost by accident
  for an attribute that is still legitimate (restore the metadata — do **not**
  touch the rows). Resolve it with
  [Unknown attribute IDs in EAV](#unknown-attribute-ids-in-eav) — do not wave it
  through.
- every schema name in `schema_registry` has a resolvable `<name>.json` in `SCHEMA_DIR` (`#314` startup check)
- no active attribute reuses a `retired` attributeID, main-column binding, or folded parquet column (`#342` startup check)
- every active `column_binding.col_name` is a column `entity_main` has (`#557` startup check)
- no stale integer-width export is reported (`#501`): run the validator with
  `--width-export-cutover` set to the time the `#384` build's `cdc-flush`
  first ran, and if it fails, run it again with
  `--requeue-stale-width-exports` followed by `cdc-flush`
- no bigint EAV value outside ±2^53 is reported (`#501`, `#612`, `#590`):
  rewrite each one through the API, naming the attribute, before an unrelated
  update is refused over it
- no bigint value in a `double_*` column outside ±2^53 or non-integral is
  reported (`#590`, `#618`): the validator names the row and the column of
  each `bigint` bound to a `double_*` column; rewrite each one through the
  API, naming the attribute, as described in
  [EAV integer values past their declared width](#eav-integer-values-past-their-declared-width-501)
- no unbound `date`/`datetime` image past ±2^53 or off a whole number is
  reported (`#592`): rewrite each one through the API, naming the attribute.
  The upgraded server refuses to read such rows on the OLTP route, and the
  federated tiers read them narrowed until `#621`; see
  [Date images the read path refuses](#date-images-the-read-path-refuses-592)
- hardened release deployed
- validator re-run after deploy
- smoke CRUD tests pass against existing schemas

## Rollback

If deploy fails because of newly enforced checks:

1. restore the previous server binary or image
2. keep the database unchanged unless you already applied manual cleanup SQL
3. fix the reported metadata or EAV inconsistencies — this never includes
   deleting EAV rows the validator reports in the informational block. Those are
   `#294`-preserved rows; deleting them is irreversible. See
   [Unknown attribute IDs in EAV](#unknown-attribute-ids-in-eav).
4. re-run the validator
5. retry the upgrade

**Rolling back past `#342`.** Binaries older than `#342` do not know the
`retired` key: they parse the entry as an ordinary attribute and load it
**active**. Rolling the server back against retired-marked attributes files
therefore makes those attributes — and the values `#294` preserved under
them — visible again, and drops the reuse guard entirely. If you must roll back
that far, either accept the re-exposure or revert the attributes files to their
pre-`#342` state alongside the binary.

The validator is safe to run repeatedly and is intended to be part of your pre-upgrade checklist.
