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
  EAV_TABLE=eav_data_dev
```

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
  --eav-table eav_data_dev
```

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
- `eav_data.attr_id` values all map to known metadata IDs for the same
  `schema_id` — ids belonging to a `retired` ledger entry (`#342`) are reported
  as informational rather than as failures, because those rows are the `#294`
  preserved state (`#341`)
- numeric/date/bool values are not incorrectly stored in `value_text`
- text/uuid/list values are not incorrectly stored in `value_numeric`

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
  on the bare name. Since `#314`, JSON Schema validation rejects a scalar there
  anyway, because the `$ref` resolves to an object. So rows under id 25 can only
  exist if some client once wrote a scalar, and no supported write path can
  produce new ones. `contactSnapshot` also has no property to re-add — it would
  un-retire only if that node ever resolved to a scalar again, which is not an
  operator action; treat `attributeID` 25 as permanently reserved.

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

### Registered schema with no `<schema>.json` (`#314`)

Symptom: the server refuses to start with `failed to build schema validator:
failed to load schema "<name>" for validation: schema data not found: <name>`.

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
