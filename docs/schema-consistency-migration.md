# Schema Consistency Migration Guide

This guide covers the upgrade to the schema/metadata consistency hardening introduced in PR `#121` (`709c31a`).

## What Changed

Older releases tolerated several schema and metadata inconsistencies by logging warnings or silently skipping bad records. The hardened release turns those cases into startup or request-time errors.

The new checks fail on:

- duplicate `schema_id` values in the schema registry table
- duplicate `attributeID` values inside a single `<schema>_attributes.json`
- duplicate hot-column bindings inside a single `<schema>_attributes.json`
- EAV rows whose `attr_id` is not present in metadata for that schema
- EAV rows whose value is stored in the wrong physical column (`value_text` vs `value_numeric`)
- writes that reference attributes not defined in schema metadata

A later release (`#314`) adds one more startup check and one more request-time check:

- any schema registered in `schema_registry` whose `<schema>.json` cannot be loaded and resolved fails **server startup**
- create payloads that violate their entity's JSON Schema are rejected with `400`

Both are covered below.

## Recommended Migration Flow

1. Stop writes to the target environment.
2. Back up the database.
3. Run the checked-in SQL script for DB-level sanity checks.
4. Run the checked-in Go validator for full metadata-aware validation.
5. Fix all reported issues.
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
- `eav_data.attr_id` values all map to known metadata IDs for the same `schema_id`
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

That means the table contains rows that current metadata cannot decode. There are two very different reasons this can happen, and they call for opposite actions. **Determine which case you are in before touching any row**: check whether that `attr_id` was ever a legitimate attribute in an earlier generation of the schema's `<schema>_attributes.json` (version control on the schema files is the record) and was later removed.

**Case (i) — never legitimate.** The rows came from a bad deployment, a mis-mapped import, or leftover test data: no schema generation ever defined that `attr_id`. Fix by either:

1. restoring the missing attribute metadata if the data is legitimate, or
2. deleting the orphaned EAV rows.

**Case (ii) — preserved by attribute removal (`#294`).** The attribute did exist and was removed from the schema. Since `#294` (tolerate-and-preserve) these rows are the **expected** state: the read path skips them and the write path preserves them untouched, so removing an attribute is non-destructive and re-adding it (same `attributeID`) restores the stored values. **Do not delete them.** Deletion is destructive and irreversible — it permanently forfeits the re-add restore path — and nothing else in the system is asking you to do it. Leave the rows in place and treat the validator finding as informational.

If you cannot establish which case applies, treat the rows as case (ii) and leave them alone; keeping undecodable rows costs storage, deleting recoverable ones costs the data.

Whichever case applies, an `attributeID` freed by removing an attribute must never be reused for a different attribute: the preserved rows would silently bind to the new attribute's name, or make the row unreadable with a storage type mismatch.

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
- `make validate-schema-consistency` returns success — except that on deployments
  whose schemas have had attributes removed, unknown-attrID findings for exactly
  those attributes are **expected** (`#294` tolerate-and-preserve) and are **not**
  a deployment blocker. The tool does not yet classify preserved rows separately
  from genuinely orphaned ones, so confirm the reported `attr_id`s against the
  removed attributes and proceed (follow-up tracked on the PR).
- every schema name in `schema_registry` has a resolvable `<name>.json` in `SCHEMA_DIR` (`#314` startup check)
- hardened release deployed
- validator re-run after deploy
- smoke CRUD tests pass against existing schemas

## Rollback

If deploy fails because of newly enforced checks:

1. restore the previous server binary or image
2. keep the database unchanged unless you already applied manual cleanup SQL
3. fix the reported metadata or EAV inconsistencies
4. re-run the validator
5. retry the upgrade

The validator is safe to run repeatedly and is intended to be part of your pre-upgrade checklist.
