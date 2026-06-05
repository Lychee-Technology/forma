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

That means the table contains rows that current metadata cannot decode. Fix by either:

1. restoring the missing attribute metadata if the data is legitimate, or
2. deleting the orphaned EAV rows if they were produced by a bad deployment or test data.

### Storage-column mismatches

Example validator output:

```text
- numeric/date/bool attributes stored in value_text: schema_id=100 attr_id=2 rows=3
```

This means the row uses the wrong physical value column for the declared `valueType`.

Fix by rewriting the bad rows into the correct column and clearing the wrong one.

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
- `make validate-schema-consistency` returns success
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
