# Text collation

On PostgreSQL, every text column of every table Forma creates is declared
`TEXT COLLATE "C"` (#816), whatever the database's default collation is. This
page says why, what it changes for queries, what `init-db` does about tables
created before the rule, and how to migrate them.

## The contract

The rule covers the collatable columns of every table `init-db` creates:

| Table | Text columns |
| --- | --- |
| schema registry | `schema_name` |
| `entity_main` | `text_01` to `text_10`, `ltbase_created_by`, `ltbase_updated_by`, `ltbase_deleted_by` |
| `eav_data` | `array_indices`, `value_text` |
| `<entity_main>_unique_key` | `key_value` |
| `<entity_main>_unique_key_state` | `attributes_hash`, `storage_hash` |
| `<schema_registry>_generation` | `content_hash`, `recorded_by`, `warnings` |
| `<schema_registry>_generation_attr` | `attr_name`, `value_type`, `items_type`, `column_name`, `encoding` |

`change_log`, the row-guard table and the schema-lease table hold no text
today; a text column added to them later falls under the same rule. `UUID`,
numeric and other non-collatable columns take no collation. There is no
setting for another collation, and Forma never changes a table it did not
create.

Why: text ranges (`gt`, `gte`, `lt`, `lte`) compare in UTF-8 byte order on
every route and tier (#728). PostgreSQL renders them `<col> COLLATE "C" <op> $n`.
The planner seeks a btree key with such a compare only when the key's
collation is `C` itself, and it matches collations by OID. A key under the
database default never qualifies, not even in a database whose default is
`C`, because the default collation has its own OID. An index takes the
collation of the column it keys, so with the column under `C` one ordinary
index serves equality and ranges alike. Without it, a range needs a second key
spelled `COLLATE "C"` beside the ordinary one (see
[Declared Indexes](schema-indexes.md#text-ranges) for the figures).

The query side does not change. The PostgreSQL route keeps naming
`COLLATE "C"` in every text range: it states the query's order whatever the
column's collation is, and tables created before this rule still need it.

## What changes for queries

Measured against a table whose text columns follow an `en_US.utf8` default:

- **Sort order changes on the PostgreSQL route.** That route sorts with
  `ORDER BY` on the column, so it sorts under the column's collation. Under
  `C`, `title` ascending is `B, Z, a, zebra, é`. Under `en_US.utf8` it was
  `a, B, é, Z, zebra`. The DuckDB route always sorted by bytes, so the two
  routes now agree, for column-bound and `eav_data` text alike.
  `TestTextSortFollowsTheColumnCollation` pins both orders.
- **Keyset pagination does not change.** A cursor query runs on the DuckDB
  route whatever the routing preference, and compares there by bytes.
- **Ranges do not change.** They have compared by bytes since #728.
- **`equals`, `in`, `not_equals`, `not_in` and uniqueness do not change.**
  PostgreSQL's deterministic collations call two strings equal only when
  their bytes are equal, `en_US.utf8` included. Unique keys already compare
  exact bytes ([Unique Keys](unique-keys.md)).
- **`starts_with` and `contains` do not change.** They render as `LIKE`
  patterns, which match characters under a deterministic collation, not
  collation order.
- **`rfc3339` date-times do not change.** Their fixed-shape images order the
  same under any collation that orders the digits
  ([rfc3339 storage contract](rfc3339-storage-contract.md)).

Forma's generated SQL applies no `lower`, `upper` or `ILIKE` to stored text.
SQL of your own does: under `C`, `lower()` and `upper()` fold ASCII letters
only, and `ORDER BY` sorts by bytes.

The production harness creates its tables under the same rule, so its
operator and route parity tests (`TestConditionOperandSemanticsAcrossRoutes`,
`TestTextUUIDOperatorsAcrossRoutes`, `TestTextRangeByteOrderAcrossRoutes`) run
on `C` columns in an `en_US.utf8` database.

## What `init-db` checks

`CREATE TABLE IF NOT EXISTS` leaves an existing table alone, so on its own a
rerun would report success over columns created before the rule. After its
`CREATE` statements, inside the same transaction, `init-db` reads each table's
columns from `pg_attribute` and `pg_collation`. Any collatable column whose
collation is not `pg_catalog."C"` fails the run. That includes
`pg_catalog."default"` in a database whose default is `C`, and `POSIX`, which
orders as `C` but has another OID. The transaction rolls back, so the failed
run creates and changes nothing. The error names each column and prints the
statements that migrate them:

```
create the tables: check the text collation of Forma's tables: 15 Forma-owned text column(s) are not under the C collation Forma's tables are created with (#816): table "entity_main" column text_01 (text) has collation pg_catalog."default", expected pg_catalog."C"; ... These tables were created before the contract; ... read docs/text-collation.md before running them, then rerun: ALTER TABLE "entity_main" ALTER COLUMN "text_01" TYPE text COLLATE "C", ...; ANALYZE "entity_main"; ALTER TABLE "eav_data" ALTER COLUMN "array_indices" TYPE text COLLATE "C", ALTER COLUMN "value_text" TYPE text COLLATE "C"; ANALYZE "eav_data";
```

`init-db` never runs those statements itself.

## Migrating a table created before the rule

The migration changes no stored value. For each table it is one
`ALTER TABLE ... ALTER COLUMN ... TYPE <type> COLLATE "C"`, covering that
table's columns, followed by `ANALYZE` of the table.
`TestInitDBRefusesTextOutsideCAndItsMigrationRestoresTheContract` and
`TestInitDBMigrationAndRollbackRestorePlannerStatistics` run the printed
statements on PostgreSQL 16 and check what they do:

- **The table is not rewritten.** The type stays binary-compatible, and the
  table keeps its storage file.
- **Every index that keys a changed column is rebuilt**, under its own name:
  `init-db`'s `text_01` to `text_03` indexes, the `eav_data` text index, the
  primary keys that include `array_indices`, `schema_name` or `key_value`, and
  declared indexes over text attributes. Other indexes are left as they are.
  The rebuild cannot fail on uniqueness: equality is bytewise before and after.
- **`ACCESS EXCLUSIVE` lock.** The statement holds it on its table from start
  to commit, so every read and write of that table waits, for about as long as
  building the rebuilt indexes from scratch takes. PostgreSQL has no online
  form of this change.
- **Planner statistics of the changed columns are deleted.** PostgreSQL drops
  a column's statistics on any `ALTER COLUMN ... TYPE`, even one that changes
  only the collation, and does not gather them again. Until the table is
  analyzed, the planner guesses: on PostgreSQL 16, a selective range over a
  20,000-row table was estimated at a third of the table and planned as a
  bitmap heap scan instead of the index-only scan it got after `ANALYZE`.
  Autovacuum's next analyze, triggered by row changes rather than by the
  `ALTER`, may be far off. That is why the printed statements follow each
  `ALTER TABLE` with `ANALYZE` of its table. `ANALYZE` reads a sample, not
  the whole table, and takes only a `SHARE UPDATE EXCLUSIVE` lock, so reads
  and writes continue while it runs.
- **Cached plans break.** A connection that prepared a statement on the table
  before the change fails it afterwards with `cached plan must not change
  result type` (SQLSTATE `0A000`), until it reconnects. Forma's pools prepare
  their statements.

The procedure:

1. **Inspect.** Run `init-db` as usual: it fails, listing the columns and the
   statements. To look without it, run this query, with the table names
   replaced by your deployment's:

   ```sql
   SELECT a.attrelid::regclass AS table_name, a.attname AS column_name,
          format_type(a.atttypid, a.atttypmod) AS type,
          quote_ident(n.nspname) || '.' || quote_ident(c.collname) AS collation
   FROM pg_attribute a
   JOIN pg_collation c ON c.oid = a.attcollation
   JOIN pg_namespace n ON n.oid = c.collnamespace
   WHERE a.attrelid IN (SELECT to_regclass(t) FROM unnest(ARRAY[
           'schema_registry', 'schema_registry_generation', 'schema_registry_generation_attr',
           'entity_main', 'entity_main_unique_key', 'entity_main_unique_key_state',
           'entity_main_row_guard', 'entity_main_schema_lease', 'eav_data', 'change_log']) AS t)
     AND a.attnum > 0 AND NOT a.attisdropped
     AND NOT (c.collname = 'C' AND n.nspname = 'pg_catalog')
   ORDER BY 1, a.attnum;
   ```

2. **Estimate the lock.** The lock lasts about as long as building the
   rebuilt indexes. `pg_relation_size` of each index gives a first measure;
   running the statements on a restored copy of the database gives a better
   one.
3. **Stop Forma's processes** that use the database: servers, Lambda
   functions, embedders, `cdc-flush`, `cdc-init` and `manifest-reconcile`. Their
   requests would queue behind the lock, and their pooled connections would
   fail afterwards with `0A000`. If they cannot stop, restart them after step
   5.
4. **Run the statements** `init-db` printed, all of them and in order, in a
   maintenance window: each table's `ALTER TABLE`, then its `ANALYZE`. Set a
   lock timeout so a long transaction you missed fails the statement instead
   of stalling everything queued behind it:

   ```sql
   SET lock_timeout = '10s';
   ALTER TABLE "entity_main" ALTER COLUMN "text_01" TYPE text COLLATE "C", ...;
   ANALYZE "entity_main";
   ```

   Each statement commits on its own, so a table already migrated stays
   migrated if a later one fails. Rerunning `init-db` lists only what is left,
   so if an `ALTER TABLE` committed but its `ANALYZE` did not run, the rerun
   does not print that `ANALYZE` again: run `ANALYZE <table>` yourself.
5. **Check the statistics are back.** Each migrated table that holds rows has
   a `pg_stats` row for every column it changed (table names replaced by your
   deployment's):

   ```sql
   SELECT tablename, attname FROM pg_stats WHERE tablename IN ('entity_main', 'eav_data');
   ```

   A column missing there has no statistics: `ANALYZE` its table before going
   on. (An empty table has nothing to sample, and gets statistics from
   autovacuum once rows arrive.)
6. **Rerun `init-db`.** It passes, and finds the declared indexes present:
   the rebuilt ones key the column under `C`, which is the column's collation.
7. **Drop the byte-order indexes** you built for ranges while the columns
   followed the default ([Declared Indexes](schema-indexes.md#text-ranges)).
   The ordinary index now serves those ranges, so the extra key only costs
   writes and storage: `DROP INDEX CONCURRENTLY <name>`.
8. **Start Forma's processes.**

**Rollback.** Before a statement commits, cancel it or `ROLLBACK`: nothing
changes. After it commits, the same `ALTER TABLE` with `COLLATE "default"` (or
the collation the inspection listed) restores the old collation, at the same
cost and under the same lock, and deletes the columns' statistics again. Run
`ANALYZE` of the table after it, and check `pg_stats` as in step 5, before
resuming traffic:

```sql
SET lock_timeout = '10s';
ALTER TABLE "entity_main" ALTER COLUMN "text_01" TYPE text COLLATE "default", ...;
ANALYZE "entity_main";
```

`init-db` then refuses again, and the PostgreSQL route sorts in the old order
again.

If the lock is not acceptable yet, leave the columns as they are and do not
rerun `init-db` until you migrate. The deployment keeps working as before:
ranges answer the same rows, and the byte-order indexes of
[Declared Indexes](schema-indexes.md#text-ranges) restore their seeks, at the
cost measured there. #815 tracks the remaining work for such tables.

## Aurora DSQL

DSQL's collation is fixed at `C`, and it refuses a `COLLATE` clause in a
column definition: `make lint-dsql` reports it under dsql-lint's `collation`
rule. Forma's DDL for DSQL therefore declares plain `TEXT`, derived from the
PostgreSQL form by dropping the clause, and the catalog check above does not
run there.

What is not verified: whether DSQL's planner lets an ordinary index on such a
column seek the `COLLATE "C"` range the query renders, and how its catalog
reports the column's collation. Only a real cluster can tell (#825).
MiniStack's proxy is no evidence either way: `make test-dsql-ministack` passed
while its ledger upgrade suite still created tables with the clause. `init-db`
does not run on DSQL yet (#690).
