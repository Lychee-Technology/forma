# Row IDs and Unique Keys

Forma enforces two kinds of identity among the live rows of a schema (#651):

- the **row ID**, which a caller may now choose on create;
- **unique keys**: tuples of 1 to 4 attributes that the schema file declares
  unique.

Both are enforced on PostgreSQL atomically with the write. A violation is
`forma.ErrUniqueConflict`, found before the operation writes the entity, and
the operation leaves nothing written. Callers no longer need a check-then-act
read to keep an identity unique.

`Tx.Upsert` (create, or resolve a conflict on a named target) is the second
half of #651. It is not available yet.

## Caller-supplied row IDs

`Create`, `BatchCreate` (atomic and best-effort) and `Tx.Create` read
`EntityOperation.RowID`:

- `uuid.Nil` asks Forma to assign a UUIDv7, as before.
- Any other value is the new row's ID. Forma does not check the UUID's
  version.

A create whose row ID a live row of the schema already has writes nothing. It
returns `ErrUniqueConflict`, publishing `schema <schema> already has a row
with ID <row id>`. Where it surfaces:

- **`Create`** returns it.
- **`Tx.Create`** returns it as an expected outcome. The Tx stays usable and
  can commit its other writes (see [Transactions](transactions.md#poisoning)).
- **An atomic `BatchCreate`** fails with it, and nothing in the batch is
  written.
- **A best-effort `BatchCreate`** reports that operation as failed with code
  `UNIQUE_CONFLICT` and applies the others.

Two operations of one `BatchCreate` that name the same non-nil row ID are
`ErrInvalidInput`, refused before any write.

A row ID whose row was deleted can be created again. As for any recreate, the
new row's version is stamped above every tombstone the delete retained (#274).

The HTTP API has no way to send a row ID on create. Request bodies are raw
attribute maps, so an `id` or `row_id` body key could collide with a real
attribute. Carriers for it are #655.

**Compatibility.** Before #651, `Create` and `BatchCreate` ignored the
`RowID` field and always assigned a fresh ID. A caller that set it now gets
the row ID it sent. A caller that set it by accident (for example by reusing
an `EntityOperation`) now creates under that ID, or receives
`ErrUniqueConflict` when that row is live.

## Declaring a unique key

Put `x-forma-unique-keys` at the top level of the schema file
(`<schema>.json`):

```json
{
  "type": "object",
  "properties": {
    "instance_id": { "type": "string" },
    "idempotency_key": { "type": "string" },
    "name": { "type": "string" },
    "version": { "type": "integer" }
  },
  "x-forma-unique-keys": [
    { "id": 1, "name": "idempotency", "attributes": ["instance_id", "idempotency_key"] },
    { "id": 2, "name": "natural",     "attributes": ["name", "version"] }
  ]
}
```

The value must be an array. A schema without keys leaves the key out. `null`,
like any other non-array value, fails the load. Each entry takes exactly these
keys:

- `id`: an integer from 1 to 32767, unique within the schema. It identifies
  the key in storage. **Never reuse an id** for a different key, and give a
  key a new id when its attribute list changes (see
  [Changing or removing a key](#changing-or-removing-a-key)).
- `name`: matches `^[a-z][a-z0-9_]{0,30}$`, unique within the schema. Errors
  name the key by it.
- `attributes`: 1 to 4 distinct attributes, in order. Each must be an active
  scalar attribute of the schema, column-bound or EAV, of any value type
  except `list`. An attribute inside an array of objects (`lines.sku`) is
  refused, because a row holds many values of it. So is an attribute bound to
  a column Forma rewrites on later writes (`ltbase_updated_at`,
  `ltbase_updated_by`, `ltbase_deleted_at`, `ltbase_deleted_by`);
  `ltbase_created_at` and `ltbase_created_by` are allowed. An attribute bound
  to `ltbase_created_at` compares the creation stamp Forma assigns, not a
  value the write sets. Every row of one atomic `BatchCreate` gets the same
  stamp, so two rows of that batch that agree on the key's other attributes
  conflict.

Other keys, such as `partial` or `where`, are refused rather than ignored. A
declaration that breaks a rule fails the schema load, so the server refuses
to start. The error names the schema file, the schema, the key and the
attribute:

```
schema outbox_message discovered in schema directory: schema file schemas/outbox_message.json: schema outbox_message: unique key natural: attribute tags is a list; a unique key needs scalar attributes
```

`forma.JSONSchema.UniqueKeys` exposes a loaded schema's declarations.

## What a key compares

Among the live rows of one schema, no two rows share a value of a built key.
Keys never span schemas.

A row with **any** of a key's attributes absent or null is not subject to
that key. It holds no entry for it, and any number of such rows coexist. A key
on `(tenant, email)` constrains only the rows that have both.

A key value is the tuple of the attributes' values, each compared by its
typed canonical form:

| Value type | Canonical form | Consequence |
| --- | --- | --- |
| `text` | the exact UTF-8 bytes | `"Ann"` and `"ann"` differ. No case folding, collation or Unicode normalization. |
| `smallint`, `integer`, `bigint` | decimal | `1`, `1.0` and `"1"` are equal. |
| `numeric` | Go's shortest `float64` form, `-0` as `0` | `1.5` and `"1.50"` are equal. Values that differ beyond `float64` precision are equal. |
| `date`, `datetime` | unix milliseconds | Instants that differ below a millisecond are equal. |
| `uuid` | lowercase canonical form | Case and spelling of the input do not matter. |
| `bool` | `t` or `f` | |

The components are joined with a byte-length prefix each, so `("ab", "c")`
and `("a", "bc")` differ. A joined value over 512 bytes is stored as
`sha256:` followed by its hex digest, so the key table's primary key stays
bounded however long the text is.

Keys are enforced in PostgreSQL, which holds every live row. The S3 tiers
are copies of it, and keys do not consult them.

## How a write enforces keys

Every write of a row of a keyed schema runs these steps in its transaction:

1. Compute the key values the row holds once written, and compare them with
   the entries it holds now.
2. After the row locks, insert an entry for every value it takes, in one
   statement sorted by (schema, key, value) that skips the values the key
   table already holds. If another row holds one of them, delete the entries
   the statement inserted and return `ErrUniqueConflict`. The entity has not
   been written.
3. Write the entity, then delete the entries of the values the row released.

Writers of one value serialize on the key table's primary key; there is no
lock per value. An insert that meets another transaction's uncommitted entry
waits for that transaction to end. If it commits, the waiter finds its entry
and returns `ErrUniqueConflict`; if it rolls back, the waiter takes the value.
Concurrent creators of one value therefore end with exactly one commit. A
value that an update moves away from, or a delete releases, can be taken by
another writer once that write commits; a creator already waiting for it then
succeeds.

Writes of keyed rows pin `READ COMMITTED`, whatever the session's default
isolation level. Under a snapshot level a loser would see `40001` for an entry
committed after its snapshot instead of `ErrUniqueConflict`.

What each surface does:

- **Create** takes the row's values.
- **Update** that changes a key attribute moves the key: it releases the old
  value and takes the new one. An update that leaves a key's attributes alone
  does not touch that key's entry.
- **Delete** releases every key of the row, so its values can be taken again
  at once.
- **An atomic batch** reserves every operation's values together, against
  the entries as stored before the batch. So a batch that swaps two rows'
  values is refused, though the swap would hold at commit, and two operations
  that take the same value conflict. Either way the whole batch fails with
  `ErrUniqueConflict` and writes nothing.
- **A best-effort batch** runs each operation as its own write, in order. An
  operation fails with code `UNIQUE_CONFLICT` when another row holds a value
  it takes, an earlier operation of the batch included.
- **`Tx.Create`, `Tx.Update`, `Tx.Delete`** follow the same rules, and an
  `ErrUniqueConflict` leaves the Tx usable. The entries the refused write
  inserted and deleted again stay uncommitted until the Tx ends, so another
  writer of those values waits for the Tx to commit or roll back.
- **`Tx.UpdateWhere` and `Tx.DeleteWhere`** check each row as `Update` and
  `Delete` would. A conflict at the first row leaves the Tx usable; a conflict
  after a row was written poisons it, so the Tx never commits part of a
  predicate mutation.

### Deadlocks

One operation's statement takes its values in sorted order, so two
operations never wait on each other over values they both take in one
statement. A transaction that takes values over several statements can: the
rows of one `Tx.UpdateWhere` take their values row by row, and so do the
operations of one Tx. Two transactions that take overlapping values in
opposite orders can deadlock. PostgreSQL then aborts one of them, which
receives `ErrRetryableConflict` rather than `ErrUniqueConflict`; running it
again settles the outcome.

### Messages

The published message of a key violation names the schema and the key, never
the value, which may be sensitive:

```
schema account already has a row with this value of unique key tenant_email
```

The operator detail adds the key's id and the row that holds the value. Over
HTTP a violation is `409` with the published message (see
[Error Handling](error-handling.md)).

### The key tables

Keys live in two tables named after the `entity_main` table they guard.
`init-db` creates both. A deployment whose schemas declare no keys never
reads or writes them, so it starts without them.

- `<entity_main>_unique_key` holds one entry per (live row, built key) whose
  attributes are all present. Its primary key
  `(ltbase_schema_id, key_id, key_value)` is the uniqueness itself: an
  ordinary table, not a partial unique index. A secondary index on
  `(ltbase_schema_id, ltbase_row_id)` finds a row's entries.
- `<entity_main>_unique_key_state` records each built key with a hash of its
  attribute names, their value types and the version of the canonical
  encoding.

Both tables are derived state. They are not in `change_log`, are never
flushed to S3, and DuckDB never reads them.

## Building a key

Every writer whose schema files declare a key maintains it from the moment
it loads them: its writes take and release entries. The rows written before
then have no entries, so the key is not enforced against them until it is
**built**: its entries computed from the schema's live rows and its state
recorded. The server, through `factory.NewEntityManagerWithConfig`, **refuses
to start** while a key is not built for its declared attributes. Every
message names the remedy:

```
unique key check failed: unique keys are not built; stop every writer, then: unique key idempotency of schema event_record is not built: run forma-tools build-unique-keys --schema event_record --key idempotency
```

The check refuses three states:

- a declared key that is not built (`is not built: run forma-tools build-unique-keys --schema S --key K`);
- a key built for another attribute list under the same id (`was built for
  another attribute list: a key's attributes cannot change under its id;
  declare the new attribute list under a new id`);
- a key still built that the schema no longer declares (`no longer declares a
  unique key that is still built (key id N): run forma-tools build-unique-keys
  --schema S`).

When the state table is missing and a schema declares a key, the server asks
for `forma-tools init-db`.

### New schemas: `init-db`

`init-db --schema-dir` builds the keys of every schema that has no live rows,
since there is nothing to scan. It prints `Built unique key K of schema S (no
rows to scan)` for each. For a schema that already has rows, it prints the
`build-unique-keys` command to run and leaves the key unbuilt.

An empty schema does not lift the rule below: every writer that runs a schema
file without the declaration must be stopped before the first writer that
enforces it starts. A writer without the declaration keeps writing rows with
no entries, so a row it writes after the build goes unprotected and can
duplicate a value. Repair it with a rebuild (below).

### Existing rows: `build-unique-keys`

Adding a key to a schema that has rows is an offline step:

1. **Stop every writer**: every server, Lambda and embedder that writes the
   schema. The build reads the rows and then writes the entries; a write it
   misses would leave the key table short, and that row's value would go
   unprotected.
2. Deploy the schema file with the new declaration.
3. Run the build with the same storage flags as `init-db`:

   ```bash
   forma-tools build-unique-keys --schema event_record --key idempotency \
     --schema-dir schemas --db-host … --db-name … --db-user … --db-password …
   ```

   Without `--key` it builds every key the schema declares that is not built
   yet, and drops the keys the schema no longer declares.
4. Start the writers. Every writer must run the schema file with the
   declaration, so a rolling deploy that keeps old writers running alongside
   new ones is not safe for this step.

The build first unbuilds each key it builds: it deletes the key's state, then
its entries. It then scans the schema's live rows in pages of 1,000, writes
the entries in batches of 1,000, and records the key's state last. Each statement is its own transaction, which keeps every transaction
under Aurora DSQL's 3,000-row limit. A build that fails or is interrupted at
any point leaves the keys it was building unbuilt, and the server refuses to
start until a build succeeds. A key already built for its attributes is
reported as `already built` and left alone, so a re-run is safe.

**Rebuilding.** `--rebuild` builds a key again although it is built for its
attributes: it clears the key's entries and state, scans the rows and writes
them anew. Use it, with every writer stopped, when a writer that did not
maintain the key wrote rows after the key was built. Such a writer may also
have duplicated a value, so a rebuild can fail on duplicates (below). The key
then stays unbuilt rather than built over the entries the rebuild was meant
to repair. A rebuild never changes what a key means: a key built for another
attribute list is still refused.

The build holds every value of the keys it builds in memory, with the row IDs
that hold each, until it writes them. Its memory grows with the number of rows
times the number of keys built in one run. For a very large schema, build one
key at a time with `--key`.

**Duplicates.** When live rows already share a value of a key, the build
fails. It writes no entry for any key of the run, and every key of the run is
left unbuilt, including the keys without duplicates and the keys a `--rebuild`
found built. It lists each group of rows that share one value, by row ID only:

```
schema event_record: 2 values of unique key idempotency are shared by more than one live row, so the key is left unbuilt; resolve these rows (each group shares one value) and build again: [0190… 0190…] [0191… 0191…]
```

Resolve each group (delete or change all but one row), then run the build
again. The keys are unbuilt by then, so the build needs no `--rebuild`. A
writer whose schema files declare an unbuilt key refuses to start, so resolve
the rows through a writer whose schema files do not declare it, and stop that
writer before the build.

### Changing or removing a key

A key's attribute list never changes under its id. Changing one attribute's
value type in the attributes file changes the key too. To change a key:

1. Declare the new attribute list under a **new id** (the name may stay) and
   remove the old declaration.
2. With every writer stopped, run `forma-tools build-unique-keys --schema S`.
   It builds the new key and drops the old one.

To remove a key, remove its declaration and run `forma-tools
build-unique-keys --schema S`, which drops its entries and state without
scanning rows. The server refuses to start until the drop runs. A key that
stays built after its declaration goes away stops being maintained; if the
same id were declared again over the same attributes, it would look built over
stale entries.

## Aurora DSQL

Status: **DSQL: specified, not verified (#633)**.

The key table is a guard-row design. Uniqueness is a primary-key collision on
an ordinary table, which DSQL enforces at commit through optimistic
concurrency, so it needs no partial index. The key table, the canonical
encoding, the insert-then-undo reservation and the API are backend-neutral.
The row locks every write takes before it reserves keys are PostgreSQL
advisory locks, the backend-specific locking seam (#648, #633).

On DSQL two concurrent creators of one value both insert the same primary key,
and one commit fails with `40001`. The contract is therefore: **exactly one
creator commits, and each other creator receives `ErrUniqueConflict`, if it
saw the committed entry, or `ErrRetryableConflict`, on an OCC abort.** Forma
does not retry. A caller that retries an `ErrRetryableConflict` then receives
`ErrUniqueConflict`. Tests that assert `ErrUniqueConflict` for every loser
carry the `_PostgresOnly` suffix.

Every entry a write inserts or deletes is a modified row of its transaction,
counted with the entity's main, EAV and `change_log` rows against DSQL's
3,000-row limit. A row with two keys adds two rows to its create. Inside a Tx,
`WithTxMaxPhysicalRows` counts the entries too
([Transactions](transactions.md#limits)). An atomic batch has no such bound,
keyed or not: one within `Performance.MaxBatchSize` can exceed the limit and
fail as a whole, with an error Forma does not classify yet (#692).

## Not supported

- Keys across schemas.
- Case-insensitive or collation-aware comparison.
- Keys over `list` attributes, or over attributes inside arrays.
- Building a key while writers run.
- Partial keys (a `where` clause).
- Enforcement in the S3 tiers.
