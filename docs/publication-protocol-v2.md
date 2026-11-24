# Parquet publication protocol V2

Status: the contract #888 proposes for EPIC #887, fully delivered across registration,
`cdc-flush`, `cdc-init`, the federated reader, `compactor`, and `manifest-reconcile`.
The SQL and its semantics are in `internal/publication`, with tests on PostgreSQL and on
the MiniStack lane. The manifest objects (document, digest, keys, write-once store,
verifying cache) are in `internal/manifestv2` (#898). `cdc-flush` and `cdc-init` publish
only through `internal/cdcv2` (#899), into which `internal/cdc` adapts the DuckDB export
as its `DeltaStager` and `BaseExporter`. The factory builds every federated engine with
`federated.WithPublicationReader` over `internal/federatedv2` (#900), and `init-db` writes
each registered schema's record ([The publication record](#the-publication-record)). The
compactor publishes compact generations through `internal/cdcv2` too (#890).
`manifest-reconcile` runs read-only diagnosis, `--gc` retention/orphan collection under
storage binding (`--bind-storage`), and `--verify-checksums` over V2 generations (#891).
End-to-end fault/crash/concurrency validation, legacy-V1 guards,
P95/cost deltas, and the EPIC close-out ledger live in
[publication-protocol-v2-evidence.md](publication-protocol-v2-evidence.md) (#892).
Behaviour on a real Aurora DSQL cluster has not been checked yet: #893 (tracked under
#817) owns that external validation gate, and [Verification](#verification) lists what
this contract assumes until then.

## Why

A schema's rows live in two stores. The `change_log` flush marks, which tell a reader
which rows the parquet tiers hold, commit in PostgreSQL; the parquet objects and the
inventory that lists them live in S3. If the inventory and the marks could move
independently, a reader could pair a snapshot's marks with an inventory that does not
hold the marked rows, or the reverse, and every reader and writer would need
workarounds for that gap: a widened dirty barrier (#252), a "parquet history" probe when
nothing is listed (#720), zero-row placeholder objects to tell an empty inventory from a
missing one (#519), and no durable record of a `cdc-init` export (#760).

The protocol closes the gap at its source. PostgreSQL holds the only pointer. Writers
move it with compare-and-set, and in the same transaction they commit the marks that
depend on it. Readers choose the generation in the same snapshot as the dirty set. S3
holds only immutable objects, so whatever key a snapshot names still means what it meant
when it committed.

## Decisions

1. **One durable record per schema in PostgreSQL** (`<entity_main>_publication`) names
   the schema's current immutable manifest generation. No other pointer exists. The
   record is created by `init-db` when the schema is registered, and is never
   deleted. A dropped schema keeps it in a `DROPPED` state that readers and writers
   fail closed on.
2. **Manifest generations are immutable S3 objects.** Each committed generation is one
   object under a key that contains its generation number and token. It is written
   once, and only garbage collection deletes it.
3. **The pointer references the immutable generation directly**, with the object's
   SHA-256. V2 has no mutable root or bootstrap object in S3 (see
   [Why no mutable root](#why-no-mutable-root)).
4. **One PostgreSQL commit publishes a generation and marks its rows.** The
   compare-and-set is the transaction's first statement, and the flush marks follow on
   the same transaction. S3 staging all happens before `BEGIN`.
5. **Readers read the record in the snapshot they read the dirty set in**, as that
   snapshot's first statement.
6. **Garbage collection never trusts a commit to land on time.** A reader has a hard
   scan deadline, and a superseded generation is kept for a grace counted from a time the
   collector stamps after it saw the replacement committed. An unreferenced object is
   deleted only below a horizon the collector writes to the record first, which every
   later publication's compare-and-set checks.
7. **V2 is the only protocol.** There is no glob discovery, no mutable root manifest, and
   no adoption of parquet state that no V2 generation records. An unlisted object is
   either staged or garbage, never lost data, so the repair for a damaged inventory is a
   re-init with `cdc-init`.

## The publication record

```sql
CREATE TABLE <entity_main>_publication (
  schema_id         SMALLINT NOT NULL,
  epoch             BIGINT   NOT NULL,   -- lifecycle epoch; drop and re-activation increment it
  state             TEXT     NOT NULL,   -- 'UNPUBLISHED' | 'PUBLISHED' | 'AWAITING_INIT' | 'DROPPED'
  generation        BIGINT   NOT NULL,   -- allocation counter: 0 until first publication, never reset
  parent_generation BIGINT   NOT NULL,   -- the current generation's CAS predecessor; 0 if none
  manifest_key      TEXT     NOT NULL,   -- '' exactly when not PUBLISHED
  manifest_sha256   TEXT     NOT NULL,   -- '' exactly when not PUBLISHED
  lineage           UUID,                -- NULL exactly when not PUBLISHED
  cas_token         UUID     NOT NULL,   -- changes on every transition
  updated_at        BIGINT   NOT NULL,   -- database clock, epoch ms
  gc_horizon        BIGINT   NOT NULL,   -- the collector's fence on staging times
  PRIMARY KEY (schema_id)
)
```

On PostgreSQL the text columns are `COLLATE "C"`. On DSQL they are plain `TEXT`, since
DSQL's collation is already C and it refuses the clause (#816). The table uses only
plain columns: no enum type, `CHECK`, sequence, array, JSONB or foreign key, so nothing
depends on DSQL supporting those. `publication.Read` validates the state invariants
itself and refuses a row that no transition writes. The table is named after
`entity_main`, like the schema-lease table, because every holder (flush, init,
compactor, reader, GC) is given that name.

**Fields.**

- **`epoch`** fences the schema's lifecycle. Every compare-and-set names it, so a writer
  staged under an old epoch can never commit.
- **`generation`** is the allocation counter: the last number a publication took. It is
  never reset, not even by a drop, so a generation number is never reused and neither is
  a manifest key built from it. Only a `PUBLISHED` record names it as its current
  generation. In any other state it is just the counter.
- **`parent_generation`** has one meaning in the record, the history and the manifest:
  the generation the compare-and-set replaced, in the same epoch. It is 0 for an epoch's
  first generation and for a record with no current generation. It is not the
  compactor's input (the manifest's `input_generation`) and not the counter: the first
  publication after a drop and re-registration takes the next number but has parent 0.
- **`cas_token`** is minted by the transaction that writes the row. It is also the
  committed generation's identity, which `Resolve` uses after a lost commit answer.
  Being random, it rules out an ABA match even when epoch and generation compare equal.
- **`lineage`** identifies the content line one `cdc-init` export started (or the first
  flush of an unpublished schema). Flush and compaction keep it. The `cdc-init` handoff
  is fenced on it.
- **`gc_horizon`** is a database time the collector raises before it deletes anything
  unreferenced. No publication whose staging began at or before it commits
  ([Garbage collection](#garbage-collection-and-reader-lifetime)).

**Lifecycle of the row.**

- *Initialize.* The registration transaction that inserts the schema's registry row also
  inserts the record: epoch 1, `UNPUBLISHED`, generation 0, and sets the id's lifecycle
  epoch to 1 ([Lifecycle epoch](#lifecycle-epoch)). The insert uses
  `ON CONFLICT DO NOTHING`, so registering again never resets a schema, and never turns a
  `DROPPED` or `AWAITING_INIT` record into an unpublished one. It writes nothing for an id
  whose lifecycle epoch survives without a record: that id lost its record, epoch 1 is
  one of its old epochs, and `Initialize` answers `ErrRecordLost`.
- *A schema without a record or history.* `init-db` writes the record of every registered
  schema that has no record and no publication history (no lifecycle epoch,
  forgotten-generation floor or generation row), whether this run registers it or an earlier one did, under
  the schema lock in the registration transaction. It first checks the `UNPUBLISHED`
  invariant below (no flushed `change_log` entry, no `entity_main` row without an entry).
  If it holds, the record is `Initialize`d. If it is broken, the run fails and the whole
  registration transaction rolls back: no V2 writer leaves such rows without generation
  history, and an `UNPUBLISHED` record would hide whatever a parquet tier holds of them.
  Forma does not adopt such state. The remedy is to restore the publication tables
  together with the data tables they belong to, or to purge the schema's rows and
  register it again. A schema lock `init-db` cannot take means a writer still runs: a
  schema registered before the run is skipped, keeps no record (it fails closed) and is
  retried by the next run; a schema the run registers fails the run.
- *An absent record is an error* (`publication.ErrNoRecord`), never "unpublished". A
  reader that took absence for unpublished would answer from the hot tier alone and
  drop every row only the parquet tiers hold.
- *Awaiting init.* `AWAITING_INIT` arises only from lost-record recovery, for a schema
  whose generation history survives (*A lost record*, below).
  Readers fail closed on it (`ErrAwaitingInit`), and the only publication it admits is a
  `cdc-init`, whose export replaces the parquet tiers.
- *Drop.* The transaction that deletes the registry row runs `Reset`: epoch + 1,
  `DROPPED`, an empty manifest, a NULL lineage, parent 0 and a new token, and the id's
  lifecycle epoch raised to the new epoch. The row stays.
  While it is `DROPPED` the schema's `entity_main`, `eav_data` and `change_log` rows, and
  the unique-key entries and build state derived from them, are purged in chunks, and
  nothing reaches them through the record. `forma-tools
  drop-schema` is this transition (#896): under the schema lock it deletes the registry
  row and the schema ledger and runs `Reset` in one transaction, then purges in chunks,
  each holding the record `DROPPED` at its epoch (`HoldDropped`), so a purge stops once
  the id is activated again. A registry row deleted without it leaves a record that is not
  `DROPPED`, and `init-db` refuses to register the id again over it. A reader fails closed
  (`ErrDropped`). A publisher that reads the record after the drop stages nothing, and
  `Publish` refuses a `DROPPED` record in any case, so nothing exported from the rows
  being purged is published. A writer or handoff that read the record before the drop
  names the old epoch and token, and misses.
- *A lost record.* The record row is never deleted, but an operator's statement or a
  partial restore can remove it. A schema without a record whose lifecycle epoch or
  generation history survives had a V2 record. A new one at epoch 1 could carry an epoch
  a running reader or writer still pins, together with the metadata of the registration
  that held it, and pass its epoch check; a counter at 0 would publish a generation number
  again. The generation history cannot bound the lost epoch: a drop and a re-registration
  move the epoch without a generation row, so the history's highest epoch can be any
  number of epochs behind. `init-db` recovers the record under the schema lock with
  `InitializeAfterLoss`, at the epoch after the id's lifecycle epoch, which every epoch a
  record of the id carried is at or below, so no running process pins it, and with the
  history's highest generation as the counter. A schema still registered is recovered
  `AWAITING_INIT` when generation history survives (its next `cdc-init` replaces the
  parquet tiers), and otherwise `UNPUBLISHED` if its rows satisfy the `UNPUBLISHED`
  invariant, with the same refusal as above if they do not; an id the run registers again
  is recovered `DROPPED`, then activated. The processes that
  pinned an older epoch fail closed until they restart, as after a drop.

  When the lifecycle epoch is lost too and generation history survives, `InitializeAfterLoss`
  refuses with `ErrLifecycleLost`, and `init-db` fails: no epoch above the history is known
  to be free of a stale pin. The recovery is the operator's. Stop every server, Lambda,
  `cdc-flush` and `cdc-init` that loaded the schema, so that no process holds a pin of it,
  then write the id's lifecycle epoch at or above the history's highest epoch,
  `INSERT INTO <entity_main>_publication_epoch (schema_id, epoch, updated_at) VALUES
  (<id>, <epoch>, 0)`, and run `init-db`: it recovers the record one epoch above. An id
  whose record, lifecycle epoch and generation history are all lost cannot be told from
  one that never had a record: `init-db` initializes it at epoch 1 if its rows satisfy
  the `UNPUBLISHED` invariant, and refuses it otherwise. The three tables are one
  deployment's state and are restored together.
- *Re-registration.* When the id is registered again, the registration transaction runs
  `Activate` instead of `Initialize`. It checks, in that transaction, that no row of the
  id remains in `entity_main`, `eav_data` or `change_log` (`ErrResidue` otherwise), then
  moves the record to the next epoch, `UNPUBLISHED`, under a new token, and raises the
  id's lifecycle epoch to it. Nothing read before activation carries that epoch.
- *Epoch pin.* A process reads each schema's epoch when it loads the schema's metadata
  and keeps it with them. A reader pass or a writer that later reads a record of another
  epoch fails closed: the schema was dropped, and maybe re-created, since the process
  loaded what it knows about it. The pin binds only if it is read *before* the metadata
  loads and still holds after: a pin read after the load could already be a re-created
  schema's epoch, paired with the dropped schema's metadata. Every copy of the metadata
  a process uses is bound this way, the caller-facing schema registry included: it
  resolves the schema, its sort attributes and its decoding on every query, and the caller
  loads it before the factory reads anything. So the server and the Lambda read every
  record's epoch before they load the registry (`factory.LoadFileSchemaRegistryContext`,
  which sets `forma.Config.SchemaRegistryEpochs`), the factory reads them again before it
  loads its own metadata, and it pins a schema only if its epoch is the same in both
  readings and when it pins it; with DuckDB enabled it refuses to build the reader without
  `SchemaRegistryEpochs`. `cdc-flush` and `cdc-init` are given the epochs read before
  their registry loaded (`CDCConfig.RegistryEpochs`, `cdc.ReadRegistryEpochs`), refuse a
  run without them before any I/O, since an epoch a run reads itself proves nothing about
  a registry loaded before it, and refuse, under the schema lock, a record at another
  epoch.
- *Re-onboarding* an existing schema is a `cdc-init` (a new lineage in the same epoch).
  It never resets, for the same reason: a reset would hide every flushed row.

The record fences readers and publishers. It does not fence the data plane: the write
path does not read the record, so a process that still holds the dropped schema's
metadata can still insert its rows. Forma loads schemas from files at process start and
has no runtime drop today, so a drop is a deployment step, and its order closes the gap:
the drop runs once every process that loaded the old schema has stopped. A row a
straggler commits before activation is residue, and `Activate` refuses. A straggler
that writes after activation is the one case the protocol does not catch
([Open questions](#open-questions)).

**The `UNPUBLISHED` invariant.** In an `UNPUBLISHED` epoch, every `change_log` entry of the
schema is unflushed and every live row has one. It holds at registration (no rows), at
activation (no rows, checked in the same transaction), and wherever `init-db` writes an
`UNPUBLISHED` record for a schema that already has rows: it checks the invariant under
the schema lock, in the transaction that writes the record, and refuses the schema when
it is broken. Only two transactions mark entries: a publication, which leaves
`UNPUBLISHED` in the same commit, and a handoff, which requires `PUBLISHED`. So nothing
breaks the invariant. The protocol forbids writes that bypass `change_log`, including
clearing it.

### Generation history

```sql
CREATE TABLE <entity_main>_publication_generation (
  schema_id, generation  -- PRIMARY KEY
  epoch, kind,           -- kind: 'flush' | 'compact' | 'init'
  parent_generation, manifest_key, manifest_sha256, lineage, cas_token,
  committed_at  BIGINT NOT NULL,           -- start of the committing transaction
  superseded_at BIGINT NOT NULL DEFAULT 0, -- start of the replacing transaction; 0 while current
  retired_at    BIGINT NOT NULL DEFAULT 0  -- stamped by the collector after the replacement committed
)
```

The publication transaction inserts the new generation's row and stamps the replaced
generation's `superseded_at`. A reset stamps it too. Within one epoch, generations are
consecutive. Rebase decisions read `kind` (`KindsSince`), retention reads `retired_at`,
and `Resolve` reads `cas_token`. The collector reads `manifest_sha256` and `cas_token`
too (`Retained` and `Expired` answer both): they are what it verifies each manifest
against before it trusts the keys the manifest lists. `superseded_at` marks a generation as replaced, but
retention never counts from it: it is the replacing transaction's start, and nothing
bounds how long after its start that transaction commits.

### Lifecycle epoch

```sql
CREATE TABLE <entity_main>_publication_epoch (
  schema_id  SMALLINT NOT NULL,  -- PRIMARY KEY
  epoch      BIGINT   NOT NULL,  -- the highest epoch any record of the id carried
  updated_at BIGINT   NOT NULL
)
```

The id's durable lifecycle identity, kept apart from the record so that it outlives a
lost record row. Every transition that moves a record's epoch (`Initialize`,
`Reset`, `Activate`, `InitializeAfterLoss`) raises it to the new
epoch in its own transaction, with an `INSERT … ON CONFLICT DO UPDATE … WHERE epoch <
new epoch` that must write the row. A transition whose new epoch is not above it fails
with `ErrEpochReused`, and its transaction does not commit: only a record that went back
to an older epoch (restored from an older state, or written outside the package) meets
that, and moving it on would hand a stale process its pinned epoch again. The row is never
deleted. Readers and writers do not read it; only the lifecycle transitions and
`ReadHistory` do.

## Immutable manifest generations

**Object keys.**

- Manifest: `{prefix}/{schema_id}/_manifests/e{epoch}/g{generation:020d}-{token}.json`
- Parquet: `{prefix}/{schema_id}/e{epoch}/{base|delta}/{uuidv7}.parquet`

Every object is written once with a conditional put (`If-None-Match: *`), and is never
overwritten. Two writers that stage the
same generation number write different keys, because the token differs. Correctness
never depends on the layout: readers fetch the one manifest key the record names and
scan exactly the keys it lists. They never list objects and never glob.

**Serialization.**

```json
{
  "format": "forma-manifest/2",
  "schema_id": 100, "epoch": 1, "generation": 7, "parent_generation": 6,
  "lineage": "…", "token": "…", "kind": "compact", "input_generation": 5,
  "files": [
    {"tier": "base", "key": "data/100/e1/base/0190….parquet", "size_bytes": 1048576,
     "sha256": "…", "row_count": 5000, "row_id_min": "…", "row_id_max": "…",
     "columns": {"ltbase_row_id": "UUID", "…": "…"}}
  ]
}
```

`parent_generation` is the record's: the generation the compare-and-set replaced, 0 for
the epoch's first. `input_generation` appears on a `compact` manifest only and names the
generation whose files the compactor merged. After a rebase over flushes the two differ.
`files` is sorted by `key`. The record's `manifest_sha256` is the SHA-256 of the exact
stored bytes, as 64 lowercase hex digits, and the generation's history row keeps the same
value. `Publish` refuses any other form, so no commit names a digest the collector would
refuse. A reader refuses a manifest whose digest differs, whose identity fields
(`schema_id`, `epoch`, `generation`, `token`) differ from the record's, or whose `format`
it does not know. The digest and identity checks bind the manifest to the schema the
record names (#520, #522). Every file field shown is required, and V2 has no footer-probe fallback for a
missing `columns`. `sha256` is the object's content digest. Readers do not recompute it
per scan. Verification passes do. How a writer obtains it (a full read, or an S3
additional checksum) is the writer child's decision.

**Empty versus never published.** `PUBLISHED` with `files: []` means the parquet tiers hold
nothing, and the reader runs no DuckDB statement. `UNPUBLISHED` means no generation exists,
and there is no object to read. The two cannot be confused, so writers no longer need
zero-row placeholder objects.

**What a generation replaces.**

| Kind | Generation = | Built from |
| --- | --- | --- |
| `flush` | the parent's files + the new delta files | the parent |
| `compact` | the parent's files − the merged sources + the merge outputs | the input generation's files, rebased onto the parent over flushes only |
| `init` | the export's base files only. All delta and earlier base files leave the inventory | the export |

In every row the parent is the generation the compare-and-set replaced.

## The commit boundary

```text
-- staging, before BEGIN: parquet objects, then the manifest object (conditional put)
BEGIN;
  UPDATE publication SET ... generation = generation + 1 ...          -- compare-and-set, first
   WHERE schema_id = $1 AND epoch = $2 AND generation = $3 AND cas_token = $4
     AND $staged_at >= now_ms - $budget AND $staged_at > gc_horizon
   RETURNING generation;                                              -- 0 rows: stop, ROLLBACK
  UPDATE publication_generation SET superseded_at = now_ms WHERE ... -- the replaced generation
  INSERT INTO publication_generation (...);                           -- the new generation
  <flush mark of the generation's rows at their listed versions>      -- same transaction
COMMIT;
```

`publication.Publish` runs the first three statements, and the caller sends the marks on
the same transaction. `Publish`, `Reset`, `Activate`, `HoldLineage` and both
`Initialize` functions take a `publication.Tx`, which only `SQLTx` (a `database/sql`
transaction) and `PgxTx` (a pgx transaction, for init-db and the other pgx callers)
build. A pool is refused when the caller compiles, because through a pool each
statement commits on its own. The caller opens the transaction, sends its own statements
on it, and alone commits or rolls it back.

`cdc-flush` keeps its exact-version marks. On PostgreSQL that is
`markFlushedSkipLocked` (#686), which never waits on a writer. On DSQL it is the plain
statement, where a conflict with a writer aborts the whole transaction with `40001`, and
the publication is sent again with the same token.

**Why S3 staging cannot expose partial data.** A staged object becomes visible only through
a committed record. A reader never lists or globs. Its only way into S3 is the
`manifest_key` its snapshot shows, and that key changes only when the transaction commits.
An object whose publication never commits is unreachable. GC reclaims it
([Garbage collection](#garbage-collection-and-reader-lifetime)). The design needs no
distributed transaction between S3 and PostgreSQL, because nothing depends on S3 state
that no commit names.

**Invariant B (no mark without its pointer).** Every committed flush mark of an entry
`(r, v)` was committed either:

- (a) in the transaction that committed a generation whose files hold `r` at `v`, or
- (b) in a handoff transaction that held the record at a lineage `L`, published, whose
  init generation holds `r` at `v`.

A record recovered `AWAITING_INIT` after a loss starts an epoch above the one in which the
schema's marks were committed, so no generation of the new epoch descends from the
generation that marked them. Readers fail closed on the record, and its next generation is
an `init`, which re-exports every live row; [Invariant R](#readers) treats that case.

*Proof.* Only two code paths mark. (a) In a publication transaction the compare-and-set
runs first. When it matches no row, `Publish` returns an error before any mark is sent,
and the caller rolls back. When it matches, pointer and marks commit or roll back
together, so every snapshot sees both or neither. (b) A handoff transaction's first
statement is `HoldLineage`, which reads the record with `SELECT … FOR UPDATE` filtered on
epoch, lineage and `PUBLISHED`. On PostgreSQL the row lock makes any transition of the
record wait until the handoff ends. `TestHoldLineageHoldsOffAReplacement` shows the wait.
On DSQL, `FOR UPDATE` puts the row into the transaction's conflict check, so the handoff
and a concurrent transition cannot both commit. That part is assumed until #817 checks it.
In either case, when the handoff commits, the record still carries `L`. `init-db` writes
an `UNPUBLISHED` record only where no flushed entry exists, so no mark predates a record
that names no generation. ∎

**DSQL budgets.** DSQL caps a transaction at 3,000 modified rows, 10 MiB and five
minutes. A publication modifies 3 rows of its own plus its marks. So on DSQL a flush
generation holds at most 1,000 entries (today's `markRowsPerStatement`), and the default
10,000-row batch publishes as ten generations. Even if updating `flushed_at`, a
primary-key column, counted as two modified rows, the total would be 2,003. Nobody has
measured this, and #817 records it. The transaction does no S3 I/O, so its duration is
the statements alone. The writer bounds it with a context deadline `T_pub` (proposed
default 30 s, well under DSQL's five minutes). That deadline is for liveness only. A
client deadline does not stop a `COMMIT` already sent from landing later, so neither the
reader's grace nor the orphan rule depends on it. On PostgreSQL a generation keeps
today's batch size, because the mark already commits in one statement.

### The `cdc-init` handoff

`cdc-init` exports every live row and publishes an `init` generation under a new lineage.
That publication transaction carries no marks, because the export can be far larger than
one transaction may modify. Correctness does not need marks. An exported row whose entry
stays unflushed is dirty, so the anti-join discards its parquet copy and the hot leg
serves it until a later flush exports it again. The handoff is the optimisation that
avoids re-flushing the whole export. The export records, for every row it writes, the
version of that row's unflushed entry, read in the same snapshot as the row. After the
init generation commits, the handoff marks those entries at exactly those versions, in
transactions of at most 1,000 entries. Each transaction starts with `HoldLineage`, and
the handoff stops at the first `ErrLineageReplaced`. A row written since its export has
moved past its listed version, so its mark is skipped and it stays dirty. V2 never
clears `change_log`.

## Writers: compare-and-set, rebase, outcome unknown

**Competing publishers.** The schema lock (advisory lock, or the DSQL lease) keeps flush,
init and reconcile apart for liveness. Safety comes from the compare-and-set alone. Two
transactions that move the record from the same `(epoch, generation, token)` cannot both
commit. On PostgreSQL the second waits on the first's row lock, re-evaluates its `WHERE`
and matches nothing. At `REPEATABLE READ`, on MiniStack and on DSQL, one of the two gets
`40001`. `publication.Conflict` treats both as a lost race.
`TestPublishRacingRivalsCommitOne` and `TestPublishLosesToACommittedRival` pin this. A
writer whose lease expired, or a compactor running outside the lock, is therefore never
a correctness problem.

**Rebase.** A writer that loses reads the new head and the kinds committed since its
parent (`KindsSince`, which refuses a gap in the history), then asks `Rebase`:

| Writer \ committed since | flush | compact | init | other epoch |
| --- | --- | --- | --- | --- |
| flush | rebase | rebase | refuse | refuse |
| compact | rebase | refuse | refuse | refuse |
| init | refuse | rebase | refuse | refuse |

The rules follow from what each kind changes:

- **Flush.** A flush appends rows exported at exact versions, possibly older than versions
  the head already holds. Last-writer-wins keeps those copies harmless as long as the
  head still holds each row's newest version, tombstones included. Flushes append and
  compactions keep the newest version, so the flush may rebase over both. An init
  exports live rows only, so it drops the tombstone of a row deleted after the flush
  exported it. Suppose a flush exports `r` live at `v1` and loses its lease. A rival
  flush publishes `r`'s deletion at `v2` and marks it, and an init replaces the
  inventory. If the stale flush then rebased, its `r@v1` would be `r`'s only parquet
  copy. Its own mark of `(r, v1)` skips, but the deletion entry is already clean, so the
  dirty barrier would not hide the resurrected row. A flush therefore never rebases over
  an init. It abandons its staging and selects the unflushed entries again.
  `TestRebaseRefusesAFlushAcrossAnInit` stages this sequence.
- **Compaction.** A compaction may only rebase over appends. Another compaction or an
  init may have removed or replaced a file it merged. A compaction keeps every tombstone
  it merges. One that dropped tombstones would be a content replacement like an init,
  and a flush could no longer rebase over it.
- **Init.** An init replaces content with an export. A compaction in between changed no
  content. A flush in between appended rows the export may predate, and replacing them
  would leave their entries marked over missing data.

A rebased writer builds a new manifest object from the head's files under a fresh token.
It keeps its parquet objects, its marks and its original `staged_at`. A refused writer
abandons its staging.

**Epoch fencing.** A reset changes `epoch`, and so does the activation that ends the
`DROPPED` interval, so every compare-and-set and every `HoldLineage` staged under an
earlier epoch misses. `TestResetFencesEveryWriterOfTheOldEpoch`
pins this.

**Commit outcome unknown.** When `COMMIT` fails without an answer, the writer calls
`Resolve(cur, next)` in a new transaction, at any time afterwards. A `FATAL` or `PANIC`
answer counts as none, whatever its SQLSTATE: the session ended, and a pooler answers so
for a backend it lost after it forwarded the `COMMIT`. Only an `ERROR` is the server's
refusal, a `40001` sent again with the same token. `Resolve` reads the
record and the history row of the attempt's number in one statement, so one snapshot
answers both:

- `Committed`: the history row holds the attempt's token. Or the row is gone and the
  record still carries the fresh lineage only this attempt introduced (a `cdc-init`, or
  the first flush of an unpublished schema). The writer is done, and an init runs its
  handoff.
- `Lost`: another token holds the number, so this writer's transaction can never commit.
  It rebases.
- `NotCommitted`: the record still carries the attempt's epoch, generation and token, so
  nothing committed from that expectation. The writer sends the *same* publication
  again, with the same expectation and token. If the lost commit lands first, the resend
  loses the compare-and-set, or fails with `40001` when the commit landed after its
  snapshot, and the writer asks `Resolve` again, which then answers `Committed`. It asks on
  every miss or `40001` of a staging whose commit went unanswered, the last allowed
  transaction's too, so a commit that landed never reads as attempts exhausted. On PostgreSQL the
  resend waits on the original's row lock, so the two never both commit. On DSQL the
  conflict check covers it.

  A `NotCommitted` is not final while the lost commit may still be on its way. So when the
  writer would give such a staging up (the attempts ran out, a resend was refused with
  `ErrStagingExpired`, or a resend failed), it first settles it with `Fence(cur)`. That
  transaction writes the record row, changing nothing, while the record still carries the
  expectation, and commits. The original's compare-and-set wrote that row before its
  `COMMIT` was sent. On PostgreSQL the fence waits on that row lock until the original
  ends, and on DSQL whichever of the two commits second fails with `40001`. So a fence
  that held the record and committed proves the commit can never land: only then is the
  staging abandoned (the orphan rule reclaims it) or reported as attempts exhausted. A
  fence that finds the record moved hands over to `Resolve`, whose answer is final now. A
  `Committed` there, however late the commit landed, counts as committed, and an init
  runs its handoff. When no fence can settle it (the database is out, or the caller's
  context ended), the writer reports `ErrUnresolved`, never `ErrAbandoned`. The error
  carries the expectation, the token and, for an init, the exported entries at their
  listed versions. The staged objects must then stay, and a flush does not start over.
  The caller settles it later with a fresh context: `Settle` for a flush, `ResumeInit`
  for an init. Each runs the same fence and `Resolve` again. A commit found then,
  however late it landed, is committed: a flush's marks committed with it, and
  `ResumeInit` runs the init's handoff from the kept entries, at exactly their export
  versions and fenced by the init's lineage, so a row written since stays dirty. A
  fence that holds the record, or a `Lost` or `Unknown`, answers `ErrAbandoned`, and the
  caller runs the flush or the init again. Still unsettled, it answers a fresh
  `ErrUnresolved`. A process that exits before it settles leaves the init's entries
  dirty for the next flush, as a crash between an init's commit and its handoff does.
  `TestInitSettlesALostCommitAfterItsLastResolve`,
  `TestInitFenceWaitsForACommitStillInFlight`,
  `TestFlushSettlesAStagingThatExpiresDuringAnUncertainResend`,
  `TestFlushReportsAnOutcomeItCannotSettle` and
  `TestResumeInitHandsOffAnInitWhoseFenceFailed` pin this.

  A flush found committed through `Resolve` reports the marks of the transaction that
  committed. That is one of those whose `COMMIT` went without an answer, never a resend
  that failed. When they marked different counts, the report gives the range
  (`Marked`, `Unconfirmed`). `TestFlushReportsTheMarksOfTheCommitThatLanded` pins this.
- `Unknown`: the record has moved past the expectation and the history row is gone. An
  outage or a pause outlasted the collector's grace, and the generation was superseded
  and forgotten, or the epoch changed. The attempt can never commit now, and whether it
  did cannot be recovered. The writer abandons it and starts over from the head. That is
  safe for every kind. A flush selects the still-unflushed entries, and a committed
  attempt left its own rows marked. A compaction reads the head's files. An init whose
  lineage is no longer on the record would have its handoff stopped by
  `ErrLineageReplaced` anyway.

`Resolve` never answers `NotCommitted` from a missing history row alone, so the
collector's retention needs no promise about how long a writer may take to ask.
`TestResolveAfterALostCommitAnswer` and `TestResolveTellsAForgottenOutcomeFromAnUncommittedOne`
pin the outcomes.

**Idempotent restart.** A writer that crashed left either nothing in PostgreSQL (its
transaction rolled back) or a committed generation with its marks. The restarted writer
selects entries `flushed_at = 0` again and stages fresh keys. Rows of a rolled-back
attempt are re-exported. A duplicate `(row_id, version)` across two committed generations
can only arise from a rebased resend, and LWW deduplicates it as today.

**Staging budget.** `staged_at` is the database time the writer read before its first S3
write for the generation. The compare-and-set refuses a commit once
`now − staged_at > W`, or once `staged_at` is at or below the record's `gc_horizon`
(`ErrStagingExpired` for both). The budget gives a writer a predictable expiry. The
horizon is what the orphan rule relies on.

## Readers

1. Record `t0` on the local monotonic clock. `BEGIN REPEATABLE READ READ ONLY` (the
   snapshot S). As S's first statement, `publication.Read` returns the record and
   `ReadAt`, the transaction's database time.
2. Absent, `AWAITING_INIT`, `DROPPED`, or an epoch other than the one the process
   pinned: fail the read as a read-path consistency error (`ErrNoRecord`, then
   `Record.Readable`). `UNPUBLISHED`: answer from the hot leg alone, with no DuckDB
   statement. `PUBLISHED`: continue.
3. Read the dirty set (and in the hot form, the hot keys) in S, as today.
4. Get the manifest by `manifest_key` and check it (digest, identity, format). Objects
   never change, so a process-wide cache keyed by `manifest_key` is never stale.
5. Scan exactly the listed keys in DuckDB, discarding dirty `row_id`s.
6. Read the page's hot rows in S, and end S.
7. Abort the pass, retryably, if it has not finished by `t0 + R_max`. That includes
   cancelling a running DuckDB scan.

A listed key that is missing is loss: the manifest is the one-directional contract. The read
fails loudly and classifies, and GC never makes this happen within the retention window.

**As built (#900, wired in #896).** Every pass in `internal/federated` takes these steps,
and resolves nothing before its snapshot. The factory builds the engine with
`WithPublicationReader` whenever DuckDB is enabled; an engine without a reader fails every
pass as misconfigured, non-degradably, rather than answer from the hot leg alone. The epoch
pins are read once at startup, in one snapshot beside the schema metadata
(`federatedv2.LoadEpochPins`): a schema without a record gets no pin and fails closed
(`federatedv2.ErrNotPinned`) until the process restarts, and the metadata is never
reloaded, so the pins never need to follow a reload. A few choices the steps above leave
open:

- The dirty set is the strict one (`flushed_at = 0`) in both forms, and the hot keys are
  read over the unflushed entries alone. The flush-grace widening (#252) made up for a path
  set resolved before the snapshot. A generation read in S holds every row S shows flushed,
  so nothing widens the barrier, and the parquet-history probe (#720) has nothing to decide.
- The manifest is fetched after the dirty set and before the hot keys. Every listed object
  must reach DuckDB as one concrete object: a listed address holding a glob metacharacter
  (`*`, `?`, `[`), which DuckDB would expand and match against unlisted objects, fails the
  read with the consistency error (`federated.ErrPublishedPathNotConcrete`) before any
  DuckDB statement. The pre-read
  schema and type checks (#189, #732) take the manifest's `columns` in place of a footer
  probe. A generation that lists objects cached as corrupt (#722) is refused without a scan. `duckdb.hotLegMaxDirtyRows` bounds what a pass stages in DuckDB, so it does
  not apply to a pass without a parquet leg: every live row of an `UNPUBLISHED` schema is
  dirty, and nothing is staged. The execution plan reports such a pass's dirty set as read,
  not staged.
- DuckDB admission (the breaker, and a DuckDB client being there) applies only once the
  snapshot's generation lists objects to scan. A pass without a parquet leg needs no DuckDB,
  so an open breaker never refuses it, and a publication fault is read and reported before
  admission could answer for it with a degradable rejection.
- The consistency error is `federated.PublicationConsistencyError`, which matches
  `federated.ErrPublicationInconsistent` and the fault it wraps (`publication.ErrNoRecord`,
  `ErrAwaitingInit`, `ErrDropped`, `publication.ErrRecordInvalid` for a row the read
  returned and refused, `federatedv2.ErrEpochChanged`, a
  `manifestv2.ConsistencyError`) and names schema, epoch and generation, with the lost keys
  when objects are missing. It is not degradable, hands the breaker its probe back rather than
  counting a failure, and is never `ErrFederatedReadFailed`. A record the snapshot could not
  read, or a manifest the store could not answer, is a failed read instead: the first
  `ErrPostgresReadFailed`, the second `ErrFederatedReadFailed`, both degradable.
- A listed object is found missing only after a read fails: the reader probes the failed
  read's objects (`HeadObject` on S3) before any other classification, so a happy path costs
  no probe. A probe that fails does not stop the others or take back a key another found
  missing, so a confirmed loss is the consistency error even beside a failed probe. A probe
  the request's deadline ends keeps that error, so the read stays non-degradable.
- S3 answers `403 AccessDenied`, not `404`, for a missing key to a principal without
  `s3:ListBucket` on the bucket, so a 403 on the manifest fetch or on a probe proves neither
  presence nor loss. The read fails closed with the consistency error wrapping
  `forma.ErrPublishedObjectUnverifiable`, naming no lost key, neither degraded nor charged to
  the breaker. A reader's principal needs `s3:ListBucket` on the bucket besides
  `s3:GetObject` for a loss to be reported as one.
- `R_max` is armed at `t0` with `context.WithTimeoutCause`. A pass it ends fails with
  `federated.ErrPassDeadline` and is retried once on a fresh snapshot, as the #720 race was. An
  expired deadline is not degradable (#465): `ErrPassDeadline` is refused by the degraded
  fallback on its own, and a retry that fails, a breaker rejection included, keeps the first
  pass's deadline in its error. A pass `R_max` ends is not charged to the breaker and
  is not probed for lost objects: `R_max` bounds how long a pass holds its generation against
  garbage collection, not how fast DuckDB answers, which `DuckDBConfig.QueryTimeout` bounds.
  A consistency error the pass confirmed before `R_max` ended it
  is not retried: when the missing-object probe finds a listed key lost and `R_max` then
  stops a probe of another object, the read fails with the
  `PublicationConsistencyError` naming that key, not with `ErrPassDeadline`, so a retry on
  a fresh snapshot cannot hide a loss already found. A consistency error the retry meets (an
  absent record, a missing or invalid manifest, a lost object) is the request's answer: the
  first pass, whose chain may carry `ErrFederatedReadFailed` from a scan `R_max` ended, is
  kept beside it as text only, so the fault is never a failed read.

**Invariant R (the scanned generation matches S).** For every row `r`, the answer holds
`r`'s state as of S.

*Proof.* Suppose S shows `r` with an unflushed entry. The anti-join discards every parquet
copy, and the hot leg returns `r` as of S. Otherwise `r`'s latest entry in S is flushed,
or `r` is a live row without an entry. Take the flushed case first, with entry `(r, v)`.
By Invariant B, the transaction that marked it committed (a) a generation H holding
`r@v`, or (b) the handoff of a lineage whose init generation holds `r@v`. That commit
precedes S, so S's generation G equals H or descends from it in the same epoch. A reset
in between would require purging `r`. Every descendant still holds `r@v` as `r`'s newest
parquet version:

- A flush appends files only. A rebased flush may append a copy of `r` older than `v`.
  It rebased over flushes and compactions only, which kept `r@v`, so LWW still picks
  `r@v`.
- A compaction keeps each row's newest version, tombstones included. That is today's LWW merge, and the compaction child must keep it.
- An init re-exports every live row, and `r`, unchanged since `v`, exports as `r@v`.

A newer version of `r` in any file would mean a newer entry, contradicting the choice of
`(r, v)` as the latest. A row without an entry can only come from an init export, which
holds it, and descendants keep it. In a record recovered `AWAITING_INIT` the first
generation of the new epoch is an init, which exports every live row, including those
whose entries an earlier epoch flushed. A row deleted before S has a deletion entry, which is either dirty or flushed. A
flushed deletion has a tombstone that LWW applies. Alternatively an init after the
deletion holds no copy of the row, and no later generation adds one: a flush exports only
rows with an unflushed entry, and a flush staged before that init cannot rebase over it.
In an `UNPUBLISHED` S, the invariant puts every live row in the hot leg. ∎

This replaces the cutoff widening (#252), the parquet-history probe (#720), the
re-resolution after the pass, and the path set that was frozen before the snapshot.

### Why no mutable root

A mutable S3 root (a `manifest.json` rewritten to point at the newest generation) adds a
second resolution step outside the snapshot. A reader would see the record at G, read the
root at G+2, and scan a generation whose marks its snapshot lacks, or the reverse. Fixing
that means the root must name a generation the reader then cross-checks against
PostgreSQL. At that point PostgreSQL is the pointer anyway, and the root is only a second
copy that can go stale. A pointer to immutable objects cannot be reused stale either:
every transition writes a new number and a new token, and a key is never rewritten.

## Garbage collection and reader lifetime

The protocol uses five time parameters:

- `R_max`: the reader's pass deadline.
- `W`: the staging budget.
- `C`: a clock margin covering the drift between the local monotonic clock, the database
  clock and S3's `LastModified`.
- `G`: the retention grace.
- `A`: the orphan age the collector's horizon trails the database clock by.

`T_pub`, the publication transaction deadline, is not among them. A client deadline
cancels the client's wait, not a `COMMIT` already sent. A transaction that passed its
compare-and-set may commit long after its start and after any deadline, and both
`superseded_at` and `committed_at` record its start. So no rule below assumes a
transaction commits within a bound of its start.

Proposed defaults are for the child issues to fix. The two rules below hold for any
values with `G ≥ R_max + C`. `A ≥ W + C` keeps the horizon from fencing a writer that
is inside its budget, and any `A` is safe.

- **Retention from the retire stamp.** A collector that sees a generation superseded in
  a snapshot ends that snapshot, then runs `Retire` in a new transaction. `Retire` stamps
  `retired_at` with that transaction's database time, once. The generation is retained
  until `retired_at ≤ now − G` (`Retained`, `Expired`). Let a reader choose generation
  g in snapshot S, whose database time is `ReadAt`. The reader saw g current, so g's
  replacement committed after S began, whether it was a publication or a reset. The
  collector saw that commit in a snapshot that ended before the retiring transaction
  began, so `retired_at > ReadAt`. The reader started its deadline `t0` before S, so it
  has finished or aborted by `ReadAt + R_max + C ≤ retired_at + G`. GC deletes nothing
  a retained generation names before then. A late replacement only delays the stamp.
  `TestRetireCountsFromTheReplacingCommit` stages a publication and a reset whose commit
  lands after the reader's snapshot. In both, `superseded_at` predates the snapshot and
  `retired_at` follows it.
- **Orphans below a committed horizon.** Before a pass deletes anything unreferenced, it
  runs `RaiseHorizon`. That statement writes `gc_horizon = max(gc_horizon, now − A)` to
  the record and commits, and the pass uses the answer as H. Every compare-and-set
  requires `staged_at > gc_horizon`, and the raise writes the same row as every
  publication. So the raise and a publication can never both commit unordered. On
  PostgreSQL the raise waits for a publication holding the row lock, or the
  publication's compare-and-set waits for the raise and then re-checks the horizon. At
  `REPEATABLE READ` and on DSQL, one of the two fails with `40001`. The raise does not
  change the token, so a publication that loses that race resends unchanged. Once H has
  committed, every publication is either committed and visible to the snapshot the pass
  reads next, or still to commit and therefore refused unless its `staged_at > H`. A
  staged object with `LastModified + C < H` was staged by a writer with
  `staged_at ≤ LastModified + C < H`, which can never commit. A pass whose raise does
  not commit deletes no orphan. `TestRaiseHorizonFencesAStagedPublication` and
  `TestRaiseHorizonWaitsOutAPublicationInFlight` pin both orders.

**Collection order.** Each step repeats safely after a crash. `PlanCollection` computes
steps 5 to 7 from what steps 1 to 4 read.

1. `RaiseHorizon`, committed. Its answer is H.
2. List the objects under the schema's prefix.
3. In one snapshot, read `Retained(G)` and `Expired(G)`, then end the snapshot. Run
   `Retire` for every retained generation that is superseded and not yet retired. Those
   generations expire in a later pass.
4. Get the manifest of every retained and expired generation, and verify each one that
   exists before reading its `files` (`ManifestFiles`): the SHA-256 of the stored bytes
   must equal the history row's `manifest_sha256`, the bytes must be one JSON object of
   format `forma-manifest/2`, its `schema_id`, `epoch`, `generation` and `token` must
   equal the schema's and the row's (`token` is the row's `cas_token`), and every file
   must name a key. The digest is checked first, so a corrupted manifest that still
   parses never reaches the file list.
   - If a retained generation's manifest is missing, stop: the pass cannot tell what
     readers may still scan, and deletes nothing (`ErrRetainedManifestMissing`).
   - If any manifest that exists fails verification, retained or expired, stop and
     delete nothing (`ErrManifestInvalid`). A retained manifest that lied could omit a
     key a reader still scans, which steps 5 and 6 would then delete as expired-only or
     orphaned. An expired manifest that lied could list a key no committed generation
     lists, such as an object a publication still in flight staged after H, and step 5
     deletes an expired manifest's keys without the horizon's check. So an invalid
     expired manifest is never trusted, never deleted and its row never forgotten: the
     pass refuses as a whole, and so does every later pass until an operator repairs
     the fault. Manifests are immutable and their digests committed with their rows, so
     a failure is corruption or a writer bug, never a state the protocol produces. The
     refusal costs liveness only: nothing is deleted while it lasts.
   - A missing expired manifest does not stop the pass (step 6).
5. Delete each listed object that is not protected and either is listed by an expired
   generation's manifest or is an orphan. Protected means it is a retained generation's
   manifest object or a key that manifest lists. An orphan is named by no history row,
   either as a manifest key or through a manifest that exists, and has
   `LastModified + C < H`. An old current manifest is a manifest object, not a file, and
   stays protected however old it is.
6. When step 5 has finished, delete the expired generations' manifest objects. An
   expired manifest goes only after the objects it alone listed. So if the next pass
   finds an expired manifest missing, its exclusive objects are already gone. That pass
   forgets the row and deletes nothing on its account. An object it should have taken
   but somehow still exists is an orphan, decided by the horizon.
7. When step 6 has finished, `Forget` each expired history row in its own transaction,
   atomically deleting the expired row and advancing `forgotten_generation` on
   `_publication_epoch` (`GREATEST(forgotten_generation, g)`). A row that outlives a
   crash makes the next pass repeat the deletes instead of leaking them, and once
   forgotten the durable `forgotten_generation` floor proves history completeness
   without keeping a dummy expired row forever.

**Write-path history consistency and fresh-epoch recovery.** `Publish` and `Reset`
verify that the current generation's history row matches every field of the record
(`epoch`, `parent_generation`, `manifest_key`, `manifest_sha256`, `lineage`,
`cas_token`) and still carries `superseded_at = 0 AND retired_at = 0`. If out-of-band
database damage broke or deleted that row, `Publish` and `Reset` fail closed with
`publication.ErrHistoryInconsistent` and roll back rather than overwriting history.
Recovery uses fresh-epoch recovery (`DELETE` record row -> `init-db`
`InitializeAfterLoss` -> `cdc-init`): `InitializeAfterLoss` allocates a fresh epoch
above `_publication_epoch.epoch`, supersedes the surviving head row with
`retired_at = 0`, and arms a monotonic post-commit recovery fence
(`recovery_epoch = rec.Epoch, recovery_retired_at = 0`). The first `--gc` pass
stamps `recovery_retired_at = now()` (`RetireRecovery`) and deletes nothing until the
full retention grace `G` elapses after that stamp, protecting any reader pinned
before recovery even if a concurrent `Reset` or `Publish` runs next.

**Storage binding (`_publication_storage`).** Before `--gc` deletes anything, the
deployment must be bound to the exact S3 destination its clients send requests to
(bucket URL, bucket name, and data prefix, derived by `internal/s3endpoint`). The
writers bind it in the transaction of the deployment's first publication and check
it before every staging and in every publication transaction, failing closed on
another target (`publication.ErrStorageBoundElsewhere`, #911);
`manifest-reconcile --bind-storage` binds it ahead of them. The writers' and the
collector's SDK clients refuse HTTP 3xx redirects and reject ambiguous
custom-endpoint paths (repeated slashes, trailing slashes on non-root paths, dot
segments, escaped paths), so none of their requests reaches another bucket or
tenant route. The compactor's DuckDB merge engine reads its sources apart from
that client; the compactor configures it with the client's endpoint and
refuses to run unless both read the bucket from the same URL, with the same
region and addressing (`docs/manifest-reconcile.md`, "Storage
binding").

A writer can never commit a reference to an object GC deleted:

- The head's manifest and objects are retained.
- A writer that composed its manifest from an older head fails the compare-and-set.
- An object the writer staged itself was staged after its `staged_at`. If it is below
  the horizon, so is `staged_at`, and the compare-and-set refuses.

`TestRetentionKeepsSupersededGenerationsForTheGrace` pins the queries.
`TestPlanCollectionProtectsRetainedManifests`,
`TestPlanCollectionRecoversAfterDeletingAnExpiredManifest`,
`TestPlanCollectionStopsOnAMissingRetainedManifest` and
`TestPlanCollectionStopsOnAnInvalidManifest` pin steps 4 to 7. The last stores, under a
retained and under an expired manifest key, bytes that differ from the row's digest, a
manifest of another generation, token, epoch or schema sealed with a matching digest, an
unknown format, a missing `files` or key, and bytes that do not parse. Each would drop a
retained key from protection or list an in-flight staged object, and each stops the pass
with an empty plan.

## State transitions

| Event | From | One PostgreSQL transaction | To | `change_log` | S3 / failure |
| --- | --- | --- | --- | --- | --- |
| Register schema | no row | `Initialize` with the registry insert | e1, `UNPUBLISHED`, g0 | — | none |
| Initialize a record-less schema | no row, no history | `UNPUBLISHED`-invariant check + `Initialize`, under the schema lock | e1, `UNPUBLISHED`, g0 | — | invariant broken → the run fails and the registration rolls back. Lock held → skipped (pre-existing schema) or the run fails (schema it registers) |
| Recover a lost record | no row, history survives | `InitializeAfterLoss` above the lifecycle epoch, under the schema lock | `AWAITING_INIT` with generation history; else `UNPUBLISHED` if the invariant holds; `DROPPED` then `Activate` for an id the run registers | marks stay | readers fail closed until the init. Lifecycle epoch lost with history surviving → `ErrLifecycleLost` |
| Never published (read) | `UNPUBLISHED` | — | — | all entries dirty | hot leg only, no DuckDB |
| First flush | `UNPUBLISHED`, g | CAS → g+1 `flush`, new lineage, parent 0, + marks | `PUBLISHED` g+1 | batch marked at listed versions | stage delta + manifest first. CAS miss → rebase. Any failure before COMMIT → nothing visible |
| Repeated flush | `PUBLISHED` g | CAS → g+1 `flush`, same lineage, + marks (≤ 1,000 on DSQL) | g+1 | as above | as above. `40001` → resend the same publication. Head moved by an init → abandon, select again |
| Init (onboarding) | `UNPUBLISHED`, `AWAITING_INIT` or `PUBLISHED` g | CAS → g+1 `init`, new lineage, no marks | `PUBLISHED` g+1 | none in this transaction | export, then manifest. CAS miss → rebase over compactions only |
| Init handoff | `PUBLISHED`, lineage L | `HoldLineage(L)` + ≤ 1,000 marks, repeated | unchanged | exported entries marked at export versions | `ErrLineageReplaced` → stop. Unmarked rows stay dirty |
| Re-init | `PUBLISHED` g, lineage L | as init, new lineage L′ | g+1 | none | the old lineage's handoff stops. Old objects expire after `G` |
| Compaction | `PUBLISHED` g | CAS → g+1 `compact`, same lineage | g+1 | none | merge outputs + manifest first. Rebase over flushes only |
| Drop | any but `DROPPED` | `Reset` with the registry delete | e+1, `DROPPED`, no current generation | purged afterwards, in chunks | readers fail closed. Every CAS/hold misses or is refused. Old objects expire after `G` |
| Re-create | e+1 `DROPPED` | residue check + `Activate` with the registry insert | e+2, `UNPUBLISHED`, new token | — | `ErrResidue` while the id still has rows, unique-key entries or key build state |
| Failed publication | g | rolled back | g | unchanged | objects unreferenced. Orphan rule reclaims them |
| Commit outcome unknown | g | `Resolve` in a new transaction | g or g+1 | with the pointer | `NotCommitted` → resend the same publication, and `Fence` before giving it up. `Lost` → rebase. `Unknown` → abandon, start over from the head |
| Rollback to an older generation | — | not a transition | — | — | Refused by design: marks committed by later generations would point at missing rows. Re-init rebuilds from PostgreSQL instead |
| Orphan cleanup | any | `RaiseHorizon`; `Retained`/`Expired`, `Retire`; then `Forget` | `gc_horizon` raised | — | see collection order |

## Sequences

```mermaid
sequenceDiagram
  participant F as cdc-flush
  participant S3
  participant PG as PostgreSQL
  participant R as reader
  F->>PG: read record (e, g, token)
  F->>PG: SELECT now_ms (staged_at)
  F->>S3: put delta objects, put manifest g+1 (If-None-Match)
  R->>PG: BEGIN RR; Read → g; dirty set (rows dirty)
  F->>PG: BEGIN; CAS g→g+1; history; mark rows; COMMIT
  R->>S3: get manifest g (immutable); scan
  Note over R: rows dirty in S, served hot; g still retained
  R->>PG: hot rows in S; END
  R->>PG: next pass: BEGIN RR; Read → g+1; rows flushed
```

```mermaid
sequenceDiagram
  participant I as cdc-init
  participant C as compactor
  participant PG as PostgreSQL
  I->>PG: read record (g, L)
  I->>I: export all live rows + versions (hours)
  C->>PG: CAS g→g+1 compact (wins)
  I->>PG: CAS from g misses (ErrRecordMoved)
  I->>PG: KindsSince(g, g+1) = [compact] → Rebase ok
  I->>PG: CAS g+1→g+2 init, lineage L′
  loop chunks of ≤ 1,000
    I->>PG: BEGIN; HoldLineage(L′); mark at export versions; COMMIT
  end
```

```mermaid
sequenceDiagram
  participant W as writer
  participant GC as collector
  participant PG as PostgreSQL
  participant S3
  W->>PG: SELECT now_ms (staged_at = s)
  W->>S3: put objects, put manifest
  W->>PG: BEGIN; CAS (s > gc_horizon) holds the record row
  GC->>PG: RaiseHorizon (waits on the row, or 40001 and again)
  W->>PG: COMMIT, however late
  PG-->>GC: H committed after the writer's commit
  GC->>S3: list
  GC->>PG: Retained/Expired: the writer's generation is retained
  Note over GC: no CAS with staged_at ≤ H commits any more, so objects below H are orphans
```

## Verification

The tests run on the integration PostgreSQL at `READ COMMITTED` and at `REPEATABLE READ`,
and on the MiniStack lane (`run_suite publication`). Each test pins one of the claims
above:

- `TestPublishCommitsPointerAndMarksAtOneBoundary`: one boundary. Snapshots before,
  during and after.
- `TestPublishRolledBackLeavesNeitherPointerNorMarks`: rollback.
- `TestPublishLosesToACommittedRival`, `TestPublishRacingRivalsCommitOne`: CAS and races.
- `TestPublishRefusesAnExpiredStaging`: staging budget.
- `TestPublishRefusesTransitionsTheProtocolLacks`: illegal transitions.
- `TestPublishThroughAPgxTransaction`: the pgx callers' own transaction.
- `TestRebaseFollowsTheKindsBetween`, `TestRebaseTable`: rebase rules, and the history gap.
- `TestRebaseRefusesAFlushAcrossAnInit`: the resurrection sequence.
- `TestParentIsTheCASPredecessor`: `parent_generation` after a rebased compaction and
  after a drop and re-registration.
- `TestResetFencesEveryWriterOfTheOldEpoch`: `DROPPED`, writers of the old epoch and
  publishers that read after the drop, the residue check, activation and no generation
  reuse.
- `TestAwaitingInitAdmitsOnlyACDCInit`: the state lost-record recovery leaves a schema with
  generation history in.
- `TestResolveAfterALostCommitAnswer`,
  `TestResolveTellsAForgottenOutcomeFromAnUncommittedOne`: outcome unknown, including a
  forgotten history row.
- `TestHoldLineageFencesTheHandoff`, `TestHoldLineageHoldsOffAReplacement`: handoff
  fence.
- `TestRetentionKeepsSupersededGenerationsForTheGrace`,
  `TestRetireCountsFromTheReplacingCommit`: retention from the retire stamp.
- `TestRaiseHorizonFencesAStagedPublication`,
  `TestRaiseHorizonWaitsOutAPublicationInFlight`: the collector's fence and a late commit.
- `TestPlanCollection*`: the retained set, recovery after a deleted expired manifest, the
  missing retained manifest that stops a pass, and the corrupted or wrong retained or
  expired manifest that stops it too.
- `TestInitializeWritesOneUnpublishedRecord`: absent ≠ unpublished.

`TestDSQLCorpus` exports every statement to the DSQL lint gate.

`init-db`'s record step is tested in `cmd/tools` on the integration PostgreSQL:
`TestInitDBInitializesOnlySchemasThatSatisfyTheUnpublishedInvariant` (a flushed entry, or
a row without an entry, fails the run), `TestInitDBSkipsASchemaWhoseLockIsHeld`,
`TestInitDBRegistrationCommitsWithItsRecordOrNotAtAll`,
`TestInitDBRecoversALostRecordAboveTheEpochStaleProcessesPin` and
`TestInitDBRefusesALostRecordWhoseLifecycleEpochIsLostToo`.

The `cdc-flush` and `cdc-init` writers (#899) are tested in `internal/cdcv2` on the same
targets, as the `aurora-dsql` engine on the MiniStack lane (`run_suite cdcv2`). Every test
also checks Invariant B as a reader sees it (a row whose newest entry is flushed at `v` has
its newest copy in the head at `v`) and that every committed generation's manifest is still
the one its row names:

- `TestFlushFaultAtEveryBoundaryLeavesOnlyOrphans`, `TestInitFaultLeavesNothingVisible`: a
  fault at object staging, manifest upload, `BEGIN`, the compare-and-set, the marks or
  `COMMIT`, and an interrupted or partly staged export, leave the record, the history and
  `change_log` as they were.
- `TestFlushResolvesACommitWithoutAnAnswer`, `TestInitResolvesACommitWithoutAnAnswer`,
  `TestFlushResolvesACommitWhoseSessionEnded`, `TestFlushAbandonsAnOutcomeItCannotRecover`:
  `Committed`, `NotCommitted` (the same token sent again, no second manifest), a `COMMIT`
  that landed and was answered with a `FATAL` error whatever its code, a lost `COMMIT` that
  lands, with its marks, before its resend's snapshot or after it (the resend's miss or
  `40001` goes back to `Resolve`, never to a rebase or exhaustion, even on the last allowed
  transaction) and `Unknown`, and a handoff chunk whose answer was lost, whose entries the
  report counts as unconfirmed rather than marked.
- `TestFlushResendsASerializationFailureWithTheSameToken`,
  `TestFlushResendsAfterThePublishDeadline`, `TestFlushAbandonsAnExpiredStaging`: `40001`,
  `T_pub` and `W`.
- `TestConcurrentFlushersCommitOnePerParent`, `TestFlushStagedBeforeAnInitStartsOver`,
  `TestInitStagedBeforeAFlushIsAbandoned`, `TestWritersRebaseOverACompaction`,
  `TestWritersStagedBeforeADropNeverCommit`: the rebase table and epoch fencing, with real
  racing transactions.
- `TestFlushGenerationSize`: ten generations of 1,000 entries for a 10,000-entry DSQL batch.
  `TestNewBoundsAPostgreSQLBatchToOneMark`, `TestMarkBindsMaxMarkEntries`: a PostgreSQL batch
  is marked in one statement, so it is held to 32,766 entries, what 65,535 bind parameters
  allow.
- `TestInitLeavesRowsThatMovedDirty`, `TestReinitStopsTheReplacedLineagesHandoff`,
  `TestInitRetryIsIdempotent`, `TestInitOfAnEmptySchemaPublishesAnEmptyGeneration`: the
  export versions (a row written between the snapshot's version read and the exporter's
  content read is exported at the listed version and stays dirty at the new one), a fully
  shadowed export that stays recorded, re-initialization, and an empty export.

The compaction writer (#890) is tested in `internal/cdcv2` on the same targets, with the
same two checks after every test:

- `TestCompactPublishesOneGenerationFromItsInput`: one compact generation whose parent and
  `input_generation` are the generation read, holding each row's newest version, tombstones
  included, while the replaced generation's manifest and every object it lists stay readable.
- `TestCompactRebasesOverAFlush`, `TestCompactStagedBeforeACompactionOrAnInitStartsOver`,
  `TestCompactStagedBeforeADropNeverCommits`: the compaction row of the rebase table with real
  racing transactions; a refused rebase reads the head and merges again within its restart
  budget, or reports `ErrAbandoned` with the record as the rival left it.
- `TestAFlushStagedBeforeACompactionCannotResurrectADeletedRow`: the sequence the tombstone
  rule exists for. A stale flush rebases over a flush that published a deletion and a
  compaction that merged it, and the row stays deleted.
- `TestCompactFaultAtEveryBoundaryLeavesOnlyOrphans`, `TestCompactResolvesACommitWithoutAnAnswer`:
  a fault in the merge, at staging, `BEGIN`, the compare-and-set or `COMMIT` changes nothing
  visible, and a lost `COMMIT` answer is resent with the same token or found committed.

`internal/compaction` runs the policy (promotion, dirty-ratio rewrite, the #347 source
checksum gate) end to end on the integration PostgreSQL with DuckDB merges of real parquet,
and the production e2e harness runs every compaction scenario on V2.

The reader (#900) is tested in `internal/federatedv2` on the integration PostgreSQL, over
records, history rows and manifests committed as a V2 writer commits them, and against an
in-memory snapshot in `internal/federated`, where a fake reader stages each fault:

- `TestNeverPublishedSchemaIsAnsweredFromPostgreSQL`,
  `TestPublishedPassOfAnUnpublishedSchemaAnswersFromPostgreSQL`: an `UNPUBLISHED` schema
  serves offset and cursor pages from PostgreSQL, through an engine whose DuckDB is closed,
  with no manifest fetched, no object probed and the record as the snapshot's first read.
  `TestPublishedPassOfAnEmptyGenerationRunsNoDuckDB`: `files: []`.
- `TestAGenerationCommittedDuringAPassIsSeenByTheNextPassOnly`: a generation and its marks
  committed right after a pass read the record. That pass scans the generation its snapshot
  read and serves the newly flushed row hot. The next pass reads both, and serves it from
  parquet. `TestPublishedPassScansExactlyTheListedObjects`: an object beside the listed
  ones, which a glob would read, stays out of the answer.
- `TestAGenerationWithLostObjectsFailsTheRead`, `TestAReadFailsClosedOnItsRecordOrManifest`,
  `TestPublishedPassFailsClosedOnAPublicationFault`: #759's zero and partial match, an absent
  record, `AWAITING_INIT`, `DROPPED`, an epoch the process did not pin, and a missing or
  corrupt manifest fail with the typed error under `AllowPartialDegradedMode`. The
  degraded-fallback counter (#714) records nothing, and a breaker that opens on its first
  failure stays closed. `TestPublishedPassTellsAFailedReadFromAPublicationFault`: a record
  or manifest that could not be read degrades instead.
- `TestPublishedPassPastItsDeadlineIsAbortedAndRetried`,
  `TestPublishedPassUnderTheRequestDeadlineIsNotRetried`: `R_max` cancels the running scan
  and earns one retry on a fresh snapshot; the request's own deadline earns none.
  `TestAPassDeadlineIsNotChargedToTheBreakerAndNeverDegrades`,
  `TestARetryRefusedAfterAPassDeadlineKeepsTheDeadline`,
  `TestAProbeThatRunsOutOfTimeKeepsTheDeadline`: through `Query` under
  `AllowPartialDegradedMode`, with a breaker that opens on its first failure, the deadline
  stays non-degradable whether the retry runs out too or is refused, and whether the deadline
  ends the scan or the missing-object probe.
  `TestAPublicationFaultTheRetryMeetsIsNotAFailedRead`: after a scan `R_max` ended, a retry
  that meets an absent record, an invalid manifest or a lost object fails with the typed
  error, which is not `ErrFederatedReadFailed`, does not degrade and is not charged to the
  breaker.
- `TestAGenerationListingAnAddressDuckDBWouldExpandFailsClosed`,
  `TestPublishedPassRefusesAListedAddressDuckDBWouldExpand`: a layout prefix such as
  `da[t]a` passes manifest verification, and its keys would let DuckDB read an unlisted copy;
  the read fails closed with no DuckDB statement.
  `TestMissingInKeepsEveryConfirmedLossBesideAFailedProbe` and the mixed case of
  `TestPublishedPassFailsClosedOnAPublicationFault`: a confirmed loss beside a failed probe.
  `TestAMalformedRecordFailsClosedAndAFailedRecordReadDegrades`: a `PUBLISHED` row naming no
  manifest fails closed, while a record table PostgreSQL cannot read degrades.
- `TestPublishedPassWithoutAParquetLegNeedsNoDuckDBAdmission`,
  `TestPublishedPassUnderAnOpenBreaker`, `TestPublishedPassPlanReportsStagingOnlyWithAParquetLeg`:
  admission and the plan's staging claim follow the generation's objects.

**Not verified. Assumed until #817 checks a real cluster:**

- That `SELECT … FOR UPDATE` puts the record into DSQL's conflict check, so a handoff and
  a transition cannot both commit.
- That a compare-and-set race on DSQL fails one transaction with `40001` at `COMMIT`.
  MiniStack's backend waits instead, as PostgreSQL does.
- How many modified rows DSQL counts for an update of `flushed_at`, a primary-key column.
- What `now()` means in a DSQL transaction. `ReadAt` and `retired_at` assume it is the
  transaction's start on one cluster-wide clock.
- That 1,000 marks plus three rows commit within the budgets under production load.
- That `RaiseHorizon` and a publication, which write the same record row, never both
  commit unordered on DSQL. Whichever commits second must fail with `40001`.
  `TestRaiseHorizonWaitsOutAPublicationInFlight` skips on a cluster.
- That a DSQL transaction which begins after another transaction's snapshot ended reads a
  `now()` no earlier than every commit that snapshot saw. `Retire` relies on this.

## Ownership

Each component of the protocol was delivered by one child of #887:

| Area | What it does, and where | Child |
| --- | --- | --- |
| `init-db` table DDL | `Statements` for the configured `entity_main`, in init-db's table transaction. `init-db` creates the tables on PostgreSQL. On Aurora DSQL, #895 covers statement validity only: `make lint-dsql` and the MiniStack strict lane accept the DSQL rendering, but `init-db` cannot execute there, because it has no engine selection and sends its DDL in one transaction, which DSQL rejects. An executable DSQL `init-db` (one DDL statement per transaction, strict-lane fresh init and rerun) is #690's | #895 (DSQL execution: #690) |
| manifest objects | `internal/manifestv2`: the document and its deterministic bytes and digest, the `flush` and `init` file lists, the object keys, a write-once put (`If-None-Match: *`) on S3 and the same contract on a local directory, and a process-wide cache of manifests verified with `ManifestFiles` and the full-document checks. A missing or refused manifest is a `ConsistencyError` | #898 |
| `init-db` records | `ensurePublicationRecords` (`cmd/tools/init_db_records.go`), one step of `registerSchemas`' pgx transaction (`PgxTx`), for every registered schema: a record is kept; a `DROPPED` one is activated (its residue checked in `entity_main`, `eav_data`, `change_log` and the unique-key entry and build-state tables); a missing one is written under `pg_try_advisory_xact_lock` on the key `cdc-flush` and `cdc-init` take, `UNPUBLISHED` (`Initialize`) once the `UNPUBLISHED` invariant is checked, or by lost-record recovery (`InitializeAfterLoss`) when publication history survives. A schema whose rows break the invariant without history fails the run. A schema registered before the run whose lock is held keeps no record, is reported, and is retried by the next run. A schema the run registers commits with its record or not at all: a held lock, a record that is not `DROPPED`, or a broken invariant fails the run and rolls its registry row back. The drop (`Reset`, then the purge under `HoldDropped`) is `forma-tools drop-schema` (`cmd/tools/drop_schema.go`) | #896 |
| `cdc-flush` | Stage, then `Publish` + marks in one transaction; DSQL generations of ≤ 1,000 entries; `Resolve` on a lost commit. The publication loop is `internal/cdcv2`'s `Writer.Flush` (#899), behind a `DeltaStager`; `internal/cdc`'s `deltaStager` writes the DuckDB export to a local scratch file and then conditionally puts it under a fresh key, with the epoch the run loads from the record | #896 (built in #899) |
| `cdc-init` | Export versions, `init` generation, handoff under `HoldLineage`. `internal/cdcv2`'s `Writer.Init` (#899) reads each export snapshot's versions as its first statement and hands the exporter (`BaseExporter`) that snapshot; `internal/cdc`'s `baseExporter` reads the rows on it. A lost `COMMIT` is settled once in-process (`ResumeInit`). An empty export publishes an empty generation, never a placeholder object | #896 (built in #899), closing #760 |
| compactor | `Publish(KindCompact)` with `Rebase` over flushes, through `internal/cdcv2`'s `Writer.Compact` behind a `CompactStager`, which `internal/compaction` implements. The merge keeps every row's newest version, tombstones included; a promotion merges the delta files into a new base object, since a key names its tier. The compactor deletes nothing inline: superseded objects are retired by garbage collection | #890 |
| federated reader | `Read` in the pass snapshot, `Readable`, the epoch pin, manifest by key, `R_max` deadline: the pass behind `federated.WithPublicationReader` (`internal/federated/pgx_publication.go`) over its source in `internal/federatedv2`, with the pins loaded beside the schema metadata ([As built](#readers)) | #900, wired in #896, carrying #759 |
| `manifest-reconcile` | Lock-free diagnosis that verifies retained and expired manifests and checks completeness above `forgotten_generation`; `--gc` runs `RaiseHorizon`, `Retire`, `RetireRecovery`, `Forget` and `PlanCollection` in the collection order above, under storage binding (`BindStorageTx` in the writers' publication transactions, or `--bind-storage`); `--verify-checksums` walks retained generations (`internal/reconcile`, `cmd/tools/manifest_reconcile.go`). A damaged inventory is repaired by a re-init with `cdc-init` | #891 |
| fault matrix, operator docs, EPIC evidence | production-shape E2E lifecycle/fault/crash/concurrency matrix (`internal/e2e_harness/production`), DSQL `40001` and 3-way concurrency tests (`internal/cdcv2`), the static legacy-V1 guard (`internal/publication/legacy_v1_guard_test.go`), P95/cost deltas, operator doc audit, and the [evidence ledger](publication-protocol-v2-evidence.md) | #892 |
| live Aurora DSQL behaviour | the assumptions in [Verification](#verification) | #893, under #817 |

Sequencing: #888, then #895, then #896, then #890 (it uses the publication interface
#896 lands), then #891, then #892. #896 was built in #898 to #901: the manifest objects,
the flush and init writer and the reader source landed first with no caller (#898 to
#900), and the production e2e harness routed its publication access through helpers
(#901). #896 then switched registration, the writers and the reader over in one merge.

Once the consumers moved, a static test in `internal/publication`
(`TestOnlyTheProtocolOwnersMoveAPublicationRecord`) keeps the record's transitions with
their owners: outside the package, `Initialize`, `InitializeAfterLoss` and `Activate`
only in `cmd/tools/init_db_records.go`, `Publish`, `HoldLineage` and `Fence` only in
`internal/cdcv2`, `Reset` and `HoldDropped` only in `cmd/tools/drop_schema.go`,
`RaiseHorizon`, `Retire`, `RetireRecovery` and `Forget` only in `internal/reconcile`, and
`BindStorage` only in `cmd/tools/manifest_reconcile.go`. The one other non-test file allowed
`Initialize` and `Publish` is the federated e2e harness's `publication.go`, which registers
the benchmark's schemas and publishes its synthetic parquet, which has no PostgreSQL rows
for a writer to export; `cmd/benchmark` runs it outside any test. Test files are not scanned:
the production e2e harness writes its fixtures' records and stages fault generations from
test files. It refuses a dot import of the package and any production string literal that
spells a publication table name, so every statement over the tables is the package's own.
Reading a record is free.

**#759 is the reader scope, and lands in #896.** Its original evidence, a zero-match or
partial-match glob that returns 200 with silently incomplete data, cannot arise: the
reader scans only the keys its generation lists, with no glob discovery, no per-schema
fallback, no compatibility flag and no read-path repair. A `PUBLISHED` record
whose manifest is missing, fails the checks in step 4 of the reader protocol, or lists a
missing object fails the read as a read-path consistency error. #900 gives that error its
type, `federated.PublicationConsistencyError`; it does not degrade, is never relabelled `ErrFederatedReadFailed` and is never
charged to the DuckDB breaker. `UNPUBLISHED` answers from PostgreSQL without listing S3, so
#720's never-flushed behaviour holds. #759's tests cover its original zero-match and
partial-match loss alongside #720.

**#760 closes with #896.** #760's scope, the durable initialization record `design.md` asks
for, is the `init` generation and its history row, which #896 writes when it moves
`cdc-init` onto `Publish`. No second history table or per-query probe is added for it. #896
carries #760's init-specific regressions: idempotent retry, interrupted export, partial
staging, a lost pointer CAS, an unknown commit outcome, and re-onboarding an already
published schema. Its PR closes #760 alongside #896.

## Open questions

- Values for `T_pub`, `W`, `C`, `G` and `A`. `W` must cover the longest staging, which is a
  `cdc-init` export. `R_max` is a setting of its own, not the federated query timeout,
  because `DuckDBConfig.QueryTimeout` may be zero (no bound) and garbage collection needs
  one. #900 sets its default, `federated.DefaultPublishedPassDeadline`, to 60 s: twice the
  query budget's default, so under the default configuration the query budget expires
  first and `R_max` only bounds a deployment that raised or disabled it. `G` must then be
  at least 60 s + `C`. #896 does not expose `R_max` in the configuration: the factory
  passes the default, so `G` has one value to cover. Whether a deployment needs another is
  open.
- Whether the DSQL flush should shrink its batch to 1,000 rows, or keep 10,000 and
  publish ten generations per batch. Ten manifests per batch multiply manifest objects
  and history rows, and GC bounds both.
- Whether the purge after a drop belongs to a tool of its own, or to the drop.
- Whether the data plane should read the record's epoch in its write transactions, so
  that a process still holding a dropped schema's metadata cannot write into a
  re-created one. Until then that order is the drop runbook's.
