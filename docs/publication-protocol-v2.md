# Parquet publication protocol V2

Status: the contract #888 proposes for EPIC #887. The SQL and its semantics are in
`internal/publication`, with tests on PostgreSQL and on the MiniStack lane. The manifest
objects (document, digest, keys, write-once store, verifying cache) are in
`internal/manifestv2` (#898), and the `cdc-flush` and `cdc-init` publication paths built on
both are in `internal/cdcv2` (#899). Nothing in `cdc-flush`, `cdc-init`, the compactor, the
federated reader or `manifest-reconcile` uses any of them yet: the other #887 children move
those consumers onto them ([Ownership](#ownership-and-migration)).
Behaviour on a real Aurora DSQL cluster has not been checked. #817 tracks that, and
[Verification](#verification) lists what this contract assumes until then.

## Why

Under V1 a schema's parquet inventory is one mutable S3 object, `manifest.json`, which
writers rewrite under an ETag compare-and-set. The `change_log` flush marks that tell a
reader which rows the parquet tiers hold commit in PostgreSQL, in separate transactions
after the manifest write. Nothing ties the two stores together, so every reader and
writer carries workarounds for the gap:

- The reader resolves the manifest before it opens its PostgreSQL snapshot. To make up
  for that it widens the dirty barrier with a cutoff (#252), probes `change_log` for
  "parquet history" when a glob lists nothing (#720), resolves the source again after the
  pass, and retries.
- An empty manifest and a missing one both send reads to the per-schema glob. So
  `cdc-init` and the compactor write a zero-row object rather than publish an empty set
  (#519), and a lost manifest over live data reads as "never flushed".
- `cdc-init` leaves no record of its export in PostgreSQL (#760). Neither does the
  onboarding handoff that clears `change_log` after it.
- On DSQL the flush mark is sent as several transactions (#633), and one of them can
  commit while another fails.

V2 removes the gap rather than patching around it. PostgreSQL holds the only pointer.
Writers move it with compare-and-set, and in the same transaction they commit the marks
that depend on it. Readers choose the generation in the same snapshot as the dirty set.

## Decisions

1. **One durable record per schema in PostgreSQL** (`<entity_main>_publication`) names
   the schema's current immutable manifest generation. No other pointer exists. The
   record is created when the schema is registered, or by the V2 cutover, and is never
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
7. **No legacy compatibility.** V2 has no glob fallback, does not read V1
   `manifest.json`, and does not adopt V1 inventories ([Removed](#removed-from-v1)).

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
  inserts the record: epoch 1, `UNPUBLISHED`, generation 0. The insert uses
  `ON CONFLICT DO NOTHING`, so registering again never resets a schema, and never turns a
  `DROPPED` or `AWAITING_INIT` record into an unpublished one.
- *An absent record is an error* (`publication.ErrNoRecord`), never "unpublished". A
  reader that took absence for unpublished would answer from the hot tier alone and
  drop every row only the parquet tiers hold.
- *Awaiting init.* The V2 cutover writes `AWAITING_INIT` for a schema that has parquet
  history from V1 ([Upgrade](#removed-from-v1)). Readers fail closed on it
  (`ErrAwaitingInit`), and the only publication it admits is a `cdc-init`.
- *Drop.* The transaction that deletes the registry row runs `Reset`: epoch + 1,
  `DROPPED`, an empty manifest, a NULL lineage, parent 0 and a new token. The row stays.
  While it is `DROPPED` the schema's `entity_main`, `eav_data` and `change_log` rows are
  purged in chunks, and nothing reaches them through the record. A reader fails closed
  (`ErrDropped`). A publisher that reads the record after the drop stages nothing, and
  `Publish` refuses a `DROPPED` record in any case, so nothing exported from the rows
  being purged is published. A writer or handoff that read the record before the drop
  names the old epoch and token, and misses.
- *Re-registration.* When the id is registered again, the registration transaction runs
  `Activate` instead of `Initialize`. It checks, in that transaction, that no row of the
  id remains in `entity_main`, `eav_data` or `change_log` (`ErrResidue` otherwise), then
  moves the record to the next epoch, `UNPUBLISHED`, under a new token. Nothing read
  before activation carries that epoch.
- *Epoch pin.* A process reads each schema's epoch when it loads the schema's metadata
  and keeps it with them. A reader pass or a writer that later reads a record of another
  epoch fails closed: the schema was dropped, and maybe re-created, since the process
  loaded what it knows about it.
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
activation (no rows, checked in the same transaction), and for a schema the cutover
initialized without parquet history (checked under the schema lock with every V1 marker
stopped). Only two transactions mark entries: a publication, which leaves `UNPUBLISHED`
in the same commit, and a handoff, which requires `PUBLISHED`. So nothing breaks the
invariant. V2 forbids writes that bypass `change_log`, including the V1 handoff that
cleared it.

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

## Immutable manifest generations

**Object keys.**

- Manifest: `{prefix}/{schema_id}/_manifests/e{epoch}/g{generation:020d}-{token}.json`
- Parquet: `{prefix}/{schema_id}/e{epoch}/{base|delta}/{uuidv7}.parquet`

Every object is written once with a conditional put (`If-None-Match: *`), or copied from
`_tmp/` to a key nobody has used, and is never overwritten. Two writers that stage the
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
it does not know. The digest check covers what V1's schema stamp (#520, #522) covered,
and more. Every file field shown is required, and V2 has no footer-probe fallback for a
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
  init generation holds `r` at `v`, or
- (c) by a V1 writer before the cutover, in a schema the cutover left `AWAITING_INIT`,
  whose first V2 generation is therefore an `init`.

*Proof.* Only two V2 code paths mark. (a) In a publication transaction the compare-and-set
runs first. When it matches no row, `Publish` returns an error before any mark is sent,
and the caller rolls back. When it matches, pointer and marks commit or roll back
together, so every snapshot sees both or neither. (b) A handoff transaction's first
statement is `HoldLineage`, which reads the record with `SELECT … FOR UPDATE` filtered on
epoch, lineage and `PUBLISHED`. On PostgreSQL the row lock makes any transition of the
record wait until the handoff ends. `TestHoldLineageHoldsOffAReplacement` shows the wait.
On DSQL, `FOR UPDATE` puts the row into the transaction's conflict check, so the handoff
and a concurrent transition cannot both commit. That part is assumed until #817 checks it.
In either case, when the handoff commits, the record still carries `L`. (c) The cutover
runs with every V1 marker stopped, and writes `UNPUBLISHED` only where `change_log` held
no flushed entry, so every V1 mark is in an `AWAITING_INIT` schema. `Publish` admits only
an `init` from that state. ∎

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
  conflict check covers it. A resend refused with `ErrStagingExpired` abandons its
  staging, and the orphan rule reclaims it.
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

A listed key that is missing is loss, as under V1's one-directional contract. The read
fails loudly and classifies, and GC never makes this happen within the retention window.

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
holds it, and descendants keep it. In case (c) of Invariant B the first V2 generation is
an init, which exports every live row, including those V1 flushed or left without an
entry. A row deleted before S has a deletion entry, which is either dirty or flushed. A
flushed deletion has a tombstone that LWW applies. Alternatively an init after the
deletion holds no copy of the row, and no later generation adds one: a flush exports only
rows with an unflushed entry, and a flush staged before that init cannot rebase over it.
In an `UNPUBLISHED` S, the invariant puts every live row in the hot leg. ∎

This replaces the cutoff widening (#252), the parquet-history probe (#720), the
re-resolution after the pass, and the path set that was frozen before the snapshot.

### Why no mutable root

A mutable S3 root (a V1-style `manifest.json` pointing at the newest generation) adds a
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
7. When step 6 has finished, `Forget` each expired history row. A row that outlives a
   crash makes the next pass repeat the deletes instead of leaking them.

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
| Cutover, no history | no row | history check + `Initialize`, under the schema lock | e1, `UNPUBLISHED`, g0 | — | V1 writers stopped first |
| Cutover, history | no row | history check + `InitializeAwaitingInit`, under the schema lock | e1, `AWAITING_INIT`, g0 | V1 marks stay | readers fail closed until the init |
| Never published (read) | `UNPUBLISHED` | — | — | all entries dirty | hot leg only, no DuckDB |
| First flush | `UNPUBLISHED`, g | CAS → g+1 `flush`, new lineage, parent 0, + marks | `PUBLISHED` g+1 | batch marked at listed versions | stage delta + manifest first. CAS miss → rebase. Any failure before COMMIT → nothing visible |
| Repeated flush | `PUBLISHED` g | CAS → g+1 `flush`, same lineage, + marks (≤ 1,000 on DSQL) | g+1 | as above | as above. `40001` → resend the same publication. Head moved by an init → abandon, select again |
| Init (onboarding) | `UNPUBLISHED`, `AWAITING_INIT` or `PUBLISHED` g | CAS → g+1 `init`, new lineage, no marks | `PUBLISHED` g+1 | none in this transaction | export, then manifest. CAS miss → rebase over compactions only |
| Init handoff | `PUBLISHED`, lineage L | `HoldLineage(L)` + ≤ 1,000 marks, repeated | unchanged | exported entries marked at export versions | `ErrLineageReplaced` → stop. Unmarked rows stay dirty |
| Re-init | `PUBLISHED` g, lineage L | as init, new lineage L′ | g+1 | none | the old lineage's handoff stops. Old objects expire after `G` |
| Compaction | `PUBLISHED` g | CAS → g+1 `compact`, same lineage | g+1 | none | merge outputs + manifest first. Rebase over flushes only |
| Drop | any but `DROPPED` | `Reset` with the registry delete | e+1, `DROPPED`, no current generation | purged afterwards, in chunks | readers fail closed. Every CAS/hold misses or is refused. Old objects expire after `G` |
| Re-create | e+1 `DROPPED` | residue check + `Activate` with the registry insert | e+2, `UNPUBLISHED`, new token | — | `ErrResidue` while the id still has rows |
| Failed publication | g | rolled back | g | unchanged | objects unreferenced. Orphan rule reclaims them |
| Commit outcome unknown | g | `Resolve` in a new transaction | g or g+1 | with the pointer | `NotCommitted` → resend the same publication. `Lost` → rebase. `Unknown` → abandon, start over from the head |
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

## Removed from V1

V2 has no compatibility path. These go, in the child that moves each consumer:

- The per-schema glob fallback (`QuerySource.Fallback`) and every glob listing on the
  read path.
- The mutable `manifest.json`, its ETag compare-and-set, `manifest.AppendFile`,
  `ReplaceTierFiles` and the ambiguous-save confirmation of `cdc-init`.
- The zero-stamp and foreign-stamp rules. The record and the digest replace them.
- The `#474` unlisted-flush probe.
- The flush mark sent as several DSQL transactions.
- The cutoff, the parquet-history probe and the re-resolution described under
  [Readers](#readers).
- Clearing `change_log` at onboarding.
- `manifest-reconcile --repair`'s re-adoption of unlisted objects. Under V2 an unlisted
  object is either staged or garbage, never lost data, so repair is a re-init.

**Upgrade.** A one-time cutover creates every record without reading V1 manifests. V2
cannot fence code that predates it, so the cutover runs in an order that keeps V1
writers and their marks out:

1. Stop every V1 writer: `cdc-flush`, `cdc-init`, the compactor and
   `manifest-reconcile`. Their V2 replacements refuse a schema without a record
   (`ErrNoRecord`), so a V2 writer that starts early does nothing.
2. For each registered schema, take the schema lock that every V1 marker (`cdc-flush`
   and the onboarding handoff) takes. In one transaction, run the parquet-history check
   (today's `parquetHistoryQuery`: a flushed entry, or a live row without an entry) and
   insert the record. With no history the schema satisfies the `UNPUBLISHED` invariant,
   and the insert is `Initialize`. With history it is `InitializeAwaitingInit`. If the
   cutover cannot take the lock, a V1 writer is still running, and the schema is retried
   later.
3. Replace the readers. Until step 2, a V1 reader serves V1's state, which nothing
   changes any more, and a V2 reader fails closed on `ErrNoRecord`. After step 2 a V2
   reader answers from the hot leg for an `UNPUBLISHED` schema and fails closed on an
   `AWAITING_INIT` one. V1 readers must be gone before step 4, because they would miss
   rows that V2 marks.
4. Start the V2 writers, and run a V2 `cdc-init` for each `AWAITING_INIT` schema. Until
   that init publishes, flushes and compactions of the schema are refused. The init's
   export replaces whatever V1 wrote.

A reader therefore sees either a fail-closed state (no record, or `AWAITING_INIT`), an
`UNPUBLISHED` record whose invariant was checked with no marker running, or a V2
generation. `TestAwaitingInitAdmitsOnlyACDCInit` pins the state. A schema with history
is out for reads until its init publishes, which is a read outage per schema sized by
the export. Leaving such a schema without a record would not work: an init can only
compare-and-set an existing record, and a separate `Initialize` would expose
`UNPUBLISHED` over flushed rows. Adopting a V1 manifest as generation 1 was rejected,
because nothing proves its relation to the committed marks, which is the gap V2 exists
to close. A V1 writer binary started after the cutover is not fenced: its marks in an
`UNPUBLISHED` schema would break the invariant. Retiring V1 writers is a precondition
of the deployment that runs the cutover.

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
- `TestAwaitingInitAdmitsOnlyACDCInit`: the cutover state.
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

## Ownership and migration

This PR is the contract. Each consumer moves in one child of #887, and each child removes
the V1 code it replaces:

| Area | Moves to | Child |
| --- | --- | --- |
| `init-db` table DDL | `Statements` for the configured `entity_main`, in init-db's table transaction. `init-db` creates the tables on PostgreSQL. On Aurora DSQL, #895 covers statement validity only: `make lint-dsql` and the MiniStack strict lane accept the DSQL rendering, but `init-db` cannot execute there, because it has no engine selection and sends its DDL in one transaction, which DSQL rejects. An executable DSQL `init-db` (one DDL statement per transaction, strict-lane fresh init, upgrade and rerun) is #690's. The tables only: nothing inserts, reads or updates them | #895 (DSQL execution: #690) |
| V2 manifest objects | `internal/manifestv2`: the document and its deterministic bytes and digest, the `flush` and `init` file lists, the object keys, a write-once put (`If-None-Match: *`) on S3 and the same contract on a local directory, and a process-wide cache of manifests verified with `ManifestFiles` and the full-document checks. A missing or refused manifest is a `ConsistencyError`. Nothing in production imports it until #896 | #898 |
| `init-db` registration, the upgrade cutover | `Initialize`, or `Activate` for a dropped id, in `registerSchemas`' pgx transaction (`PgxTx`). The cutover's history check and `InitializeAwaitingInit` under the schema lock | #896 (writers and reader, built in #899 to #901) |
| `cdc-flush` (`flush_batch.go`, `manifest.go`, `flush_manifest_guard.go`, `mark_flushed_dsql.go`) | stage, then `Publish` + marks in one transaction. DSQL generations of ≤ 1,000 entries. `Resolve` on a lost commit. Removes `--glob-only` and the manifest-disabled publication path. The publication loop is `internal/cdcv2`'s `Writer.Flush` (#899), behind a `DeltaStager` that writes the parquet; #896 adapts the DuckDB export to it and calls it | #896 (writers and reader, built in #899 to #901) |
| `cdc-init` (`init_publish.go`, export batches) | export versions, `init` generation, handoff under `HoldLineage`. `internal/cdcv2`'s `Writer.Init` (#899) reads each export snapshot's versions as its first statement and hands the exporter (`BaseExporter`) that snapshot; #896 adapts the DuckDB export to it and calls it | #896 (writers and reader, built in #899 to #901), closing #760 |
| compactor (`compactor.go`, `provider.go`) | `Publish(KindCompact)` with `Rebase` over flushes. No inline deletion of replaced objects; their retirement belongs to #891 | #890 (compaction) |
| federated reader (`internal/manifest/query_source*.go`, `internal/federated` path resolution, `parquet_empty_scan.go`, `change_log_reads.go` history probe) | `Read` in the pass snapshot, `Readable`, the epoch pin, manifest by key, `R_max` deadline. `docs/federated-query/design.md` §6.1 is rewritten here | #896 (writers and reader, built in #899 to #901), carrying #759 |
| `manifest-reconcile` `--gc`, `--repair`, verification | `RaiseHorizon`, `Retire` and `PlanCollection` in the collection order above. Repair becomes re-init. Verification walks retained generations | #891 (GC) |
| fault matrix, operator docs, EPIC evidence | crash injection at every staging, publish and commit boundary; a guard that fails if `QuerySource.Fallback`, the level-3 glob or a glob-only flag returns | #892 |
| live Aurora DSQL behaviour | the assumptions in [Verification](#verification) | #893, under #817 |

Sequencing: #888, then #895, then #896, then #890 (it uses the publication interface
#896 lands), then #891, then #892. #896 is built in #898 to #901. The parts nothing calls
land first and stay dormant: #898 the manifest objects, and the flush writer, init and
reader slices that build on them (#899, #900). The writers and the reader still switch in
one merge. #889's writer scope is split in two because the
writers cannot land on `main` ahead of the reader. A V2 flush stages its objects under
`e{epoch}/`, stops appending to `manifest.json` and marks its rows flushed in the
transaction of its `Publish`, and today's reader reads neither the record nor `e{epoch}/`,
so every row such a flush marks would disappear from query answers. Registration and the
cutover cannot go first either: a record written while V1 `cdc-flush` still runs goes
stale, because V1 keeps flushing into a schema whose record says `UNPUBLISHED` and the
cutover only inserts records for schemas that have none, and the V2 reader would then
answer that schema from the hot leg alone. So #896 moves the writers and the reader in one
change. Ahead of it land only the table DDL (#895), which nothing reads or writes, and
code that nothing calls (#898 to #900). Until
#896, two checks hold that line, and neither alone proves it. A static test in
`internal/publication` checks imports and literals: outside the package, only init-db's
DDL step and its collation check may import it, only for `Statements`, `TablesFor` and
the table-name suffixes, never through a dot import, and no production file may spell a
publication table name in a string literal. It does not inspect the SQL those files
build, so an allowlisted file could still assemble a record statement from a name
`TablesFor` returns. What shows that init-db sends none is the traced SQL of its fresh,
upgrade and rerun runs in `TestInitDBCreatesThePublicationTablesOnceAndWritesNoRecord`.
`internal/manifestv2` and `internal/cdcv2` may use all of `internal/publication`, and the
same static test refuses a production import of either from any file outside the two, so no
binary reaches the manifest or the writer code before #896 either.

**#759 is the reader scope, and lands in #896.** Its original evidence, a zero-match or
partial-match glob that returns 200 with silently incomplete data, is what the reader
cutover must close. #759 deletes `QuerySource.Fallback`, the level-3 glob discovery and the config and read branches
that reach them, with no compatibility flag and no read-path repair. A `PUBLISHED` record
whose manifest is missing, fails the checks in step 4 of the reader protocol, or lists a
missing object fails the read as a read-path consistency error. #759 gives that error its
type; it does not degrade, is never relabelled `ErrFederatedReadFailed` and is never
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

- Values for `R_max`, `T_pub`, `W`, `C`, `G` and `A`, and whether `R_max` is the
  federated query timeout or a separate setting. `W` must cover the longest staging,
  which is a `cdc-init` export.
- Whether the DSQL flush should shrink its batch to 1,000 rows, or keep 10,000 and
  publish ten generations per batch. Ten manifests per batch multiply manifest objects
  and history rows, and GC bounds both.
- Whether the purge after a drop belongs to a tool of its own, or to the drop.
- Whether the data plane should read the record's epoch in its write transactions, so
  that a process still holding a dropped schema's metadata cannot write into a
  re-created one. Until then that order is the drop runbook's.
