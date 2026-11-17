# Parquet publication protocol V2

Status: the contract #888 proposes for EPIC #887. The SQL and its semantics are in
`internal/publication`, with tests on PostgreSQL and on the MiniStack lane. Nothing in
`cdc-flush`, `cdc-init`, the compactor, the federated reader or `manifest-reconcile` uses
it yet: the other #887 children move them onto it ([Ownership](#ownership-and-migration)).
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
   record is created when the schema is registered and is never deleted.
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
6. **Retention is time-based on the database clock.** Each generation's history row
   records when it was superseded. A reader has a hard scan deadline. GC deletes nothing
   a generation superseded less than a grace ago lists.
7. **No legacy compatibility.** V2 has no glob fallback, does not read V1
   `manifest.json`, and does not adopt V1 inventories ([Removed](#removed-from-v1)).

## The publication record

```sql
CREATE TABLE <entity_main>_publication (
  schema_id         SMALLINT NOT NULL,
  epoch             BIGINT   NOT NULL,   -- lifecycle epoch; drop increments it
  state             TEXT     NOT NULL,   -- 'UNPUBLISHED' | 'PUBLISHED'
  generation        BIGINT   NOT NULL,   -- 0 until first publication; only increases, across epochs too
  parent_generation BIGINT   NOT NULL,
  manifest_key      TEXT     NOT NULL,   -- '' exactly when UNPUBLISHED
  manifest_sha256   TEXT     NOT NULL,   -- '' exactly when UNPUBLISHED
  lineage           UUID,                -- NULL exactly when UNPUBLISHED
  cas_token         UUID     NOT NULL,   -- changes on every transition
  updated_at        BIGINT   NOT NULL,   -- database clock, epoch ms
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
- **`generation`** is the commit counter. It is never reset, not even by a drop, so a
  generation number is never reused and neither is a manifest key built from it.
- **`cas_token`** is minted by the transaction that writes the row. It is also the
  committed generation's identity, which `Resolve` uses after a lost commit answer.
  Being random, it rules out an ABA match even when epoch and generation compare equal.
- **`lineage`** identifies the content line one `cdc-init` export started (or the first
  flush of an unpublished schema). Flush and compaction keep it. The `cdc-init` handoff
  is fenced on it.

**Lifecycle of the row.**

- *Initialize.* The registration transaction that inserts the schema's registry row also
  inserts the record: epoch 1, `UNPUBLISHED`, generation 0. The insert uses
  `ON CONFLICT DO NOTHING`, so registering again never resets a published schema.
- *An absent record is an error* (`publication.ErrNoRecord`), never "unpublished". A
  reader that took absence for unpublished would answer from the hot tier alone and
  drop every row only the parquet tiers hold.
- *Reset (drop).* The transaction that deletes the registry row increments the epoch and
  sets `UNPUBLISHED`, an empty manifest, a NULL lineage and a new token. The row stays,
  so a re-created schema that reuses the id continues from the next epoch. The id may be
  registered again only once the schema's `entity_main`, `eav_data` and `change_log`
  rows are purged, because `UNPUBLISHED` promises that every live row has an unflushed
  entry.
- *Re-onboarding* an existing schema is a `cdc-init` (a new lineage in the same epoch).
  It never resets, for the same reason: a reset would hide every flushed row.

**The `UNPUBLISHED` invariant.** In an `UNPUBLISHED` epoch, every `change_log` entry of the
schema is unflushed and every live row has one. It holds at registration (no rows) and
after drop, purge and re-registration. Only two transactions mark entries: a
publication, which leaves `UNPUBLISHED` in the same commit, and a handoff, which requires
`PUBLISHED`. So nothing breaks the invariant. V2 forbids writes that bypass `change_log`,
including the V1 handoff that cleared it.

### Generation history

```sql
CREATE TABLE <entity_main>_publication_generation (
  schema_id, generation  -- PRIMARY KEY
  epoch, kind,           -- kind: 'flush' | 'compact' | 'init'
  parent_generation, manifest_key, manifest_sha256, lineage, cas_token,
  committed_at  BIGINT NOT NULL,           -- database clock of the committing transaction
  superseded_at BIGINT NOT NULL DEFAULT 0  -- 0 while current
)
```

The publication transaction inserts the new generation's row and stamps the replaced
generation's `superseded_at`. A reset stamps it too. Within one epoch, generations are
consecutive. Rebase decisions read `kind` (`KindsSince`), retention reads
`superseded_at`, and `Resolve` reads `cas_token`.

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
  "lineage": "…", "token": "…", "kind": "flush",
  "files": [
    {"tier": "base", "key": "data/100/e1/base/0190….parquet", "size_bytes": 1048576,
     "sha256": "…", "row_count": 5000, "row_id_min": "…", "row_id_max": "…",
     "columns": {"ltbase_row_id": "UUID", "…": "…"}}
  ]
}
```

`files` is sorted by `key`. The record's `manifest_sha256` is the digest of the exact
stored bytes. A reader refuses a manifest whose digest differs, whose identity fields
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

| Kind | Generation = | Parent |
| --- | --- | --- |
| `flush` | parent's files + the new delta files | head |
| `compact` | parent's files − the merged sources + the merge outputs | the generation the compactor read |
| `init` | the export's base files only. All delta and earlier base files leave the inventory | head |

## The commit boundary

```text
-- staging, before BEGIN: parquet objects, then the manifest object (conditional put)
BEGIN;
  UPDATE publication SET ... generation = generation + 1 ...          -- compare-and-set, first
   WHERE schema_id = $1 AND epoch = $2 AND generation = $3 AND cas_token = $4
     AND $staged_at >= now_ms - $budget
   RETURNING generation;                                              -- 0 rows: stop, ROLLBACK
  UPDATE publication_generation SET superseded_at = now_ms WHERE ... -- the replaced generation
  INSERT INTO publication_generation (...);                           -- the new generation
  <flush mark of the generation's rows at their listed versions>      -- same transaction
COMMIT;
```

`publication.Publish` runs the first three statements, and the caller sends the marks on
the same transaction. `cdc-flush` keeps its exact-version marks. On PostgreSQL that is
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

*Proof.* Only two code paths mark. (a) In a publication transaction the compare-and-set
runs first. When it matches no row, `Publish` returns an error before any mark is sent,
and the caller rolls back. When it matches, pointer and marks commit or roll back
together, so every snapshot sees both or neither. (b) A handoff transaction's first
statement is `HoldLineage`, which reads the record with `SELECT … FOR UPDATE` filtered on
epoch, lineage and `PUBLISHED`. On PostgreSQL the row lock makes any transition of the
record wait until the handoff ends. `TestHoldLineageHoldsOffAReplacement` shows the wait.
On DSQL, `FOR UPDATE` puts the row into the transaction's conflict check, so the handoff
and a concurrent transition cannot both commit. That part is assumed until #817 checks it.
In either case, when the handoff commits, the record still carries `L`. ∎

**DSQL budgets.** DSQL caps a transaction at 3,000 modified rows, 10 MiB and five
minutes. A publication modifies 3 rows of its own plus its marks. So on DSQL a flush
generation holds at most 1,000 entries (today's `markRowsPerStatement`), and the default
10,000-row batch publishes as ten generations. Even if updating `flushed_at`, a
primary-key column, counted as two modified rows, the total would be 2,003. Nobody has
measured this, and #817 records it. The transaction does no S3 I/O, so its duration is
the statements alone. The writer bounds it with a context deadline `T_pub` (proposed
default 30 s, well under DSQL's five minutes). GC relies on that deadline. On PostgreSQL a
generation keeps today's batch size, because the mark already commits in one statement.

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
| flush | rebase | rebase | rebase | refuse |
| compact | rebase | refuse | refuse | refuse |
| init | refuse | rebase | refuse | refuse |

The rules follow from what each kind changes:

- **Flush.** A flush appends rows exported at exact versions, which are correct over any
  head of the same epoch. A row that moved on keeps its newer entry unflushed, so the
  flush's mark skips it.
- **Compaction.** A compaction may only rebase over appends. Another compaction or an
  init may have removed or replaced a file it merged.
- **Init.** An init replaces content with an export. A compaction in between changed no
  content. A flush in between appended rows the export may predate, and replacing them
  would leave their entries marked over missing data.

A rebased writer builds a new manifest object from the head's files under a fresh token.
It keeps its parquet objects, its marks and its original `staged_at`. A refused writer
abandons its staging.

**Epoch fencing.** A reset changes `epoch`, so every compare-and-set and every
`HoldLineage` staged under the old epoch misses. `TestResetFencesEveryWriterOfTheOldEpoch`
pins this.

**Commit outcome unknown.** When `COMMIT` fails without an answer, the writer calls
`Resolve(generation, token)` in a new transaction:

- `Committed`: done.
- `Lost`: another token holds the number, so this writer's transaction can never commit.
  It rebases.
- `NotCommitted`: the writer sends the *same* publication again, with the same
  expectation and token. If the lost commit lands first, the resend loses the
  compare-and-set and `Resolve` then answers `Committed`. On PostgreSQL the resend waits
  on the original's row lock, so the two never both commit. On DSQL the conflict check
  covers it.

`TestResolveAfterALostCommitAnswer` pins the sequence.

**Idempotent restart.** A writer that crashed left either nothing in PostgreSQL (its
transaction rolled back) or a committed generation with its marks. The restarted writer
selects entries `flushed_at = 0` again and stages fresh keys. Rows of a rolled-back
attempt are re-exported. A duplicate `(row_id, version)` across two committed generations
can only arise from a rebased resend, and LWW deduplicates it as today.

**Staging budget.** `staged_at` is the database time the writer read before its first S3
write for the generation. The compare-and-set refuses a commit once
`now − staged_at > W` (`ErrStagingExpired`). The test compares database clock with
database clock, so the orphan rule below can rely on it.

## Readers

1. Record `t0` on the local monotonic clock. `BEGIN REPEATABLE READ READ ONLY` (the
   snapshot S). As S's first statement, `publication.Read` returns the record and
   `ReadAt`, the transaction's database time.
2. Absent: fail the read as a read-path consistency error. `UNPUBLISHED`: answer from
   the hot leg alone, with no DuckDB statement. `PUBLISHED`: continue.
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

- A flush appends files only.
- A compaction keeps each row's newest version, tombstones included. That is today's LWW merge, and the compaction child must keep it.
- An init re-exports every live row, and `r`, unchanged since `v`, exports as `r@v`.

A newer version of `r` in any file would mean a newer entry, contradicting the choice of
`(r, v)` as the latest. A row without an entry can only come from an init export, which
holds it, and descendants keep it. A row deleted before S has a deletion entry: dirty, or
flushed as a tombstone that LWW applies. In an `UNPUBLISHED` S, the invariant puts every
live row in the hot leg. ∎

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

The protocol uses four time parameters:

- `R_max`: the reader's pass deadline.
- `T_pub`: the publication transaction deadline.
- `W`: the staging budget.
- `C`: a clock margin covering the drift between the local monotonic clock, the database
  clock and S3's `LastModified`.

Proposed defaults are for the child issues to fix. The two rules below hold for any
values:

- **Retention grace `G ≥ R_max + T_pub + C`.** Let a reader choose generation g in a
  snapshot whose database time is `ReadAt`. The transaction that superseded g committed
  after the snapshot began, and its `now()` (transaction start) is at least
  `ReadAt − T_pub`. So `superseded_at + G ≥ ReadAt + R_max + C`, which is later than the
  reader's deadline. GC deletes nothing that a generation retained under `G` lists
  (`publication.Retained`).
- **Orphan age `> W + T_pub + C`.** An object no history row lists may be deleted once
  its `LastModified` is older than this. A writer stages after reading `staged_at` and
  commits by `staged_at + W + T_pub`. After that point the object can never become
  referenced. A rebase keeps the original `staged_at` for this reason.

**Collection order** (each step repeats safely after a crash):

1. List objects under the schema prefix.
2. Read `Retained(G)` and `Expired(G)` in one snapshot, after the listing, so a
   generation committed during the listing is retained. Read their manifests to build
   the referenced set.
3. Delete the listed objects that no retained generation lists and that are either
   listed by an expired generation or older than the orphan age.
4. Delete the expired generations' manifest objects.
5. `Forget` each expired history row, last. A row that outlives a crash makes the next
   run delete its objects again, instead of leaking them.

A writer can never commit a reference to an object GC deleted:

- Objects of the head are retained.
- A writer that composed its manifest from an older head fails the compare-and-set.
- An object the writer staged itself is younger than the orphan age whenever its commit
  can still land.

`TestRetentionKeepsSupersededGenerationsForTheGrace` pins the queries.

## State transitions

| Event | From | One PostgreSQL transaction | To | `change_log` | S3 / failure |
| --- | --- | --- | --- | --- | --- |
| Register schema | no row | `Initialize` with the registry insert | e1, `UNPUBLISHED`, g0 | — | none |
| Never published (read) | `UNPUBLISHED` | — | — | all entries dirty | hot leg only, no DuckDB |
| First flush | `UNPUBLISHED`, g | CAS → g+1 `flush`, new lineage, + marks | `PUBLISHED` g+1 | batch marked at listed versions | stage delta + manifest first. CAS miss → rebase. Any failure before COMMIT → nothing visible |
| Repeated flush | `PUBLISHED` g | CAS → g+1 `flush`, same lineage, + marks (≤ 1,000 on DSQL) | g+1 | as above | as above. `40001` → resend the same publication |
| Init (onboarding) | `UNPUBLISHED` or `PUBLISHED` g | CAS → g+1 `init`, new lineage, no marks | g+1 | none in this transaction | export, then manifest. CAS miss → rebase over compactions only |
| Init handoff | `PUBLISHED`, lineage L | `HoldLineage(L)` + ≤ 1,000 marks, repeated | unchanged | exported entries marked at export versions | `ErrLineageReplaced` → stop. Unmarked rows stay dirty |
| Re-init | `PUBLISHED` g, lineage L | as init, new lineage L′ | g+1 | none | the old lineage's handoff stops. Old objects expire after `G` |
| Compaction | `PUBLISHED` g | CAS → g+1 `compact`, same lineage | g+1 | none | merge outputs + manifest first. Rebase over flushes only |
| Drop | any | `Reset` with the registry delete | e+1, `UNPUBLISHED`, g kept | purged afterwards, in chunks | every old-epoch CAS/hold misses. Old objects expire after `G` |
| Re-create | e+1 `UNPUBLISHED`, no rows left | registry insert. `Initialize` is a no-op | unchanged | — | refused while the id still has rows |
| Failed publication | g | rolled back | g | unchanged | objects unreferenced. Orphan rule reclaims them |
| Commit outcome unknown | g | `Resolve` in a new transaction | g or g+1 | with the pointer | `NotCommitted` → resend the same publication. `Lost` → rebase |
| Rollback to an older generation | — | not a transition | — | — | Refused by design: marks committed by later generations would point at missing rows. Re-init rebuilds from PostgreSQL instead |
| Orphan cleanup | any | `Retained`/`Expired`, then `Forget` | unchanged | — | see collection order |

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

**Upgrade.** A one-time cutover creates records without reading V1 manifests. A schema
for which PostgreSQL records no parquet history (today's `parquetHistoryQuery` is false:
no flushed entry, and no live row without an entry) satisfies the `UNPUBLISHED`
invariant and is initialized. Every other schema is left without a record, so its reads
fail (`ErrNoRecord`) until a V2 `cdc-init` re-onboards it. That is a read outage per
schema, sized by the export. Adopting a V1 manifest as generation 1 was rejected, because
nothing proves its relation to the committed marks, which is the gap V2 exists to close.

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
- `TestRebaseFollowsTheKindsBetween`, `TestRebaseTable`: rebase rules, and the history gap.
- `TestResetFencesEveryWriterOfTheOldEpoch`: epoch fencing, and no generation reuse.
- `TestResolveAfterALostCommitAnswer`: outcome unknown.
- `TestHoldLineageFencesTheHandoff`, `TestHoldLineageHoldsOffAReplacement`: handoff
  fence.
- `TestRetentionKeepsSupersededGenerationsForTheGrace`: GC queries.
- `TestInitializeWritesOneUnpublishedRecord`: absent ≠ unpublished.

`TestDSQLCorpus` exports every statement to the DSQL lint gate.

**Not verified. Assumed until #817 checks a real cluster:**

- That `SELECT … FOR UPDATE` puts the record into DSQL's conflict check, so a handoff and
  a transition cannot both commit.
- That a compare-and-set race on DSQL fails one transaction with `40001` at `COMMIT`.
  MiniStack's backend waits instead, as PostgreSQL does.
- How many modified rows DSQL counts for an update of `flushed_at`, a primary-key column.
- What `now()` means in a DSQL transaction. `ReadAt` and `superseded_at` assume it is the
  transaction's start on one cluster-wide clock.
- That 1,000 marks plus three rows commit within the budgets under production load.

## Ownership and migration

This PR is the contract. Each consumer moves in one child of #887, and each child removes
the V1 code it replaces:

| Area | Moves to | Child |
| --- | --- | --- |
| `init-db` registration, table DDL, the upgrade cutover | `Statements`, `Initialize` in `registerSchemas`' transaction | writer |
| `cdc-flush` (`flush_batch.go`, `manifest.go`, `flush_manifest_guard.go`, `mark_flushed_dsql.go`) | stage, then `Publish` + marks in one transaction. DSQL generations of ≤ 1,000 entries. `Resolve` on a lost commit | writer |
| `cdc-init` (`init_publish.go`, export batches) | export versions, `init` generation, handoff under `HoldLineage`. This closes #760's durable initialization record | writer |
| compactor (`compactor.go`, `provider.go`) | `Publish(KindCompact)` with `Rebase` over flushes | compaction |
| federated reader (`internal/manifest/query_source*.go`, `internal/federated` path resolution, `parquet_empty_scan.go`, `change_log_reads.go` history probe) | `Read` in the pass snapshot, manifest by key, `R_max` deadline. `docs/federated-query/design.md` §6.1 is rewritten here | reader |
| `manifest-reconcile` `--gc`, `--repair`, verification | collection order above. Repair becomes re-init. Verification walks retained generations | GC |

#888 names #759 alongside #760 as work to place. #760's scope, the durable
initialization record `design.md` asks for, is the `init` generation and its history row.
This document does not restate #759's scope, so where it lands is for review to decide
against the table above.

## Open questions

- Values for `R_max`, `T_pub`, `W`, `C` and `G`, and whether `R_max` is the federated
  query timeout or a separate setting.
- Whether the DSQL flush should shrink its batch to 1,000 rows, or keep 10,000 and
  publish ten generations per batch. Ten manifests per batch multiply manifest objects
  and history rows, and GC bounds both.
- Whether the purge after a drop belongs to a tool of its own, or to the drop.
