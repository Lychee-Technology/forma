# manifest-reconcile: diagnosis and garbage collection under Protocol V2

`forma-tools manifest-reconcile` checks each schema's parquet publication and,
with `--gc`, deletes what no reader can still scan. Since #891 it works on
Protocol V2 ([`publication-protocol-v2.md`](publication-protocol-v2.md)): the
schema's publication record in PostgreSQL, the generation history, and the
immutable manifest objects those name. It reads no V1 `manifest.json`, and it
has no repair.

## Why it changed

Under V1 a schema's inventory was one mutable `manifest.json`, so the tool
diffed an S3 listing against it and repaired what looked missing: it re-adopted
unlisted delta objects and promoted unlisted init exports, behind coverage and
liveness probes that had to tell lost data from compaction leftovers, and a
mis-pointed manifest template made the whole base tier look orphaned (#463).

Under V2 none of that inference is needed or safe:

- An object no committed generation lists is staging that never committed, or
  a generation's leftovers. It is never lost data, so there is nothing to adopt.
- What readers may scan is exact: the current generation and every superseded
  one still inside its retention grace. Nothing has to be guessed from object
  names.
- A manifest or an object a committed generation lists cannot be rebuilt from
  what S3 still holds without trusting it blindly. The recovery is a re-init
  (`cdc-init`), which exports every live row from PostgreSQL into a new
  generation.

Compaction (#890) deletes nothing when it publishes, so every compaction leaves
the replaced generation and its objects behind for this tool to collect.

## Modes

**Diagnosis (default).** Read-only on PostgreSQL and S3, and lock-free: it takes
no schema lock, which on Aurora DSQL is a lease row it would write, so a
principal that may only `SELECT` the publication tables and the registry can run
it, and it never blocks a writer. For each schema it reports the record's state,
epoch and generation, every finding below, and the plan a collection would run
against the record's current horizon, with every object and manifest key it
would delete. Run it as the periodic health check.

Without the lock a publication or a collection can run beside it. A publication
is harmless: the listing comes before the snapshot, and a listed key the
snapshot's generations name but the listing lacks is probed before it is called
lost. A collection can delete a generation's manifest and objects once it
finds that generation expired, after the diagnosis's snapshot saw it retained.
That holds for the generation the snapshot saw current too: a diagnosis that
runs longer than G can see it replaced, retired by one collection and
collected by a later one. A missing manifest or object, of the current
generation or a superseded one, is therefore reported only if its generation
is still retained when the history is read again, after every manifest and
object was read; the report counts the findings it left out, refuses to
show a plan from its stale snapshot, and marks the schema `inconclusive`: the
diagnosis established neither that the missing keys were collected nor that
they were lost, so the run exits 2 and the diagnosis must be run again. A
missing history row is always reported: the snapshot read it with the record.

**`--gc`.** The collector, in the protocol's collection order
([Garbage collection](publication-protocol-v2.md#garbage-collection-and-reader-lifetime)):

1. Raise the record's `gc_horizon` to the database clock minus `--orphan-age`.
   The raise writes the record row, so it waits for a publication in flight and
   is ordered with it; no publication staged at or before the horizon commits
   afterwards.
2. List the objects under `{data-prefix}/{schema_id}/`.
3. In one read-only snapshot, read the record, the retained generations, the
   expired ones and the schema's lifecycle fence (when its lifecycle epoch last
   moved; see "What is deleted, and what never is"). After the snapshot ends, stamp `retired_at` on every superseded
   generation not yet retired. Those expire a grace later, in a later pass.
4. Fetch every retained and expired manifest and verify it with the reader's
   full check (digest, identity, wire form, and every listed key under the
   schema and epoch).
5. Delete every object that is listed only by expired generations, or that no
   generation names and was staged before the horizon: its `LastModified` plus
   S3's one-second precision plus `--clock-margin` is before the horizon.
6. Once every such delete succeeded, delete the expired manifests.
7. Once those succeeded, forget the expired history rows, all but the highest,
   which stays as the history's floor (see "What is deleted, and what never is").

A pass that fails at any step leaves a state the next pass plans from again: an
expired manifest still present lists its objects again, and one already deleted
leaves only its row to forget. Re-running is always safe.

Every collection takes the schema lock `cdc-flush` and `cdc-init` take, and
skips a schema whose lock is held. The lock is for liveness only: the horizon and the
retention grace are what make a delete safe, so a lost lease or a compactor
running outside the lock cannot make the collector delete something a reader or
a writer still needs.

**`--verify-checksums`.** Adds a full GET of every object a retained generation
lists, once per key, and compares its SHA-256 and size with every retained
entry that names the key. A shared base object that went bad is reported
against each generation that lists it, the current one included, and an entry
only the current generation committed wrongly is reported although an older
entry for the same key matches. It is the only
check of an object's bytes at rest besides compaction's pre-merge gate (#347).

`--repair` is refused. `--manifest-template`, `--manifest-prefix`,
`--allow-empty-manifest-schema`, `--gc-grace`, `--etag-retries` and
`--verify-stamps` are gone: V2 has no mutable manifest to locate, no template
to mis-point, and the manifest's `columns` are covered by its digest and the
object's by `--verify-checksums`.

## What is deleted, and what never is

A collection deletes only keys a V2 writer stages under the configured
`--data-prefix` for the schema (`{prefix}/{id}/_manifests/e{epoch}/g…-{token}.json`
and `{prefix}/{id}/e{epoch}/{base|delta}/{uuidv7}.parquet`), and of those only:

- an object listed only by expired generations, and an expired generation's
  manifest;
- an object no generation names, retained or expired, whose `LastModified` plus
  one second plus `--clock-margin` (rounded up to the millisecond) is before the
  horizon. S3 keeps `LastModified` in whole seconds and lists it truncated, so
  an object written at 12.700 lists as 12.000; the second covers that, and the
  margin the drift between S3's clock and the database's. Its staging began
  before the horizon, so its publication can never commit.

And it deletes or forgets any of them only under the storage target the
deployment is bound to (see "Storage binding"), and only once a manifest of the schema's
generation history, retained or expired, is in the bucket under `--data-prefix`
and verifies against the digest PostgreSQL committed for it.

It never deletes:

- the current generation's manifest or an object it lists, however old;
- a superseded generation's manifest or objects before the retention grace
  after its retirement has passed;
- staging not yet below that cutoff, which may still commit;
- any key that is not V2-shaped for the schema: V1 leftovers (`manifest.json`,
  `{id}/{uuid}.parquet`, `_tmp/`), another tool's files, another layout's keys.
  These are reported as `unrecognized_object`.

A pass deletes nothing at all for a schema when it cannot prove what readers
may scan: a run target that is not the bound storage, a missing or invalid record,
a record without its lifecycle epoch row or with one at another epoch, a lifecycle
epoch that moved less than G ago (below), a published record whose history row is
missing or disagrees with it, a gap in the generation history
(below), a retained manifest that is missing, any manifest that fails
verification (retained or expired, and the current one also against the record,
as readers verify it), or a manifest key the configured prefix does not build. A
manifest read that S3 refuses (403) or that fails is a tool failure, never a
missing manifest, and so is a listing that answers an object without a
`LastModified`: an object of unknown age is never taken to be older than the
horizon.

Every committed generation takes the next number of the record's counter. A
generation below the highest expired one is past every reader, whether its row
was forgotten or lost, so the collection keeps that highest expired row (its
manifest and objects deleted) as the history's floor, and requires a row for
every number above it up to the counter. A number without one is a
`missing_history` finding: a reader may still hold that generation, and nothing
names what it lists.

A lost record's recovery cannot keep that proof by itself. A reader may have
pinned the lost record's head generation N, and N's history row may be lost
with the record: the recovery then rebuilds the counter from the surviving
N−1, the history shows no gap, and nothing names N's manifest or the objects
only it lists. So every pass, the diagnosis's plan included, deletes nothing for
a schema whose lifecycle epoch moved less than G ago. The epoch row's
`updated_at` is that fence: every drop, registration and recovery stamps it,
and the recovery stamps it with the database clock after it found the record
missing, so after the snapshot of every reader of the lost record. No later
stamp lowers it: a drop or registration whose transaction began before the
loss keeps the recovery's later stamp. Such a reader is done R_max + C after
its snapshot, within G.

The stamp fences the record only when the epoch row names the record's epoch.
Every transition writes the two in one transaction, so they differ only when one
was restored or written outside the protocol. A restore that puts back the epoch
row's copy from before a recovery would put back the old epoch with a stamp long
past G, while the recovered record, one epoch above, is younger than G. So a row
at another epoch than the record's is a `lifecycle_epoch_mismatch` finding, and
the pass deletes nothing. The report says
`garbage collection refused: the schema's lifecycle epoch moved at …` with the
time it resumes; it is not a finding, and needs no action. A drop or a
registration delays collection by the same G, which costs storage only.

## Storage binding

Every V2 deployment builds the same keys for a schema id, so the key layout
alone cannot tell this deployment's objects from another's (#463). Nor can a
manifest that verifies: it proves the bucket holds this database's committed
inventory, not that it is the bucket this database's writers publish into. A
copy of the bucket holds the same manifests. Suppose generation N is copied into
a second bucket, and the deployment using that bucket publishes N+1 into it
while this database still names N. A collector of this database misdirected to
the copy would verify N, and N+1's manifest and the objects only it lists would
look like old orphans to it.

So the deployment is bound to one storage target, the S3 endpoint, the bucket
and the data prefix, in the `<entity_main>_publication_storage` table. Nothing
in a bucket can bind it. The binding is the operator's statement of where the
writers publish, made once:

```bash
# With the writers' own --s3-endpoint, --s3-region, --s3-bucket and --data-prefix
# (cdc-flush's and cdc-init's -s3-prefix), copied from their configuration, and
# in an environment that resolves the S3 endpoint as theirs does.
forma-tools manifest-reconcile --s3-bucket B --data-prefix data --schema-registry-table schema_registry --bind-storage
```

The endpoint bound and compared is the one the run's S3 client sends its
requests to, not the `--s3-endpoint` flag. Without the flag, the AWS SDK takes
an endpoint from its configuration (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`,
a profile's `endpoint_url` or `services` section), so a run whose flags are
unchanged can still be sent to another service holding a bucket of the same
name. The run therefore resolves the endpoint from the client it lists and
deletes through: `--s3-endpoint` when given, otherwise the SDK's configured
endpoint, otherwise the AWS endpoint of `--s3-region` (for example
`https://s3.us-east-1.amazonaws.com`), which names the partition a bucket name
is unique in. FIPS, dual-stack and accelerated endpoints reach the same bucket
and are not part of it. `--bind-storage` prints the target it bound, endpoint
included; a run whose resolved endpoint differs reports `storage_not_bound`
and deletes nothing. A client whose endpoint a custom resolver chooses cannot be
named by a binding, and the run fails (exit 1) before it connects.

It creates the table if needed (as its own DDL statement, as Aurora DSQL
requires), writes the binding unless one exists, and exits. A binding to
another target is kept and the run fails. A trailing slash on the endpoint or
the prefix names the same target. Every run reads the binding, and a run whose
target is not the bound one, or a deployment with no binding, reports
`storage_not_bound` for every schema: the collection retires superseded
generations but deletes and forgets nothing, and the diagnosis shows no plan.
If the writers really moved to another target, delete the table's row by hand
once they publish there, and bind again.

The binding proves only that the run's client is sent to the target the
operator declared. It cannot catch a binding made with the wrong values, so take
them from the writers' configuration and environment, not from the collector's.

The verified-manifest check remains as a second guard. A collector bound and
pointed at the wrong prefix finds a published schema's current manifest missing
(`missing_generation`) or foreign (`foreign_manifest`), and refuses. A schema
with no manifest to find, an `UNPUBLISHED` one or one whose every manifest was
collected, has nothing in the bucket that matches its history, so the pass
refuses to delete from S3 for it (`garbage collection refused: no manifest of
the schema's generation history is in the bucket …`) and keeps its old keys
until a generation of the schema publishes. That refusal is not a finding.

## Retention

| Flag | Parameter | Default | Rule |
| --- | --- | --- | --- |
| `--retention-grace` | G | 15m | At least the reader's pass deadline R_max (60s, `federated.DefaultPublishedPassDeadline`) plus C. A smaller value is refused, and so is a C whose sum with R_max does not fit a duration. |
| `--clock-margin` | C | 1m | Positive. Covers the drift between the readers' clocks, the database clock and S3's `LastModified`; S3's whole-second precision is added on top of it, and a margin under a millisecond counts as one. |
| `--orphan-age` | A | 2h | At least the writers' staging budget W (1h, `cdcv2.DefaultStagingBudget`) plus C. Any value is safe; a smaller one would refuse publications still inside their budget, so it is refused. A C whose sum with W does not fit a duration is refused before any connection opens. |

G is compared in full. Every statement that asks whether a stamp is at least G
old subtracts G rounded up to the millisecond, plus one millisecond, from the
database time. The stamps (`retired_at`, the epoch row's `updated_at`) and the
database time are each truncated to the millisecond, so a stamp s records an
event before s + 1 ms. Compared against G truncated, the smallest accepted grace
(60 s + 1 ns) would have ended after as little as 59.999 s.

A reader that chose generation N finishes or aborts within R_max of its
snapshot. N's replacement committed after that snapshot, and the collector
stamps `retired_at` in a transaction that begins after it saw that commit, so
N's objects outlive every reader of N by at least G − R_max − C ≥ 0. The
federated reader enforces R_max on every pass (`federated.ErrPassDeadline`). A
deployment that raises the reader's deadline must raise `--retention-grace`
with it.

Generation history and old objects therefore cost storage for G after the next
pass that follows their replacement. Running `--gc` every P minutes collects a
replaced generation within about G + 2P.

## Findings and recovery

Each finding names the schema, the epoch and generation it concerns (and
whether that is the current one), the object key where there is one, and the
safe action. Any finding makes the run exit 2.

| Finding | Meaning | Collection | Action |
| --- | --- | --- | --- |
| `no_record` | The schema is registered but has no publication record. Readers and writers fail closed. | refused | Run `init-db`: it recovers a lost record at a fresh epoch. |
| `record_invalid` | The record row is one no transition writes. Readers, writers and the collector fail closed on it. | refused | Recover at a fresh epoch: delete the row, run `init-db`, then `cdc-init` (see "An invalid record" below). Never restore an older copy of the row. |
| `storage_not_bound` | The S3 endpoint the run's client uses (from `--s3-endpoint`, the SDK's endpoint configuration or `--s3-region`), its `--s3-bucket` and its `--data-prefix` are not the deployment's bound storage target, or it has none (see "Storage binding"). | refused | Not bound: run `--bind-storage` once with the writers' values and environment. Bound elsewhere: run with the bound values, and clear an endpoint override (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, a profile's `endpoint_url`) the writers do not use; if the writers moved, delete the binding row by hand and bind again. |
| `missing_lifecycle_epoch` | The schema has a record but no lifecycle epoch row, so nothing shows when it was last recovered. | refused | Write the row back at the record's epoch, stamped with the database time: `INSERT INTO <entity_main>_publication_epoch (schema_id, epoch, updated_at) VALUES (<id>, <epoch>, <now in epoch ms>)`. Collection resumes G after the stamp. |
| `lifecycle_epoch_mismatch` | The lifecycle epoch row names another epoch than the record: one of them was restored or written outside the protocol, and the row's stamp does not show when the record was last recovered. | refused | Row below the record: write it back at the record's epoch with the database time, `UPDATE <entity_main>_publication_epoch SET epoch = <epoch>, updated_at = <now in epoch ms> WHERE schema_id = <id>`, never a backup copy; collection resumes G after the stamp. Row above the record: the record went back, so treat it as `record_invalid`. |
| `awaiting_init` | The V2 cutover left the schema for its first `cdc-init`. Readers fail closed. | runs | Run `cdc-init` for the schema. |
| `missing_generation` | The record names a generation whose manifest object is missing, or that has no history row. | refused | A lost manifest: re-initialize with `cdc-init`; collection resumes once the lost generation expires. A lost history row: do not restore it from a backup (see "Lost history rows" below). Collection stays refused until a generation above the unrecorded one expires, which takes two publications: the next one (a flush, a compaction or a `cdc-init`) supersedes the unrecorded generation, which is then a `missing_history` finding; a later publication supersedes the next one's generation, a pass retires it, and G passes. A single re-init followed by G is not enough. |
| `history_mismatch` | The record's generation has a history row, but the row disagrees with the record (epoch, parent generation, manifest key, digest or token), or carries a `superseded_at` or `retired_at` while the generation is current. | refused | Not a lost row, and not the lost row's recovery (see "A history row that disagrees with the record" below). Rewrite neither the row nor the record, and restore neither from a backup. Reads follow the record: only if the manifest fails against the record (an `invalid_manifest` finding of the generation says so) do they fail, and a re-init restores them. The action says whether collection can resume: if the row's manifest verifies against the row, once a later publication replaces the generation; otherwise leave collection refused, since no publication or expiry clears it. |
| `missing_history` | The history has no row for a generation number above the highest expired row and at or below the record's counter. | refused | Do not restore the rows from a backup (see "Lost history rows" below). Collection resumes by itself once a generation above the gap expires: a later publication (a `cdc-init` included) supersedes it, a pass retires it, and G passes. |
| `missing_retained_manifest` | A superseded generation inside its grace has no manifest. | refused | None for new reads. Collection resumes once the generation expires. |
| `invalid_manifest` | A manifest's bytes are not the ones PostgreSQL committed: corruption or a writer bug. | refused | Restore the object from a versioned or backed-up copy whose SHA-256 is the history row's `manifest_sha256`. For the current generation, a re-init restores reads. |
| `foreign_manifest` | A generation's manifest is not under `--data-prefix`. | refused | Run with the prefix the writers publish under. |
| `lost_object` | A retained generation lists an object S3 confirms missing (a 404 on a probe, never a failed call). | runs | Current generation: re-initialize with `cdc-init`; reads fail closed until then. Superseded only: none for new reads; it expires. |
| `corrupt_object` | `--verify-checksums` found other bytes than the manifest lists. | runs | Current generation: re-initialize with `cdc-init` (compaction refuses the object as a source). Otherwise it expires. |
| `unrecognized_object` | A key under the schema's prefix that no V2 writer stages. | runs | Remove it by hand once you know what wrote it. |

Never repair a schema by editing its record, writing a manifest by hand, or
publishing a generation over objects found in S3. A generation's marks commit
with it, so a rebuilt inventory that lacks a row's newest version, or holds a
deleted row without its tombstone, serves wrong answers without any error.
A re-init cannot do that: it exports every live row at its current version and
replaces the parquet tiers.

**Lost history rows.** Do not restore a lost generation history row from a
backup. Suppose N's row is lost and N+1 publishes before the restore. N+1's
publication already ran the update that supersedes N, and it matched no row. A
backup taken while N was current restores N with `superseded_at = 0`, and no
later publication supersedes N again. The collector never retires it, so it is
retained forever, and if its manifest is gone too, collection stays refused
forever. Nothing the tool can check proves that a backup row carries the exact
generation identity and the retirement state the protocol would have given it,
so the documented recovery is the publication-and-expiry sequence in the table
above: no step of it writes a history row by hand.

**A history row that disagrees with the record** (`history_mismatch`). The row
of the record's generation is there, so this is not a lost row: it never becomes
a `missing_history` gap, and the two-publication recovery above does not apply.
One of the two rows was restored from a backup or written outside the protocol,
since every publication writes them in one transaction. Do not rewrite either to
make them agree, and do not restore the manifest object to match the row: the
pass cannot prove which of them is the committed state.

- *Current reads* follow the record, which readers verify the manifest
  against. If the manifest verifies against the record, reads succeed and need
  nothing. If it does not (an `invalid_manifest` finding of the same generation
  says so in its detail), reads fail closed, and a re-init with `cdc-init`
  restores them.
- *Garbage collection* is a separate question. A re-init, or any later
  publication, makes another generation current, but the disagreeing row stays
  in the history, and every pass verifies the manifest it names against it,
  retained or expired. The finding's action says which case applies:
  - The manifest verifies against the row, and the row carries no retention
    stamp (for example only the record's parent generation, or only the row's
    `superseded_at`, is wrong). Collection resumes once a later publication
    replaces the generation. The row then keeps the generation's objects until
    a pass retires it and G passes, like any superseded generation. The
    collector never retires the current generation's row, even when it carries
    a `superseded_at`, so its grace cannot run while readers still choose it.
  - Otherwise (for example the row's digest or manifest key is not the
    object's, or the row was stamped retired while it was current), the
    manifest never verifies against the row. No publication and no expiry
    clears the refusal: the expired row is reported `invalid_manifest` on every
    pass. Leave garbage collection refused for the schema, and do not run
    `--gc` for it until the cause is known. A refusal deletes nothing; garbage
    only accumulates.

**An invalid record.** Do not restore the row from a backup. A backup's
pointer from the same epoch can still pass every reader check and name an
intact manifest whose digest verifies, while the flush marks of every later
generation stay committed in PostgreSQL: readers would then treat rows as
flushed into a generation they do not scan, and silently answer old values or a
deleted row. Nothing in PostgreSQL or S3 proves a backup copy is the exact
committed state. Recover at a fresh epoch instead, which never serves the
invalid row or any older pointer:

1. Leave the invalid row in place while you prepare. Readers, writers and the
   collector fail closed on it, so nothing reads, publishes or deletes for the
   schema meanwhile.
2. Delete it: `DELETE FROM <entity_main>_publication WHERE schema_id = <id>`.
   Readers and writers keep failing closed, now on a missing record.
3. Run `init-db`. It recovers the missing record under the schema lock, one
   epoch above the id's lifecycle epoch, `AWAITING_INIT` while generation
   history survives, with the history's highest generation as its counter
   ([A lost record](publication-protocol-v2.md)). Every process that pinned the
   old epoch misses, a writer that staged over the former head included. If
   the lifecycle epoch row is missing too, `init-db` refuses; follow the
   protocol doc's procedure for that case first.
4. Run `cdc-init` for the schema. It exports every live row from PostgreSQL at
   its current version into the fresh epoch's first generation, and readers
   resume.

The recovery stamps the lifecycle fence, so collection stays refused for G, and
then retires and collects the old epoch's generations as below.

A lost record recovered by `init-db` supersedes the generation it named, in the
recovery's transaction; the next collection retires it, and it expires and is
collected a grace later, as any superseded generation. Collection stays refused
for G after the recovery in any case, so a reader of a lost head whose history
row was lost too finishes before anything it lists can be deleted.

A `DROPPED` schema keeps its record. Its generations were superseded by the
drop, and expire and are collected like any other's. An `UNPUBLISHED` schema
needs no action. A failed first flush may have left staging, which is kept
until the schema's first generation publishes: until then no manifest binds the
bucket to the schema, and it is collected by the first pass after that.

## Running it

```bash
# Read-only health check of every schema (registered or with a record).
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry

# Collect garbage, on a schedule.
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry --gc

# One schema, with a byte scrub.
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry --schema-id 7 --verify-checksums
```

Bind the deployment's storage once before the first run (see "Storage
binding"); until then every schema reports `storage_not_bound` and nothing is
deleted.

`--data-prefix` and `--entity-main-table` must be the writers' (`cdc-flush`
and `cdc-init`'s `--s3-prefix`, and the `entity_main` the publication tables
and the schema lock are named after). The database flags are the compactor's,
including `--pg-engine` and `--pg-use-iam`.

The run visits every schema in the registry and every schema with a record, so
a dropped schema is still collected and a registered schema without a record is
reported.

Exit codes: 0 when every schema is clean, 2 when any schema has a finding, a
collection skipped it because its lock was held, or its diagnosis was
inconclusive because a collection beside it expired what it found missing, 1 when a pass failed (a database or S3 call,
a listing that could not be followed to its end, a lost lease). Garbage awaiting
collection is not a finding.

Under `--gc` the report lists every key deleted and every row forgotten, from
what the pass did rather than from its plan, and says how many planned deletes
a pass that failed part way left for the next one.

The collector's principal needs `s3:ListBucket`, `s3:GetObject` and
`s3:DeleteObject` on the bucket; the diagnosis's, `s3:ListBucket` and
`s3:GetObject`, and `SELECT` on the publication tables, the storage binding
table and the registry. `--bind-storage` needs `CREATE` on the schema (for the
table, once) and `INSERT` on the table, and no S3 access. Without `s3:ListBucket`, S3 answers 403 for a
missing key, which the tool reports as a failure rather than a loss.

## Not verified

On Aurora DSQL the collector relies on two assumptions #817 tracks: that
`RaiseHorizon` and a publication, which write the same row, never both commit
unordered, and that `Retire`'s `now()` is no earlier than every commit a
snapshot that ended before it saw.
