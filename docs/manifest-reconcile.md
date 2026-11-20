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
principal that may only `SELECT` the publication tables (the displaced inventories
included) and the registry can run
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
collected by a later one. A missing manifest or object is therefore left
out only on evidence of that expiry, read after every manifest and object was
read: the history's expiry floor (the highest expired generation) has risen
from below the generation to at or above it, the generation's row is no
longer retained, and, for the generation the snapshot saw current, the record
no longer names it either. A collection deletes nothing of a generation above
the floor: such a generation is retained, or its row is missing, a gap that
stops every collection. So a replacement alone is no evidence (R11-F2): a
generation the snapshot saw current with its row and its manifest missing,
replaced while the diagnosis read, still reports its manifest's loss, since
nothing could have collected it. Neither is a row stamped retired outside the
protocol, which the snapshot read already, and a generation the record still
names was never replaced: its loss is reported whatever its history row says.
When the manifest the record names is left out, the row's finding says so
instead of pointing to it. The report counts the findings it left out, refuses to show a plan from its stale
snapshot, and marks the schema `inconclusive`: the diagnosis established
neither that the missing keys were collected nor that they were lost, so the
run exits 2 and the diagnosis must be run again. The evidence is an expiry,
never an observed delete: a generation can pass its grace while the diagnosis
reads with no collection running at all, its manifest having been missing
before. So the report says a collection may have deleted what the generation
listed, or that it was missing before, and never that a collection ran
(R12-F2). A missing history row is
always reported: the snapshot read it with the record.

The current generation is checked as the record names it, which is what
readers verify and scan, whether or not its history row is there or agrees
with the record. A missing or disagreeing row is one finding, with the row's
recovery; a missing or invalid manifest, or a lost object, of the record's
generation is another, with the re-init that restores reads. The row's
finding says whether the record's manifest verifies against the record, and
nothing more: that is a check of the manifest, not of the data it lists, so
it never says that reads succeed. A `lost_object` finding of the same
generation reports an object that is missing, and reads then fail closed until
a re-init (R11-F3). A `corrupt_object` finding reports an object whose bytes
are not those the manifest lists. That is an integrity fault readers do not
detect: they pass the listed paths and column metadata to DuckDB and never hash
an object against its manifest, so a read may return incorrect results without
an error, or fail with a scan error. A re-init restores correct reads too
(R12-F3).

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
   schema and epoch). Fetch the manifest of every displaced inventory whose
   replaced generation's row expired, and verify it with the same full check
   against the identity, digest and token the displacement kept: its keys are
   deleted whatever their age, so a key of another epoch must never pass
   (R12-F1).
5. Delete every object that is listed only by expired generations or expired
   displaced inventories, or that no generation names and was staged before
   the horizon: its `LastModified` plus S3's one-second precision plus
   `--clock-margin` is before the horizon.
6. Once every such delete succeeded, delete the expired manifests and the
   expired displaced manifests.
7. Once those succeeded, forget the expired history rows, all but the highest,
   which stays as the history's floor (see "What is deleted, and what never is"),
   and then the expired displacements.

A pass that fails at any step leaves a state the next pass plans from again: an
expired manifest still present lists its objects again, and one already deleted
leaves only its row to forget; a displacement is forgotten only once its manifest
is deleted, so it names its inventory again. Re-running is always safe.

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
check of an object's bytes at rest besides compaction's pre-merge gate (#347):
the federated reader does not hash the objects it scans, so a corrupt object is
found here or by compaction, and may never fail a read.

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

So the deployment is bound to one storage target in the
`<entity_main>_publication_storage` table: the URL the writers' S3 client sends
the bucket's requests to (the endpoint and the bucket's addressing), the bucket
and the data prefix. Nothing
in a bucket can bind it. The binding is the operator's statement of where the
writers publish, made once:

```bash
# With the writers' own --s3-endpoint, --s3-region, --s3-use-path, --s3-bucket and
# --data-prefix (cdc-flush's and cdc-init's -s3-prefix), copied from their
# configuration, and in an environment that resolves the S3 endpoint as theirs does.
forma-tools manifest-reconcile --s3-bucket B --data-prefix data --schema-registry-table schema_registry --bind-storage
```

The endpoint bound and compared is where the run's S3 client sends the bucket's
requests, not the `--s3-endpoint` flag. The run builds a LIST and a DELETE
through the client it lists and deletes with, sends neither, and takes the
bucket's URL from them. Two things decide that URL besides the bucket:

- **The endpoint.** It is `--s3-endpoint` when given. Without the flag, the
  AWS SDK takes an endpoint from its configuration (`AWS_ENDPOINT_URL_S3`,
  `AWS_ENDPOINT_URL`, a profile's `endpoint_url` or `services` section), so a
  run whose flags are unchanged can still be sent to another service holding a
  bucket of the same name. Without any of them it is the AWS endpoint of
  `--s3-region`, which names the partition a bucket name is unique in. FIPS,
  dual-stack and accelerated AWS endpoints reach the same bucket, and are left
  out there.
- **The addressing.** Path-style addressing (`--s3-use-path`) sends the
  requests to the endpoint's host with the bucket as the first path segment,
  `https://s3.storage.example/lake`. Virtual-hosted addressing sends them to a
  host named after the bucket, `https://lake.s3.storage.example`. A gateway at a
  custom endpoint can route those two hosts to different backends, so they are
  different targets, and a run that only changes `--s3-use-path` is not the
  bound one. At AWS the URL is virtual-hosted unless the bucket name has a dot, for example
  `https://lake.s3.us-east-1.amazonaws.com`. The tools apply `--s3-use-path`
  only together with `--s3-endpoint`.

`--bind-storage` prints the target it bound, URL included. That URL is the
bucket's, an output to compare, never an input: do not pass it back as
`--s3-endpoint`. Run every `manifest-reconcile` with the writers' own base
endpoint (`--s3-endpoint`, or none where they have none) and their addressing
(`--s3-use-path` exactly when they use it), and the client builds the same
bucket URL. Given the printed URL as its base endpoint, a path-style client
would add the bucket a second time (`https://s3.storage.example/lake/lake`)
and report `storage_not_bound`. A run whose client sends the bucket's requests
anywhere else reports `storage_not_bound` and deletes nothing. A client whose
endpoint a custom resolver chooses cannot be named by a binding, and the run
fails (exit 1) before it connects.

**Redirects are refused.** The binding names the URL a request is built for,
and that is all it can name: the AWS SDK's own HTTP client follows a 307 or
308 with the request's method, so a server at the bound URL (a gateway, a
proxy, or a hostile endpoint) could send a DELETE on to another route, and the
SDK would report that route's answer as the delete's. So every request a
`manifest-reconcile` run sends, LIST, GET, HEAD and DELETE alike, goes through
an HTTP client that follows no redirect (`reconcile.NewS3Objects`), and only a
store built that way tells the reconciler its destination. A redirected
request fails with `the S3 request was redirected, and manifest-reconcile
follows no redirect`, and is not retried. Under `--gc` that fails the pass
(exit 1) at the step it reached: no object is deleted elsewhere, and since a
step starts only after the one before it succeeded, no history row is
forgotten; the next pass plans from what is left. A redirected read is a
failure too, never a missing object. The error names the request's method,
its URL without the query, and the redirect's status, never the redirect's
`Location`: a Location can carry a credential (a signed URL's query, a
user-info password), so it reaches neither the report nor a log. Read it from
the server or gateway's own log when you need it. If a run fails so, the
endpoint the writers and the binding name is not the one that serves the
bucket: find out why before collecting, and never work around it by following
the redirect's target with `--s3-endpoint` unless the writers publish there
too.

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
| `storage_not_bound` | The URL the run's client sends the bucket's requests to (from `--s3-endpoint`, the SDK's endpoint configuration or `--s3-region`, and `--s3-use-path`), its `--s3-bucket` and its `--data-prefix` are not the deployment's bound storage target, or it has none (see "Storage binding"). | refused | Not bound: run `--bind-storage` once with the writers' values and environment. Bound elsewhere: run with the writers' base `--s3-endpoint` and `--s3-use-path`, which build the bound URL (never the printed bucket URL as `--s3-endpoint`), and clear an endpoint override (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, a profile's `endpoint_url`) the writers do not use; if the writers moved, delete the binding row by hand and bind again. |
| `missing_lifecycle_epoch` | The schema has a record but no lifecycle epoch row, so nothing shows when it was last recovered. | refused | Write the row back at the record's epoch, stamped with the database time: `INSERT INTO <entity_main>_publication_epoch (schema_id, epoch, updated_at) VALUES (<id>, <epoch>, <now in epoch ms>)`. Collection resumes G after the stamp. |
| `lifecycle_epoch_mismatch` | The lifecycle epoch row names another epoch than the record: one of them was restored or written outside the protocol, and the row's stamp does not show when the record was last recovered. | refused | Row below the record: write it back at the record's epoch with the database time, `UPDATE <entity_main>_publication_epoch SET epoch = <epoch>, updated_at = <now in epoch ms> WHERE schema_id = <id>`, never a backup copy; collection resumes G after the stamp. Row above the record: the record went back, so treat it as `record_invalid`. |
| `awaiting_init` | The V2 cutover left the schema for its first `cdc-init`. Readers fail closed. | runs | Run `cdc-init` for the schema. |
| `missing_generation` | The record names a generation whose manifest object is missing, or that has no history row. The two are separate findings, and both appear when both are lost: the record's manifest is checked whether or not its row is there. | refused | A lost manifest: re-initialize with `cdc-init`; collection resumes once the lost generation expires. A lost history row: do not restore it from a backup (see "Lost history rows" below). Collection stays refused until a generation above the unrecorded one expires, which takes two publications: the next one (a flush, a compaction or a `cdc-init`) supersedes the unrecorded generation, which is then a `missing_history` finding; a later publication supersedes the next one's generation, a pass retires it, and G passes. A single re-init followed by G is not enough. |
| `history_mismatch` | The record's generation has a history row, but the row disagrees with the record (epoch, parent generation, manifest key, digest or token), or carries a `superseded_at` or `retired_at` while the generation is current. A row stamped retired is reported so even when its stamp is past the grace. | refused | Not a lost row, and not the lost row's recovery (see "A history row that disagrees with the record" below). Rewrite neither the row nor the record, and restore neither from a backup. Reads follow the record: the detail says whether the manifest the record names verifies against it, which covers the manifest only; if it does not, a `missing_generation` or `invalid_manifest` finding of the record's manifest says why, and if an object it lists is missing, a `lost_object` finding of the generation says so and reads fail closed; if an object's bytes differ from the manifest's entry, a `corrupt_object` finding says so, and reads, which do not check an object's bytes, may return incorrect results or fail with a scan error. Either way a re-init restores correct reads. Either side may be the one that changed, so the next publication writes the row back from the record and keeps the row's inventory as displaced: collection stays refused until that generation's row expires (a pass retires it after seeing the replacement committed, and G passes), and then resumes unless the record's manifest still fails against the row. One replacement does not by itself make an immediate collection safe. |
| `missing_history` | The history has no row for a generation number above the highest expired row and at or below the record's counter. | refused | Do not restore the rows from a backup (see "Lost history rows" below). Collection resumes by itself once a generation above the gap expires: a later publication (a `cdc-init` included) supersedes it, a pass retires it, and G passes. |
| `missing_retained_manifest` | A superseded generation inside its grace has no manifest. | refused | None for new reads. Collection resumes once the generation expires. |
| `invalid_manifest` | A manifest's bytes are not the ones PostgreSQL committed: corruption or a writer bug. For a displaced inventory, its manifest fails the reader's full check against the displacement, a key of another epoch included. | refused | Restore the object from a versioned or backed-up copy whose SHA-256 is the history row's `manifest_sha256`. For the current generation, a re-init restores reads. For a displaced inventory whose bytes match the displacement's digest, delete the displacement's row by hand instead (see "A history row that disagrees with the record" below). |
| `foreign_manifest` | A generation's manifest is not under `--data-prefix`. | refused | Run with the prefix the writers publish under. |
| `lost_object` | A retained generation lists an object S3 confirms missing (a 404 on a probe, never a failed call). | runs | Current generation: re-initialize with `cdc-init`; reads fail closed until then. Superseded only: none for new reads; it expires. |
| `corrupt_object` | `--verify-checksums` found other bytes than the manifest lists: an integrity fault. Readers do not hash an object against its manifest, so a read that scans it may return incorrect results without an error, or fail with a scan error; it does not reliably fail closed. | runs | Current generation: re-initialize with `cdc-init` (compaction refuses the object as a source). Otherwise only a read still pinned to the generation scans it, and it expires. |
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
make them agree, and do not restore a manifest object to match the row.

- *Current reads* follow the record. Readers verify the manifest the record
  names against the record, and never read the row, so the pass checks that
  manifest against the record, as they do, and not the bytes of the manifest
  the row names, which may be another object. The finding's detail says
  whether it verifies, and that is all it says: a verified manifest is no
  proof that the objects it lists are there. If it does not verify, a
  `missing_generation` or `invalid_manifest` finding of the record's manifest
  key says why; if it does, a `lost_object` finding of the generation reports
  an object that is missing, and reads fail closed. A `corrupt_object` finding
  reports an object whose bytes differ from the manifest's entry, which
  readers do not check: reads may return incorrect results or fail with a scan
  error. Either way a re-init with `cdc-init` restores correct reads.
- *Garbage collection* stays refused while the generation is current, and
  for a full grace after it is replaced. Nothing in PostgreSQL or S3 shows
  which of the two was changed. If the row was, it can name another valid
  manifest of the same generation, one a publication that lost the race
  staged, while readers scan the record's. If the record was, a reader that
  chose the generation before the change scans the manifest the row names,
  and readers that chose it after scan the record's. Either inventory may be
  pinned, so neither may become an orphan while a reader of the generation
  can still be inside its pass deadline. A `retired_at` on the current
  generation is no retirement either: carried past the replacement, it would
  expire the generation at once.

  So the transition that replaces the generation (a flush, a compaction, a
  `cdc-init` or a drop) writes the replaced generation's row back from the
  record it replaces: epoch, parent, manifest key, digest, lineage and token.
  It stamps `superseded_at` with its own start and clears `retired_at`. When
  the row named another epoch, manifest key, digest or token, the same
  transaction first keeps what the row named in
  `<entity_main>_publication_displaced` (a displaced inventory). The row then
  keeps the record's inventory until a pass retires it, after seeing the
  replacement committed, and G passes, as for any superseded generation. While
  the row is retained, every pass refuses to delete or forget anything for the
  schema, and says so ("garbage collection refused: a replaced generation's
  history row disagreed with the record ..."), whatever the age of the objects:
  the displaced manifest is in no history row, so its objects would otherwise
  be orphans, and the objects it shares with an expired generation would be
  that generation's alone. Once the row has expired, no reader that chose the
  generation under either inventory is left, and the displaced inventory is
  collected as an expired generation's is (R11-F1). Its manifest is verified
  against the digest and token the displacement kept; the objects only it
  lists are deleted with the expired generations' in step 5, whatever their
  age, never one a retained generation lists; the manifest is deleted in step
  6; and the pass forgets the displacement only after every delete succeeded.
  It does not wait for `--orphan-age`, which is usually longer than G (2h
  against 15m by default): after a drop the expired generations' manifests are
  deleted in that same pass, and the displacement is then the only evidence
  that the keys are this deployment's. A pass that fails before the end
  leaves the displacement, and the next pass verifies its manifest again and
  deletes the rest, so the cleanup is retried until it finishes, whatever is
  left of the schema in the bucket. A displaced manifest that fails the
  reader's full check (its digest, identity and wire form, every listed key of
  the displacement's schema and epoch with the tier its entry names, and the
  layout's prefix) is an `invalid_manifest` finding, or `foreign_manifest` for
  a key outside the layout: the pass deletes and forgets nothing, and the
  schema is reported as needing attention. The digest alone is not enough: a
  manifest whose bytes match it can still list a staged object of the epoch a
  drop moved to, far above the orphan cutoff, and the displaced inventory's
  keys are deleted whatever their age. If the detail reports another SHA-256
  than the displacement's, restore the object from a copy with that digest.
  Otherwise the displacement does not prove which keys are its inventory:
  delete its row of `<entity_main>_publication_displaced` by hand. The
  replaced generation's row has expired, so no reader scans it, and its
  manifest and the objects only it listed are then orphans, deleted only once
  they are older than the orphan cutoff. Never delete a key it lists by hand. One
  replacement therefore does not by itself make an immediate collection safe:
  collection resumes a full grace after the first pass that follows the
  replacement.

  If the record's manifest itself does not verify against the record, the row
  now names a manifest that fails, and an `invalid_manifest` finding keeps
  collection refused. So does a row whose kind is not the manifest's kind,
  since the record carries no kind to write back. The collector never retires
  the current generation's row, even when it carries a `superseded_at`, so its
  grace cannot run while readers still choose it.

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
inconclusive because a generation it found something missing of expired while it ran (a collection may have
deleted it, or it was missing before), 1 when a pass failed (a database or S3 call,
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
