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
object was read; the report counts the findings it left out and refuses to
show a plan from its stale snapshot. A missing history row is always reported:
the snapshot read it with the record.

**`--gc`.** The collector, in the protocol's collection order
([Garbage collection](publication-protocol-v2.md#garbage-collection-and-reader-lifetime)):

1. Raise the record's `gc_horizon` to the database clock minus `--orphan-age`.
   The raise writes the record row, so it waits for a publication in flight and
   is ordered with it; no publication staged at or before the horizon commits
   afterwards.
2. List the objects under `{data-prefix}/{schema_id}/`.
3. In one read-only snapshot, read the record, the retained generations and the
   expired ones. After the snapshot ends, stamp `retired_at` on every superseded
   generation not yet retired. Those expire a grace later, in a later pass.
4. Fetch every retained and expired manifest and verify it with the reader's
   full check (digest, identity, wire form, and every listed key under the
   schema and epoch).
5. Delete every object that is listed only by expired generations, or that no
   generation names and was staged before the horizon less `--clock-margin`.
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
  `--clock-margin` is before the horizon. Its staging began before the horizon,
  so its publication can never commit.

It never deletes:

- the current generation's manifest or an object it lists, however old;
- a superseded generation's manifest or objects before the retention grace
  after its retirement has passed;
- staging newer than the horizon less the clock margin, which may still
  commit;
- any key that is not V2-shaped for the schema: V1 leftovers (`manifest.json`,
  `{id}/{uuid}.parquet`, `_tmp/`), another tool's files, another layout's keys.
  These are reported as `unrecognized_object`.

A pass deletes nothing at all for a schema when it cannot prove what readers
may scan: a missing or invalid record, a published record whose history row is
missing or names another manifest or parent, a gap in the generation history
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

## Retention

| Flag | Parameter | Default | Rule |
| --- | --- | --- | --- |
| `--retention-grace` | G | 15m | At least the reader's pass deadline R_max (60s, `federated.DefaultPublishedPassDeadline`) plus C. A smaller value is refused, and so is a C whose sum with R_max does not fit a duration. |
| `--clock-margin` | C | 1m | Positive. Covers the drift between the readers' clocks, the database clock and S3's `LastModified`. |
| `--orphan-age` | A | 2h | At least the writers' staging budget W (1h, `cdcv2.DefaultStagingBudget`) plus C. Any value is safe; a smaller one would refuse publications still inside their budget, so it is refused. A C whose sum with W does not fit a duration is refused before any connection opens. |

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
| `record_invalid` | The record row is one no transition writes. | refused | Restore the row from a backup. |
| `awaiting_init` | The V2 cutover left the schema for its first `cdc-init`. Readers fail closed. | runs | Run `cdc-init` for the schema. |
| `missing_generation` | The record names a generation whose manifest object is missing, or whose history row is missing or names another manifest. | refused | A lost or mismatched manifest: re-initialize with `cdc-init`; collection resumes once the lost generation expires. A lost history row: restore it from a backup. Otherwise collection stays refused until a generation above the unrecorded one expires, which takes two publications: the next one (a flush, a compaction or a `cdc-init`) supersedes the unrecorded generation, which is then a `missing_history` finding; a later publication supersedes the next one's generation, a pass retires it, and G passes. A single re-init followed by G is not enough. |
| `missing_history` | The history has no row for a generation number above the highest expired row and at or below the record's counter. | refused | Restore the rows from a backup. Otherwise collection resumes by itself once a generation above the gap expires: a later publication (a `cdc-init` included) supersedes it, a pass retires it, and G passes. |
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

A lost record recovered by `init-db` supersedes the generation it named, in the
recovery's transaction; the next collection retires it, and it expires and is
collected a grace later, as any superseded generation.

A `DROPPED` schema keeps its record. Its generations were superseded by the
drop, and expire and are collected like any other's. An `UNPUBLISHED` schema
needs no action; a failed first flush may have left staging, which is
collected once it is below the horizon.

## Running it

```bash
# Read-only health check of every schema (registered or with a record).
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry

# Collect garbage, on a schedule.
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry --gc

# One schema, with a byte scrub.
forma-tools manifest-reconcile --s3-bucket B --schema-registry-table schema_registry --schema-id 7 --verify-checksums
```

`--data-prefix` and `--entity-main-table` must be the writers' (`cdc-flush`
and `cdc-init`'s `--s3-prefix`, and the `entity_main` the publication tables
and the schema lock are named after). The database flags are the compactor's,
including `--pg-engine` and `--pg-use-iam`.

The run visits every schema in the registry and every schema with a record, so
a dropped schema is still collected and a registered schema without a record is
reported.

Exit codes: 0 when every schema is clean, 2 when any schema has a finding or a
collection skipped it because its lock was held, 1 when a pass failed (a database or S3 call,
a listing that could not be followed to its end, a lost lease). Garbage awaiting
collection is not a finding.

Under `--gc` the report lists every key deleted and every row forgotten, from
what the pass did rather than from its plan, and says how many planned deletes
a pass that failed part way left for the next one.

The collector's principal needs `s3:ListBucket`, `s3:GetObject` and
`s3:DeleteObject` on the bucket; the diagnosis's, `s3:ListBucket` and
`s3:GetObject`, and `SELECT` on the publication tables and the registry. Without `s3:ListBucket`, S3 answers 403 for a
missing key, which the tool reports as a failure rather than a loss.

## Not verified

On Aurora DSQL the collector relies on two assumptions #817 tracks: that
`RaiseHorizon` and a publication, which write the same row, never both commit
unordered, and that `Retire`'s `now()` is no earlier than every commit a
snapshot that ended before it saw.
