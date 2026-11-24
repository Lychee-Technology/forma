# `manifest-reconcile` (Protocol V2)

`forma-tools manifest-reconcile` diagnoses Protocol V2 publication state and,
under `--gc`, collects expired generations and horizon-fenced orphans
(`docs/publication-protocol-v2.md`, "Garbage collection and reader lifetime").

An unlisted object under the layout is uncommitted staging or garbage (never
lost data), and a lost manifest or object of a committed generation cannot be
rebuilt from S3, so the tool never adopts or rebuilds anything: the recovery of
a lost generation is a `cdc-init`.

## Modes and exit codes

- **Read-only diagnosis (default, without `--gc`):** takes no schema lock and
  makes no database writes (safe for a read-only PostgreSQL principal and
  Aurora DSQL). Reads each schema's publication record, generation history,
  lifecycle row, and S3 listing; verifies every retained and expired manifest;
  probes listed objects for loss; reports what `--gc` would delete.
- **Garbage collection (`--gc`):** takes the writer schema lock (`--pg-engine`),
  raises `gc_horizon`, retires superseded generations and any unretired
  lost-record recovery fence, deletes unprotected objects of expired generations
  and horizon-fenced orphans, deletes expired manifests, and atomically forgets
  each expired history row while advancing `forgotten_generation`.
- **Storage binding (`--bind-storage`):** creates `<entity_main>_publication_storage`
  if needed, binds this database deployment to the exact S3 destination its
  client uses, and exits. Optional, since writers bind on their first
  publication ("Storage binding" below). Cannot be combined with `--gc`.
- **Checksum scrub (`--verify-checksums`):** hashes every distinct object listed
  by retained generations (one full `GET` per unique key) and checks both SHA-256
  and size against every retained manifest entry naming it.

Exit codes:

| Exit code | Meaning |
| --- | --- |
| `0` | Clean run (routine garbage awaiting `--gc` is clean) |
| `2` | Residual discrepancies, refused collections (e.g. active recovery fence or unpublished schema with unverified objects), skipped schemas (lock contended), or inconclusive lock-free diagnosis (rerun needed) |
| `1` | Tool, configuration, S3, redirect, or database failure |

## Storage binding

Garbage collection deletes nothing until the deployment is bound to the exact
S3 storage target its writers publish into, recorded in
`<entity_main>_publication_storage` (one row):

1. **Effective bucket URL:** derived from the requests the AWS SDK S3 client
   itself builds (`ListObjectsV2`, `GetObject`, `PutObject`, `DeleteObject`)
   under its own routing configuration: `--s3-endpoint` or
   `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` / the region's endpoint,
   `--s3-use-path` addressing, and FIPS, dual-stack and transfer acceleration
   settings. Writers and the collector use the same derivation
   (`internal/s3endpoint`). Custom endpoints with ambiguous paths (repeated
   slashes such as `/tenant//`, trailing slashes on non-root paths, dot
   segments, escaped paths, query/fragment, or userinfo) and custom endpoint
   resolvers are rejected rather than guessed.
2. **Bucket and data prefix:** `--s3-bucket` and `--data-prefix` (matching
   `cdc-flush` and `cdc-init`'s `-s3-prefix`).
3. **No redirects:** neither the collector's nor the writers' S3 clients
   follow a redirect. A 301, 302, 303, 307 or 308 response that carries a
   `Location` header is refused (`ErrRedirectRefused`) without reading its
   body or leaking `Location` header credentials; any other 3xx reaches the
   AWS SDK as an S3 error. For the collector that is exit `1`; for a writer
   it fails the staging, and nothing is published.

### How a deployment gets bound

`cdc-flush`, `cdc-init` and the compactor bind it themselves (#911):

- **Before staging anything**, a writer reads the binding. If it names another
  target, the writer fails with `publication.ErrStorageBoundElsewhere`, naming
  both targets, and sends nothing to S3.
- **In every publication transaction**, the writer checks the binding again
  and, if the deployment is unbound, binds its own target in that same
  transaction. The binding therefore commits exactly when a generation does:
  a run that publishes nothing (an empty flush, a compaction that merges
  nothing or is refused) and a publication that fails, rolls back or is
  abandoned bind nothing. Concurrent first publishers at different targets
  cannot both commit: exactly one binding commits, and the other
  publication fails with `ErrStorageBoundElsewhere`. Its staged objects stay
  unpublished at its own target, outside the bound one.
- A mismatch is refused immediately; there is no warn-only window. A writer
  configured for another target than the one GC is bound to would publish
  generations GC never sees.

A writer whose object store is not an S3 store over an AWS SDK client (a
local directory, or an embedder's own client type) names no target and
binds or checks nothing; bind such a deployment with `--bind-storage`.

The compactor reads its merge sources twice: its SDK client, the one it
checks the binding with, hashes each source against its manifest digest
(unless `--skip-input-checksum-verify`), and its DuckDB merge engine then
reads the same keys again for the merge. The merge engine is configured from
`--s3-endpoint`, `--s3-region`, `--s3-use-ssl` and `--s3-use-path` alone, so
the compactor refuses to start (`ErrMergeRouteMismatch`), before it opens the
database or the bucket, when its SDK client is routed otherwise: an endpoint
taken from `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` or a shared config
`endpoint_url` instead of `--s3-endpoint`, another region, FIPS, dual-stack
or transfer acceleration, or an `--s3-endpoint` scheme that disagrees with
`--s3-use-ssl`. Without `--s3-endpoint` the SDK client sends to the region's
AWS endpoint over HTTPS, so `--s3-use-ssl=false` is refused there too. Pass the endpoint and region as flags. What is enforced is
that both read through the same configured endpoint, region and addressing;
the merge engine's reads do not go through the no-redirect client.

`--bind-storage` stays as an explicit path: it binds the deployment ahead of
the writers, or confirms that this run's flags and environment name the bound
target. An existing binding to a different target is never overwritten.

```bash
forma-tools manifest-reconcile \
  --bind-storage \
  --pg-host ... --pg-db ... --pg-user ... --pg-engine postgresql \
  --schema-registry-table schema_registry \
  --entity-main-table entity_main \
  --s3-bucket my-bucket --data-prefix data
```

### Creating the binding table

Writers never run DDL; a publication transaction holds none, as Aurora DSQL's
one-DDL-per-transaction rule requires. A deployment without the table fails
its writers with `publication.ErrStorageTableMissing` before they stage, so
create it before upgraded writers run:

- **PostgreSQL:** rerun `init-db`; it creates the table with the other
  publication tables.
- **Aurora DSQL:** `init-db` cannot create Forma's tables there yet (#690).
  Either run `--bind-storage` with the writers' flags and environment, which
  creates the table **and binds that run's target**, or create the table by
  hand, as its own transaction, and let the first publication bind (shown
  for `--entity-main-table entity_main`):

  ```sql
  CREATE TABLE IF NOT EXISTS entity_main_publication_storage (
      binding     SMALLINT NOT NULL,
      endpoint    TEXT     NOT NULL,
      bucket      TEXT     NOT NULL,
      data_prefix TEXT     NOT NULL,
      bound_at    BIGINT   NOT NULL,
      PRIMARY KEY (binding)
  )
  ```

### Upgrading a deployment that has already published

A new deployment needs nothing beyond the table: its first publication binds
the writer's target. A deployment that published generations before #911 and
has no binding is different. Its first upgraded writer binds whatever target
it is configured with, and nothing proves the older generations are there: a
`cdc-init`, or a flush of a schema that never published, reads nothing from
the old target. Bind such a deployment explicitly, before upgraded writers
run:

1. Stop every writer and every `manifest-reconcile --gc` of the deployment.
2. Create the binding table ("Creating the binding table").
3. Run a read-only `manifest-reconcile` (no `--gc`) of the upgraded
   `forma-tools` with the writers' flags and environment. It verifies every
   retained and expired manifest and probes the listed objects at the target
   it derives. Go on only if its blocking findings are `storage_not_bound`
   alone, for a missing binding, and it reports no `lost_object`; otherwise
   the flags do not name where the generations are.
4. Run `--bind-storage` with the same flags and environment, and check the
   target it prints.
5. Upgrade and start the writers. Keep `--gc` off until every writer of the
   deployment runs an upgraded build: a writer from before #911 checks no
   binding, and would publish wherever it is configured.

### Bindings made before #911

Before #911, for a client without a base endpoint (`--s3-endpoint` or an
`AWS_ENDPOINT_URL*` override), the derivation left out FIPS, dual-stack and
transfer acceleration settings: a `--bind-storage` run with any of them
enabled recorded the ordinary regional host. Upgraded writers derive the host
they really send to, and fail closed against such a binding. If the writers
use any of those settings, run a read-only `manifest-reconcile` of the
upgraded `forma-tools` with the writers' flags and environment before
upgrading the writers: a `storage_not_bound` finding whose detail names the
ordinary host means the row must be corrected as described next.

### Changing the binding

A binding is only a record of where the generations are. Editing it moves no
object, no manifest and no reader, and a binding that names another target
than where the current and retained generations live makes GC look in the
wrong place. Change it only to correct a binding that is wrong, so that it
names where the deployment's generations actually are:

1. Stop every writer (`cdc-flush`, `cdc-init`, the compactor) and every
   `manifest-reconcile --gc` of the deployment, and wait for runs in flight to
   finish.
2. Delete the row of `<entity_main>_publication_storage`.
3. Run `--bind-storage` with the writers' exact flags and environment, and
   check the target it prints.
4. Start the writers and GC again.

Stopping them is required, not a precaution. Each publication transaction
reads the binding, but a transaction that read the old row before it was
deleted is not fenced by that read and can still commit a generation at the
old target; a GC pass that already read the old binding still deletes
against it.

Moving a deployment's data to another bucket, endpoint or data prefix is not
a rebinding and is not covered here. It needs a migration plan of its own,
verified before writers or GC resume, that keeps every current and retained
generation readable, and readers' configuration, consistent with the new
target. Do not delete the row and restart writers at a new target as a way to
move: GC would then look only at the new target, readers would still read the
old one, and no current or retained generation would have moved.

## Retention and time flags

| Flag | Default | Invariant |
| --- | --- | --- |
| `--retention-grace` (`G`) | `15m` | Must satisfy `G >= R_max + C` (`R_max = 60s` default reader pass deadline). |
| `--clock-margin` (`C`) | `1m` | Must be positive; covers DB/reader/S3 clock skew plus S3 1-second `LastModified` truncation. |
| `--orphan-age` (`A`) | `2h` | Must satisfy `A >= W + C` (`W = 1h` default staging budget). |

## Findings and recovery

Blocking findings refuse all deletions and forgets for the affected schema until
resolved; non-blocking findings report issues while still allowing safe
collection of expired/orphaned garbage.

| Finding | Blocking | Meaning & safe operator action |
| --- | --- | --- |
| `storage_not_bound` | Yes | Missing binding or target mismatch. Writers bind a new deployment on their first publication; to bind ahead of them, run `--bind-storage` once with writers' flags. A deployment that published before #911 must be bound explicitly, as "Upgrading a deployment that has already published" describes. On a mismatch, fix this run's flags/env (`AWS_ENDPOINT_URL_S3`); change the binding only as "Changing the binding" describes. |
| `no_record` | Yes | Missing publication record row. Run `init-db` under the schema lock to recover at a fresh epoch. |
| `record_invalid` | Yes | Corrupt publication record. Never restore a record row from backup; delete the record row, run `init-db` (arms fresh-epoch recovery fence), restart readers/writers, and run `cdc-init`. |
| `missing_lifecycle_epoch` / `lifecycle_epoch_mismatch` | Yes | Missing or mismatched `<entity_main>_publication_epoch` row. Restore lifecycle epoch monotonicity and run fresh-epoch recovery. |
| `history_mismatch` / `missing_generation` (missing row) | Yes | Current generation's history row is missing or disagrees with the record (`Publish` and `Reset` fail closed with `ErrHistoryInconsistent`). Never restore a history row from backup or hand-edit the record; run fresh-epoch recovery (delete record row -> `init-db` -> restart readers/writers -> `cdc-init`). |
| `missing_history` | Yes | Gap in `(floor, current - 1]`. Do not restore backup rows; wait for a later generation above the gap to expire, or run fresh-epoch recovery. |
| `missing_generation` (missing manifest) | Yes | Current manifest missing in S3 (reads fail closed). Re-initialize with `cdc-init`; GC resumes once the lost generation retires and expires. |
| `missing_retained_manifest` | Yes | Superseded retained manifest missing in S3. No impact on new reads; GC resumes once the generation expires. |
| `invalid_manifest` | Yes | Retained or expired manifest failed SHA-256 or structural verification. Restore exact matching bytes or re-initialize with `cdc-init`. |
| `foreign_manifest` | Yes | Manifest key outside `--data-prefix`. Re-run with the writers' `--data-prefix`. |
| `awaiting_init` | No | Lost-record recovery left the schema awaiting `cdc-init`; its generation history survives. Run `cdc-init` for the schema: readers fail closed until its next generation publishes. |
| `lost_object` | No | Listed parquet object confirmed missing by `HEAD`. If current, re-initialize with `cdc-init` (reads fail closed); if superseded, expires after grace. |
| `corrupt_object` | No | `--verify-checksums` found SHA-256 or size divergence. Readers do not hash parquet files and may return incorrect results or scan errors; if current, re-initialize with `cdc-init`. |
| `unrecognized_object` | No | Key the layout's writers do not build (e.g. hand-made or foreign objects). Never deleted by GC; remove manually after verification. |

## Fresh-epoch recovery and the recovery fence

When recovering from a lost/invalid record or a `history_mismatch`:

1. Stop writers for the schema (or allow `ErrHistoryInconsistent` to keep them fenced).
2. Delete the schema's row from `<entity_main>_publication`.
3. Run `init-db`: `InitializeAfterLoss` allocates a fresh epoch above the
   lifecycle epoch, supersedes the surviving head row (leaving `retired_at = 0`),
   and sets `recovery_epoch = rec.Epoch, recovery_retired_at = 0` on
   `<entity_main>_publication_epoch`.
4. Restart readers/writers so they pick up the new epoch, and run `cdc-init`.
5. The first `manifest-reconcile --gc` pass after recovery stamps
   `recovery_retired_at = now()` (post-commit) and deletes nothing until the full
   retention grace `G` elapses after that stamp, protecting any reader pinned
   before recovery even if a concurrent `Reset` or `Publish` runs next.
