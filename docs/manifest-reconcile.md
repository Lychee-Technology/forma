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
  client uses (or checks it against an existing binding), and exits. Optional:
  the V2 writers bind the deployment on their first publication. Cannot be
  combined with `--gc`.
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

Garbage collection deletes nothing until the deployment is bound, in
`<entity_main>_publication_storage`, to the exact S3 storage target its writers
publish into. `init-db` creates the table. A target is three values:

1. **Effective bucket URL:** derived directly from the AWS SDK S3 client's
   `ListObjectsV2` and `DeleteObject` request URLs (`--s3-endpoint` or
   `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` / region endpoint, with
   `--s3-use-path` addressing). Custom endpoints with ambiguous paths (repeated
   slashes such as `/tenant//`, trailing slashes on non-root paths, dot
   segments, escaped paths, query/fragment, or userinfo) are rejected rather
   than collapsed.
2. **Bucket and data prefix:** `--s3-bucket` and `--data-prefix` (matching
   `cdc-flush` and `cdc-init`'s `-s3-prefix`).
3. **No redirects:** `manifest-reconcile`'s S3 client follows no redirect.
   A 301, 302, 303, 307 or 308 response that carries a `Location` header is
   refused (`ErrRedirectRefused`, exit `1`) without reading its body or
   leaking `Location` header credentials. Any other 3xx, including one without
   a `Location`, reaches the AWS SDK as an S3 error response, which also fails
   the pass with exit `1`. Either way, destructive requests never leave the
   bound endpoint.

### Writers bind it

`cdc-flush`, `cdc-init` and the `compactor` bind the deployment themselves
(#911). Before a run stages anything, the writer derives its own target with
the same function `manifest-reconcile` uses for its client (the bucket URL its
AWS SDK client sends a `LIST` and a `DELETE` to, its `-s3-bucket`, and its data
prefix), then:

- with no binding yet, binds the deployment to that target;
- with a binding to the same target, publishes as before;
- with a binding to another target, fails before it stages or publishes,
  naming both targets (`publication.ErrStorageBoundElsewhere`). The binding is
  kept. A writer publishing into a target the collector is not bound to would
  put objects where garbage collection never looks, and a collector bound to
  the wrong target would judge the writers' objects against another bucket.

A mismatch is refused at once, with no warn-only window. A deployment bound
before #911 was bound by an operator who ran `--bind-storage` with the writers'
own flags and environment, through the same derivation, so its writers derive
the same target; one whose writers derive another target has a binding that
garbage collection would already act on wrongly.

A writer whose S3 client destination cannot be told (a custom endpoint
resolver, or an ambiguous custom endpoint path, see above) fails the same way
before it stages. A writer given a client that is not the AWS SDK's own
(`*s3.Client`), as an embedded caller of `internal/cdc` may, binds and checks
nothing: bind such a deployment with `--bind-storage`.

A deployment whose `init-db` ran before the table existed has no binding
table, and its writers fail with `publication.ErrStorageTableMissing` until
`init-db` (or `--bind-storage`, which also creates it) runs again.

### `--bind-storage`

`--bind-storage` binds the deployment to the target of the run's own client, or
checks that target against an existing binding, and exits. Use it to bind ahead
of the writers' first publication, or to confirm that the flags and environment
the collector runs with name the writers' target. Run it with the writers'
exact S3 flags and environment:

```bash
forma-tools manifest-reconcile \
  --bind-storage \
  --pg-host ... --pg-db ... --pg-user ... --pg-engine postgresql \
  --schema-registry-table schema_registry \
  --entity-main-table entity_main \
  --s3-bucket my-bucket --data-prefix data
```

An existing binding to a different target is never overwritten, by either
path. To move a deployment to a new bucket, endpoint or prefix, stop its
writers, delete the row in `<entity_main>_publication_storage` by hand, then
either start the writers with the new target (their first run binds it) or run
`--bind-storage` with it first. Writers still running with the old target fail
closed once another target is bound.

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
| `storage_not_bound` | Yes | Missing binding or target mismatch. A missing binding is written by the writers' first publication, or by `--bind-storage` run with the writers' flags; for a mismatch, fix the run's flags/env (`AWS_ENDPOINT_URL_S3`). |
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
