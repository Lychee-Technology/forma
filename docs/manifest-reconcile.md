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

1. **Effective bucket URL:** derived directly from the request URLs the AWS SDK
   S3 client itself builds for a `ListObjectsV2` of the bucket and a
   `DeleteObject`, `PutObject` and `GetObject` of a key in it, through the
   client's own routing options: `--s3-endpoint` or `AWS_ENDPOINT_URL_S3` /
   `AWS_ENDPOINT_URL` / the region endpoint, `--s3-use-path` addressing, and
   FIPS, dual-stack and transfer acceleration when the client's configuration
   enables them (`AWS_USE_FIPS_ENDPOINT`, `AWS_USE_DUALSTACK_ENDPOINT`, or the
   shared config's `use_fips_endpoint` / `use_dualstack_endpoint`). The object
   requests must go to one URL and the `LIST` to its bucket path, or the client
   is refused. Custom endpoints with ambiguous paths (repeated slashes such as
   `/tenant//`, trailing slashes on non-root paths, dot segments, escaped
   paths, query/fragment, or userinfo) are rejected rather than collapsed, and
   so is a configuration the SDK cannot route (FIPS over a custom endpoint).
2. **Bucket and data prefix:** `--s3-bucket` and `--data-prefix` (matching
   `cdc-flush` and `cdc-init`'s `-s3-prefix`).
3. **No redirects:** `manifest-reconcile`'s S3 client and the writers' follow
   no redirect (`s3endpoint.WithoutRedirects`). A 301, 302, 303, 307 or 308
   response that carries a `Location` header is refused
   (`ErrRedirectRefused`, exit `1` for `manifest-reconcile`, a failed run for
   a writer) without reading its body or leaking `Location` header
   credentials. Any other 3xx, including one without a `Location`, reaches
   the AWS SDK as an S3 error response, which also fails the request. Either
   way, no request leaves the bound endpoint: no collector `DELETE`, and no
   writer `PUT`, which an HTTP client that followed a 303 would otherwise
   resend as a `GET` whose `200` the SDK takes for the `PUT`'s success,
   publishing an object that was never stored.

### Writers bind it

`cdc-flush`, `cdc-init` and the `compactor` bind the deployment themselves
(#911). Each derives its own target with the same function
`manifest-reconcile` uses for its client (the bucket URL above, its
`-s3-bucket`, and its data prefix), then:

- **before it stages anything,** checks that target against the binding. With a
  binding to another target, it fails, naming both targets
  (`publication.ErrStorageBoundElsewhere`), before it sends anything to S3 or
  moves the record. The binding is kept. A writer publishing into a target the
  collector is not bound to would put objects where garbage collection never
  looks, and a collector bound to the wrong target would judge the writers'
  objects against another bucket.
- **in its publication transaction,** binds a deployment that has no binding
  yet. The bind is a statement of the same transaction as the generation's
  compare-and-set and marks, so the binding becomes durable exactly when the
  generation commits. A run that publishes nothing never binds: an empty
  flush, a compaction of an unpublished schema or one that merges nothing, and
  a run refused by a check a staging makes, such as a compactor configured
  with another prefix than the published one. Neither does a publication that
  fails, whatever stops it: a failed `BEGIN`, an expired staging, a lost
  compare-and-set that may not rebase, a refused or rolled-back `COMMIT`. A
  writer whose `COMMIT` went without an answer takes the deployment as bound
  only once it has established that the publication committed. So a
  misconfigured writer whose publications never commit leaves the deployment
  unbound for the writers that do publish.
- with a binding to the same target, publishes as before.

Two writers of an unbound deployment at different targets may both pass the
check and stage, since neither has bound anything yet. Only one of their
publications can commit the binding. On PostgreSQL the second writer's bind
waits for the first's transaction. On Aurora DSQL it fails with a
serialization conflict, and the writer sends the publication again. Either
way, when the first commits, the second's publication fails with
`ErrStorageBoundElsewhere`, and what it staged stays unpublished, outside the
bound target. If the first rolls back instead, the second binds its own
target. Once a writer has seen the deployment bound to its target, it does not
read the binding again until it restarts.

A mismatch is refused at once, with no warn-only window. A deployment bound
before #911 was bound by an operator who ran `--bind-storage` with the writers'
own flags and environment, so its writers normally derive the same target;
one whose writers derive another target has a binding that garbage collection
would already act on wrongly. One case differs: the derivation before this
release ignored FIPS, dual-stack and transfer acceleration when no custom
endpoint was set, and recorded the ordinary regional host (for example
`https://my-bucket.s3.us-west-2.amazonaws.com`) even for a client that sends
to `https://my-bucket.s3.dualstack.us-west-2.amazonaws.com`. See
[Checking a binding made before #911](#checking-a-binding-made-before-911).

A writer whose S3 client destination cannot be told (a custom endpoint
resolver, an ambiguous custom endpoint path, see above) fails the same way
before it stages, and so does a writer whose AWS SDK client sends through an
HTTP client of a type whose redirect handling cannot be replaced: only the
SDK's default HTTP client and a plain `*http.Client` can be made to follow no
redirect. A writer given a client that is not the AWS SDK's own
(`*s3.Client`), as an embedded caller of `internal/cdc` may, binds and checks
nothing: bind such a deployment with `--bind-storage`.

A deployment without the binding table fails its writers with
`publication.ErrStorageTableMissing`, before they stage:

- **PostgreSQL:** run `init-db` again; it creates the table with the other
  publication tables. Rerun it before upgraded writers publish.
- **Aurora DSQL:** `init-db` cannot run on DSQL yet (#690). Create the table with
  its DSQL statement, on its own, as [Operating the CDC tools on
  DSQL](aurora-dsql.md#operating-the-cdc-tools-on-dsql) gives it. Or run
  `--bind-storage` (below) with `--pg-engine aurora-dsql`: it creates the
  table, each DDL in its own transaction, and also binds the deployment to
  that run's target, so run it only with the writers' exact S3 flags and
  environment.

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

### Checking a binding made before #911

A binding made by `--bind-storage` before this release recorded the bucket URL
without FIPS, dual-stack or transfer acceleration. When the writers and the
collector run without those options, nothing changes. Otherwise:

1. Run `--bind-storage` with the writers' exact flags and environment. It
   prints the binding when it matches. When it fails with
   `ErrStorageBoundElsewhere`, compare the two targets it names.
2. If they differ only in the endpoint host, and that host is the FIPS,
   dual-stack or accelerate form of the same bucket in the same region (the
   bucket name, the data prefix and the region match), both URLs reach the
   same objects. Rebind by the procedure below. No object moves: steps 4 and
   5 do not apply.
3. If anything else differs, the binding names another bucket, prefix or
   endpoint than the one the writers publish into. Do not rebind to make the
   error go away: find out which target holds the live generations, and treat
   it as a move (below).

Upgraded writers refuse to publish under a binding that names the other host,
so check before upgrading them.

### Changing the binding

An existing binding to a different target is never overwritten, by any
writer or by `--bind-storage`. The binding only names where garbage collection
deletes and where writers may publish. Editing it moves no object and changes
no reader's configuration: the current generation and every retained one stay
at the target they were published to, and readers keep reading them there.

To rebind (the endpoint spelling above, or a move):

1. Stop every writer (`cdc-flush`, `cdc-init`, `compactor`) and every
   `manifest-reconcile --gc` of the deployment, and wait for runs in flight to
   finish. A writer that has already seen the binding keeps publishing to its
   own target without reading the binding again, so it must not be running
   when the binding changes.
2. Delete the row in `<entity_main>_publication_storage`.
3. Run `--bind-storage` with the new target's exact flags and environment.
4. For a move to another bucket or endpoint, migrate the data before anything
   resumes, by one of the two plans below. Deleting the row and restarting the
   writers is not a migration: the old target's objects stay where they are,
   garbage collection under the new binding never reclaims them, and a reader
   or the collector that looks for them at the new target fails.
5. Only then start the writers with the new target, and `--gc` last, no
   earlier than its plan allows.

Keep the data prefix when you move. A manifest key names its prefix, so
every generation published under the old prefix stays there. Under a new
`--data-prefix`, garbage collection reports each of them as `foreign_manifest`,
retained or expired. That finding blocks, so the collector neither deletes nor
forgets for the schema. This procedure therefore does not cover a prefix
change.

**Copy every retained generation (preferred).** A generation stays retained
until garbage collection forgets its history row: the current one, each one
superseded less than the retention grace `G` ago, and each expired one not yet
collected. Readers pinned to any of them, and the collector, which verifies
the manifests and probes the objects of every one of them, still need it.

1. With writers and `--gc` stopped, copy every object under the data prefix,
   manifests and parquet files, from the old target to the new one, with the
   same keys and the same bytes.
2. Run `manifest-reconcile` against the new target (without `--gc`) and then
   with `--verify-checksums`. Go on only when they report no
   `missing_generation`, `missing_retained_manifest`, `invalid_manifest`,
   `lost_object` or `corrupt_object`.
3. Configure every reader for the new target before any writer resumes.
   Leave the old target as it is until no reader is configured for it any more
   and the reader pass deadline (`R_max`) has passed: a pass that started
   before the switch still reads there. Its record is unchanged, since no
   writer runs, and its manifest is at both targets.

**Re-initialize, then let the old generations expire.** Use this plan when the
objects cannot be copied. `cdc-init` at the new target publishes a new current
generation for a schema there. It copies none of the older generations: their
manifests and objects stay at the old target only, and they stay retained
until the grace `G` after the re-init superseded them.

A reader takes the current manifest's key from the record and reads it at the
one target it is configured for. Once a schema's `cdc-init` commits, its
record names a manifest that exists only at the new target, and until then it
names one that exists only at the old target. No reader configuration reads
every schema throughout, so this plan takes the deployment's reads offline:
readers are stopped before the first re-init and started again only after the
last. When reads cannot stop, use the copy plan.

1. Keep the old target and every object in it readable.
2. Stop every reader of the deployment, every process that serves federated
   reads of its schemas, and wait until each one has exited. A pass ends by
   its deadline `R_max`, so a reader that drains its passes when it stops
   exits within `R_max` of the stop. A reader still running when the first
   re-init commits fails its next pass of that schema: the record then names a
   manifest its target does not hold.
3. Run `cdc-init` for every schema, with the new target's exact flags and
   environment.
4. Run `manifest-reconcile` against the new target (without `--gc`). Go on only
   when it reports no `missing_generation` and no `lost_object` for any
   schema: every schema's current generation is then at the new target. It
   reports `missing_retained_manifest` for the generations the re-inits
   superseded, as expected: their manifests are at the old target only.
5. Configure every reader for the new target, and only then start them. Do not
   start one with the old configuration: it fails every pass, since no current
   manifest is at the old target any more.
6. Do not run `--gc` until every generation published at the old target has
   expired, that is, until `G` has passed since the last schema's re-init. Run
   earlier, the collector finds their manifests missing at the new target and
   reports `missing_retained_manifest`, a blocking finding. Once they have
   expired, `--gc` forgets them without deleting anything at the old target.
7. Delete the old target's objects by hand, and only after `--gc` has
   forgotten every generation published there and no reader or writer is
   configured for it any more.

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
| `storage_not_bound` | Yes | Missing binding or target mismatch. A missing binding is written by the writers' first publication, or by `--bind-storage` run with the writers' flags; for a mismatch, fix the run's flags/env (`AWS_ENDPOINT_URL_S3`, `AWS_USE_DUALSTACK_ENDPOINT`, `AWS_USE_FIPS_ENDPOINT`), or see [Checking a binding made before #911](#checking-a-binding-made-before-911) and [Changing the binding](#changing-the-binding). |
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
