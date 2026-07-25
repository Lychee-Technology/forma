# Error Handling

## Overview

Forma uses two error classes in the schema/metadata pipeline.

## Write-path validation errors

Write operations wrap `forma.ErrInvalidInput` when the caller provides data that
cannot be accepted.

Examples:

- unknown write attribute names in `transformer.flattenToAttributes`
- invalid value conversion in `populateTypedValue`
- explicit `null` writes to schema-defined fields

These errors are intended to surface as user-facing `4xx` responses.

## Read-path consistency errors

Read operations return plain errors when persisted data and metadata disagree.

Examples:

- unknown attribute IDs encountered while rebuilding entities from EAV rows
- duplicate schema IDs or duplicate attribute IDs during metadata loading
- storage column mismatches such as a text attribute stored in `value_numeric`

These errors indicate metadata drift, corrupted state, or an incomplete
deployment, and should be treated as operator-visible consistency failures.

### `ErrParquetSetInconsistent`

`forma.ErrParquetSetInconsistent`, carried by
`forma.ParquetSetInconsistentError{SchemaID, MissingKeys}`, marks a
federated read whose manifest lists parquet objects that do not exist in
storage. The manifest is the authoritative record of the schema's cold/warm
tier, so a listed-but-absent object means that tier **has lost data** — the
rows in that object are simply gone from the result set.

- **Plain error, not `ErrInvalidInput`.** Nothing about the request is wrong;
  persisted data and the metadata indexing it disagree. It is an
  operator-visible consistency failure, not a user-facing `4xx`.
- **Not degradable.** It surfaces even under
  `federated.allow_partial_degraded_mode`, unlike the transient DuckDB/S3
  failures that fall back to Postgres-only. Degrading here would return
  exactly the silently short answer this classification exists to make loud.
- **Trigger precondition.** Only reachable when the server resolves parquet
  paths from the manifest source (`duckdb.manifestTemplate` configured — see
  `docs/federated-query/design.md` §4.3.1). With per-request path hints, or
  with glob paths, the classification is skipped: a hinted path set whose
  object is missing still fails the scan, but as a plain
  `ErrFederatedReadFailed`, while a glob quietly expands to whatever objects
  survive and returns a shorter result set with no error at all.
- **Direction of the contract.** Only `manifest ⊆ live objects` is enforced.
  Extra unlisted objects are tolerated and invisible to reads.
- **Operator action.** The message names the schema and the missing
  bucket-relative keys. Run the reconciliation tool to diagnose and repair:
  see `docs/manifest-reconcile.md`.

### `ErrNoParquetPaths`

`forma.ErrNoParquetPaths`, carried by
`forma.NoParquetPathsError{SchemaID, SourceConfigured}`, marks a
DuckDB-routed federated read whose parquet path set resolved **empty**: no
per-request render hint, and either no configured parquet source or a source
whose manifest lists no files while the fallback glob is disabled (see
`docs/federated-query/design.md` §4.3.1).

- **Plain error, not `ErrInvalidInput`.** With a per-request hint the caller
  can be at fault and the error wraps `ErrInvalidInput` instead (an
  unrenderable or degenerate `s3_parquet_path_template`). Reaching *this*
  error means no hint was supplied, so the read surface — server
  configuration, or manifest state — is what cannot answer the query.
- **Not degradable.** Like `ErrParquetSetInconsistent`, it surfaces even under
  `federated.allow_partial_degraded_mode`. Every query that reaches the DuckDB
  engine wants warm and/or cold data (hot-only and `prefer_hot` requests
  short-circuit to Postgres before this point), so a Postgres-only fallback
  would be silently short precisely where the cold tier was requested.
- **Why it is not a read failure.** Before #299 the empty set rendered
  `read_parquet(<no value>)`, and the resulting DuckDB parser error classified
  as the degradable `ErrFederatedReadFailed` — loud, but indistinguishable
  from a transient S3 outage to any programmatic discriminator, so degraded
  mode turned a configuration mistake into a quietly incomplete answer.
- **Not a normal empty state.** A never-flushed schema resolves the level-3
  fallback glob, not an empty set; reaching this error means the fallback is
  disabled too (`duckdb.s3DataPrefix` empty).
- **Operator action.** The message names the schema and distinguishes the two
  causes: configure the read surface (`duckdb.manifestTemplate` +
  `duckdb.s3Bucket`), set `duckdb.s3DataPrefix` to re-enable the fallback
  glob, or repair the schema's manifest (`docs/manifest-reconcile.md`).

### `ErrManifestSchemaMismatch`

`forma.ErrManifestSchemaMismatch`, carried by
`forma.ManifestSchemaMismatchError{RequestedSchemaID, ManifestSchemaID, Path}`,
marks a manifest object whose recorded `schema_id` disagrees with the schema
being read. It is raised before any path reaches a scan.

- **Why it is loud rather than a short read.** A manifest addresses one schema
  by path convention alone. Nothing downstream re-checks it: the parquet scan
  does not filter rows by schema (files are per-schema by path) and the
  projection stamps whatever it scans as the *requested* schema. So a path
  collision between two schemas would not merely under-read — it would serve
  another schema's rows under this schema's identity.
- **Not degradable.** Cross-schema contamination is the opposite of a partial
  answer, so `allow_partial_degraded_mode` does not absorb it. It is also not
  relabelled as `ErrFederatedReadFailed` on the way out of path resolution,
  which would have handed it to the degraded fallback.
- **Relationship to config validation.** `duckdb.manifestTemplate` is
  probe-validated at startup against two schema IDs, which catches a collapsed
  template (a constant path, a `{{.SchemaId}}` typo) but cannot prove
  injectivity over the whole schema domain. This error is the runtime
  enforcement of what that check can only sample.
- **Compatibility.** A manifest whose `schema_id` is zero is treated as
  unstamped rather than as schema 0 — schema IDs are always positive, and
  rejecting zero would break reads for deployments still holding objects
  written before the field existed.
- **Operator action.** The message names both schema IDs and the manifest key.
  Fix the template so each schema resolves a distinct object, then repair the
  affected manifests (`docs/manifest-reconcile.md`).

## Public HTTP error surface

`internal/httpapi` treats the response body as an untrusted destination. The
split follows the two error classes above, and is enforced by
`respondErrorWithStatus` in `internal/httpapi/error_response.go`. That is the
only gate **for manager-layer errors**: `respondError` merely classifies and
delegates to it, and `executeGet` (`internal/httpapi/server.go:175`) calls
`respondErrorWithStatus` directly so it can choose its own 404 wording.

Handlers also call `writeError` directly — 33 sites in `server.go` — and those
bodies are verbatim without passing the gate. They are safe because every one of
them reports a request-parsing failure (`parsePath`, `readEntityJSONBody`,
`parseUUID`, `parseCreateObjects`, `parseSortParams`); none touches the manager,
the engine, S3, or `PG_CONN`. What holds that line is the source-level guard
`TestWriteErrorAlwaysCarriesALiteral4xxStatus`
(`internal/httpapi/error_leak_test.go`), which fails the build unless every
direct site passes a literal 4xx constant from an allowlist — so a new handler
cannot echo a runtime-classified status without going through `respondError`.

**Disclosure is decided by the error, not by the status.** A body is verbatim
only when the error *provably* wraps a client sentinel — `errors.Is` against
`forma.ErrInvalidInput`, `forma.ErrNotFound`, or `forma.ErrConflict`
(`isClientError`). Everything else is redacted, whatever status it carries.

The full condition is `status < http.StatusInternalServerError &&
isClientError(err)`. The status conjunct is belt-and-braces, not the decision:
it cannot promote anything to verbatim, only hold it back. A caller that passes
an explicit 5xx to `respondErrorWithStatus` gets a redacted body even if the
error wraps a sentinel.

This is deliberate, and the naive version was wrong. `classifyManagerError`
derives the HTTP status by substring-matching the whole error chain, and driver
text trips those probes: DuckDB reports a missing S3 object as
`HTTP Error: … 404 (Not Found).`, which contains `not found`. Gating disclosure
on the status would therefore have classified the single most likely #301
scenario as 4xx and echoed the S3 URL — and, on a `postgres_scan` attach failure,
the password — straight back to the client.

**The HTTP status is unchanged** by redaction: a misclassified read-path error
still returns its classified status, just with an opaque body.

**Redacted bodies (#301)** carry a fixed message, a stable `error_class` token,
and an `error_id`. No error text crosses. There are exactly two fixed messages
(`publicErrorMessage`): `internal read error` for the three typed read-path
classes, and `internal error` for `errorClassInternal`. **`internal` is the
common case in production** — it absorbs `ErrFederatedReadFailed`,
`ErrPostgresReadFailed`, metadata drift, and transform failures — so a client
asserting on the literal `internal read error` would break on the majority of
redacted responses. Discriminate on `error_class`, never on `error`. The full chain goes to
`zap.S().Errorw` — *always*, whatever the status, because `cmd/server` runs
`zap.NewProduction()` at Info level and routing a redacted 4xx to `Debugw` would
have leaked nothing but recorded nothing either. An operator retrieves the detail
from the `error_id` the caller quotes.

**Accepted cost — the redacted 4xx.** An error that classifies 4xx by heuristic
alone and wraps no sentinel now gets an opaque body. The known instance is #296:
`cannot sort by unknown attribute 'nope' in schema 'lead'`
(`internal/entity_query_sort.go:84`) wraps nothing, so it keeps its correct
`400` but answers `{"error": "internal error", "error_class": "internal",
"error_id": …}` — the guidance no longer reaches the caller. This is the most
common case where a 4xx body carries `error_class` and `error_id`, not the only
one: *every* heuristic-classified status that wraps no sentinel carries both,
including the DuckDB `404 (Not Found)` case above. `error_class` on a 4xx
therefore does not identify an unknown sort attribute — do not build retry or UX
logic on that inference. It is pinned by
`TestRespondErrorRedactsHeuristicOnly4xx`. `classifyManagerError`'s trigger-word
list is the worklist of call sites that should start wrapping
`forma.ErrInvalidInput`. An opaque validation message is strictly better than a
leaked credential.

```json
{
  "success": false,
  "error": "internal read error",
  "error_class": "parquet_set_inconsistent",
  "error_id": "9f2c1a7e-…"
}
```

That example is the `parquet_set_inconsistent` shape; the far more frequent one
pairs `"error_class": "internal"` with `"error": "internal error"`.

Classes: `parquet_set_inconsistent`, `no_parquet_paths`,
`manifest_schema_mismatch`, and `internal` for everything else. They resolve via
`errors.Is`, never message text.

### Why an allowlist

Redaction cannot be a blocklist of known-sensitive error types, because the
sharpest leak does not come from one. The federated template interpolates
`postgres_scan('{{.PG_CONN}}', …)`, and `PG_CONN` is built by
`federated.DuckDBPostgresConnStringFromPool` as
`host=… user=… password=… dbname=…`. When DuckDB cannot attach, its own message
is:

```
IO Error: Unable to connect to Postgres at "host=… user=… password=… dbname=…": …
```

That text is driver-authored, so only a deny-by-default rule contains it. A
missing parquet object likewise yields the full `s3://bucket/prefix/key` and the
resolved endpoint URL.

The success path has the matching rule: `toExecutionPlan`
(`internal/entity_query_service.go:138`) allowlists plan fields and drops
`src.SQL`, `src.Params`, `plan.Notes`, and `merge.Notes` for the same reason —
the source SQL embeds the `postgres_scan` connection string, and notes can echo
raw engine errors. Pinned by `TestToExecutionPlan_DoesNotLeakCredentials`.

### Accepted disclosures inside the allowlist

The allowlist is a gate on *provenance*, not on content: an error that wraps a
client sentinel is disclosed verbatim. Two live cases put more than the caller's
own input into a 4xx body. Both are accepted, not bugs — recorded here so the
boundary is stated rather than discovered.

- **Postgres driver prose on a 409.**
  `internal/postgres_persistent_repository_main_table.go:25` wraps `pgErr.Detail`
  with `forma.ErrConflict`, so a unique-violation body carries driver-authored
  text naming physical columns, e.g.
  `Key (schema_id, row_id)=(…) already exists.` No credentials or object keys
  cross, but this is the one remaining case where driver text reaches a public
  body. If the detail ever needs to stop leaking the physical column layout, the
  fix is to summarise at the wrap site, not to widen the redaction gate.
- **Schema identifiers on a 404.** The eleven `forma.ErrNotFound` chains in
  `internal/schemameta/file_registry.go:199-299` split two ways. Four are
  name-keyed (`:222`, `:227`, `:276`, `:281`) and echo only the schema name the
  caller put in the URL — nothing is disclosed that the caller did not supply.
  The other seven (`:199`, `:204`, `:209`, `:242`, `:247`, `:294`, `:299`) are
  ID-keyed and render the internal `int16`, e.g. `schema not found for ID: 402`.
  A caller never supplies that: the URL carries a schema *name* and the system
  resolves the ID internally, so this genuinely exposes an internal identifier.
  It is accepted because a schema ID is a low-value opaque integer — not a
  credential, not a storage path, and not something an attacker can act on
  without already holding the access the 404 just denied. Worth naming because
  "no schema id" is a stated constraint for *redacted* bodies, and this is
  exactly where that constraint stops applying.

### Status change: create errors are now classified

`handleCreate` previously answered every `BatchCreate` failure with a hardcoded
`500`. It now calls `respondError`, so create failures answer their *classified*
status like every other handler. This is a public contract change, and it is
independent of redaction.

The clearest case is an unknown schema. `POST /api/v1/nosuchschema` returns
**`404` instead of `500`**, and its body is **verbatim, not redacted**:

```json
{
  "success": false,
  "error": "batch create failed: operation[0]: failed to get schema: schema not found: nosuchschema: not found"
}
```

That follows from the gate rather than contradicting it. The chain built by
`file_registry.go:222` wraps `forma.ErrNotFound`, and `batchCreateAtomic` wraps
that in turn, so `classifyManagerError` returns `404` from its **sentinel**
branch and `isClientError` is true — the verbatim branch, logged at `Debugw`,
with no `error_class` and no `error_id`. The substring fallback is never
consulted.

Clients keying on `500` to detect create failures must key on `success: false`
plus the status instead.

### Known gap

Credentials still reach error strings *inside* the process, so a Go embedder
using `factory.NewEntityManager*` can capture them in its own logs. Scrubbing at
the engine's error wraps is tracked separately.

## Message style

Use explicit messages that name:

- the logical value type
- the column or attribute that is wrong
- the expected state

Examples:

- `storage type mismatch for numeric: value_text should not be populated (expected value_numeric)`
- `unknown attribute id 999 for schema 402 (attribute not in metadata cache)`
- `attribute 'age' (attrID=2): invalid value: invalid input: cannot convert string to float64`
