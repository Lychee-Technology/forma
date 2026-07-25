# Redact operator detail from public HTTP error bodies

**Issue:** [#301](https://github.com/Lychee-Technology/forma/issues/301)
**Epic:** [#304](https://github.com/Lychee-Technology/forma/issues/304), Phase 1 (#250 residuals)
**Date:** 2026-07-25

## Problem

`internal/httpapi/server.go` writes `fmt.Sprintf("<op> failed: %v", err)` into the
response body at every manager error site, so the entire error chain crosses to
the API client.

The issue records this as an internal-storage-layout leak, reachable for the
first time via #250's `ParquetSetInconsistentError{SchemaID, MissingKeys}`. The
audit found the leak is wider than recorded, and that the sharpest item in it is
not one of our error types.

### Evidence

The federated DuckDB template interpolates
`postgres_scan('{{.PG_CONN}}', …)` (`internal/sqlgen/advanced_query_template_duckdb.go:41,76,77,87`),
and `DuckDBPostgresConnStringFromPool` (`internal/federated/engine.go:477`)
builds that parameter as `host=… port=… user=… password=… dbname=…`.

Running the three realistic failure shapes against the pinned driver
(`github.com/duckdb/duckdb-go/v2 v2.5.6`) produces:

```
IO Error: Unable to connect to Postgres at "host=db.internal port=5432
user=forma password=SUPERSECRET dbname=forma": could not translate host name …

Parser Error: syntax error at or near "<"
LINE 1: SELECT * FROM read_parquet(<no value>) x JOIN postgres_scan('host=…

HTTP Error: Unable to connect to URL
"https://forma-prod.s3.amazonaws.com/lakehouse/base/schema_22/part-0001.parquet":
404 (Not Found).
```

The first message contains the **Postgres password verbatim** in the driver's own
prose — not merely in the truncated `LINE 1:` echo. That text is wrapped at
`internal/federated/duckdb_query_execute.go:49` and reaches the client through
`writeError(w, classifyManagerError(err), fmt.Sprintf("query failed: %v", err))`
(`internal/httpapi/server.go:236`).

So the public 5xx surface currently leaks, in order of severity:

| Origin | Leaked to client |
| --- | --- |
| DuckDB `postgres_scan` attach failure | `host=`, `user=`, **`password=`**, `dbname=` |
| DuckDB parquet read failure | full `s3://bucket/prefix/key` + resolved S3 endpoint URL |
| `ParquetSetInconsistentError.MissingKeys` | every bucket-relative object key |
| `NoParquetPathsError` | server configuration key names |

**This is why the fix must be an allowlist.** The password originates in driver
text, not in a Forma error type, so no blocklist of known-sensitive typed errors
would have caught it.

### Two supporting findings

1. **There is no error logging to relocate detail into.** Handlers log requests
   (`zap.S().Infow`) but every error path calls only `writeError` — the package
   contains zero `Errorw`. Redacting the body without adding operator logging
   would destroy the detail rather than move it.
2. **The success path is already fixed; this is its error-path counterpart.**
   `toExecutionPlan` (`internal/entity_query_service.go:143-147`) carries an
   explicit `// SECURITY:` allowlist dropping `src.SQL` / `src.Params` /
   `plan.Notes`, for the same reason — "the DuckDB source SQL embeds the
   postgres_scan connection string (with the DB password)". Only a comment
   protects it; no test does.

## Decisions taken

These were settled during design. Do not re-litigate during implementation.

1. **Deny-by-default at the HTTP seam.** All 5xx bodies become a generic payload;
   4xx keeps its verbatim message. Chosen over a per-type allowlist (which
   leaves the password leaking) and over an engine-level credential scrub
   (deferred — see [Follow-up](#follow-up)).
2. **`handleCreate` routes through `classifyManagerError`.** Its hardcoded 500
   (`server.go:112`) currently returns write-path validation errors as 500, so
   blanket redaction would make create failures opaque. Reclassifying restores
   the documented contract (write-path validation → 4xx, `docs/error-handling.md`)
   and keeps the message. Accepted behavior change: **invalid create payloads
   return 400 instead of 500.** Same reclassification shape as epic item #296.
3. **Body carries `error_class` + `error_id`.** A stable machine token for
   discrimination, plus a per-error UUID echoed on the log line, so support can
   grep the exact entry from a token the caller quotes. Correlating by timestamp
   and schema alone is unreliable under concurrent traffic.
4. **`ErrParquetSetInconsistent` is promoted to the root `forma` package**,
   following the #299 precedent (see below).

### Decision 3 reversed after review: the body also carries `schema_id`

Decision 3 above is kept as written, because it is what the design settled on and
what shipped through review round 2. It is superseded, not deleted.

The originating issue #301 asked for "error class + schema id, no object keys".
Design excluded the schema id, and `docs/error-handling.md` recorded "no schema
id" as a stated constraint on redacted bodies. Review flagged that as a deviation
from the originating spec requiring the issue owner's sign-off, and **the issue
owner ruled that the redacted body should carry `schema_id`.**

- `APIResponse` gains `SchemaID int16` with `json:"schema_id,omitempty"`.
  `omitempty` is a lossless "absent" encoding here only because schema IDs are
  always positive in this codebase — the same invariant that makes a manifest
  `schema_id` of zero mean *unstamped* rather than schema 0.
- `errorSchemaID(err)` resolves it with `errors.As` from
  `ParquetSetInconsistentError.SchemaID`, `NoParquetPathsError.SchemaID`, and
  `ManifestSchemaMismatchError.RequestedSchemaID` — the schema the client asked
  to read, deliberately not the foreign `ManifestSchemaID` stamped on the object.
  `errors.As` rather than a type assertion, because these carriers arrive wrapped
  several levels deep.
- Populated on the **redacted branch only**, so verbatim 4xx bodies serialize
  byte-identically to pre-#301.
- Logged as its own structured field when non-zero, so operators filter on
  `schema_id` rather than parsing the message.

The rationale is the one decision 3's exclusion never actually contradicted: a
schema ID is a low-value opaque integer, and `docs/error-handling.md` already
accepted one crossing verbatim on ID-keyed 404s. Carrying it lets a client
correlate a redacted failure without an operator round-trip.

## Contract

**Revised during implementation.** This section originally said redaction keys
off the classified status. That was wrong, and the review caught it:
`classifyManagerError` derives the status by substring-matching the whole chain,
and driver text trips those probes in practice. DuckDB renders a missing object as
`HTTP Error: … 404 (Not Found).` — which contains `not found`, classifies **404**,
and would have taken the verbatim branch, leaking the S3 URL. A password-bearing
chain was reproduced doing the same. The most likely #301 scenario would still
have leaked.

So the two decisions are **decoupled**:

- **Status** is whatever `classifyManagerError` says.
- **Disclosure** requires *positive* evidence that the caller is at fault:
  `errors.Is` against `ErrInvalidInput`, `ErrNotFound`, or `ErrConflict`. Anything
  else is redacted, whatever its status.
- **Every redacted response logs at `Errorw`**, whatever its status. `cmd/server`
  runs `zap.NewProduction()` at Info level, so routing a redacted 4xx to `Debugw`
  would have leaked nothing but recorded nothing either.

**Revised again in review round 2.** The paragraphs above describe the state
mid-implementation and are kept because the reasoning that got there is still the
reason disclosure is gated on sentinels rather than on status. What shipped goes
one step further: the substring heuristic was deleted outright, so
`classifyManagerError` reads sentinels only and anything without one is `500`.

Two things forced that. Redacting the body does not repair the *status*: a client,
cache or alerting rule that receives `404` for an S3 failure concludes the resource
is absent and may stop retrying or cache the negative result — and #301 asked for
read-path consistency errors to answer a generic 5xx, which `AGENTS.md` backs.
Separately, the heuristic was the last site classifying an error by string
comparison, which `AGENTS.md` forbids.

The accepted cost recorded here — "an error that classifies 4xx by heuristic alone
and wraps no sentinel now gets an opaque body" — **no longer applies**, and its
premise that #296 was the only such site was disproved. Six sites across four
packages depended on the heuristic, including the write-path `missing required
attribute` check; all now wrap `ErrInvalidInput` with their message text
unchanged. The maintained list is the table in `docs/error-handling.md`.

### 4xx — verbatim when provably a client error

These describe caller-supplied input and the caller needs to know what to fix.
The write path touches neither S3 nor `PG_CONN`. Body *text* is unchanged and the
`error_class`/`error_id` fields stay absent.

One status code moves as a consequence of decision 2: an invalid create payload
returns the same `batch create failed: <detail>` body under 400 instead of 500.
No body text changes anywhere.

### 5xx — deny-by-default

```
HTTP 500
{
  "success": false,
  "error": "internal read error",
  "error_class": "parquet_set_inconsistent",
  "error_id": "9f2c1a7e-…"
}
```

No error text crosses the boundary. The full chain goes to the log:

```
ERROR request failed  error_id=9f2c1a7e-… error_class=parquet_set_inconsistent
  schema=orders op="query failed"
  error="execute duckdb query: schema 22 manifest lists 2 parquet object(s)
         missing from storage: base/schema_22/p1.parquet, …"
```

`error_class` and `error_id` are `omitempty` on `APIResponse`, so success and 4xx
responses serialize exactly as before. Adding fields is backward compatible for
clients.

**Superseded by the decision-3 reversal above.** The shipped 5xx body also
carries `"schema_id": 22` when the chain holds a typed read-path carrier, and
omits the key entirely when it does not. The log line gains a matching
`schema_id=22` field. The block above is left as the design-time shape; the live
contract is `docs/error-handling.md`.

## Error class vocabulary

Resolved with `errors.Is` / `errors.As` only — never string matching, per the
root package contract.

| Token | Matched by | Public message |
| --- | --- | --- |
| `parquet_set_inconsistent` | `forma.ErrParquetSetInconsistent` | `internal read error` |
| `no_parquet_paths` | `forma.ErrNoParquetPaths` | `internal read error` |
| `manifest_schema_mismatch` | `forma.ErrManifestSchemaMismatch` | `internal read error` |
| `internal` | default | `internal error` |

`internal` deliberately absorbs `ErrFederatedReadFailed`, `ErrPostgresReadFailed`,
metadata drift, and transform failures. A client-facing retryability taxonomy is
a separate design; #301 is about redaction, and `internal` is safe for all of
them.

### Promotion of `ErrParquetSetInconsistent`

`ErrParquetSetInconsistent` and `ParquetSetInconsistentError` currently live only
in `internal/federated/errors.go:37-74`. `internal/httpapi` must not import
`internal/federated` — that would pull DuckDB CGO into the HTTP package's test
build, which is pure Go and fast today.

Both move to the root `forma` package and are aliased back, which is exactly what
#299 already did for the sibling errors. The rationale is recorded verbatim at
`errors.go:45-51`:

> They must be matchable by embedders that reach the engine through
> `factory.NewEntityManager*` and cannot import an internal package […] These
> are aliases, not copies: `errors.Is`/`errors.As` behave identically whether a
> caller reaches for the `forma.*` or `federated.*` name.

Mechanically:

- `forma` gains `ErrParquetSetInconsistent`, `ParquetSetInconsistentError` (with
  its `Error()` and `Unwrap()`), joining the existing read-path lakehouse error
  block.
- `internal/federated/errors.go` keeps
  `var ErrParquetSetInconsistent = forma.ErrParquetSetInconsistent` and
  `type ParquetSetInconsistentError = forma.ParquetSetInconsistentError`,
  extending the existing alias block at `errors.go:52-60`.
- Type aliases mean every existing construction site and test — including
  `internal/federated/parquet_source.go:93`, `errors_test.go:125`,
  `parquet_source_test.go:111`, and the two production e2e tests — compiles
  unchanged.

Additive public API. No breaking change.

## Code shape

### New file

`internal/httpapi/error_response.go` holds the whole error-response concern:
`classifyManagerError` and `writeError` move there, joined by the new code.

`server.go` drops 747 → roughly 700 lines. **This remains over the 500-line cap.**
The file is a tracked violator folded into #220, which the epic sequences
*after* #301 and #282 precisely because both edit it. Extracting the error
concern into its own file pre-carves part of that seam instead of adding to the
violation; #220 finishes the job.

### The helper

```go
// respondError classifies err, logs the full chain for operators, and writes a
// client-safe body. 4xx keeps the verbatim message — it describes caller input.
// 5xx is redacted to a class token plus a correlation id, because operator-detail
// errors carry S3 object keys and the postgres_scan connection string, including
// its password (#301).
func respondError(w http.ResponseWriter, op string, err error, logFields ...any)

// respondErrorWithStatus is the same, for the one caller that has already
// classified in order to pick its message.
func respondErrorWithStatus(w http.ResponseWriter, status int, op string, err error, logFields ...any)
```

Neither returns a status: no caller branches on it, and an ignored return would
trip the linter.

Behavior:

1. `status := classifyManagerError(err)` (unless supplied).
2. `status < 500` → `writeError(w, status, fmt.Sprintf("%s: %v", op, err))`;
   log at `Debugw` so production gains no new noise.
3. `status >= 500` → mint `error_id` via `uuid.New()`, resolve `error_class`, log
   `zap.S().Errorw(op, …logFields, "error_id", …, "error_class", …, "error", err.Error())`,
   then write the redacted body.

### Call sites

All eight manager error sites convert:

| Line | Handler | Note |
| --- | --- | --- |
| 112 | `handleCreate` | hardcoded 500 → classified (decision 2) |
| 176 | `executeGet` | keeps its status-dependent message, see below |
| 236 | `handleQuery` | |
| 287 | `handleUpdate` | |
| 312 | `handleSingleDelete` | |
| 369 | `handleBatchDelete` | |
| 412 | `handleSearch` | |
| 453 | `handleAdvancedQuery` | |

`executeGet` picks `"record not found"` over `"get failed"` from the classified
status (`server.go:171-176`). It keeps that structure and calls
`respondErrorWithStatus`, so the 404 body text is preserved exactly rather than
changed as a side effect. Nothing currently asserts on that string, but changing
it is not this issue's business.

## Testing

Red first, per repo practice.

### The load-bearing test — leak canary

A stubbed manager returns a chain shaped like the real one, carrying two
canaries:

```go
inner := fmt.Errorf(`IO Error: Unable to connect to Postgres at ` +
    `"host=h user=u password=SUPERSECRET-CANARY dbname=d"`)
err := fmt.Errorf("execute duckdb query: %w: %w",
    &forma.ParquetSetInconsistentError{
        SchemaID:    22,
        MissingKeys: []string{"base/schema_22/CANARY-KEY.parquet"},
    }, inner)
```

Table over **all eight endpoints**, asserting for each that the response body
contains neither canary, no `password=`, and no `s3://`; and that
`error_class == "parquet_set_inconsistent"` with `error_id` parsing as a UUID.

The same test installs a `zaptest/observer` core via `zap.ReplaceGlobals` and
asserts the logged entry **does** contain both canaries and an `error_id` equal to
the one in the body. This half is what proves detail was relocated rather than
destroyed — the point of finding #1. Precedent: `factory/factory_test.go:562-574`.

### Supporting tests

- **Class tokens** — table over the three promoted sentinels wrapped at depth,
  plus an unclassified error mapping to `internal`. Proves `errors.Is` traversal,
  not string matching.
- **4xx verbatim** — an `ErrInvalidInput`-wrapping chain keeps its full message
  and emits neither `error_class` nor `error_id`.
- **Create reclassification** — `BatchCreate` returning an
  `ErrInvalidInput`-wrapping error yields 400 with the message intact; an
  unclassified error yields a redacted 500.
- **Read-path errors classify as 5xx** — pins that none of the three sentinels'
  message texts trips `classifyManagerError`'s 4xx heuristics into stealing them
  out of the redacted branch. (Checked by hand: `ManifestSchemaMismatchError`
  says "must resolve", which does not match the `"must be"` probe. Fragile
  enough to deserve a test.)
- **Source guard** — a mechanical line scan of the package's non-test `.go` files
  failing on any line that calls `writeError(` while also mentioning
  `classifyManagerError(` or `http.StatusInternalServerError`. After the refactor
  `writeError` is reachable only with literal 4xx statuses and caller-derived
  text; a future handler that copies an old call site, or writes
  `writeError(w, http.StatusInternalServerError, err.Error())`, fails the build.
  Grep-gate precedent: #260.
- **Success-path pin** — assert `toExecutionPlan` drops `SQL`, `Params`, and
  `Notes`, feeding a plan whose `DataSourcePlan.SQL` contains a
  `password=` canary. Today only the `// SECURITY:` comment protects that
  allowlist.

## Audit deliverable

`docs/error-handling.md` gains a **Public HTTP error surface** section recording
the boundary and its rationale. The findings also go on the PR as comments, per
repo rules.

Scope resolution for the issue's "audit adjacent operator-detail errors":

| Surface | Verdict |
| --- | --- |
| 5xx response bodies | **Fixed** — deny-by-default |
| Success-path `ExecutionPlan` | Already allowlisted (`toExecutionPlan`, #259 P0); now pinned by a test |
| 4xx response bodies | Reviewed, left verbatim — caller-input echo only |
| `/health` | Static payload, nothing to leak |
| `cmd/tools` stderr | Operator surface by definition; out of scope |
| In-process error strings (Go embedders) | **Deliberate gap** — see below |

## Follow-up

Decision 1 fixes the HTTP seam, which means `PG_CONN` still reaches error strings
inside the process. A Go embedder using `factory.NewEntityManager*` will capture
the password in its own logs. File a follow-up issue for scrubbing credentials at
the federated engine's error wraps — the deferred option from this design — so
the gap is tracked rather than silently accepted.

## Out of scope

- Engine-level credential scrubbing (follow-up above).
- A client-facing retryability taxonomy over `ErrFederatedReadFailed` /
  `ErrPostgresReadFailed`.
- Splitting `server.go` below 500 lines (#220).
- Structured logging middleware or request-ID propagation from inbound headers.
  `error_id` is minted per error, not per request; correlating across a request's
  multiple errors is not a need #301 has.

## Verification

```bash
make lint          # golangci-lint pinned v1.64.8 — do not upgrade the pin
make test
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m
make test-e2e-production
```

Issue-specific evidence required before any completion claim:

- The canary test fails **before** the fix (proving the leak is real through the
  HTTP layer, not just at the driver) and passes after.
- The log-observer assertion passes, proving full detail survives in operator
  logs.
- A `>=500` response body, dumped in full, contains no `password=`, no `s3://`,
  and no object key.
- `make test` confirms no existing test asserted on 5xx body text. (Surveyed:
  only status codes are asserted, at `internal/httpapi/server_test.go:225,251`;
  `tests/e2e` has no error-body assertions.)
