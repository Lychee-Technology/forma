# #301 Redact Operator Detail From Public HTTP Error Bodies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the public HTTP 5xx surface from echoing operator detail — S3 object keys and the Postgres password — by redacting every 5xx body to a class token plus a correlation id, and relocating the full error chain into operator logs.

**Architecture:** Deny-by-default at the HTTP seam. A single `respondError` helper replaces all eight manager-error call sites in `internal/httpapi/server.go`: it classifies, logs the full chain via `zap.S().Errorw`, and writes a body carrying no error text. 4xx keeps its verbatim message. `ErrParquetSetInconsistent` is promoted from `internal/federated` to the root `forma` package so `internal/httpapi` can classify it without importing DuckDB CGO.

**Tech Stack:** Go, `go.uber.org/zap` (+ `zaptest/observer`), `github.com/google/uuid`, stdlib `testing` (no testify in `internal/httpapi`; testify `require` at the repo root).

**Spec:** `docs/superpowers/specs/2026-07-25-issue-301-redact-http-error-bodies-design.md`

> **Superseded in places by code review (rounds 1-2).** This plan is kept as the
> record of what was planned and why. Two contract statements it repeats
> throughout are no longer what shipped, and the code and doc snippets quoted in
> the tasks below still show the intermediate state:
>
> 1. **Status classification no longer uses substring matching.** The heuristic
>    was deleted entirely; `classifyManagerError` reads sentinels only and
>    anything without one is `500`. Redaction alone did not fix the status, and a
>    `404` for an S3 failure misleads clients, caches and alerting. It was also
>    the last string-comparison classification site, which `AGENTS.md` forbids.
> 2. **The "accepted cost" of an opaque 4xx no longer exists,** and its premise
>    that #296 was the only affected site was disproved — six sites across four
>    packages depended on the heuristic, including the write-path
>    `missing required attribute` check. All now wrap `forma.ErrInvalidInput`
>    with the human-authored message prefix unchanged; the `%w` rendering
>    appends `: invalid input` to the full string (#309).
>
> A third change has no counterpart here at all: credentials are scrubbed
> (`internal/redact`) before anything is logged or returned, and
> `federated.DuckDBPostgresConnStringFromPool` now quotes its DSN values so the
> scrubber can bound them.
>
> The current contract lives in `docs/error-handling.md`, "Public HTTP error
> surface". Prefer it over anything below.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (`coding-standard.md`).
- Always wrap errors with context: `fmt.Errorf("failed to X: %w", err)` — never bare `return err`.
- Match errors with `errors.Is` / `errors.As`. **Never** compare error strings.
- Write-path validation wraps `forma.ErrInvalidInput` (→ 4xx); read-path consistency errors stay plain (→ operator-visible 5xx).
- `make lint` uses golangci-lint pinned to **v1.64.8** — do not upgrade the pin.
- Run single tests with the Makefile's env: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./pkg -run TestName -v`.
- Test style is per-package: `internal/httpapi` uses stdlib `testing` with `t.Fatalf`; the root `forma` package uses `testify/require`. Match the package you are editing.
- No auto-merge. Review findings go on the PR as comments.

## Correction to the spec

The spec's testing section says the success-path allowlist in `toExecutionPlan` is protected "only by a comment; no test does." **That is wrong** — `TestToExecutionPlan_DoesNotLeakCredentials` already exists at `internal/entity_query_service_plan_test.go:60` with a `secret` canary, asserting the marshalled plan contains neither the password nor `postgres_scan`. No new test is needed there. Task 6 records it as *verified* in the audit rather than adding one.

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `errors.go` | Modify | Root package gains `ErrParquetSetInconsistent` + `ParquetSetInconsistentError` |
| `errors_test.go` | Create | Pins the promoted sentinel/type contract |
| `internal/federated/errors.go` | Modify | Definition replaced by aliases to the root |
| `internal/federated/errors_test.go` | Modify | Adds cross-package alias identity assertion |
| `internal/httpapi/error_response.go` | Create | The whole error-response concern: classification, class vocabulary, redaction, logging |
| `internal/httpapi/error_response_test.go` | Create | Unit tests for class vocabulary + `respondError` |
| `internal/httpapi/server.go` | Modify | `APIResponse` fields; 8 call sites; moved funcs removed |
| `internal/httpapi/server_test.go` | Modify | `mockEntityManager` gains injectable `BatchDelete` error |
| `internal/httpapi/error_leak_test.go` | Create | All-endpoint canary test + source-level guard |
| `docs/error-handling.md` | Modify | New "Public HTTP error surface" section |

`server.go` goes 747 → roughly 700 lines. It stays over the 500-line cap; it is a tracked violator folded into #220, which the epic sequences *after* this issue. Extracting the error concern pre-carves that seam rather than adding to the violation. Do **not** attempt the rest of the split here.

---

### Task 1: Promote `ErrParquetSetInconsistent` to the root package

**Files:**
- Modify: `errors.go` (add after the `ManifestSchemaMismatchError` block, ~line 88)
- Modify: `internal/federated/errors.go:37-43` (remove sentinel), `:52-60` (extend aliases), `:62-74` (remove type)
- Create: `errors_test.go`
- Modify: `internal/federated/errors_test.go` (append one test)

**Interfaces:**
- Consumes: nothing.
- Produces: `forma.ErrParquetSetInconsistent` (`error`), `forma.ParquetSetInconsistentError` (struct with `SchemaID int16`, `MissingKeys []string`, methods `Error() string` and `Unwrap() error`). `federated.ErrParquetSetInconsistent` and `federated.ParquetSetInconsistentError` remain valid as aliases. Task 2 classifies on `forma.ErrParquetSetInconsistent`.

- [ ] **Step 1: Write the failing test**

Create `errors_test.go`:

```go
package forma

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParquetSetInconsistentErrorIsPublic pins the #301 promotion: the carrier
// and its sentinel live in the root package so internal/httpapi can classify
// them for redaction without importing internal/federated, which would pull
// DuckDB CGO into a pure-Go test build. Same rationale as #299's promotion of
// ErrNoParquetPaths / ErrManifestSchemaMismatch.
func TestParquetSetInconsistentErrorIsPublic(t *testing.T) {
	inner := &ParquetSetInconsistentError{
		SchemaID:    22,
		MissingKeys: []string{"base/schema_22/a.parquet", "base/schema_22/b.parquet"},
	}
	err := fmt.Errorf("execute duckdb query: %w", inner)

	require.ErrorIs(t, err, ErrParquetSetInconsistent)

	var typed *ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(22), typed.SchemaID)
	require.Equal(t, []string{"base/schema_22/a.parquet", "base/schema_22/b.parquet"}, typed.MissingKeys)

	require.ErrorContains(t, err, "schema 22 manifest lists 2 parquet object(s) missing from storage")
	require.ErrorContains(t, err, "base/schema_22/a.parquet")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test . -run TestParquetSetInconsistentErrorIsPublic -v`

Expected: FAIL to compile — `undefined: ParquetSetInconsistentError`, `undefined: ErrParquetSetInconsistent`.

- [ ] **Step 3: Add the sentinel and carrier to the root package**

In `errors.go`, add `"strings"` to the import block (it currently imports only `errors` and `fmt`), then append after the `ManifestSchemaMismatchError.Unwrap` method:

```go
// ErrParquetSetInconsistent marks a federated read whose manifest lists parquet
// objects that do not exist in storage. The manifest is the authoritative record
// of the schema's cold/warm tier, so a listed-but-absent object means that tier
// has lost data. Not degradable: surfacing it even under
// AllowPartialDegradedMode is the whole point — degrading here would return
// exactly the silently short answer this classification exists to make loud
// (#187 scenario 2).
//
// It lives here rather than in internal/federated for the same reason as the two
// errors above, plus one specific to #301: internal/httpapi must classify it to
// redact its object keys, and cannot import internal/federated without pulling
// DuckDB CGO into a pure-Go test build.
var ErrParquetSetInconsistent = errors.New("parquet set inconsistent with manifest")

// ParquetSetInconsistentError carries the schema and the missing object keys so
// the message names the offending state, per the read-path error style.
//
// MissingKeys holds bucket-relative S3 object keys — operator detail that must
// not cross a public transport. internal/httpapi redacts it from 5xx bodies
// (#301); any new transport owes the same treatment.
type ParquetSetInconsistentError struct {
	SchemaID    int16
	MissingKeys []string
}

func (e *ParquetSetInconsistentError) Error() string {
	return fmt.Sprintf("schema %d manifest lists %d parquet object(s) missing from storage: %s",
		e.SchemaID, len(e.MissingKeys), strings.Join(e.MissingKeys, ", "))
}

func (e *ParquetSetInconsistentError) Unwrap() error { return ErrParquetSetInconsistent }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test . -run TestParquetSetInconsistentErrorIsPublic -v`

Expected: PASS.

- [ ] **Step 5: Replace the internal definition with aliases**

In `internal/federated/errors.go`:

1. Delete the `ErrParquetSetInconsistent` doc comment and `var` (currently lines 37-43).
2. Delete the `ParquetSetInconsistentError` type, its `Error()`, and its `Unwrap()` (currently lines 62-74).
3. Extend the existing alias block so it reads:

```go
var (
	ErrNoParquetPaths         = forma.ErrNoParquetPaths
	ErrManifestSchemaMismatch = forma.ErrManifestSchemaMismatch
	// ErrParquetSetInconsistent joined them for #301: internal/httpapi
	// classifies it to redact the object keys it carries out of public 5xx
	// bodies, and cannot import this package without pulling DuckDB CGO into a
	// pure-Go test build.
	ErrParquetSetInconsistent = forma.ErrParquetSetInconsistent
)

type (
	NoParquetPathsError         = forma.NoParquetPathsError
	ManifestSchemaMismatchError = forma.ManifestSchemaMismatchError
	ParquetSetInconsistentError = forma.ParquetSetInconsistentError
)
```

4. **Fix the imports.** `fmt` and `strings` were used only by the deleted `Error()` method and must be removed from the import block. `errors` stays (the other sentinels use `errors.New`); `forma` stays.

- [ ] **Step 6: Append the cross-package identity assertion**

Append to `internal/federated/errors_test.go` (it already imports `errors`, `fmt`, and `require`; add `"github.com/lychee-technology/forma"` if absent):

```go
// TestParquetSetInconsistentAliasIsIdentical pins that the #301 promotion is an
// alias, not a copy: a value built through the federated name is matched by the
// root sentinel, so internal/httpapi's classification sees the errors the engine
// actually produces.
func TestParquetSetInconsistentAliasIsIdentical(t *testing.T) {
	err := fmt.Errorf("execute duckdb query: %w",
		&ParquetSetInconsistentError{SchemaID: 9, MissingKeys: []string{"k.parquet"}})

	require.ErrorIs(t, err, forma.ErrParquetSetInconsistent)
	require.ErrorIs(t, err, ErrParquetSetInconsistent)

	var typed *forma.ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(9), typed.SchemaID)
}
```

- [ ] **Step 7: Verify nothing else broke**

Run:
```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go build ./...
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test . ./internal/federated -v -run 'ParquetSetInconsistent'
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet ./internal/federated
```

Expected: build clean, all `ParquetSetInconsistent` tests PASS. Existing sites — `internal/federated/parquet_source.go:93`, `errors_test.go:125`, `parquet_source_test.go:111`, `internal/e2e_harness/production/factory_manifest_e2e_test.go:326`, `parquet_manifest_consistency_e2e_test.go:47` — compile unchanged because type aliases are identical types.

- [ ] **Step 8: Commit**

```bash
git add errors.go errors_test.go internal/federated/errors.go internal/federated/errors_test.go
git commit -m "refactor(errors): #301 promote ErrParquetSetInconsistent to the root package

internal/httpapi must classify it to redact its S3 object keys from public 5xx
bodies, and cannot import internal/federated without pulling DuckDB CGO into a
pure-Go test build. Aliased back per the #299 precedent, so every existing call
site and test compiles unchanged.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Error class vocabulary and the extracted error-response file

**Files:**
- Create: `internal/httpapi/error_response.go`
- Create: `internal/httpapi/error_response_test.go`
- Modify: `internal/httpapi/server.go` — add `APIResponse` fields (`:585-589`), delete `writeError` (`:598-604`) and `classifyManagerError` (`:705-747`), drop the now-unused `errors` import

**Interfaces:**
- Consumes: `forma.ErrParquetSetInconsistent` from Task 1.
- Produces: `errorClass(err error) string`; the constants `errorClassParquetSetInconsistent`, `errorClassNoParquetPaths`, `errorClassManifestSchemaMismatch`, `errorClassInternal`; `publicErrorMessage(class string) string`; `classifyManagerError(err error) int` and `writeError(w http.ResponseWriter, statusCode int, message string) error` (relocated, signatures unchanged). `APIResponse` gains `ErrorClass` and `ErrorID` string fields. Task 3 builds `respondError` on all of these.

This task is behavior-neutral: nothing calls `errorClass` yet.

- [ ] **Step 1: Write the failing test**

Create `internal/httpapi/error_response_test.go`:

```go
package httpapi

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
)

// TestErrorClassResolvesSentinelsThroughWraps pins that the public error-class
// vocabulary is resolved with errors.Is through arbitrary wrap depth, never by
// matching message text — the #301 body carries no message text, so the token is
// the only thing a client can discriminate on.
func TestErrorClassResolvesSentinelsThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "parquet set inconsistent behind two wraps",
			err: fmt.Errorf("execute duckdb query: %w: %w",
				&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"base/k.parquet"}},
				fmt.Errorf("IO Error: driver text")),
			want: errorClassParquetSetInconsistent,
		},
		{
			name: "no parquet paths",
			err:  fmt.Errorf("resolve paths: %w", &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true}),
			want: errorClassNoParquetPaths,
		},
		{
			name: "manifest schema mismatch",
			err: fmt.Errorf("manifest parquet source: %w",
				&forma.ManifestSchemaMismatchError{RequestedSchemaID: 1, ManifestSchemaID: 2, Path: "manifests/1.json"}),
			want: errorClassManifestSchemaMismatch,
		},
		{
			name: "unclassified falls back to internal",
			err:  fmt.Errorf("db timeout"),
			want: errorClassInternal,
		},
		{
			name: "federated read failure is not given its own token",
			err:  fmt.Errorf("execute duckdb query: federated read failed: IO Error"),
			want: errorClassInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorClass(tt.err); got != tt.want {
				t.Fatalf("errorClass = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPublicErrorMessageCarriesNoDetail pins that the only prose a redacted 5xx
// body carries is a fixed string per class — no schema id, no path, no driver
// text.
func TestPublicErrorMessageCarriesNoDetail(t *testing.T) {
	tests := map[string]string{
		errorClassParquetSetInconsistent: "internal read error",
		errorClassNoParquetPaths:         "internal read error",
		errorClassManifestSchemaMismatch: "internal read error",
		errorClassInternal:               "internal error",
	}

	for class, want := range tests {
		if got := publicErrorMessage(class); got != want {
			t.Fatalf("publicErrorMessage(%q) = %q, want %q", class, got, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -run 'TestErrorClass|TestPublicErrorMessage' -v`

Expected: FAIL to compile — `undefined: errorClass`, `undefined: errorClassInternal`, `undefined: publicErrorMessage`.

- [ ] **Step 3: Create the error-response file**

Create `internal/httpapi/error_response.go`:

```go
package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/lychee-technology/forma"
)

// Public error-class tokens surfaced on redacted 5xx bodies (#301). They are the
// only thing a client can discriminate on, because the body carries no error
// text: operator-detail errors reach the HTTP boundary holding bucket-relative
// S3 object keys and, when DuckDB fails to attach postgres_scan, the Postgres
// password verbatim inside the driver's own message.
//
// errorClassInternal deliberately absorbs ErrFederatedReadFailed,
// ErrPostgresReadFailed, metadata drift, and transform failures. A client-facing
// retryability taxonomy is a separate design; the redacted body is safe for all
// of them.
const (
	errorClassParquetSetInconsistent = "parquet_set_inconsistent"
	errorClassNoParquetPaths         = "no_parquet_paths"
	errorClassManifestSchemaMismatch = "manifest_schema_mismatch"
	errorClassInternal               = "internal"
)

// errorClass maps an error to its public token using errors.Is, so wrapped
// chains classify identically to bare sentinels. Never match on message text:
// the read-path errors are typed carriers precisely so callers do not have to.
func errorClass(err error) string {
	switch {
	case errors.Is(err, forma.ErrParquetSetInconsistent):
		return errorClassParquetSetInconsistent
	case errors.Is(err, forma.ErrNoParquetPaths):
		return errorClassNoParquetPaths
	case errors.Is(err, forma.ErrManifestSchemaMismatch):
		return errorClassManifestSchemaMismatch
	default:
		return errorClassInternal
	}
}

// publicErrorMessage returns the fixed prose for a class. It must stay free of
// schema ids, object keys, configuration key names, and driver text — everything
// specific belongs on the operator log line instead.
func publicErrorMessage(class string) string {
	if class == errorClassInternal {
		return "internal error"
	}
	return "internal read error"
}
```

Note: the `strings` import is for `classifyManagerError`, which arrives in Step 4. Steps 3 and 4 compile as a pair — do not run `go build` between them.

- [ ] **Step 4: Move `writeError` and `classifyManagerError` into it**

Cut both functions **verbatim, comments included** from `server.go` (`writeError` at `:598-604`, `classifyManagerError` at `:705-747`) and paste them into `error_response.go` after `publicErrorMessage`. Then remove `"errors"` from `server.go`'s import block — after the move, `classifyManagerError` was its only user (`server.go:714,717,720`).

Verify with `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go build ./internal/httpapi` — an unused import is a compile error, so this is self-checking.

- [ ] **Step 5: Add the response fields**

In `server.go`, replace the `APIResponse` struct (`:585-589`) with:

```go
type APIResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	// ErrorClass and ErrorID are populated on every redacted response (#301):
	// a stable machine token for client discrimination, and a correlation id
	// echoed on the operator log line that holds the full error chain. Not a
	// 5xx-only pair — redaction is gated on sentinel evidence rather than on
	// the status, so a 4xx classified by substring heuristic alone carries both
	// fields too. Both are omitempty, so success bodies and verbatim 4xx bodies
	// are unchanged.
	ErrorClass string `json:"error_class,omitempty"`
	ErrorID    string `json:"error_id,omitempty"`
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -v
```

Expected: PASS, including the pre-existing `TestClassifyManagerError` and `TestHandleGetErrorMapping` — this task changes no behavior.

- [ ] **Step 7: Commit**

```bash
git add internal/httpapi/error_response.go internal/httpapi/error_response_test.go internal/httpapi/server.go
git commit -m "refactor(httpapi): #301 extract error-response concern, add error class vocabulary

Moves classifyManagerError and writeError into error_response.go and adds the
public error-class tokens plus APIResponse.ErrorClass/ErrorID. Behavior-neutral:
nothing resolves a class yet. Pre-carves part of #220's server.go split rather
than growing the file further.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `respondError` — redact 5xx, log the full chain

**Files:**
- Modify: `internal/httpapi/error_response.go` (append)
- Modify: `internal/httpapi/error_response_test.go` (append)

**Interfaces:**
- Consumes: `errorClass`, `publicErrorMessage`, `classifyManagerError`, `writeError`, `writeJSON`, `APIResponse` from Task 2.
- Produces: `respondError(w http.ResponseWriter, op string, err error, logFields ...any)` and `respondErrorWithStatus(w http.ResponseWriter, status int, op string, err error, logFields ...any)`. Neither returns a value. Task 4 rewrites all eight handler call sites onto them.

- [ ] **Step 1: Write the failing tests**

Append to `internal/httpapi/error_response_test.go`. Extend its import block to:

```go
import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)
```

```go
// leakCanaries are the two strings that must never appear in a response body.
// The password one is the real shape: DuckDB's postgres_scan attach failure puts
// the connection string — password included — into its own prose, verified
// against duckdb-go v2.5.6. The key one is ParquetSetInconsistentError.MissingKeys.
const (
	canaryPassword = "password=SUPERSECRET-CANARY"
	canaryKey      = "base/schema_22/CANARY-KEY.parquet"
)

// operatorDetailError builds an error chain shaped like the one the federated
// engine actually returns: a typed read-path carrier wrapping raw driver text.
func operatorDetailError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`IO Error: Unable to connect to Postgres at "host=h port=5432 user=u %s dbname=d"`, canaryPassword))
}

// TestRespondErrorRedacts5xxAndLogsFullChain is the load-bearing #301 test. The
// body must carry no operator detail; the log must carry all of it, under an
// error_id matching the body's, so redaction relocates detail instead of
// destroying it. Before #301 there was no handler error logging at all, so the
// log half is not incidental.
func TestRespondErrorRedacts5xxAndLogsFullChain(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", operatorDetailError(), "schema", "orders")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, forbidden := range []string{
		canaryPassword, canaryKey, "password=", "s3://", "IO Error",
		"manifest lists", "schema 22", "postgres_scan",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("5xx body leaked %q; body: %s", forbidden, body)
		}
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if resp.Success {
		t.Fatalf("expected success=false")
	}
	if resp.Error != "internal read error" {
		t.Fatalf("expected generic message, got %q", resp.Error)
	}
	if resp.ErrorClass != errorClassParquetSetInconsistent {
		t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
	}
	if _, err := uuid.Parse(resp.ErrorID); err != nil {
		t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, err)
	}

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 ERROR log entry, got %d", len(entries))
	}
	entry := entries[0]
	if entry.Message != "query failed" {
		t.Fatalf("expected log message %q, got %q", "query failed", entry.Message)
	}

	fields := entry.ContextMap()
	logged, _ := fields["error"].(string)
	for _, required := range []string{canaryPassword, canaryKey, "schema 22"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("operator log lost %q; logged error was: %s", required, logged)
		}
	}
	if fields["error_id"] != resp.ErrorID {
		t.Fatalf("log error_id %v does not match body %q", fields["error_id"], resp.ErrorID)
	}
	if fields["error_class"] != errorClassParquetSetInconsistent {
		t.Fatalf("log error_class = %v", fields["error_class"])
	}
	if fields["schema"] != "orders" {
		t.Fatalf("caller log field lost: schema = %v", fields["schema"])
	}
}

// TestRespondErrorKeeps4xxMessageVerbatim pins the other half of the contract:
// a client error describes caller-supplied input, so the caller still gets the
// full message and no correlation fields are emitted.
func TestRespondErrorKeeps4xxMessageVerbatim(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	err := fmt.Errorf("attribute 'age' (attrID=2): %w: cannot convert string to float64", forma.ErrInvalidInput)
	rec := httptest.NewRecorder()
	respondError(rec, "create failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if !strings.Contains(resp.Error, "create failed: attribute 'age' (attrID=2)") {
		t.Fatalf("4xx message was not preserved verbatim: %q", resp.Error)
	}
	if !strings.Contains(resp.Error, "cannot convert string to float64") {
		t.Fatalf("4xx message was truncated: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("4xx must not emit correlation fields, got class=%q id=%q", resp.ErrorClass, resp.ErrorID)
	}
	if logs.Len() != 0 {
		t.Fatalf("a client error must not log at ERROR level, got %d entries", logs.Len())
	}
}

// TestRespondErrorWithStatusHonoursCallerStatus pins the executeGet path, which
// picks its message from the classified status and so passes the status in.
func TestRespondErrorWithStatusHonoursCallerStatus(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	rec := httptest.NewRecorder()
	respondErrorWithStatus(rec, http.StatusNotFound, "record not found",
		fmt.Errorf("wrap: %w", forma.ErrNotFound), "schema", "orders")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "record not found: wrap") {
		t.Fatalf("expected verbatim 404 message, got %s", rec.Body.String())
	}
}
```

`zap.NewNop()` is used where the test only needs the global logger silenced; `observer.New(zap.ErrorLevel)` is used where the log content is being asserted. No `zapcore` import is needed.

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -run TestRespondError -v`

Expected: FAIL to compile — `undefined: respondError`, `undefined: respondErrorWithStatus`.

- [ ] **Step 3: Implement the helpers**

Append to `internal/httpapi/error_response.go`, and extend its imports to include `"fmt"`, `"github.com/google/uuid"`, and `"go.uber.org/zap"`:

```go
// respondError classifies err, records the full chain for operators, and writes
// a client-safe body.
//
// 4xx keeps the verbatim message: it describes caller-supplied input, the caller
// needs to know what to fix, and nothing on the write path touches S3 or the
// postgres_scan connection string.
//
// 5xx carries no error text at all (#301). Errors reaching this point hold
// bucket-relative S3 object keys and — when DuckDB fails to attach
// postgres_scan — the Postgres password verbatim inside the driver's own
// message. Redaction is an allowlist rather than a blocklist of known-sensitive
// types precisely because that password originates in driver text, not in a
// Forma error type. The detail is not discarded: it goes to the log under an
// error_id the client can quote back.
func respondError(w http.ResponseWriter, op string, err error, logFields ...any) {
	respondErrorWithStatus(w, classifyManagerError(err), op, err, logFields...)
}

// respondErrorWithStatus is respondError for callers that have already
// classified in order to choose their message (executeGet's 404 wording).
func respondErrorWithStatus(w http.ResponseWriter, status int, op string, err error, logFields ...any) {
	fields := make([]any, 0, len(logFields)+8)
	fields = append(fields, logFields...)
	fields = append(fields, "status", status)

	if status < http.StatusInternalServerError {
		fields = append(fields, "error", err.Error())
		zap.S().Debugw(op, fields...)
		_ = writeError(w, status, fmt.Sprintf("%s: %v", op, err))
		return
	}

	class := errorClass(err)
	errorID := uuid.NewString()
	fields = append(fields, "error_class", class, "error_id", errorID, "error", err.Error())
	zap.S().Errorw(op, fields...)

	_ = writeJSON(w, status, APIResponse{
		Success:    false,
		Error:      publicErrorMessage(class),
		ErrorClass: class,
		ErrorID:    errorID,
	})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -run TestRespondError -v`

Expected: all three PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/httpapi/error_response.go internal/httpapi/error_response_test.go
git commit -m "feat(httpapi): #301 respondError redacts 5xx bodies and logs the full chain

5xx responses carry a class token and a correlation id, never error text: the
chain holds S3 object keys and, on a postgres_scan attach failure, the Postgres
password inside DuckDB's own message. The full chain now reaches zap.S().Errorw
under a matching error_id — handlers previously logged no errors at all.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Wire all eight handlers

**Files:**
- Modify: `internal/httpapi/server.go` — lines 112, 171-177, 236, 287, 312, 369, 412, 453
- Modify: `internal/httpapi/server_test.go` — `mockEntityManager` (`:17-32` fields, `:78-80` `BatchDelete`)
- Create: `internal/httpapi/error_leak_test.go`

**Interfaces:**
- Consumes: `respondError`, `respondErrorWithStatus` from Task 3.
- Produces: `mockEntityManager` fields `batchDeleteResult *forma.BatchResult` and `batchDeleteErr error`, used by Task 4's own tests only.

- [ ] **Step 1: Make `BatchDelete` errors injectable**

In `internal/httpapi/server_test.go`, add to the `mockEntityManager` struct:

```go
	batchDeleteResult *forma.BatchResult
	batchDeleteErr    error
```

and replace its `BatchDelete` method:

```go
func (m *mockEntityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if m.batchDeleteResult != nil || m.batchDeleteErr != nil {
		return m.batchDeleteResult, m.batchDeleteErr
	}
	return nil, fmt.Errorf("not implemented")
}
```

Both delete handlers go through `BatchDelete` — `handleSingleDelete` wraps the single row in a batch (`server.go:305-310`) — so this one field covers both.

- [ ] **Step 2: Write the failing tests**

Create `internal/httpapi/error_leak_test.go`:

```go
package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// endpointCase drives one HTTP entry point with a failing manager.
type endpointCase struct {
	name    string
	method  string
	target  string
	body    string
	failing func(*mockEntityManager, error)
}

func allEndpointCases(rowID uuid.UUID) []endpointCase {
	return []endpointCase{
		{
			name: "create", method: http.MethodPost, target: "/api/v1/lead",
			body:    `{"name":"x"}`,
			failing: func(m *mockEntityManager, err error) { m.batchCreateErr = err },
		},
		{
			name: "get", method: http.MethodGet, target: "/api/v1/lead/" + rowID.String(),
			failing: func(m *mockEntityManager, err error) { m.getErr = err },
		},
		{
			name: "query", method: http.MethodGet, target: "/api/v1/lead?page=1&items_per_page=10",
			failing: func(m *mockEntityManager, err error) { m.advancedErr = err },
		},
		{
			name: "update", method: http.MethodPut, target: "/api/v1/lead/" + rowID.String(),
			body:    `{"name":"y"}`,
			failing: func(m *mockEntityManager, err error) { m.updateErr = err },
		},
		{
			name: "single delete", method: http.MethodDelete, target: "/api/v1/lead/" + rowID.String(),
			failing: func(m *mockEntityManager, err error) { m.batchDeleteErr = err },
		},
		{
			name: "batch delete", method: http.MethodDelete, target: "/api/v1/lead",
			body:    `["` + rowID.String() + `"]`,
			failing: func(m *mockEntityManager, err error) { m.batchDeleteErr = err },
		},
		{
			name: "search", method: http.MethodGet, target: "/api/v1/search?schemas=lead&q=john",
			failing: func(m *mockEntityManager, err error) { m.crossSchemaErr = err },
		},
		{
			name: "advanced query", method: http.MethodPost, target: "/api/v1/advanced_query",
			body: `{"schema_name":"lead",` +
				`"condition":{"l":"and","c":[{"a":"status","v":"equals:hot"}]},` +
				`"page":1,"items_per_page":10}`,
			failing: func(m *mockEntityManager, err error) { m.advancedErr = err },
		},
	}
}

// TestNoEndpointLeaksOperatorDetail is the #301 acceptance test. Every HTTP
// entry point is driven with the error chain the federated engine really
// produces, and no response body may carry any part of it. The log must carry
// all of it.
func TestNoEndpointLeaksOperatorDetail(t *testing.T) {
	rowID := uuid.New()

	for _, tc := range allEndpointCases(rowID) {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			restore := zap.ReplaceGlobals(zap.New(core))
			defer restore()

			manager := &mockEntityManager{}
			tc.failing(manager, operatorDetailError())
			srv := NewServer(manager, Options{})

			var reader *bytes.Reader
			if tc.body == "" {
				reader = bytes.NewReader(nil)
			} else {
				reader = bytes.NewReader([]byte(tc.body))
			}
			req := httptest.NewRequest(tc.method, tc.target, reader)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("expected status 500, got %d; body: %s", rec.Code, rec.Body.String())
			}

			body := rec.Body.String()
			for _, forbidden := range []string{
				canaryPassword, canaryKey, "password=", "s3://", "IO Error",
				"manifest lists", "schema 22", "postgres_scan",
			} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("%s body leaked %q; body: %s", tc.name, forbidden, body)
				}
			}

			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.ErrorClass != errorClassParquetSetInconsistent {
				t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
			}
			if _, err := uuid.Parse(resp.ErrorID); err != nil {
				t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, err)
			}

			entries := logs.All()
			if len(entries) == 0 {
				t.Fatalf("%s logged no ERROR entry, so the detail was destroyed rather than relocated", tc.name)
			}
			logged, _ := entries[len(entries)-1].ContextMap()["error"].(string)
			for _, required := range []string{canaryPassword, canaryKey} {
				if !strings.Contains(logged, required) {
					t.Fatalf("%s operator log lost %q; logged: %s", tc.name, required, logged)
				}
			}
		})
	}
}

// TestCreateValidationErrorIsClientError pins the #301 decision that
// handleCreate stops reporting write-path validation as 500. The documented
// contract is that ErrInvalidInput surfaces as 4xx, and only that
// reclassification keeps create failures legible once 5xx bodies are redacted.
func TestCreateValidationErrorIsClientError(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := &mockEntityManager{
		batchCreateErr: fmt.Errorf("attribute 'age' (attrID=2): %w: cannot convert string to float64", forma.ErrInvalidInput),
	}
	srv := NewServer(manager, Options{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lead", bytes.NewReader([]byte(`{"age":"x"}`)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for a validation error, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "cannot convert string to float64") {
		t.Fatalf("expected the validation detail preserved, got %s", rec.Body.String())
	}
}

// TestReadPathErrorsClassifyAs5xx pins status accuracy for the three read-path
// carriers: classifyManagerError falls back to substring matching, and
// ManifestSchemaMismatchError already says "must resolve", one word away from
// its "must be" probe. A misclassification here would return a misleading 4xx.
//
// It is NOT what keeps object keys out of the body — Task 3's revised gate makes
// disclosure depend on positive sentinel evidence (isClientError), not on the
// status, precisely because driver text trips these heuristics in practice
// (DuckDB renders a missing object as "404 (Not Found)"). This test guards the
// status; error_response_test.go guards the disclosure.
func TestReadPathErrorsClassifyAs5xx(t *testing.T) {
	errs := map[string]error{
		"parquet set inconsistent": &forma.ParquetSetInconsistentError{
			SchemaID: 22, MissingKeys: []string{"base/k.parquet"},
		},
		"no parquet paths configured":  &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: false},
		"no parquet paths from source": &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true},
		"manifest schema mismatch": &forma.ManifestSchemaMismatchError{
			RequestedSchemaID: 1, ManifestSchemaID: 2, Path: "manifests/1.json",
		},
	}

	for name, err := range errs {
		t.Run(name, func(t *testing.T) {
			wrapped := fmt.Errorf("execute duckdb query: %w", err)
			if got := classifyManagerError(wrapped); got != http.StatusInternalServerError {
				t.Fatalf("read-path error classified as %d, which escapes redaction; message was: %v", got, wrapped)
			}
		})
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -run 'TestNoEndpointLeaks|TestCreateValidationError|TestReadPathErrors' -v`

Expected:
- `TestNoEndpointLeaks*` FAIL — every subtest reports a leaked canary, because handlers still format `%v` into the body. **This failure is the proof the bug is real through the HTTP layer; record the output in the PR.**
- `TestCreateValidationError` FAIL — got 500, want 400.
- `TestReadPathErrors` PASS already (no message trips the heuristics today); it is a regression guard, so a green first run is expected and correct.

- [ ] **Step 4: Rewrite the eight call sites**

In `internal/httpapi/server.go`, each replacement is one line for seven of them:

```go
// :112  handleCreate — hardcoded 500 becomes classified (#301 decision 2)
respondError(w, "batch create failed", err, "schema", schemaName)

// :236  handleQuery
respondError(w, "query failed", err, "schema", schemaName, "page", page, "itemsPerPage", itemsPerPage)

// :287  handleUpdate
respondError(w, "update failed", err, "schema", schemaName, "rowID", rowIDStr)

// :312  handleSingleDelete
respondError(w, "delete failed", err, "schema", schemaName, "rowID", rowID.String())

// :369  handleDelete
respondError(w, "batch delete failed", err, "schema", schemaName, "requested", len(rowIDStrs))

// :412  handleSearch
respondError(w, "cross-schema search failed", err, "schemas", schemaNames, "page", page)

// :453  handleAdvancedQuery
respondError(w, "advanced query failed", err, "schema", payload.SchemaName, "page", payload.Page)
```

`executeGet` (`:169-178`) keeps its status-dependent wording, so it uses the explicit-status variant:

```go
	record, err := s.manager.Get(r.Context(), queryReq)
	if err != nil {
		status := classifyManagerError(err)
		msg := "get failed"
		if status == http.StatusNotFound {
			msg = "record not found"
		}
		respondErrorWithStatus(w, status, msg, err, "schema", schemaName, "rowID", rowID.String())
		return
	}
```

Drop the `_ = ` assignment at every converted site — `respondError` returns nothing.

- [ ] **Step 5: Run the full package suite**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -v`

Expected: everything PASS, including the pre-existing `TestHandleGetErrorMapping` (status codes unchanged) and `TestClassifyManagerError`.

If `go build` complains that `fmt` is now unused in `server.go`, leave it — the 4xx sites at `:76, :83, :139, :151, :195, :202, :215, :253, :265, :271, :329, :335, :349, :429, :470, :477` still use `fmt.Sprintf`.

- [ ] **Step 6: Commit**

```bash
git add internal/httpapi/server.go internal/httpapi/server_test.go internal/httpapi/error_leak_test.go
git commit -m "fix(httpapi): #301 stop echoing operator detail into 5xx response bodies

All eight manager-error sites route through respondError. Before this, a failed
federated read returned the DuckDB driver's message verbatim, which carries the
Postgres password on a postgres_scan attach failure and the full S3 object key
on a missing parquet. handleCreate also stops reporting write-path validation as
500, so create failures stay legible under redaction.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Source guard against reintroduction

**Files:**
- Modify: `internal/httpapi/error_leak_test.go` (append)

**Interfaces:**
- Consumes: nothing. Produces: nothing. A standalone guard.

- [ ] **Step 1: Write the failing test**

Append to `internal/httpapi/error_leak_test.go`, adding `"os"`, `"path/filepath"`, and `"regexp"` to its imports:

```go
// TestWriteErrorIsNeverReachedWithAServerError is a source-level guard. The
// canary tests above only cover handlers that exist today; this one fails the
// build when a new handler reintroduces the #301 leak by copying an old call
// site or writing writeError(w, http.StatusInternalServerError, err.Error()).
//
// After #301, writeError is reachable only with literal 4xx statuses. Anything
// that pairs it with classifyManagerError or a 500 must go through
// respondError instead. Grep-gate precedent: #260.
func TestWriteErrorIsNeverReachedWithAServerError(t *testing.T) {
	forbidden := regexp.MustCompile(
		`writeError\(\s*w\s*,\s*(classifyManagerError\(|http\.StatusInternalServerError)`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		scanned++

		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if loc := forbidden.FindIndex(src); loc != nil {
			line := 1 + strings.Count(string(src[:loc[0]]), "\n")
			t.Errorf("%s:%d formats a server error into a client body; use respondError instead (#301): %q",
				name, line, string(src[loc[0]:loc[1]]))
		}
	}

	if scanned == 0 {
		t.Fatalf("guard scanned no source files — it would pass vacuously")
	}
}
```

- [ ] **Step 2: Run test to verify it passes, then prove it can fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/httpapi -run TestWriteErrorIsNeverReached -v`

Expected: PASS (Task 4 removed every such site).

Now prove the guard is not vacuous. Temporarily add this line inside `handleHealth` in `server.go`:

```go
	_ = writeError(w, http.StatusInternalServerError, "probe")
```

Re-run the guard. Expected: **FAIL**, naming `server.go` and the line number. Then remove the probe line and re-run to confirm PASS. Do not commit the probe.

- [ ] **Step 3: Commit**

```bash
git add internal/httpapi/error_leak_test.go
git commit -m "test(httpapi): #301 source guard against reintroducing the 5xx body leak

Fails the build if writeError is ever paired with classifyManagerError or a 500,
so a new handler cannot leak operator detail by copying a pre-#301 call site.
Verified non-vacuous by temporarily planting a violation.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Document the boundary, run full verification, file the follow-up

**Files:**
- Modify: `docs/error-handling.md` (append a section before `## Message style`)

**Interfaces:**
- Consumes: everything above. Produces: nothing consumed by code.

- [ ] **Step 1: Document the public HTTP error surface**

Insert into `docs/error-handling.md`, immediately before the `## Message style` heading:

```markdown
## Public HTTP error surface

`internal/httpapi` treats the response body as an untrusted destination. The
split follows the two error classes above, and is enforced by
`respondError` in `internal/httpapi/error_response.go`.

**Disclosure is decided by the error, not by the status.** A body is verbatim
only when the error *provably* wraps a client sentinel — `errors.Is` against
`forma.ErrInvalidInput`, `forma.ErrNotFound`, or `forma.ErrConflict`
(`isClientError`). Everything else is redacted, whatever status it carries.

This is deliberate, and the naive version was wrong. `classifyManagerError`
derives the HTTP status by substring-matching the whole error chain, and driver
text trips those probes: DuckDB reports a missing S3 object as
`HTTP Error: … 404 (Not Found).`, which contains `not found`. Gating disclosure
on the status would therefore have classified the single most likely #301
scenario as 4xx and echoed the S3 URL — and, on a `postgres_scan` attach failure,
the password — straight back to the client.

> **Round-2 note.** The reasoning above is why disclosure is gated on sentinels,
> and that still holds. But `classifyManagerError` no longer substring-matches at
> all — the heuristic was deleted, so the misclassification it describes is not
> merely redacted, it cannot happen. See `docs/error-handling.md`.

**The HTTP status is unchanged** by redaction: a misclassified read-path error
still returns its classified status, just with an opaque body.

**Redacted bodies (#301)** carry a fixed message, a stable `error_class` token,
and an `error_id`. No error text crosses. The full chain goes to
`zap.S().Errorw` — *always*, whatever the status, because `cmd/server` runs
`zap.NewProduction()` at Info level and routing a redacted 4xx to `Debugw` would
have leaked nothing but recorded nothing either. An operator retrieves the detail
from the `error_id` the caller quotes.

**Accepted cost.** An error that classifies 4xx by heuristic alone and wraps no
sentinel now gets an opaque body. The known instance is #296 (unknown sort
attribute); `classifyManagerError`'s trigger-word list is the worklist of call
sites that should start wrapping `forma.ErrInvalidInput`. An opaque validation
message is strictly better than a leaked credential.

> **Round-2 note: this cost was not accepted, it was removed.** The trigger-word
> list was worked through rather than left as a worklist: six sites across four
> packages now wrap `forma.ErrInvalidInput`, so none of them returns an opaque
> body. The claim that #296 was the only instance was wrong — the write-path
> `missing required attribute` check was the most consequential omission.

```json
{
  "success": false,
  "error": "internal read error",
  "error_class": "parquet_set_inconsistent",
  "error_id": "9f2c1a7e-…"
}
```

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
(`internal/entity_query_service.go`) allowlists plan fields and drops
`DataSourcePlan.SQL`, `Params`, and `Notes` for the same reason, pinned by
`TestToExecutionPlan_DoesNotLeakCredentials`.

### Known gap

Credentials still reach error strings *inside* the process, so a Go embedder
using `factory.NewEntityManager*` can capture them in its own logs. Scrubbing at
the engine's error wraps is tracked separately.
```

- [ ] **Step 2: Run lint and the full unit suite**

```bash
make lint
make test
```

Expected: both clean. If `golangci-lint` flags `respondErrorWithStatus` for argument count or `error_leak_test.go` for cyclomatic complexity, split the endpoint loop body into a helper rather than adding a `nolint` directive.

- [ ] **Step 3: Run the e2e suites**

```bash
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m
make test-e2e-production
```

Expected: PASS. These exercise `internal/federated`'s errors through Task 1's alias change. If `TestConcurrentFlushSnapshot` or `UpdateBeforeExport` fails on a same-millisecond ordering assertion, that is the known flake tracked by #276 — re-run to confirm, and say so explicitly rather than reporting it as green.

- [ ] **Step 4: Capture the redacted body as evidence**

Add this temporary test, run it, paste its output into the PR, then delete it:

```go
func TestZZDumpRedactedBody(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()
	manager := &mockEntityManager{advancedErr: operatorDetailError()}
	srv := NewServer(manager, Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/lead?page=1", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	t.Logf("status %d body %s", rec.Code, rec.Body.String())
}
```

- [ ] **Step 5: Commit the docs**

```bash
git add docs/error-handling.md
git commit -m "docs(errors): #301 document the public HTTP error surface boundary

Records the sentinel-gated verbatim / redacted split, the error_class vocabulary, and
why redaction has to be an allowlist: the worst leak is DuckDB's own
attach-failure message, which quotes the Postgres password.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: File the follow-up issue**

```bash
gh issue create \
  --title "Scrub PG_CONN credentials from federated engine error strings" \
  --label enhancement --label federated-query \
  --body "Follow-up from #301.

#301 redacted the public HTTP 5xx surface, so API clients no longer see the
Postgres password that DuckDB quotes back on a \`postgres_scan\` attach failure:

\`\`\`
IO Error: Unable to connect to Postgres at \"host=… user=… password=… dbname=…\": …
\`\`\`

The credential still reaches the error string itself. A Go embedder using
\`factory.NewEntityManager*\` will capture it in its own logs, and so will any
future transport that does not repeat #301's redaction.

Direction: scrub at the source rather than at each consumer — wrap
\`duck.Query\` failures through a helper that rewrites \`password=…\` (and the
whole \`PG_CONN\` substring) before the text enters an error chain. Candidate
sites: \`internal/federated/duckdb_query_execute.go:49,68\` and
\`internal/federated/engine.go:382\`. \`DuckDBPostgresConnStringFromPool\`
(\`internal/federated/engine.go:477\`) is where the string is built, so it can
also expose the exact substring to redact.

Verify with the #301 canary shape: assert the *error string returned from the
engine* — not just the HTTP body — carries no \`password=\`."
```

Then add the issue number to epic #304's Phase 1 checklist, and post the plan's evidence (the pre-fix canary failure output and the post-fix redacted body) as a comment on #301.

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: contract → Tasks 3-4; class vocabulary → Task 2; promotion → Task 1; code shape → Tasks 2-4; the canary/observer/class/4xx/create/heuristic-guard tests → Tasks 3-4; source guard → Task 5; audit doc + follow-up → Task 6. The one spec item deliberately dropped is the `toExecutionPlan` canary test — it already exists; see [Correction to the spec](#correction-to-the-spec).

**Type consistency.** `respondError`/`respondErrorWithStatus` keep one signature from Task 3 through Task 4. `errorClass` returns the `errorClass*` constants defined in Task 2 and referenced by name in Tasks 3-4. `operatorDetailError()` and the `canaryPassword`/`canaryKey` constants are defined once in `error_response_test.go` (Task 3) and reused from `error_leak_test.go` (Task 4) — same package, so no redeclaration.

**Known ordering constraint.** Task 4's tests depend on `operatorDetailError()` from Task 3's test file. Do not run Task 4 before Task 3.
