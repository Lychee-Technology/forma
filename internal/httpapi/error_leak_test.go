package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
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
// produces, and no response body may carry any part of it. The log must carry the
// diagnosis — but not the credential: the log line this PR introduced is itself a
// destination, so the password canary is asserted absent there and the object key
// asserted present.
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
				canaryPassword, canarySecret, canarySecretHead, canarySecretTail, canaryKey, "password=", "s3://", "IO Error",
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
			for _, required := range []string{canaryKey, "password=***REDACTED***"} {
				if !strings.Contains(logged, required) {
					t.Fatalf("%s operator log lost %q; logged: %s", tc.name, required, logged)
				}
			}
			if leaksCanarySecret(logged) {
				t.Fatalf("%s operator log leaked the credential; logged: %s", tc.name, logged)
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
		batchCreateErr: forma.InvalidInputf("attribute 'age' (attrID=2): cannot convert string to float64"),
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

// TestCreateUnknownSchemaIs404AndVerbatim pins the externally visible
// behaviour change from #301: handleCreate answers its classified status, so
// POST to an unknown schema returns 404 instead of 500.
//
// It also pins that this 404 publishes. The chain is the shape the batch
// service now builds — the registry's name-keyed forma.NotFoundf leaf under a
// plain phase wrap under the batch loop's forma.WrapPublicf — so the body
// names the schema the caller sent plus the batch index, while the phase
// context ("failed to get schema") stays operator-only (#362 review, P2) and
// the body stays free of error_class/error_id, which is what distinguishes it
// from an error carrying no publication at all
// (TestUnconvertedSentinelIsRedacted4xx).
func TestCreateUnknownSchemaIs404AndVerbatim(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := &mockEntityManager{
		batchCreateErr: forma.WrapPublicf(
			fmt.Errorf("failed to get schema: %w",
				forma.NotFoundf("schema not found: %s", "nosuchschema")),
			"operation[0]"),
	}
	srv := NewServer(manager, Options{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/nosuchschema", bytes.NewReader([]byte(`{"name":"x"}`)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unknown schema on create, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if !strings.Contains(resp.Error, "operation[0]: schema not found: nosuchschema") {
		t.Fatalf("expected the published message naming the index and schema, got %q", resp.Error)
	}
	if strings.Contains(resp.Error, "failed to get schema") {
		t.Fatalf("an internal phase name reached the 404 body: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a sentinel-carrying error took the redacted branch: error_class=%q error_id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

// TestReadPathErrorsClassifyAs5xx pins status accuracy for the three read-path
// carriers. It is now structural rather than a near-miss check: classification
// reads sentinels only, and none of these carriers wraps a client sentinel, so
// each is a 500. It used to be delicate — the substring heuristic matched
// "must be", and ManifestSchemaMismatchError says "must resolve", one word away
// from a bogus 400. Removing the heuristic removed that hazard class entirely.
//
// It is NOT what keeps object keys out of the body — disclosure depends on
// positive sentinel evidence (isClientError). This test guards the status;
// error_response_test.go guards the disclosure.
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

// writeErrorAllowed4xx is the set of statuses a writeError call site may name
// literally. It is deliberately an allowlist: writeError echoes its message
// straight into the client body, so the safe statuses have to be enumerated
// rather than the unsafe ones excluded. Extend it only when a real call site
// needs another 4xx constant.
//
// It holds exactly the two constants live call sites use today. Deny-by-default
// is the point: pre-admitting http.StatusNotFound would hand a future
// `writeError(w, http.StatusNotFound, err.Error())` a free pass — a hand-written
// 404 body bypasses both the disclosure gate and classifyManagerError, so it can
// echo a storage failure that DuckDB renders as "404 (Not Found)" no matter how
// the classifier behaves. Adding an entry has to be a deliberate act, because
// that is the moment its author reads the caveat below.
var writeErrorAllowed4xx = map[string]bool{
	"http.StatusBadRequest":       true,
	"http.StatusMethodNotAllowed": true,
}

// TestWriteErrorAlwaysCarriesALiteral4xxStatus is a source-level guard. The
// canary tests above only cover the handlers that exist today; this one fails
// the build when a new handler reintroduces the #301 leak.
//
// The invariant: every writeError call in a non-test file passes a literal 4xx
// http.Status* constant from writeErrorAllowed4xx, with exactly one sanctioned
// exception — respondErrorWithStatus in error_response.go, which passes the
// variable `status` under a runtime gate (isClientError + resolvePublicMessage,
// #313) that is what actually constrains it. That exception is exempted by
// asserting it is unique, so if it moves, multiplies, or reappears in another
// file the guard fails.
//
// It is an allowlist rather than a blocklist of bad statuses because a blocklist
// can be spelled around, and the most likely regression shape spells around it
// for free: copying error_response.go's own `writeError(w, status, ...)` line
// into a handler yields a 500 body full of S3 keys that no pattern for
// `http.StatusInternalServerError` would ever see. Deny-by-default also covers
// non-500 5xx constants (503 on the degraded-mode path), bare numerics, and any
// receiver name. Grep-gate precedent: #260.
//
// NOTE: this guard only sees writeError. A handler that bypasses it — calling
// writeJSON(w, 500, APIResponse{Error: err.Error()}) directly, or reaching for
// http.Error — leaks identically and is invisible here. Widening it to every
// body-writing path was judged out of scope for #301; the boundary is recorded
// so the next author trusts the guard for exactly what it checks.
//
// NOTE: the message axis is checked by TestWriteErrorMessageIsAlwaysALiteral
// below (#360): direct sites carry string literals only; anything dynamic must
// arrive as a published carrier through respondError. This guard itself still
// reads only the *status* expression and cannot tell whether the third argument
// is safe to disclose. For the one site that builds its message at runtime,
// respondErrorWithStatus, that judgement belongs to resolvePublicMessage. Every
// other site is safe because its message is a fixed literal written into the
// source — request text no longer reaches these calls at all, having moved to
// published carriers under the gate (#360), so no direct message can carry the
// manager, the engine, S3, or PG_CONN.
func TestWriteErrorAlwaysCarriesALiteral4xxStatus(t *testing.T) {
	// Captures the status expression of each call. The receiver pattern is
	// deliberately narrow, so it does NOT match every legal Go expression —
	// `s.w`, `ctx.Writer` and `rec[0]` all fail to parse. Unparsed calls used to
	// vanish silently; the per-file reconciliation below now turns them into a
	// loud failure instead, which is what actually enforces the invariant.
	callSite := regexp.MustCompile(`writeError\(\s*\w+\s*,\s*([^,]+?)\s*,`)
	// Every textual occurrence of the call, and the one definition to discount.
	anyCall := regexp.MustCompile(`writeError\(`)
	definition := regexp.MustCompile(`func\s+writeError\(`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	scanned, calls := 0, 0
	var unlisted []string
	var unparsed []string
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
		// FindAll, not Find: every offending site must be reported, not just the
		// first one in each file.
		matched := callSite.FindAllSubmatchIndex(src, -1)
		for _, loc := range matched {
			calls++
			status := strings.TrimSpace(string(src[loc[2]:loc[3]]))
			if writeErrorAllowed4xx[status] {
				continue
			}
			line := 1 + strings.Count(string(src[:loc[0]]), "\n")
			unlisted = append(unlisted, fmt.Sprintf("%s:%d (status %q)", name, line, status))
		}

		// Reconcile against raw occurrences so a call the regex cannot parse
		// fails the guard instead of disappearing from it. Without this, a
		// handler on a struct holding its own writer — writeError(s.w, 500,
		// err.Error()) — matches nothing, is never counted, and reintroduces
		// #301 while the guard reports green.
		raw := len(anyCall.FindAllIndex(src, -1)) - len(definition.FindAllIndex(src, -1))
		if raw != len(matched) {
			unparsed = append(unparsed, fmt.Sprintf("%s (%d occurrences, %d parsed)", name, raw, len(matched)))
		}
	}

	if scanned == 0 {
		t.Fatalf("guard scanned no source files — it would pass vacuously")
	}
	if calls == 0 {
		t.Fatalf("guard matched no writeError call sites — it would pass vacuously")
	}
	if len(unparsed) > 0 {
		t.Errorf("writeError call sites the guard could not parse, so their status went unchecked: %v\n"+
			"the receiver must be a plain identifier (writeError(w, ...)); a call on a field, index, or "+
			"other expression hides its status from this guard and must not be introduced (#301)", unparsed)
		return
	}

	if len(unlisted) != 1 {
		t.Errorf("expected exactly 1 writeError site with a non-literal status "+
			"(respondErrorWithStatus in error_response.go), found %d: %v\n"+
			"every other site must pass a literal 4xx constant from writeErrorAllowed4xx; "+
			"anything classified at runtime must go through respondError instead (#301)",
			len(unlisted), unlisted)
		return
	}
	if !strings.HasPrefix(unlisted[0], "error_response.go:") {
		t.Errorf("the sanctioned non-literal writeError status moved out of error_response.go to %s; "+
			"only respondErrorWithStatus may pass a runtime-classified status (#301)", unlisted[0])
	}
}

// TestWriteErrorMessageIsAlwaysALiteral closes the axis the status guard above
// documents as unchecked: the message. Every direct writeError call site must
// pass an untyped string literal — never fmt.Sprintf, err.Error(), or any
// other expression — with the same single sanctioned exception as the status
// guard: respondErrorWithStatus in error_response.go, whose message is built
// under the disclosure gate (isClientError + resolvePublicMessage +
// redactCredentials, #313). Together the two guards make the disclosure rule
// total for this write path (#360): a handler that needs dynamic text has
// exactly one road, a published carrier through respondError.
//
// go/ast rather than the regex above: an argument expression can contain
// commas and nested calls a regex cannot parse, and go/parser fails loudly on
// what it cannot read instead of skipping it.
//
// A call is counted the moment its name matches, before its shape is examined,
// and any arity other than (w, status, message) is reported rather than
// skipped. Skipping would be the same silent-vacuity hole the status guard
// reconciles against: if writeError ever became variadic
// (format string, args ...any), 4-argument calls would drop out of the scan
// while the surviving 3-argument ones kept calls > 0, so the vacuity Fatal
// would not fire and the guard would report green for precisely the dynamic
// message it exists to forbid.
func TestWriteErrorMessageIsAlwaysALiteral(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	calls := 0
	var dynamic []string
	var unchecked []string
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name string
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					name = fun.Name
				case *ast.SelectorExpr:
					name = fun.Sel.Name
				}
				if name != "writeError" {
					return true
				}
				calls++
				if len(call.Args) != 3 {
					unchecked = append(unchecked, fset.Position(call.Pos()).String())
					return true
				}
				if lit, ok := call.Args[2].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					return true
				}
				dynamic = append(dynamic, fset.Position(call.Args[2].Pos()).String())
				return true
			})
		}
	}

	if calls == 0 {
		t.Fatalf("guard matched no writeError call sites — it would pass vacuously")
	}
	if len(unchecked) > 0 {
		t.Errorf("writeError call sites with unexpected arity, so their message went unchecked: %v\n"+
			"writeError takes exactly (w, status, message); a variadic or reshaped signature must not "+
			"be introduced without extending this guard (#360)", unchecked)
		return
	}
	if len(dynamic) != 1 {
		t.Errorf("expected exactly 1 writeError site with a non-literal message "+
			"(respondErrorWithStatus in error_response.go), found %d: %v\n"+
			"a handler must not build its own body text — publish it through a "+
			"forma.InvalidInputf carrier and respondError instead (#360)",
			len(dynamic), dynamic)
		return
	}
	if !strings.HasPrefix(dynamic[0], "error_response.go:") {
		t.Errorf("the sanctioned non-literal writeError message moved out of "+
			"error_response.go to %s; only respondErrorWithStatus may write a "+
			"runtime-built message (#360)", dynamic[0])
	}
}
