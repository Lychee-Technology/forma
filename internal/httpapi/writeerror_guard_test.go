package httpapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The two source-level guards on writeError live here, apart from the runtime
// leak canaries in error_leak_test.go, so that the next guard on this boundary
// has a home that is not a file already near the 500-line cap (#420).

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
// runtime canary tests in error_leak_test.go only cover the handlers that exist
// today; this one fails the build when a new handler reintroduces the #301 leak.
//
// The invariant: every writeError call in a non-test file passes a literal 4xx
// http.Status* constant from writeErrorAllowed4xx, with no exceptions. Until
// #361 there was exactly one — respondErrorWithStatus passed the variable
// `status` under the disclosure gate (isClientError + resolvePublicMessage,
// #313) — but the #361 review folded the disclosed write into writeJSON, so
// writeError is handler-literal territory only, and any non-literal status
// anywhere is a regression.
//
// It is an allowlist rather than a blocklist of bad statuses because a blocklist
// can be spelled around, and the most likely regression shape spells around it
// for free: hand-writing a `writeError(w, status, ...)` call with a
// runtime-classified status in a handler yields a 500 body full of S3 keys that no pattern for
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
// below (#360): every site carries a string literal only; anything dynamic
// must arrive as a published carrier through respondError. This guard itself
// still reads only the *status* expression and cannot tell whether the third
// argument is safe to disclose. Since the #361 review folded the boundary's
// disclosed write into writeJSON, no writeError site builds its message at
// runtime — the runtime judgement (resolvePublicMessage) lives entirely inside
// respondErrorWithStatus, which this guard no longer sees. Every remaining
// site is safe because its message is a fixed literal written into the
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

	if len(unlisted) != 0 {
		t.Errorf("expected no writeError site with a non-literal status "+
			"(the boundary writes through writeJSON since #361), found %d: %v\n"+
			"every site must pass a literal 4xx constant from writeErrorAllowed4xx; "+
			"anything classified at runtime must go through respondError instead (#301)",
			len(unlisted), unlisted)
	}
}

// TestWriteErrorMessageIsAlwaysALiteral closes the axis the status guard above
// documents as unchecked: the message. Every direct writeError call site must
// pass an untyped string literal — never fmt.Sprintf, err.Error(), or any
// other expression — with no exceptions: the one sanctioned site
// (respondErrorWithStatus, whose message is built under the disclosure gate,
// #313) stopped calling writeError when the #361 review folded the disclosed
// write into writeJSON. Together the two guards make the disclosure rule
// total for writeError call sites (#360) — the boundary itself
// (respondErrorWithStatus, writing through writeJSON) is trusted, not
// scanned: a handler that needs dynamic text has exactly one road, a
// published carrier through respondError.
//
// go/ast rather than the regex above: an argument expression can contain
// commas and nested calls a regex cannot parse, and go/parser fails loudly on
// what it cannot read instead of skipping it.
//
// The guard is deliberately conservative about what counts as a literal: only
// a string token written at the call site passes. A named constant holding a
// fixed string is safe but is rejected anyway: to the parser a const and a
// var are the same identifier, and telling them apart would need type
// information this syntax scan does not load. Admitting identifiers would
// admit a var assigned from err.Error() along with the const.
//
// Both guards share one more blind spot: they recognise writeError only where
// it is called by name. A method value or function variable —
// `f := writeError; f(w, status, msg)` — reaches the same body with neither
// guard seeing the call. None exists in this package; introducing one would
// take the call out of both scans.
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
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list package sources: %v", err)
	}

	fset := token.NewFileSet()
	calls := 0
	var dynamic []string
	var unchecked []string
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var fn string
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				fn = fun.Name
			case *ast.SelectorExpr:
				fn = fun.Sel.Name
			}
			if fn != "writeError" {
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

	if calls == 0 {
		t.Fatalf("guard matched no writeError call sites — it would pass vacuously")
	}
	if len(unchecked) > 0 {
		t.Errorf("writeError call sites with unexpected arity, so their message went unchecked: %v\n"+
			"writeError takes exactly (w, status, message); a variadic or reshaped signature must not "+
			"be introduced without extending this guard (#360)", unchecked)
		return
	}
	if len(dynamic) != 0 {
		t.Errorf("expected no writeError site with a non-literal message "+
			"(the boundary writes through writeJSON since #361), found %d: %v\n"+
			"a handler must not build its own body text — publish it through a "+
			"forma.InvalidInputf carrier and respondError instead (#360). A named "+
			"constant is rejected too, even one holding a fixed string: the guard "+
			"reads syntax, not types, so it cannot tell a const from a var holding "+
			"err.Error() — write the literal inline",
			len(dynamic), dynamic)
	}
}
