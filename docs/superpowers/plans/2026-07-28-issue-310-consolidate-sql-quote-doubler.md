# Issue #310: Consolidate the Three SQL Quote-Doubler Copies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three independent copies of `strings.ReplaceAll(s, "'", "''")` with one tested helper `sqlutil.EscapeLiteral`, consumed by `internal/cdc`, `internal/sqlgen`, and the federated e2e harness.

**Architecture:** Add `EscapeLiteral` to the existing `internal/sqlutil` package (currently home of `SanitizeIdentifier` — literal escaping is its natural sibling). Delete the three local copies and point every call site at the shared helper. The load-bearing doc comment on `escapeSQLLiteral` (just corrected by PR #333, issue #311) is relocated verbatim-in-substance to its only call site, not discarded. A final sweep renames every comment/test-case mention of the old function names so no stale references survive.

**Tech Stack:** Go, testify. No new dependencies.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (coding-standard.md).
- Always wrap errors with context — not applicable here (helper is infallible), but no bare `return err` anywhere touched.
- Single-test invocations must mirror the Makefile env: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ...` (avoids sandbox cache/VCS errors).
- `make lint` is pinned to golangci-lint v1.64.8 — do not upgrade the pin.
- Execution happens in a worktree created via superpowers:using-git-worktrees (per workspace AGENTS.md: clean merged branches + update local main first). Branch name suggestion: `issue-310-consolidate-quote-doubler`.
- PR must reference the issue: body includes `Closes #310`. 请不要自动合并 PR.

## Decisions Locked In (record these in the PR body)

1. **Helper location/name:** `internal/sqlutil.EscapeLiteral` — the issue itself points at `sqlutil` ("that package already exists"); verb-first name matches repo review convention and mirrors cdc's `escapeLiteral`.
2. **The e2e harness consumes the shared helper too.** The issue allowed keeping the harness copy "if importing production code there is undesirable — decide explicitly rather than by omission." Decision: consolidate. `internal/e2e_harness/federated` already imports `internal/cdc`, `internal/model`, and `internal/federated` (see `query_build.go` sibling files), so importing `internal/sqlutil` sets no new precedent.
3. **The inline `strings.ReplaceAll` copies in `internal/cdc/duckdb_exporter_test.go:11-12` stay.** They are a test oracle re-deriving the expected escaped form independently of production code; pointing them at the helper would make the assertion tautological.
4. **`escapeSQLLiteral`'s doc comment survives.** PR #333 (merged 2026-07-28, b70867f) just spent a review cycle making this comment precise. Its content moves to the single call site in `duckdb_template_renderer.go` with only the grammatical reframing needed (function-doc → call-site comment). Do not paraphrase away the #301/#307 substance.

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/sqlutil/literal.go` | Create | The one escaping rule: `EscapeLiteral` |
| `internal/sqlutil/literal_test.go` | Create | Table-driven tests for the rule |
| `internal/cdc/duckdb_exporter.go` | Modify | Delete `escapeLiteral` (L88-90); 5 call sites → `sqlutil.EscapeLiteral` |
| `internal/cdc/redact_test.go` | Modify | 3 call sites + 2 comments → `sqlutil.EscapeLiteral` |
| `internal/sqlgen/duckdb_template_renderer.go` | Modify | Delete `escapeSQLLiteral` (L351-370); comment moves to PG_CONN call site (L259-263) |
| `internal/federated/connstring.go` | Modify | Cross-reference comment (L31-33) → new helper name |
| `internal/e2e_harness/federated/query_build.go` | Modify | `benchmarkSQLLiteral` string case (L341) → helper |
| `internal/redact/connstring.go` | Modify | Comments (L35, L43) rename `escapeLiteral` mentions |
| `internal/redact/connstring_test.go` | Modify | Test-case name (L41) rename |
| `internal/httpapi/credential_redaction_test.go` | Modify | Comment + test-case name (L47-49) rename |

---

### Task 1: `sqlutil.EscapeLiteral` (TDD)

**Files:**
- Create: `internal/sqlutil/literal.go`
- Test: `internal/sqlutil/literal_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func EscapeLiteral(s string) string` in package `sqlutil` (`github.com/lychee-technology/forma/internal/sqlutil`). Doubles every `'`; adds no surrounding quotes. Tasks 2–4 depend on exactly this signature.

- [ ] **Step 1: Write the failing test**

Create `internal/sqlutil/literal_test.go`:

```go
package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEscapeLiteral(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: ""},
		{name: "no quotes unchanged", input: "host=h port=5432", expected: "host=h port=5432"},
		{name: "single quote doubled", input: "O'Brien", expected: "O''Brien"},
		{name: "every quote doubled", input: "a'b'c", expected: "a''b''c"},
		{name: "adjacent quotes each doubled", input: "x''y", expected: "x''''y"},
		{name: "quote-only string", input: "'", expected: "''"},
		// Backslash is an ordinary character in a plain '…' literal (PG
		// standard_conforming_strings and DuckDB agree): it must pass through
		// untouched while the quote beside it is still doubled.
		{name: "backslash passes through", input: `a\'b`, expected: `a\''b`},
		// The real payload shape: a pgdsn-quoted DSN embedded in
		// postgres_scan('…') / ATTACH '…' (#301, #290).
		{
			name:     "quoted DSN",
			input:    `host='h' password='p''w' dbname='d'`,
			expected: `host=''h'' password=''p''''w'' dbname=''d''`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, EscapeLiteral(tt.input))
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/sqlutil -run TestEscapeLiteral -v`
Expected: FAIL to compile with `undefined: EscapeLiteral`.

- [ ] **Step 3: Write the implementation**

Create `internal/sqlutil/literal.go`:

```go
package sqlutil

import "strings"

// EscapeLiteral doubles single quotes so a value can be embedded inside a
// single-quoted SQL string literal. PostgreSQL and DuckDB share the doubling
// rule, and both treat backslashes in a plain '…' literal as ordinary
// characters, so doubling quotes is the entire rule. It does not add the
// surrounding quotes, and it is only correct for string-literal context —
// use SanitizeIdentifier for identifiers.
//
// This is the single home of the rule; #307 showed it is load-bearing (an
// unescaped credential in postgres_scan('…') put deployment-configured text
// into SQL structure), and #310 consolidated the copies that previously
// lived in internal/cdc, internal/sqlgen, and the federated e2e harness.
func EscapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/sqlutil -v`
Expected: PASS (all `TestEscapeLiteral` cases plus existing `TestSanitizeIdentifier`).

- [ ] **Step 5: Commit**

```bash
git add internal/sqlutil/literal.go internal/sqlutil/literal_test.go
git commit -m "feat(sqlutil): #310 add EscapeLiteral, the single home of the SQL quote-doubling rule"
```

---

### Task 2: Migrate `internal/cdc`

**Files:**
- Modify: `internal/cdc/duckdb_exporter.go` (delete L88-90 func; call sites L133-134, L192-193, L199; add import)
- Modify: `internal/cdc/redact_test.go` (call sites L62, L91, L103; comments L60, L98-99)

**Interfaces:**
- Consumes: `sqlutil.EscapeLiteral(s string) string` from Task 1.
- Produces: nothing new — behavior-preserving refactor; existing cdc tests are the gate.

- [ ] **Step 1: Delete the local helper and repoint call sites**

In `internal/cdc/duckdb_exporter.go`:

1. Add `"github.com/lychee-technology/forma/internal/sqlutil"` to the import block (it already imports `internal/duckdbinit` and `internal/sqlgen`, so grouping is established).
2. Delete:

```go
// escapeLiteral doubles single quotes for safe embedding in SQL string literals.
func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
```

3. Replace all five call sites (`pgEsc := escapeLiteral(pgConnStr)`, `s3Esc := escapeLiteral(s3TmpPath)`, `mQueryEsc := escapeLiteral(mQuery)`, `eQueryEsc := escapeLiteral(eQuery)`, `clQueryEsc := escapeLiteral(clQuery)`) with `sqlutil.EscapeLiteral(...)`, e.g.:

```go
	pgEsc := sqlutil.EscapeLiteral(pgConnStr)
	s3Esc := sqlutil.EscapeLiteral(s3TmpPath)
```

4. Check whether `"strings"` is still used elsewhere in the file (it is — e.g. other helpers); leave the import if so, drop it only if the compiler complains.

- [ ] **Step 2: Repoint the test callers in `redact_test.go`**

In `internal/cdc/redact_test.go`, add the `sqlutil` import and change the three call sites:

```go
	sqlLiteral := sqlutil.EscapeLiteral(BuildPGDSN(redactHostileParams()))
```

```go
	redacted := redactConnStr(sqlutil.EscapeLiteral(BuildPGDSN(p)))
```

```go
	sqlLiteral := sqlutil.EscapeLiteral(BuildPGDSN(redactTestParams()))
```

Update the two comments that name the old function — `// through the escapeLiteral'd (doubled-quote) form embedded in a DuckDB literal.` becomes `// through the sqlutil.EscapeLiteral'd (doubled-quote) form embedded in a DuckDB literal.`, and `// feed BuildPGDSN's quoted DSN through escapeLiteral (doubling single quotes)` becomes `// feed BuildPGDSN's quoted DSN through sqlutil.EscapeLiteral (doubling single quotes)`.

**Do NOT touch** `internal/cdc/duckdb_exporter_test.go:11-12` — those inline `strings.ReplaceAll` calls are the independent test oracle (Decision 3).

- [ ] **Step 3: Run the cdc test suite**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -count=1`
Expected: PASS. Also verify no stragglers in production code: `grep -rn "escapeLiteral" internal/cdc/ --include="*.go"` should return only `duckdb_exporter_test.go`'s oracle lines (which say `strings.ReplaceAll`, not `escapeLiteral`) — i.e. **zero** hits for the name `escapeLiteral`.

- [ ] **Step 4: Commit**

```bash
git add internal/cdc/duckdb_exporter.go internal/cdc/redact_test.go
git commit -m "refactor(cdc): #310 consume sqlutil.EscapeLiteral, delete local escapeLiteral"
```

---

### Task 3: Migrate `internal/sqlgen` (+ cross-reference in `internal/federated`)

**Files:**
- Modify: `internal/sqlgen/duckdb_template_renderer.go` (delete L351-370; relocate comment to the PG_CONN block at L259-263; add import)
- Modify: `internal/federated/connstring.go` (comment L31-33)

**Interfaces:**
- Consumes: `sqlutil.EscapeLiteral(s string) string` from Task 1.
- Produces: nothing new — behavior-preserving; sqlgen render tests are the gate.

- [ ] **Step 1: Delete `escapeSQLLiteral`, relocate its comment to the call site**

In `internal/sqlgen/duckdb_template_renderer.go`:

1. Add `"github.com/lychee-technology/forma/internal/sqlutil"` to the imports.
2. Delete the entire function **and** its doc comment (L351-370, starting `// escapeSQLLiteral doubles single quotes...` through the closing brace).
3. Replace the PG_CONN injection block (currently ~L259-263) with the comment content reframed for the call site — the #301/#307 substance from PR #333 must survive verbatim-in-substance (Decision 4):

```go
	// PG_CONN is escaped because the templates interpolate it as
	// postgres_scan('{{.PG_CONN}}', …). Since #301 the DSN produced by
	// federated.DuckDBPostgresConnStringFromPool quotes its values, so it
	// legitimately contains single quotes; without escaping the rendered SQL
	// would terminate the literal early and fail to parse.
	//
	// Escaping also closes the pre-existing hole that made quoting unsafe to
	// add: before #301 a Postgres password containing a single quote was
	// interpolated raw, which broke the query and put deployment-configured
	// text into SQL structure. PG_CONN comes from the pgx pool configuration,
	// not from any HTTP caller, so this was never a caller-reachable injection
	// vector — but escaping is still required so credential characters can
	// never alter the rendered SQL. The same rule guards the CDC ATTACH path
	// (#290); both sites consume sqlutil.EscapeLiteral (#310).
	if _, ok := params["PG_CONN"]; !ok {
		if raw, ok := params["DuckDBPGConnString"].(string); ok && raw != "" {
			params["PG_CONN"] = sqlutil.EscapeLiteral(raw)
		}
	}
```

4. If `"strings"` becomes unused in this file, remove it from the imports (check with the compiler, don't guess).

- [ ] **Step 2: Update the cross-reference comment in `internal/federated/connstring.go`**

The `DuckDBPostgresConnStringFromPool` doc comment currently ends:

```go
// The result is embedded in a single-quoted DuckDB SQL literal
// (postgres_scan('{{.PG_CONN}}', …)), so the renderer escapes it; see
// escapeSQLLiteral in internal/sqlgen/duckdb_template_renderer.go.
```

Change the last line to:

```go
// (postgres_scan('{{.PG_CONN}}', …)), so the renderer escapes it with
// sqlutil.EscapeLiteral (internal/sqlgen/duckdb_template_renderer.go).
```

- [ ] **Step 3: Run the sqlgen and federated test suites**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/sqlgen ./internal/federated -count=1`
Expected: PASS. Then `grep -rn "escapeSQLLiteral" internal/ --include="*.go"` — expected: zero hits.

- [ ] **Step 4: Commit**

```bash
git add internal/sqlgen/duckdb_template_renderer.go internal/federated/connstring.go
git commit -m "refactor(sqlgen): #310 consume sqlutil.EscapeLiteral; #333 comment moves to the PG_CONN call site"
```

---

### Task 4: Migrate the federated e2e harness

**Files:**
- Modify: `internal/e2e_harness/federated/query_build.go` (string case of `benchmarkSQLLiteral`, L338-342; add import)

**Interfaces:**
- Consumes: `sqlutil.EscapeLiteral(s string) string` from Task 1.
- Produces: nothing new. This package builds only under `-tags=e2e`, so the compile gate must pass that tag.

- [ ] **Step 1: Repoint the string case**

In `internal/e2e_harness/federated/query_build.go`, add the `sqlutil` import and change only the `string` case of `benchmarkSQLLiteral` (the `int`/`int64`/`float64`/`default` cases don't quote-escape and are out of scope):

```go
func benchmarkSQLLiteral(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", sqlutil.EscapeLiteral(v))
	case int:
```

If `"strings"` becomes unused in the file, remove the import (compiler will say).

- [ ] **Step 2: Verify the e2e-tagged package still compiles**

The harness is behind a build tag, so a plain `go build ./...` won't touch it. Run:

`GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet -tags=e2e ./internal/e2e_harness/federated`
Expected: exit 0, no output. (Do not run the full federated e2e suite here — it needs Docker and 30 minutes; CI runs the `-short` variant.)

- [ ] **Step 3: Commit**

```bash
git add internal/e2e_harness/federated/query_build.go
git commit -m "refactor(e2e): #310 harness consumes sqlutil.EscapeLiteral — decided against keeping the copy"
```

---

### Task 5: Stale-name sweep + full verification

**Files:**
- Modify: `internal/redact/connstring.go` (comments L35, L43)
- Modify: `internal/redact/connstring_test.go` (test-case name L41)
- Modify: `internal/httpapi/credential_redaction_test.go` (comment + test-case name L47-49)

**Interfaces:**
- Consumes: nothing — comment/test-name-only changes.
- Produces: a codebase where `grep -rn "escapeLiteral\|escapeSQLLiteral"` returns zero hits.

- [ ] **Step 1: Rename the mentions in `internal/redact/connstring.go`**

Two comment lines name the old cdc function. **Caution from the file itself:** its comment block warns that gofmt rewrites bare `''` pairs in comment prose into typographic quotes — change ONLY the function-name tokens, do not retype or reflow the quote sequences around them.

`// (pgdsn.Quote), and escapeLiteral additionally doubles every single quote when the` → `// (pgdsn.Quote), and sqlutil.EscapeLiteral additionally doubles every single quote when the`

`//	Branch 1  password=''VALUE''   the escapeLiteral'd doubled form, tried first.` → `//	Branch 1  password=''VALUE''   the EscapeLiteral'd doubled form, tried first.`

- [ ] **Step 2: Rename the test-case names/comments**

`internal/redact/connstring_test.go` L41: `name: "escapeLiteral doubled-quote form inside a SQL literal",` → `name: "EscapeLiteral doubled-quote form inside a SQL literal",`

`internal/httpapi/credential_redaction_test.go` L47-49:

```go
		{
			// sqlutil.EscapeLiteral doubles every quote when a DSN is embedded in a
			// DuckDB SQL literal.
			name: "EscapeLiteral doubled-quote form",
```

- [ ] **Step 3: Confirm the sweep is exhaustive**

Run: `grep -rn "escapeLiteral\|escapeSQLLiteral" --include="*.go" . | grep -v "\.gocache"`
Expected: zero hits. If anything surfaces (docs, other comments), fix it in this step.

Also check non-Go references: `grep -rn "escapeSQLLiteral\|escapeLiteral" docs/ 2>/dev/null` — historical plan/spec docs under `docs/superpowers/` are records of past work and stay untouched; only living docs (e.g. `docs/federated-query/`) would need updating if hit.

- [ ] **Step 4: Full gates**

```bash
make test
make lint
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet -tags=e2e ./internal/e2e_harness/federated
```

Expected: all pass. (`make test` covers `./cdc` wrapper, `./internal/...` including sqlutil, sqlgen, federated, redact, httpapi.)

- [ ] **Step 5: Commit**

```bash
git add internal/redact/connstring.go internal/redact/connstring_test.go internal/httpapi/credential_redaction_test.go
git commit -m "docs: #310 rename stale escapeLiteral/escapeSQLLiteral mentions to sqlutil.EscapeLiteral"
```

---

## PR

Title: `refactor: #310 consolidate the three SQL quote-doubler copies into sqlutil.EscapeLiteral`

Body must include: `Closes #310`, the four locked decisions (helper location, harness consolidates + why the "keep the copy" option was rejected, test-oracle copies stay, #333 comment relocation), and the verification evidence (test/lint/vet output). Per repo rules: 请不要自动合并 PR — leave it open for review.
