# Issue #251: Partial-Read Resilience — One Corrupt Parquet Must Not Fail the Whole Federated Scan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A federated DuckDB scan over a manifest-authored parquet set that contains one corrupt object returns the readable objects' rows plus hot rows — loudly partial (execution-plan note), correctly classified, and breaker-neutral — instead of failing all-or-nothing. Flips `TestManifestConsistency_OneGoodOneBadFile` (#187 scenario 7) from characterization to contract.

**Architecture:** Verify-and-exclude, not a blind reader flag. On a scan failure that classifies as a plain read failure (not manifest inconsistency), a per-file verification pass drains each object individually; confirmed-corrupt objects go into an engine-level TTL cache, path resolution excludes cached objects, and `ExecuteDuckDBFederatedQuery` retries once. Because the excluded set changes `parquetPaths`, the #255 scope-v2 hash re-keys the plan cache automatically. Confirmed corruption never records a breaker failure — the verification pass itself proves the engine and store healthy on the other objects.

**Why not `ignore_errors`:** the issue's own suspicion is correct by design even before the Task-1 spike answers it empirically: any wholesale skip inside `read_parquet` is *silent* — no execution-plan marker, no classification, no per-object attribution — which recreates the #187 scenario-2 silent-loss class this subsystem exists to prevent. The spike documents the empirical semantics for the issue/PR record; it does not change the design.

**Tech Stack:** Go, DuckDB (go-duckdb driver via `internal/federated` engine), testcontainers e2e production harness, RustFS S3.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (`coding-standard.md`). `internal/federated/duckdb_query.go` is already 452 lines — Task 5 moves `resolveScanSources` into a new `duckdb_query_resolve.go` (same seam family as `duckdb_query_build.go`, #220).
- Always wrap errors with context: `fmt.Errorf("...: %w", err)`; match with `errors.Is`/`errors.As`, never string comparison.
- Unit tests: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run <Name> -v`.
- Full unit suite: `make test`. Lint: `make lint` (golangci-lint pinned v1.64.8 — do not upgrade).
- E2E (Docker required): `go test -v -tags=e2e ./internal/e2e_harness/production/ -run <Name> -timeout=15m`.
- Plan-note strings are contracts (#197 lesson): the e2e asserts via the exported constant, never a retyped literal.
- Execution-plan `Notes` NEVER cross the HTTP boundary (`toExecutionPlan` drops them — #301/#306 security adjudication). Do not add corrupt-object keys to any public field.
- Sub-agents: 避免 Sonnet 5；不能用 Haiku 的任务直接用 Opus 4.8（workspace CLAUDE.md）。
- Work in a git worktree branch (e.g. `issue-251-partial-read-resilience`); do not commit to `main`. PR must reference #251 and must NOT be auto-merged (repo PR rules).

## File Structure

| File | Role |
|---|---|
| Create `internal/federated/corrupt_paths.go` | `corruptParquetCache`: TTL memory of verification-confirmed corrupt objects |
| Create `internal/federated/corrupt_paths_test.go` | Unit tests (injectable clock) |
| Create `internal/federated/parquet_verify.go` | `verifyParquetPaths` (per-file full drain) + `corruptParquetRetryError` |
| Create `internal/federated/parquet_verify_test.go` | Unit tests (fake `DuckDBQueryExecutor`) |
| Create `internal/federated/duckdb_query_resolve.go` | `scanSources` struct + `resolveScanSources` (moved from `duckdb_query.go`) |
| Modify `internal/federated/engine.go` | `corruptPaths` field, constructor init, `WithCorruptPathRetention` option |
| Modify `internal/federated/parquet_source.go` | `resolveParquetPaths` filters source-authored paths through the cache |
| Modify `internal/federated/duckdb_query.go` | `StreamDuckDBFederatedQuery` consumes `scanSources`, records exclusion note; `ExecuteDuckDBFederatedQuery` single retry |
| Modify `internal/federated/duckdb_query_execute.go` | Error-path restructure: `failDuckDBScan` + `confirmCorruptPaths`, breaker semantics |
| Modify `internal/federated/duckdb_query_helpers.go` | `NotePartialParquetExclusion` const + `recordCorruptExclusion` planCtx method |
| Modify `internal/e2e_harness/production/parquet_manifest_consistency_e2e_test.go` | Flip scenario 7 to contract; add truncated variant |
| Modify `internal/e2e_harness/production/parquet_fault_helpers_e2e_test.go` | `readParquetRowIDs`, ID-set assertion helpers |
| Modify `docs/federated-query/design.md` | "Partial-read resilience (#251)" section |

**Out of scope (state in PR body):** hint-authored path sets (explicit `S3ParquetPathTemplate` — operator pinned the set, all-or-nothing preserved, pinned by `TestParquetCorruption_WrongSchemaFile_GlobHint`); glob path sets (exclusion is inexpressible in a glob; the manifest fallback glob keeps today's behavior); public-API surfacing of the partial marker (Notes are internal-only per #301/#306 — if product wants an HTTP-visible marker, file a follow-up issue).

---

### Task 1: Spike — empirical DuckDB corruption semantics (no commit)

**Files:**
- Create (temporary, deleted at end of task): `internal/federated/spike_corruption_test.go`

**Interfaces:**
- Produces: written findings (plain text, saved to the PR-notes scratch and later pasted into issue #251 as a comment) answering the issue's first bullet, plus confirmation of two design assumptions.

**Questions this spike must answer:**

1. Does `read_parquet(..., ignore_errors := true)` exist in the pinned go-duckdb version? If yes: with one good + one mid-file-corrupt file, does it skip the corrupt file wholesale, or return partial rows from it (row masking)? Either way, record the answer — it goes to issue #251 verbatim.
2. Assumption A: `DESCRIBE SELECT * FROM read_parquet(f)` **succeeds** on a mid-file-corrupt file (footer intact) — confirming footer probes cannot preflight scenario 7.
3. Assumption B: a full drain `SELECT * FROM read_parquet(f)` **fails** on both corruption classes (mid-file XOR and truncation), while `SELECT COUNT(*)` may succeed on the mid-file class (metadata shortcut). The drain is the committed verification statement.
4. Informational: does the scan error message name the culprit file? (Not load-bearing — verification does attribution — but note it.)

- [ ] **Step 1: Write the spike test** (local files, no S3, no build tag needed — the `-run` filter isolates it)

```go
package federated

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcboeker/go-duckdb/v2"
)

// Spike for #251 — NOT to be committed. Empirically pins DuckDB corruption
// semantics for the pinned driver version. Delete after recording findings.

func spikeDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("duckdb connector: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func spikeWriteParquet(t *testing.T, db *sql.DB, dir, name string, offset int) string {
	t.Helper()
	p := filepath.Join(dir, name)
	_, err := db.ExecContext(context.Background(), fmt.Sprintf(
		"COPY (SELECT uuid() AS row_id, (1000+i) AS changed_at, CAST(NULL AS BIGINT) AS deleted_at, repeat('x', 512) AS payload FROM range(%d, %d) t(i)) TO '%s' (FORMAT PARQUET)",
		offset, offset+500, p))
	if err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	return p
}

func spikeCorruptMid(t *testing.T, p string) {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(data) / 2; i < len(data)/2+64; i++ {
		data[i] ^= 0xFF
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func spikeTruncate(t *testing.T, p string) {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
}

func spikeTry(t *testing.T, db *sql.DB, label, q string) {
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Logf("%s: QUERY ERROR: %v", label, err)
		return
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Logf("%s: rows=%d then STREAM ERROR: %v", label, n, err)
		return
	}
	t.Logf("%s: OK rows=%d", label, n)
}

func TestSpike251CorruptionSemantics(t *testing.T) {
	db := spikeDuckDB(t)
	dir := t.TempDir()
	good := spikeWriteParquet(t, db, dir, "good.parquet", 0)
	mid := spikeWriteParquet(t, db, dir, "mid.parquet", 1000)
	trunc := spikeWriteParquet(t, db, dir, "trunc.parquet", 2000)
	spikeCorruptMid(t, mid)
	spikeTruncate(t, trunc)

	// Assumption A: footer probe passes on mid-file corruption.
	spikeTry(t, db, "DESCRIBE mid", fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", mid))
	spikeTry(t, db, "DESCRIBE trunc", fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", trunc))

	// Assumption B: full drain fails on both; COUNT(*) may not.
	spikeTry(t, db, "drain mid", fmt.Sprintf("SELECT * FROM read_parquet('%s')", mid))
	spikeTry(t, db, "drain trunc", fmt.Sprintf("SELECT * FROM read_parquet('%s')", trunc))
	spikeTry(t, db, "count mid", fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", mid))

	// Issue bullet 1: ignore_errors semantics over a mixed set.
	spikeTry(t, db, "set ignore_errors", fmt.Sprintf(
		"SELECT COUNT(*) FROM read_parquet(['%s','%s'], ignore_errors := true, union_by_name = true)", good, mid))
	spikeTry(t, db, "set drain ignore_errors", fmt.Sprintf(
		"SELECT * FROM read_parquet(['%s','%s'], ignore_errors := true, union_by_name = true)", good, mid))

	// Informational: culprit attribution in the plain-set error.
	spikeTry(t, db, "set plain", fmt.Sprintf(
		"SELECT * FROM read_parquet(['%s','%s'], union_by_name = true)", good, mid))
}
```

If the import path `github.com/marcboeker/go-duckdb/v2` does not match `go.mod`, use whatever the repo pins (check `grep duckdb go.mod` and mirror `internal/federated/duckdb_conn.go`'s import).

- [ ] **Step 2: Run it and record every log line**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run TestSpike251CorruptionSemantics -v`

Save the output to `docs/superpowers/plans/2026-08-02-issue-251-spike-findings.md` with one paragraph of interpretation per question above.

- [ ] **Step 3: Evaluate the decision gates**

- **GATE (blocking):** if `drain mid` does NOT fail (Assumption B broken), STOP — the verification statement is wrong; report BLOCKED with the spike output rather than proceeding. (Fallback direction to investigate before re-planning: per-column aggregates, or `parquet_metadata()` page-level checks.)
- If `DESCRIBE mid` fails (Assumption A broken — a newer DuckDB validates pages at bind time), the design still works (the failure just moves to the Query-error branch); note it in the findings and continue.
- If `ignore_errors` exists and skips whole files: record that it remains rejected for silence reasons (no marker, no classification, no attribution). If it masks rows: record that it is disqualified outright.

- [ ] **Step 4: Delete the spike file** (`rm internal/federated/spike_corruption_test.go`). Keep the findings file — it is committed later with Task 8.

---

### Task 2: Red e2e — flip scenario 7 to the #251 contract

**Files:**
- Modify: `internal/e2e_harness/production/parquet_manifest_consistency_e2e_test.go` (replace `TestManifestConsistency_OneGoodOneBadFile`, lines 72-116; add `TestManifestConsistency_OneGoodOneTruncatedFile`)
- Modify: `internal/e2e_harness/production/parquet_fault_helpers_e2e_test.go` (append helpers)

**Interfaces:**
- Consumes: `fedengine.NotePartialParquetExclusion` (string const, defined in Task 5 — the compile failure until then is part of the red state).
- Consumes existing harness seams: `seedMultiParquet`, `overwriteObjectBytes`, `corruptMidFile`, `truncateHalf`, `env.Query`, `env.AssertQueryMatches`, `env.Duck.DB`, `env.Cluster.Bucket`.
- Produces: helpers `readParquetRowIDs(ctx, t, env, key) map[string]struct{}`, `resultRowIDSet(t, res *QueryResult) map[string]struct{}`, `assertIDSetEqual(t, got, want map[string]struct{})`, `assertCorruptExclusionNote(t, res *QueryResult, key string)`.

- [ ] **Step 1: Confirm the harness record accessor**

Read `internal/e2e_harness/production/query.go` (the `QueryResult` struct around lines 57-65) and note how returned records expose their row ID (e.g. `res.Records[i].RowID` of type `uuid.UUID`, or via `*model.PersistentRecord`). Use that accessor in `resultRowIDSet` below; normalize to lowercase canonical UUID strings on both sides.

- [ ] **Step 2: Append helpers to `parquet_fault_helpers_e2e_test.go`**

```go
// readParquetRowIDs reads one (still readable) parquet object's row_id set
// directly via DuckDB. Cast to VARCHAR: go-duckdb surfaces UUID columns as
// raw bytes otherwise (#147).
func readParquetRowIDs(ctx context.Context, t *testing.T, env *Env, key string) map[string]struct{} {
	t.Helper()
	q := fmt.Sprintf("SELECT row_id::VARCHAR FROM read_parquet('s3://%s/%s')", env.Cluster.Bucket, key)
	rows, err := env.Duck.DB.QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("read parquet row_ids from %s: %v", key, err)
	}
	defer rows.Close()
	ids := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan row_id from %s: %v", key, err)
		}
		ids[strings.ToLower(id)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate row_ids from %s: %v", key, err)
	}
	if len(ids) == 0 {
		t.Fatalf("parquet %s yielded no row_ids; expected a seeded batch", key)
	}
	return ids
}

// resultRowIDSet collects the lowercase row_id set of a query result.
func resultRowIDSet(t *testing.T, res *QueryResult) map[string]struct{} {
	t.Helper()
	ids := map[string]struct{}{}
	for _, rec := range res.Records { // adapt accessor per Step 1
		ids[strings.ToLower(rec.RowID.String())] = struct{}{}
	}
	return ids
}

func setMinus(a, b map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for k := range a {
		if _, drop := b[k]; !drop {
			out[k] = struct{}{}
		}
	}
	return out
}

func assertIDSetEqual(t *testing.T, got, want map[string]struct{}) {
	t.Helper()
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("result missing expected row_id %s", k)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("result has unexpected row_id %s", k)
		}
	}
}

// assertCorruptExclusionNote requires the plan to loudly name the excluded
// object. Notes are internal-plan-only (#301/#306) — this is exactly the
// surface embedders and operators get.
func assertCorruptExclusionNote(t *testing.T, res *QueryResult, key string) {
	t.Helper()
	for _, note := range res.Plan.Notes {
		if strings.Contains(note, fedengine.NotePartialParquetExclusion) && strings.Contains(note, key) {
			return
		}
	}
	t.Errorf("plan notes lack the corrupt-exclusion marker for %s: %v", key, res.Plan.Notes)
}
```

Add imports as needed (`strings`, `fedengine "github.com/lychee-technology/forma/internal/federated"`).

- [ ] **Step 3: Replace `TestManifestConsistency_OneGoodOneBadFile` (mid-file class, full contract)**

```go
// TestManifestConsistency_OneGoodOneBadFile pins #187 scenario 7 as the #251
// contract: one valid and one page-corrupt parquet in the same scan set. The
// corrupt file's footer is intact, so the pre-read validator accepts it and
// the failure surfaces mid-scan; per-file verification then attributes it,
// path resolution excludes it (TTL cache), and one retry answers from the
// readable remainder — partial, loud (plan note names the object), and
// breaker-neutral (confirmed corruption is not engine sickness, so the
// DuckDB route survives a permanently corrupt object).
func TestManifestConsistency_OneGoodOneBadFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}
	healthyIDs := resultRowIDSet(t, healthy)

	// Capture the doomed file's row set BEFORE corrupting it: the batches are
	// disjoint and fully flushed, so the expected partial answer is exactly
	// everything else (no hot version shadows a lost row).
	badIDs := readParquetRowIDs(ctx, t, env, keys[1])
	want := setMinus(healthyIDs, badIDs)

	overwriteObjectBytes(ctx, t, env, keys[1], corruptMidFile)

	partialCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := env.Query(partialCtx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("degraded off: partial read must succeed with the corrupt file excluded (#251), got: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res), want)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("partial read must stay duckdb-routed, got: %+v", res.Plan.Routing)
	}
	assertCorruptExclusionNote(t, res, keys[1])

	// Second query answers from the exclusion cache without a failed scan —
	// still partial, still loud.
	res2, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("cached-exclusion query failed: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res2), want)
	assertCorruptExclusionNote(t, res2, keys[1])

	// A permanently corrupt object must not accumulate breaker failures:
	// well past any failure threshold the route must still be DuckDB.
	for i := 0; i < 8; i++ {
		resN, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
		if err != nil {
			t.Fatalf("query %d failed with a permanently corrupt file present: %v", i, err)
		}
		if !resN.Plan.Routing.UseDuckDB {
			t.Fatalf("query %d lost the duckdb route (breaker tripped on corruption?): %+v", i, resN.Plan.Routing)
		}
	}

	// Degraded mode no longer needs the Postgres-only fallback: the partial
	// parquet read succeeds, so the plan stays DuckDB-routed and partial.
	degraded, err := env.Query(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if err != nil {
		t.Fatalf("degraded-mode query failed: %v", err)
	}
	if !degraded.Plan.Routing.UseDuckDB {
		t.Fatalf("degraded mode fell back to postgres although the partial read succeeds: %+v", degraded.Plan.Routing)
	}
	assertIDSetEqual(t, resultRowIDSet(t, degraded), want)
}
```

- [ ] **Step 4: Add `TestManifestConsistency_OneGoodOneTruncatedFile` (footer-dead class — exercises the Query-error branch instead of the mid-stream branch)**

```go
// TestManifestConsistency_OneGoodOneTruncatedFile is the footer-dead sibling
// of OneGoodOneBadFile: truncation kills the footer, so the failure surfaces
// at Query (bind) time rather than mid-stream. Same #251 contract, other
// error branch.
func TestManifestConsistency_OneGoodOneTruncatedFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}
	healthyIDs := resultRowIDSet(t, healthy)
	badIDs := readParquetRowIDs(ctx, t, env, keys[1])
	want := setMinus(healthyIDs, badIDs)

	overwriteObjectBytes(ctx, t, env, keys[1], truncateHalf)

	partialCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := env.Query(partialCtx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("truncated file: partial read must succeed with it excluded (#251), got: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res), want)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("partial read must stay duckdb-routed, got: %+v", res.Plan.Routing)
	}
	assertCorruptExclusionNote(t, res, keys[1])
}
```

- [ ] **Step 5: Verify red for the right reason**

Run: `go test -v -tags=e2e ./internal/e2e_harness/production/ -run 'TestManifestConsistency_OneGoodOne' -timeout=15m`

Expected: **compile failure** on `fedengine.NotePartialParquetExclusion` (undefined). That is the intended red state; the behavioral red is re-verified in Task 7 Step 1 once the constant exists. Do NOT stub the constant here.

Also confirm the untouched scenario-2 test still compiles/passes: `-run TestManifestConsistency_MissingListedParquet`. (It will run — it does not reference the new constant.)

- [ ] **Step 6: Commit**

```bash
git add internal/e2e_harness/production/parquet_manifest_consistency_e2e_test.go internal/e2e_harness/production/parquet_fault_helpers_e2e_test.go
git commit -m "test(e2e): #251 flip scenario 7 to partial-read contract (red)"
```

**Amendment (review round 1):** both tests construct their Env with `WithBreaker(1, …)` (env.go:124) — with threshold 1, the first post-corruption query succeeding is the real breaker-neutrality discriminator (a stray RecordFailure opens the breaker and rejects the in-request retry); the truncated test runs its FIRST post-corruption query with `AllowPartialDegradedMode: true` (pins no-fallback on first encounter) followed by a degraded-OFF cache-path query; the first partial query in each test also asserts total-records == len(want).

---

### Task 3: `corruptParquetCache` — TTL memory of confirmed-corrupt objects

**Files:**
- Create: `internal/federated/corrupt_paths.go`
- Test: `internal/federated/corrupt_paths_test.go`

**Interfaces:**
- Produces: `newCorruptParquetCache(ttl time.Duration) *corruptParquetCache`; methods `Add(paths []string)` and `Split(paths []string) (kept, excluded []string)`; both nil-receiver-safe (`Split` on nil returns `(paths, nil)`). Field `now func() time.Time` for test clock injection.

- [ ] **Step 1: Write failing tests**

```go
package federated

import (
	"testing"
	"time"
)

func TestCorruptParquetCacheSplitExcludesUnexpired(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	kept, excluded := c.Split([]string{"s3://b/good.parquet", "s3://b/bad.parquet"})
	if len(kept) != 1 || kept[0] != "s3://b/good.parquet" {
		t.Fatalf("kept = %v", kept)
	}
	if len(excluded) != 1 || excluded[0] != "s3://b/bad.parquet" {
		t.Fatalf("excluded = %v", excluded)
	}
}

func TestCorruptParquetCacheEntryExpires(t *testing.T) {
	base := time.Unix(1000, 0)
	c := newCorruptParquetCache(5 * time.Minute)
	c.now = func() time.Time { return base }
	c.Add([]string{"s3://b/bad.parquet"})

	c.now = func() time.Time { return base.Add(5*time.Minute + time.Second) }
	kept, excluded := c.Split([]string{"s3://b/bad.parquet"})
	if len(kept) != 1 || len(excluded) != 0 {
		t.Fatalf("expired entry must be re-admitted: kept=%v excluded=%v", kept, excluded)
	}
	// The expired entry is dropped, not retained.
	if len(c.expires) != 0 {
		t.Fatalf("expired entry not evicted: %v", c.expires)
	}
}

func TestCorruptParquetCacheNilSafe(t *testing.T) {
	var c *corruptParquetCache
	c.Add([]string{"s3://b/x.parquet"}) // must not panic
	kept, excluded := c.Split([]string{"s3://b/x.parquet"})
	if len(kept) != 1 || excluded != nil {
		t.Fatalf("nil cache must pass paths through: kept=%v excluded=%v", kept, excluded)
	}
}
```

- [ ] **Step 2: Run tests, verify they fail** with "undefined: newCorruptParquetCache".

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run TestCorruptParquetCache -v`

- [ ] **Step 3: Implement**

```go
package federated

import (
	"sync"
	"time"
)

// corruptParquetCache remembers parquet objects that failed per-file
// verification (#251) so path resolution can exclude them without re-failing
// the scan every query. Entries expire after ttl — a terminal verdict must
// never be memoized forever (#326 lesson): a repaired object, a compaction
// that retires the key, or a manifest reconcile self-heals only through
// re-verification after expiry.
type corruptParquetCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	now     func() time.Time
	expires map[string]time.Time
}

func newCorruptParquetCache(ttl time.Duration) *corruptParquetCache {
	return &corruptParquetCache{ttl: ttl, now: time.Now, expires: map[string]time.Time{}}
}

// Add records paths as corrupt until ttl elapses.
func (c *corruptParquetCache) Add(paths []string) {
	if c == nil || len(paths) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := c.now().Add(c.ttl)
	for _, p := range paths {
		c.expires[p] = exp
	}
}

// Split partitions paths into kept (not known-corrupt) and excluded
// (known-corrupt, unexpired), evicting expired entries on the way.
func (c *corruptParquetCache) Split(paths []string) (kept, excluded []string) {
	if c == nil {
		return paths, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for _, p := range paths {
		exp, ok := c.expires[p]
		if ok && now.Before(exp) {
			excluded = append(excluded, p)
			continue
		}
		if ok {
			delete(c.expires, p)
		}
		kept = append(kept, p)
	}
	return kept, excluded
}
```

- [ ] **Step 4: Run tests, verify they pass.** Same command as Step 2.

- [ ] **Step 5: Commit**

```bash
git add internal/federated/corrupt_paths.go internal/federated/corrupt_paths_test.go
git commit -m "feat(federated): #251 TTL cache of verification-confirmed corrupt parquet objects"
```

---

### Task 4: `verifyParquetPaths` — per-file full-drain verification

**Files:**
- Create: `internal/federated/parquet_verify.go`
- Test: `internal/federated/parquet_verify_test.go`

**Interfaces:**
- Consumes: `DuckDBQueryExecutor` (engine.go:39, `Query(ctx, sql string, args ...any) (duckDBRowsIterator, error)`) and `duckDBRowsIterator` (duckdb_query.go:28).
- Produces: `verifyParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, paths []string) []string` (returns confirmed-unreadable paths; nil on ctx cancellation); `corruptParquetRetryError` with fields `Corrupt []string`, method `Error() string`, `Unwrap() error`.

- [ ] **Step 1: Write failing tests**

```go
package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type verifyFakeRows struct{ streamErr error }

func (r *verifyFakeRows) Next() bool             { return false }
func (r *verifyFakeRows) Scan(dest ...any) error { return nil }
func (r *verifyFakeRows) Err() error             { return r.streamErr }
func (r *verifyFakeRows) Close() error           { return nil }

// verifyFakeDuck fails Query for any SQL mentioning a failPath; midStream
// paths open fine and fail while iterating (the corruptMidFile class).
type verifyFakeDuck struct {
	failOpen   map[string]bool
	failStream map[string]bool
	queries    []string
}

func (d *verifyFakeDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	d.queries = append(d.queries, sqlStr)
	for p := range d.failOpen {
		if strings.Contains(sqlStr, p) {
			return nil, fmt.Errorf("IO Error: cannot open %s", p)
		}
	}
	for p := range d.failStream {
		if strings.Contains(sqlStr, p) {
			return &verifyFakeRows{streamErr: fmt.Errorf("corrupt page in %s", p)}, nil
		}
	}
	return &verifyFakeRows{}, nil
}

func TestVerifyParquetPathsFlagsOpenAndStreamFailures(t *testing.T) {
	duck := &verifyFakeDuck{
		failOpen:   map[string]bool{"s3://b/trunc.parquet": true},
		failStream: map[string]bool{"s3://b/mid.parquet": true},
	}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/good.parquet", "s3://b/trunc.parquet", "s3://b/mid.parquet"})
	if len(corrupt) != 2 {
		t.Fatalf("corrupt = %v, want trunc+mid", corrupt)
	}
}

func TestVerifyParquetPathsSkipsUnverifiableEntries(t *testing.T) {
	duck := &verifyFakeDuck{}
	corrupt := verifyParquetPaths(context.Background(), duck,
		[]string{"s3://b/*.parquet", "s3://b/it's.parquet"})
	if corrupt != nil {
		t.Fatalf("glob/quote entries must be skipped, got %v", corrupt)
	}
	if len(duck.queries) != 0 {
		t.Fatalf("unverifiable entries must not be probed: %v", duck.queries)
	}
}

func TestVerifyParquetPathsCancelledContextVerifiesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/x.parquet": true}}
	if got := verifyParquetPaths(ctx, duck, []string{"s3://b/x.parquet"}); got != nil {
		t.Fatalf("cancelled verification must confirm nothing, got %v", got)
	}
}

func TestCorruptParquetRetryErrorChain(t *testing.T) {
	cause := fmt.Errorf("scan: %w: boom", ErrFederatedReadFailed)
	err := &corruptParquetRetryError{Corrupt: []string{"s3://b/bad.parquet"}, cause: cause}
	if !errors.Is(err, ErrFederatedReadFailed) {
		t.Fatal("retry error must keep the ErrFederatedReadFailed classification")
	}
	if !strings.Contains(err.Error(), "s3://b/bad.parquet") {
		t.Fatalf("retry error must name the corrupt objects: %v", err)
	}
}
```

- [ ] **Step 2: Run tests, verify failure** ("undefined: verifyParquetPaths").

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run 'TestVerifyParquetPaths|TestCorruptParquetRetryError' -v`

- [ ] **Step 3: Implement**

```go
package federated

import (
	"context"
	"fmt"
	"strings"
)

// verifyParquetPaths re-reads each path individually via a full SELECT *
// drain and returns the paths whose drain failed. The drain reads a superset
// of any scan's columns, so it deterministically reproduces whatever
// per-file failure made the set scan fail; metadata-only probes (DESCRIBE,
// COUNT(*)) cannot, because DuckDB answers them without touching data pages
// (#251 spike). Corruption that decodes silently is invisible to every
// reader — the set scan included — so it can never reach this function;
// that pre-existing integrity gap is documented in the #251 spike findings
// and tracked in a follow-up issue. Glob and quote-bearing
// entries are skipped: unverifiable means unexcludable, and the main scan
// keeps its all-or-nothing behavior for them. A cancelled context confirms
// nothing — cancellation is indistinguishable from corruption for the
// remaining paths. Runs only on the read-failure path, so its cost is
// bounded by failures, not by queries.
func verifyParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, paths []string) []string {
	if duck == nil {
		return nil
	}
	var corrupt []string
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) || strings.ContainsAny(path, "*?[") {
			continue
		}
		if err := drainParquet(ctx, duck, path); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			corrupt = append(corrupt, path)
		}
	}
	return corrupt
}

// drainParquet opens one parquet object and iterates it to exhaustion.
func drainParquet(ctx context.Context, duck DuckDBQueryExecutor, path string) error {
	rows, err := duck.Query(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		return fmt.Errorf("open parquet %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read parquet %s: %w", path, err)
	}
	return nil
}

// corruptParquetRetryError marks a scan failure attributed to specific
// corrupt objects that are now cached for exclusion (#251): one immediate
// retry will scan without them. Unwrap keeps the original classification
// chain (ErrFederatedReadFailed), so a caller that does not retry — a direct
// Stream consumer — degrades exactly as before.
type corruptParquetRetryError struct {
	Corrupt []string
	cause   error
}

func (e *corruptParquetRetryError) Error() string {
	return fmt.Sprintf("corrupt parquet objects excluded, retry available %v: %v", e.Corrupt, e.cause)
}

func (e *corruptParquetRetryError) Unwrap() error { return e.cause }
```

- [ ] **Step 4: Run tests, verify pass.** Same command as Step 2.

- [ ] **Step 5: Commit**

```bash
git add internal/federated/parquet_verify.go internal/federated/parquet_verify_test.go
git commit -m "feat(federated): #251 per-file drain verification + retryable corruption error"
```

---

### Task 5: Engine wiring — cache field, resolve-time exclusion, plan note

**Files:**
- Modify: `internal/federated/engine.go` (struct ~line 47-80, constructor ~line 125-143, options ~line 82-104)
- Modify: `internal/federated/parquet_source.go` (`resolveParquetPaths`, lines 52-77)
- Create: `internal/federated/duckdb_query_resolve.go` (move `resolveScanSources` + `schemaCacheByID` out of `duckdb_query.go` lines 149-229; introduce `scanSources`)
- Modify: `internal/federated/duckdb_query.go` (`StreamDuckDBFederatedQuery` lines 119-146 consume the struct and record the note)
- Modify: `internal/federated/duckdb_query_helpers.go` (note const + planCtx recorder)
- Test: `internal/federated/parquet_source_test.go` (append; create if absent)

**Interfaces:**
- Consumes: `corruptParquetCache` (Task 3).
- Produces:
  - engine field `corruptPaths *corruptParquetCache`, initialized in `NewDBFederatedQueryEngine` with `defaultCorruptPathRetention = 5 * time.Minute`.
  - `WithCorruptPathRetention(d time.Duration) EngineOption`.
  - `resolveParquetPaths` new signature: `(paths []string, fromSource bool, excludedCorrupt []string, err error)` — `excludedCorrupt` non-nil only for source-authored sets.
  - `type scanSources struct { paths []string; fromSource bool; graceCutoffMs int64; coldMissing []sqlgen.NullScanColumn; excludedCorrupt []string }`; `resolveScanSources(ctx, q) (scanSources, error)`.
  - Exported const `NotePartialParquetExclusion = "partial parquet scan: excluded corrupt objects"` and method `(*duckDBExecutionPlanContext) recordCorruptExclusion(excluded []string)`.

- [ ] **Step 1: Write failing unit tests for resolve-time exclusion** (append to `parquet_source_test.go`; reuse an existing fake `ParquetSource` if one exists in the federated test files — grep `ParquetSource` in `internal/federated/*_test.go` — otherwise define this one)

```go
type fakePathsSource struct{ paths []string }

func (f *fakePathsSource) Paths(ctx context.Context, schemaID int16) ([]string, error) {
	return f.paths, nil
}
func (f *fakePathsSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	return nil, nil
}

func newExclusionTestEngine(src ParquetSource) *DBFederatedQueryEngine {
	return NewDBFederatedQueryEngine(nil, nil, nil, nil, forma.DuckDBConfig{}, nil, "", WithParquetSource(src))
}

func TestResolveParquetPathsExcludesCachedCorrupt(t *testing.T) {
	e := newExclusionTestEngine(&fakePathsSource{paths: []string{"s3://b/a.parquet", "s3://b/bad.parquet"}})
	e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

	paths, fromSource, excluded, err := e.resolveParquetPaths(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !fromSource {
		t.Fatal("source-authored set must report fromSource")
	}
	if len(paths) != 1 || paths[0] != "s3://b/a.parquet" {
		t.Fatalf("paths = %v", paths)
	}
	if len(excluded) != 1 || excluded[0] != "s3://b/bad.parquet" {
		t.Fatalf("excluded = %v", excluded)
	}
}

func TestResolveParquetPathsAllCorruptKeepsFullSet(t *testing.T) {
	// Excluding everything would turn total corruption into a quiet
	// ErrNoParquetPaths misconfiguration; the full set must scan and fail
	// loudly with today's classification instead.
	e := newExclusionTestEngine(&fakePathsSource{paths: []string{"s3://b/bad.parquet"}})
	e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

	paths, _, excluded, err := e.resolveParquetPaths(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || len(excluded) != 0 {
		t.Fatalf("all-corrupt set must pass through unfiltered: paths=%v excluded=%v", paths, excluded)
	}
}

func TestResolveParquetPathsHintSetNeverFiltered(t *testing.T) {
	e := newExclusionTestEngine(&fakePathsSource{paths: []string{"s3://b/a.parquet"}})
	e.corruptPaths.Add([]string{"s3://hinted/x.parquet"})
	q := &model.FederatedAttributeQuery{SchemaID: 7}
	q.DuckDBHints.S3ParquetPathTemplate = "s3://hinted/x.parquet"

	paths, fromSource, excluded, err := e.resolveParquetPaths(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if fromSource || len(excluded) != 0 || len(paths) != 1 || paths[0] != "s3://hinted/x.parquet" {
		t.Fatalf("hint sets are operator-pinned and must not be filtered: paths=%v fromSource=%v excluded=%v", paths, fromSource, excluded)
	}
}
```

Adapt the hint-injection line to the actual `DuckDBHints` field shape (see `duckDBParquetPathsForQuery` in duckdb_query_build.go:278 for how the hint is read).

- [ ] **Step 2: Run, verify failure** (signature mismatch / undefined field).

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run TestResolveParquetPaths -v`

- [ ] **Step 3: Implement engine field + option** (engine.go)

Add to the `DBFederatedQueryEngine` struct after `schemaValidator`:

```go
	// corruptPaths remembers verification-confirmed corrupt parquet objects
	// (#251) so resolveParquetPaths can exclude them. TTL-bounded — see
	// corruptParquetCache. Only source-authored path sets consult it.
	corruptPaths *corruptParquetCache
```

In `NewDBFederatedQueryEngine`, add to the literal: `corruptPaths: newCorruptParquetCache(defaultCorruptPathRetention),`

Add near the options:

```go
// defaultCorruptPathRetention bounds how long a confirmed-corrupt parquet
// object stays excluded before being re-verified (#251).
const defaultCorruptPathRetention = 5 * time.Minute

// WithCorruptPathRetention overrides how long a verification-confirmed
// corrupt parquet object stays excluded from path resolution (#251). The
// entry always expires — a terminal verdict must never be memoized forever
// (#326): repair, compaction, or manifest reconcile self-heal only through
// re-verification.
func WithCorruptPathRetention(d time.Duration) EngineOption {
	return func(e *DBFederatedQueryEngine) { e.corruptPaths = newCorruptParquetCache(d) }
}
```

- [ ] **Step 4: Rewrite `resolveParquetPaths`** (parquet_source.go lines 52-77) — hint branch returns `(hinted, false, nil, err)`; source branch becomes:

```go
	resolved, err := e.parquetSource.Paths(ctx, q.SchemaID)
	if err != nil {
		if errors.Is(err, forma.ErrManifestSchemaMismatch) {
			return nil, false, nil, fmt.Errorf("manifest parquet source: %w", err)
		}
		return nil, false, nil, fmt.Errorf("manifest parquet source: %w: %w", ErrFederatedReadFailed, err)
	}
	// Exclude verification-confirmed corrupt objects (#251). If that would
	// empty the set, scan the full set instead: total corruption must fail
	// loudly with its own classification, not as ErrNoParquetPaths.
	kept, excluded := e.corruptPaths.Split(resolved)
	if len(kept) == 0 {
		return resolved, true, nil, nil
	}
	return kept, true, excluded, nil
```

(Keep the existing doc comment, extend it with the #251 exclusion sentence. Update the two nil-engine early returns to the 4-value shape.)

- [ ] **Step 5: Extract `duckdb_query_resolve.go` and introduce `scanSources`**

Move `resolveScanSources` and `schemaCacheByID` (duckdb_query.go lines 149-229) verbatim into the new file (mechanical move — use the original text, do not retype), then change the signature to:

```go
// scanSources is the resolved storage context of one federated read.
type scanSources struct {
	paths           []string
	fromSource      bool
	graceCutoffMs   int64
	coldMissing     []sqlgen.NullScanColumn
	excludedCorrupt []string
}

func (e *DBFederatedQueryEngine) resolveScanSources(ctx context.Context, q *model.FederatedAttributeQuery) (scanSources, error) {
```

Inside, the resolution call becomes:

```go
	paths, fromSource, excludedCorrupt, err := e.resolveParquetPaths(ctx, q)
```

and every return is adapted to the struct (error returns: `return scanSources{}, ...`; the success return carries all five fields). The empty-set guard and validator block stay byte-identical in logic.

- [ ] **Step 6: Adapt `StreamDuckDBFederatedQuery`** (duckdb_query.go lines 119-146):

```go
	src, err := e.resolveScanSources(ctx, q)
	if err != nil {
		return 0, err
	}
	// A partial scan must be loud (#251): the plan names every excluded
	// object. Notes stay internal — toExecutionPlan drops them at the HTTP
	// boundary (#301/#306).
	planCtx.recordCorruptExclusion(src.excludedCorrupt)
```

and thread `src.paths`, `src.graceCutoffMs`, `src.coldMissing` into the build call and `scan{parquetPaths: src.paths, pathsFromSource: src.fromSource, dirtyIDs: dirtyIDs}` into the execute call.

- [ ] **Step 7: Note const + recorder** (duckdb_query_helpers.go — mirror the guard idiom of the existing planCtx record methods in that file):

```go
// NotePartialParquetExclusion prefixes the execution-plan note recording a
// partial parquet scan (#251): verification-confirmed corrupt objects were
// excluded and the query answered from the readable remainder. Notes never
// cross the HTTP boundary (toExecutionPlan drops them, #301/#306); embedders
// and the e2e harness assert on this exported prefix.
const NotePartialParquetExclusion = "partial parquet scan: excluded corrupt objects"

// recordCorruptExclusion notes the excluded object set on the plan. No-op
// for an empty set or when no plan was requested.
func (p *duckDBExecutionPlanContext) recordCorruptExclusion(excluded []string) {
	if len(excluded) == 0 || p == nil || p.opts == nil || !p.opts.IncludeExecutionPlan || p.opts.ExecutionPlan == nil {
		return
	}
	p.opts.ExecutionPlan.Notes = append(p.opts.ExecutionPlan.Notes,
		fmt.Sprintf("%s: %v", NotePartialParquetExclusion, excluded))
}
```

If the existing record methods lazily allocate `opts.ExecutionPlan` instead of guarding on nil, mirror that idiom exactly.

- [ ] **Step 8: Run unit tests — new ones pass, whole package still green**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -v` — fix any call site the signature change broke (`go build ./...` first if quicker). Expected: PASS.

- [ ] **Step 9: Check file sizes** — `wc -l internal/federated/duckdb_query.go internal/federated/duckdb_query_resolve.go` — both must be ≤500.

- [ ] **Step 10: Commit**

```bash
git add internal/federated/engine.go internal/federated/parquet_source.go internal/federated/duckdb_query.go internal/federated/duckdb_query_resolve.go internal/federated/duckdb_query_helpers.go internal/federated/parquet_source_test.go
git commit -m "feat(federated): #251 resolve-time corrupt-object exclusion + loud plan note"
```

---

### Task 6: Failure-path classification, breaker semantics, single retry

**Files:**
- Modify: `internal/federated/duckdb_query_execute.go` (error branches, lines 44-79)
- Modify: `internal/federated/duckdb_query.go` (`ExecuteDuckDBFederatedQuery`, lines 47-65)
- Test: `internal/federated/duckdb_query_execute_test.go` (append; create if absent)

**Interfaces:**
- Consumes: `verifyParquetPaths`, `corruptParquetRetryError` (Task 4), `e.corruptPaths` (Task 5), `classifyDuckDBReadError` (parquet_source.go:85), `CircuitBreaker` (`RecordFailure`, `RecordSuccess`, `ReleaseProbe`, `IsOpen` — circuit_breaker.go).
- Produces: `(e *DBFederatedQueryEngine) failDuckDBScan(ctx, q, sc scan, cause error, op string) error` and `(e *DBFederatedQueryEngine) confirmCorruptPaths(ctx, sc scan) []string`.

**Ordering contract (do not reorder):** missing-object classification (`classifyDuckDBReadError` → `ParquetSetInconsistentError`, non-degradable, breaker failure, NO retry) runs BEFORE corruption verification. A deleted-but-listed object is manifest inconsistency (#187 scenario 2), never "corrupt".

- [ ] **Step 1: Write failing unit tests**

Check `circuit_breaker.go` for the constructor name/signature (grep `func NewCircuitBreaker`) and use a threshold-1 breaker so a single `RecordFailure` is observable via `IsOpen()`.

```go
type execFakeSource struct{ missing []string }

func (f *execFakeSource) Paths(ctx context.Context, schemaID int16) ([]string, error) {
	return nil, nil // unused in these tests
}
func (f *execFakeSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	return f.missing, nil
}

func TestFailDuckDBScanConfirmedCorruptionSkipsBreakerAndRetries(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/bad.parquet": true}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute) // threshold 1: any RecordFailure opens it
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/good.parquet", "s3://b/bad.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7}, sc,
		fmt.Errorf("scan: %w: page corrupt", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("confirmed corruption must return the retryable error, got: %v", err)
	}
	if len(retry.Corrupt) != 1 || retry.Corrupt[0] != "s3://b/bad.parquet" {
		t.Fatalf("retry.Corrupt = %v", retry.Corrupt)
	}
	if breaker.IsOpen() {
		t.Fatal("confirmed per-file corruption must not feed the breaker (#251)")
	}
	if !errors.Is(err, ErrFederatedReadFailed) {
		t.Fatal("classification chain must survive for non-retrying callers")
	}
	kept, excluded := e.corruptPaths.Split(sc.parquetPaths)
	if len(excluded) != 1 || len(kept) != 1 {
		t.Fatalf("corrupt object must be cached for exclusion: kept=%v excluded=%v", kept, excluded)
	}
}

func TestFailDuckDBScanMissingObjectStaysInconsistentAndBreakerWorthy(t *testing.T) {
	duck := &verifyFakeDuck{}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{missing: []string{"7/x.parquet"}}))

	sc := scan{parquetPaths: []string{"s3://b/7/x.parquet", "s3://b/7/y.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7}, sc,
		fmt.Errorf("scan: %w: no such file", ErrFederatedReadFailed), "execute duckdb query")

	if !errors.Is(err, ErrParquetSetInconsistent) {
		t.Fatalf("missing listed object must classify as inconsistency, got: %v", err)
	}
	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("manifest inconsistency must never be retried as corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("inconsistency failure must still feed the breaker")
	}
}

func TestFailDuckDBScanAllPathsUnreadableIsEngineSickness(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{
		"s3://b/a.parquet": true, "s3://b/b.parquet": true,
	}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/a.parquet", "s3://b/b.parquet"}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7}, sc,
		fmt.Errorf("scan: %w: io error", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("every-object-unreadable is store/engine sickness, not per-file corruption")
	}
	if !breaker.IsOpen() {
		t.Fatal("store-wide failure must feed the breaker")
	}
}

func TestFailDuckDBScanHintPathsNeverVerified(t *testing.T) {
	duck := &verifyFakeDuck{failOpen: map[string]bool{"s3://b/bad.parquet": true}}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, forma.DuckDBConfig{}, nil, "",
		WithParquetSource(&execFakeSource{}))

	sc := scan{parquetPaths: []string{"s3://b/good.parquet", "s3://b/bad.parquet"}, pathsFromSource: false}
	err := e.failDuckDBScan(context.Background(), &model.FederatedAttributeQuery{SchemaID: 7}, sc,
		fmt.Errorf("scan: %w: page corrupt", ErrFederatedReadFailed), "execute duckdb query")

	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		t.Fatal("operator-pinned hint sets keep all-or-nothing semantics")
	}
	if len(duck.queries) != 0 {
		t.Fatalf("hint-set failure must not trigger verification probes: %v", duck.queries)
	}
}
```

- [ ] **Step 2: Run, verify failure** ("undefined: failDuckDBScan").

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -run TestFailDuckDBScan -v`

- [ ] **Step 3: Implement `failDuckDBScan` + `confirmCorruptPaths`** (duckdb_query_execute.go)

```go
// failDuckDBScan classifies a failed scan and reports it to the breaker.
// Classification order is a contract: a manifest-listed object missing from
// storage is inconsistency (#187 scenario 2) — non-degradable, breaker-worthy,
// never retried — and must win over the corruption probe. Confirmed per-file
// corruption (#251) is the one outcome that is NOT engine sickness: the
// verification pass just read every other object through the same engine and
// session, so it hands back the probe slot instead of recording a failure —
// a permanently corrupt object must not hold the breaker open forever.
func (e *DBFederatedQueryEngine) failDuckDBScan(ctx context.Context, q *model.FederatedAttributeQuery, sc scan, cause error, op string) error {
	classified := e.classifyDuckDBReadError(ctx, q, sc.parquetPaths, sc.pathsFromSource)
	var inconsistent *ParquetSetInconsistentError
	if errors.As(classified, &inconsistent) {
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return fmt.Errorf("%s: %w: %w", op, classified, cause)
	}
	if corrupt := e.confirmCorruptPaths(ctx, sc); len(corrupt) > 0 {
		e.corruptPaths.Add(corrupt)
		if e.breaker != nil {
			e.breaker.ReleaseProbe()
		}
		return &corruptParquetRetryError{Corrupt: corrupt, cause: fmt.Errorf("%s: %w: %w", op, classified, cause)}
	}
	if e.breaker != nil {
		e.breaker.RecordFailure()
	}
	return fmt.Errorf("%s: %w: %w", op, classified, cause)
}

// confirmCorruptPaths runs per-file verification when the failed scan ran
// over a source-authored multi-object set. It confirms corruption only when
// at least one object verified readable — if every object fails to read, the
// store or engine is sick, not the files, and exclusion would be both wrong
// and useless (an empty remainder cannot answer the query).
func (e *DBFederatedQueryEngine) confirmCorruptPaths(ctx context.Context, sc scan) []string {
	if !sc.pathsFromSource || len(sc.parquetPaths) < 2 || e == nil || e.duck == nil {
		return nil
	}
	corrupt := verifyParquetPaths(ctx, e.duck, sc.parquetPaths)
	if len(corrupt) == 0 || len(corrupt) >= len(sc.parquetPaths) {
		return nil
	}
	return corrupt
}
```

- [ ] **Step 4: Rewire the two error branches of `executeAndStreamDuckDB`**

Query-error branch (currently lines 45-58) becomes:

```go
	if err != nil {
		// #306: a postgres_scan attach failure echoes the whole conn string,
		// password included, in DuckDB's own prose. Scrub before the text
		// enters any chain or the execution-plan failure note — this is the
		// source, so every consumer (embedder logs, future transports) is
		// covered without repeating #301's boundary redaction.
		err = redact.Error(err)
		planCtx.recordQueryFailure(err)
		return 0, e.failDuckDBScan(ctx, q, sc, err, "execute duckdb query")
	}
```

Mid-stream branch (currently lines 62-79) becomes:

```go
	totalRecords, rowCount, err := e.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		// #306: lazy object opens mean the attach failure can surface here
		// instead of at Query; same scrub, same reason as above.
		err = redact.Error(err)
		if !errors.Is(err, ErrFederatedReadFailed) {
			// Handler errors are not read failures: they report to the
			// breaker as before and pass through unclassified.
			if e.breaker != nil {
				e.breaker.RecordFailure()
			}
			return 0, fmt.Errorf("stream duckdb federated rows: %w", err)
		}
		// Free the single pooled connection (#285 SetMaxOpenConns(1)) BEFORE
		// verification issues its own DuckDB queries — the deferred Close
		// runs too late and would deadlock the pool. sql.Rows.Close is
		// idempotent, so the defer stays harmless.
		_ = rows.Close()
		return 0, e.failDuckDBScan(ctx, q, sc, err, "stream duckdb federated rows")
	}
```

Verify the concrete iterator returned by `e.duck.Query` tolerates a second `Close()` (it wraps `*sql.Rows`, whose Close is idempotent — confirm in `internal/federated/duckdb_conn.go`). If it does not, guard with a `closed` flag instead of relying on idempotence.

- [ ] **Step 5: Single retry in `ExecuteDuckDBFederatedQuery`** (duckdb_query.go lines 47-65)

```go
func (e *DBFederatedQueryEngine) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	recs, total, err := e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		// The failed pass confirmed and cached the corrupt objects; path
		// resolution now excludes them, so one retry answers from the
		// readable remainder (#251). A second retryable failure surfaces:
		// corruption appearing mid-flight is indistinguishable from a sick
		// store and must not loop.
		recs, total, err = e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
		if err != nil {
			return nil, 0, fmt.Errorf("retry after excluding corrupt parquet %v: %w", retry.Corrupt, err)
		}
	}
	if err != nil {
		return nil, 0, err
	}
	return recs, total, nil
}

// collectDuckDBFederatedQuery is one buffered pass of the streaming path; the
// fresh slice per pass is what makes the #251 retry safe after a mid-stream
// failure already delivered partial rows.
func (e *DBFederatedQueryEngine) collectDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	var recs []*model.PersistentRecord
	total, err := e.StreamDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts, func(ctx context.Context, rp *model.PersistentRecord) error {
		recs = append(recs, rp)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return recs, total, nil
}
```

- [ ] **Step 6: Run the new tests and the whole package**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated -v`
Expected: PASS, including all pre-existing engine/breaker tests.

- [ ] **Step 7: Commit**

```bash
git add internal/federated/duckdb_query_execute.go internal/federated/duckdb_query.go internal/federated/duckdb_query_execute_test.go
git commit -m "feat(federated): #251 verify-and-exclude retry; confirmed corruption is breaker-neutral"
```

---

### Task 7: Green e2e + full regression sweep

**Files:** none new — verification only.

- [ ] **Step 1: The flipped scenario-7 tests go green**

Run: `go test -v -tags=e2e ./internal/e2e_harness/production/ -run 'TestManifestConsistency' -timeout=20m`
Expected: `TestManifestConsistency_OneGoodOneBadFile`, `TestManifestConsistency_OneGoodOneTruncatedFile`, and the untouched `TestManifestConsistency_MissingListedParquet` all PASS. If the partial query returns wrong rows, dump `res.Plan.Notes` and the artifact dir before touching production code — use superpowers:systematic-debugging.

- [ ] **Step 2: The single-file corruption scenarios keep today's loud all-or-nothing behavior** (their scan set has one parquet, so the all-corrupt guard forbids exclusion)

Run: `go test -v -tags=e2e ./internal/e2e_harness/production/ -run 'TestParquetCorruption|TestParquetPermission' -timeout=20m`
Expected: PASS unchanged.

- [ ] **Step 3: Full production harness + unit + lint**

Run, each must pass:
```bash
make test
make lint
make test-e2e-production
go test -v ./internal/e2e_harness/federated/... -tags=e2e -short -timeout=30m
```
Known flakes (rerun once before suspecting the branch): `TestConcurrentFlushSnapshot`/`UpdateBeforeExport` same-millisecond `changed_at` (#276), k6 smoke latency-ceiling (#198 pattern).

- [ ] **Step 4: Commit anything the sweep required; otherwise no commit.**

---

### Task 8: Docs, spike findings, PR

**Files:**
- Modify: `docs/federated-query/design.md`
- Commit: `docs/superpowers/plans/2026-08-02-issue-251-spike-findings.md` (from Task 1) and this plan file.

- [ ] **Step 1: Add a "Partial-read resilience (#251)" section to `docs/federated-query/design.md`** (place it near the failure-classification / degraded-mode material; match the doc's tone). Content to cover, in prose:

- One corrupt object no longer fails a manifest-authored multi-object scan: failure → `MissingIn` classification first (missing = inconsistency, never retried) → per-file drain verification → confirmed-corrupt objects cached (TTL, default 5 min, `WithCorruptPathRetention`) → resolve-time exclusion → one retry.
- Why drain, not footer probe: DuckDB answers DESCRIBE/COUNT from metadata; only a drain touches pages, and a solo drain reads a superset of the scan's columns so it reproduces any scan-detectable failure (spike findings).
- Why not `ignore_errors`: it does not exist on `read_parquet` in the pinned engine (bind-time Binder Error, duckdb v1.4.5 — spike findings); and even if it did, wholesale skipping is silent — no marker, no classification, no attribution; recreates the scenario-2 silent-loss class.
- Limitation (spike): duckdb v1.4.5 exposes no parquet page checksums; byte corruption can decode silently into wrong values (including `row_id`, which would defeat the dirty anti-join) — invisible to every reader, today's all-or-nothing scan included. #251 covers scan-detectable corruption only; the silent-mis-decode gap is a pre-existing condition tracked in the follow-up issue filed in Step 3b.
- Loudness contract: the internal plan carries `NotePartialParquetExclusion` naming each excluded object; Notes never reach HTTP callers (#301/#306) — embedder-only by design.
- Breaker contract: confirmed corruption releases the probe instead of recording a failure — the verification pass proved the engine healthy on the other objects; store-wide unreadability (all objects fail verification) still records.
- Scope: hint-authored and glob path sets keep all-or-nothing; excluding everything is forbidden (total corruption stays loud); plan-cache safety is free because the excluded set changes the scope-v2 hash.
- Retention interplay: this is what makes bounded PG retention survivable — before #251, degraded-mode completeness rested on the retention coincidence the issue names.

- [ ] **Step 2: Commit docs**

```bash
git add docs/federated-query/design.md docs/superpowers/plans/2026-08-02-issue-251-spike-findings.md docs/superpowers/plans/2026-08-02-issue-251-partial-parquet-read-resilience.md
git commit -m "docs(federated): #251 partial-read resilience design + spike findings"
```

- [ ] **Step 3: Post the spike findings as a comment on issue #251** (`gh issue comment 251 --repo Lychee-Technology/forma --body-file ...`) — it answers the issue's `ignore_errors` bullet explicitly.

- [ ] **Step 3b: File the silent-corruption follow-up issue** (`gh issue create --repo Lychee-Technology/forma`) titled "Silent parquet page corruption can mis-decode row_id without error (no page CRC verification)" — body: spike evidence (500-row file, 64-byte XOR at 50% offset, 5 row_ids lost + 5 invented, zero errors; duckdb v1.4.5 `parquet_metadata()` exposes no CRC column), impact (a mis-decoded `row_id` defeats the dirty anti-join, so a stale S3 row escapes hot masking), scope note (out of #251 — invisible to every reader, including today's all-or-nothing scan), candidate directions (writer-side page CRCs, manifest-level content checksums, reader-side verification when DuckDB exposes CRC checking). Link it from the #251 PR body.

- [ ] **Step 4: Open the PR** (per repo rules: reference #251 with closing keyword, do NOT auto-merge, wait for review)

PR body must include: the mechanism summary (verify-and-exclude + TTL cache + single retry), the `ignore_errors` evaluation verdict, the breaker adjudication (confirmed corruption ≠ engine sickness), the out-of-scope list from the plan header (hint/glob sets, HTTP-visible marker), and the e2e evidence (scenario-7 flip + regression sweep results).

---

## Self-Review Notes (already applied)

- **Spec coverage:** issue bullet 1 (`ignore_errors` semantics) → Task 1 + Task 8 Step 3; bullet 2 (per-file preflight + degraded-partial plan marker) → Tasks 3/5 (resolve-time exclusion is the "preflight"; the marker is `NotePartialParquetExclusion`, mirroring `recordDegradedFallbackPlan`'s loudness the only place Notes are visible); bullet 3 (classification + breaker) → Task 6 (`errors.Is` chain preserved; `ReleaseProbe` instead of `RecordFailure` on confirmed corruption); acceptance sketch → Task 2/7 (scenario-7 returns good-file rows + hot rows with the excluded object on the plan; the #187 test is now a contract).
- **Deliberate deviation from the issue sketch:** the issue proposes a footer-probe preflight; the harness's own corruption vector (`corruptMidFile`) keeps the footer intact, so a footer probe cannot catch it (validator already footer-probes today and scenario 7 still fails). The committed design verifies on the failure path and preflights via the TTL cache — zero healthy-path overhead. Task 1 pins this empirically before any production code changes.
- **Known judgment calls for the reviewer:** TTL default 5 min; `len(corrupt) >= len(paths)` = sickness heuristic; hint/glob sets out of scope; handler-error breaker accounting (pre-existing oddity at duckdb_query_execute.go:67 — comment says handler errors pass through unclassified, but they still RecordFailure; behavior preserved, flagged for a follow-up issue, do not fix here).
