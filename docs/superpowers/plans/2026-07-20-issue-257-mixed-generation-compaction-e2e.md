# Mixed-Generation Compaction Equivalence E2E (#257) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Characterization e2e proving the real compactor's mixed-generation merge (union_by_name, #189) keeps federated queries bit-for-bit equivalent, materializes the widened column union, and stays mergeable afterward.

**Architecture:** One new e2e file in `internal/e2e_harness/production` composes the existing #189 evolution machinery (`writeSimpleSchemaDir`/`EvolveSchema`) with the #188 equivalence helper (`assertCompactionEquivalence`, parametrized here to accept a query set). Single combined v1→v2 fixture: removed `old_col` + retyped `score` (integer→numeric) + added `new_col`. Rewrite is triggered naturally by v2 updates/delete on v1 base rows (dirty ratio 3/5 = 60%).

**Tech Stack:** Go, testcontainers (Docker: Postgres + RustFS), DuckDB. Zero production-code changes — expected green on first run; a mutation check (Task 4) proves the test bites.

**Context:** Issue https://github.com/Lychee-Technology/forma/issues/257, spun out of #189 (PR #254). Spec (user-approved): `docs/superpowers/specs/2026-07-20-issue-257-mixed-generation-compaction-equivalence-design.md`. #189 added `union_by_name=true` to the compaction merge SQL (`internal/compaction/merge_sql.go:69`), but no e2e runs the real compactor over a mixed-generation base+delta set.

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (coding-standard.md).
- Always wrap errors with context; test failure messages name the attribute/column and the expected state.
- All test commands use the Makefile env: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false` (avoids sandbox cache/VCS errors).
- E2E requires Docker. Do NOT run e2e and unit suites concurrently (known avalanche false-failure trap).
- 请不要自动合并 PR；PR body 关联 #257。
- Production code must not change. If the test surfaces a real defect, STOP and report — file an issue, don't fix inline.
- Known-flaky, NOT caused by this branch: `TestConcurrentFlushSnapshot`/`UpdateBeforeExport` same-millisecond `changed_at` flake (#276), `TestPerformance_ConcurrentQueries` ~89% hit-rate jitter.

---

### Task 0: Worktree + plan doc

**Files:**
- Create: worktree at `../forma-257` on branch `e2e/257-mixed-gen-compaction-equivalence`
- Create: `docs/superpowers/plans/2026-07-20-issue-257-mixed-generation-compaction-e2e.md` (this plan, copied into the worktree)

- [ ] **Step 1: Clean up merged branches, fast-forward main** (workspace AGENTS.md worktree rule)

```bash
cd /Users/ruoshi/code/Lychee/LTBase/forma
git fetch origin --prune
git branch --merged main | grep -v 'main' | xargs -r git branch -d
git checkout main && git merge --ff-only origin/main
```

- [ ] **Step 2: Create worktree** (use explicit `git worktree add` + explicit paths in all later commands — EnterWorktree has a known session-failure trap)

```bash
git worktree add ../forma-257 -b e2e/257-mixed-gen-compaction-equivalence main
cd /Users/ruoshi/code/Lychee/LTBase/forma-257 && pwd && git branch --show-current
```

Expected: `/Users/ruoshi/code/Lychee/LTBase/forma-257`, branch `e2e/257-mixed-gen-compaction-equivalence`. **Every subsequent command in this plan runs from this directory; every subagent dispatch MUST start with a `pwd` + branch guard.**

- [ ] **Step 3: Copy this plan + the spec into the worktree and commit**

```bash
cp /Users/ruoshi/.claude/plans/spec-vectorized-star.md docs/superpowers/plans/2026-07-20-issue-257-mixed-generation-compaction-e2e.md
cp /Users/ruoshi/code/Lychee/LTBase/forma/docs/superpowers/specs/2026-07-20-issue-257-mixed-generation-compaction-equivalence-design.md docs/superpowers/specs/
git add docs/superpowers && git commit -m "docs: #257 spec + implementation plan"
```

---

### Task 1: Parametrize the equivalence query set

`assertCompactionEquivalence` hard-codes `compactionEquivalenceQueries(schema)`, whose sort on `count` is an `e2e_wide` attribute the evolution fixture doesn't have.

**Files:**
- Modify: `internal/e2e_harness/production/compaction_e2e_test.go:27-55` (helper signature)
- Modify: call sites — `compaction_e2e_test.go:78,103,152`, `compaction_rewrite_e2e_test.go:145,201,236,276,324` (8 total)

**Interfaces:**
- Produces: `assertCompactionEquivalence(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, queries []Query, ov CompactionOverrides, label string) compaction.CompactionResult` — Task 2/3 call it with an evolution-specific query set.

- [ ] **Step 1: Add the `queries []Query` parameter**

In `compaction_e2e_test.go`, change the signature and drop the internal derivation (delete the `queries := compactionEquivalenceQueries(schema)` line; update the doc comment to say the caller supplies the query set):

```go
func assertCompactionEquivalence(
	ctx context.Context,
	t *testing.T,
	env *Env,
	schema SchemaRef,
	queries []Query,
	ov CompactionOverrides,
	label string,
) compaction.CompactionResult {
```

`compactionEquivalenceQueries` stays as-is (it becomes the wide-schema default the existing call sites pass).

- [ ] **Step 2: Update all 8 call sites mechanically**

Pattern (same at every site — insert `compactionEquivalenceQueries(wide),` as the 5th arg):

```go
result := assertCompactionEquivalence(ctx, t, env, wide,
	compactionEquivalenceQueries(wide), CompactionOverrides{}, "noop-no-deltas")
```

Sites: `compaction_e2e_test.go` lines ~78 (`"noop-no-deltas"`), ~103 (`"low-dirty-ratio"`), ~152 (`"promotion"`, keeps `CompactionOverrides{TargetBaseSizeBytes: 1}`); `compaction_rewrite_e2e_test.go` lines ~145 (`"rewrite"`), ~201 (`"rewrite-idempotency"`), ~236 (`"multi-version"`), ~276 (`"all-tombstones"`), ~324 (`"full-lifecycle"`).

- [ ] **Step 3: Compile-check the e2e build**

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet -tags=e2e ./internal/e2e_harness/production/
```

Expected: clean exit, no errors.

- [ ] **Step 4: Regression — run one existing rewrite scenario through the new signature** (full suite deferred to Task 5)

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -run 'TestCompactionRewriteEquivalence$' -timeout=10m
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/e2e_harness/production/compaction_e2e_test.go internal/e2e_harness/production/compaction_rewrite_e2e_test.go
git commit -m "test(e2e): #257 parametrize compaction equivalence query set"
```

---

### Task 2: Evolution fixture, seed, and the main equivalence test (criteria a + b)

**Files:**
- Create: `internal/e2e_harness/production/compaction_evolution_e2e_test.go`

**Interfaces:**
- Consumes: `assertCompactionEquivalence` (Task 1 signature); #189 helpers from `schema_evolution_helpers_e2e_test.go`: `writeSimpleSchemaDir(t, propsJSON, attrsJSON) string`, `seedGeneration(ctx, t, env, schema, n, profile) []*Event`, `runInitBase(ctx, t, env, schema) string`, `requireSoleParquet`, `buildEvolutionProfile(extra func(int) map[string]any) AttrProfile`, `describeParquetCols`, `requireParquetCols`, `forbidParquetCols`, `assertUsesDuckDB`; #188 helpers from `compaction_helpers_e2e_test.go`/`compaction_rewrite_e2e_test.go`: `loadSchemaManifest`, `countTier`, `assertNoDuplicateManifestEntries`, `assertManifestMatchesInventory`; harness: `UpdateEvent`, `DeleteEvent`, `mustFlush`, `env.countUnflushed`, `NewEnv(t, cluster, WithSchemaDir(v1))`.
- Produces: `evolutionSeed` struct, `seedMixedGenerationTiers`, `evolutionEquivalenceQueries(schema SchemaRef) []Query`, `assertMergedBaseUnion` — Task 3 extends the same test function.

- [ ] **Step 1: Write fixture constants + profiles + query set**

Ordinal/value scheme (contiguous per Env: base 0–4, v2 delta creates 5–8, hot 9–11): v1 `score = ordinal*10` (integers 0–40), v2 `score = ordinal*10 + 0.5` (fractional), v2 `new_col = ordinal*10`. Updates use large fractional scores (1000.5/2000.5) so corruption to INTEGER is unmissable.

```go
//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
)

// #257: mixed-generation compaction equivalence. Base parquet is written
// under v1, delta under v2 — one evolution step carrying all three mutation
// kinds (old_col removed, score retyped integer→numeric, new_col added) — and
// the REAL compactor merges them. The merge SQL's union_by_name (#189,
// internal/compaction/merge_sql.go) must materialize the widened column
// union; every assertion here characterizes behavior already on main.

const evoV1Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "old_col": { "type": "string" },
    "score": { "type": "integer" }
  }`

const evoV1Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "old_col": { "attributeID": 3, "valueType": "text" },
  "score": { "attributeID": 4, "valueType": "integer" }
}
`

const evoV2Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "score": { "type": "number" },
    "new_col": { "type": "integer" }
  }`

const evoV2Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "score": { "attributeID": 4, "valueType": "numeric" },
  "new_col": { "attributeID": 6, "valueType": "integer" }
}
`

// evoV1Profile seeds v1 rows: old_col + integer score (score = ordinal*10).
func evoV1Profile() AttrProfile {
	return buildEvolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{
			"old_col": fmt.Sprintf("old-%04d", ordinal),
			"score":   float64(ordinal * 10),
		}
	})
}

// evoV2Profile seeds v2 rows: new_col + fractional score, so any silent
// DOUBLE→INTEGER coercion in the merge corrupts a visible value.
func evoV2Profile() AttrProfile {
	return buildEvolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{
			"new_col": float64(ordinal * 10),
			"score":   float64(ordinal*10) + 0.5,
		}
	})
}

// evolutionEquivalenceQueries is the before/after snapshot set: an unsorted
// page, a sort on a generation-stable attribute, a score filter spanning both
// generations (v1 INTEGER rows and v2 DOUBLE rows in one numeric domain), and
// a new_col filter that only v2-generation rows can match (v1 rows are NULL).
func evolutionEquivalenceQueries(schema SchemaRef) []Query {
	return []Query{
		{Schema: schema, Limit: 100},
		{Schema: schema, Sorts: []Sort{{Attr: "value"}}, Limit: 100},
		{Schema: schema, Filters: []Filter{{Attr: "score", Op: "gte", Value: "15"}}, Limit: 100},
		{Schema: schema, Filters: []Filter{{Attr: "new_col", Op: "gte", Value: "0"}}, Limit: 100},
	}
}
```

- [ ] **Step 2: Write the seed helper**

```go
// evolutionSeed carries the mixed-generation state the assertions need.
type evolutionSeed struct {
	baseKey   string   // v1-shaped base parquet
	deltaKey  string   // v2-shaped delta parquet
	creates   []*Event // v1 base creates, ordinals 0-4
	updates   []*Event // v2 winners over creates[3], creates[4]
	deleted   *Event   // tombstone for creates[0]
	v2Creates []*Event // v2 delta creates, ordinals 5-8
}

// seedMixedGenerationTiers builds the #257 fixture: 5 v1 rows exported as
// base via init, evolve to v2, then — all under v2 — 2 updates + 1 delete
// against v1 base rows plus 4 new rows flushed as ONE delta (dirty ratio
// 3/5 = 60% > the 5% rewrite trigger), and 3 hot rows left unflushed.
func seedMixedGenerationTiers(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, v2Dir string) *evolutionSeed {
	t.Helper()
	s := &evolutionSeed{}
	s.creates = seedGeneration(ctx, t, env, schema, 5, evoV1Profile())
	s.baseKey = runInitBase(ctx, t, env, schema)
	if err := env.EvolveSchema(ctx, v2Dir); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}
	s.updates = []*Event{
		UpdateEvent(schema, s.creates[3].RowID, map[string]any{"score": 1000.5, "new_col": float64(400)}),
		UpdateEvent(schema, s.creates[4].RowID, map[string]any{"score": 2000.5, "new_col": float64(500)}),
	}
	s.deleted = DeleteEvent(schema, s.creates[0].RowID)
	if err := env.ApplyEvents(ctx, s.updates[0], s.updates[1], s.deleted); err != nil {
		t.Fatalf("apply v2 updates/delete to v1 base rows: %v", err)
	}
	s.v2Creates = seedGeneration(ctx, t, env, schema, 4, evoV2Profile())
	s.deltaKey = requireSoleParquet(t, "flush", mustFlush(ctx, t, env).NewObjects)
	seedGeneration(ctx, t, env, schema, 3, evoV2Profile()) // hot tier
	return s
}
```

- [ ] **Step 3: Write the merged-base union assertion (criterion b)**

`assertRewrittenBase` is NOT reusable — it scans a `title` column e2e_simple's parquet doesn't have (the DuckDB query would error). Local sibling:

```go
// scanMergedEvoRow reads one row's evolved attributes out of the merged base.
func scanMergedEvoRow(ctx context.Context, t *testing.T, env *Env, path string, rowID uuid.UUID) (n int, score sql.NullFloat64, oldCol sql.NullString, newCol sql.NullInt64) {
	t.Helper()
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*), MAX(score), MAX(old_col), MAX(new_col)
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
		rowID.String()).Scan(&n, &score, &oldCol, &newCol); err != nil {
		t.Fatalf("scan merged base row %s: %v", rowID, err)
	}
	return n, score, oldCol, newCol
}

// assertMergedBaseUnion pins #257 criterion (b): the merged base's physical
// schema is the monotonic column union with widened types, and its rows are
// exactly the LWW winners — cross-generation folds are ROW-level (a v2
// winner carries old_col NULL; values do not column-merge).
func assertMergedBaseUnion(ctx context.Context, t *testing.T, env *Env, key string, seed *evolutionSeed) {
	t.Helper()
	requireParquetCols(t, "merged base", describeParquetCols(ctx, t, env, key), map[string]string{
		"name":    "VARCHAR",
		"value":   "DOUBLE",
		"old_col": "VARCHAR", // v1 legacy column survives the union
		"new_col": "INTEGER", // v2 addition present
		"score":   "DOUBLE",  // INTEGER widened to the delta's DOUBLE
	})

	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	var total, tombstones, nullDeleted int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE deleted_at > 0),
		        COUNT(*) FILTER (WHERE deleted_at IS NULL)
		 FROM read_parquet('%s')`, path)).Scan(&total, &tombstones, &nullDeleted); err != nil {
		t.Fatalf("scan merged base %s: %v", key, err)
	}
	if total != 8 { // 5 base − 1 deleted + 4 v2 creates
		t.Errorf("merged base holds %d rows, want 8 LWW winners", total)
	}
	if tombstones != 0 || nullDeleted != 0 {
		t.Errorf("merged base holds %d tombstones, %d NULL deleted_at, want 0/0 (dropped and normalized)", tombstones, nullDeleted)
	}

	// v2 winners over v1 rows: fractional score, new_col set, old_col NULL.
	for i, up := range seed.updates {
		n, score, oldCol, newCol := scanMergedEvoRow(ctx, t, env, path, up.RowID)
		wantScore := up.Attrs["score"].(float64)
		wantNew := int64(up.Attrs["new_col"].(float64))
		if n != 1 || !score.Valid || score.Float64 != wantScore {
			t.Errorf("updated row %d: n=%d score=%v(valid=%t), want 1 row with v2 winner score %v", i, n, score.Float64, score.Valid, wantScore)
		}
		if !newCol.Valid || newCol.Int64 != wantNew {
			t.Errorf("updated row %d: new_col=%v(valid=%t), want %d", i, newCol.Int64, newCol.Valid, wantNew)
		}
		if oldCol.Valid {
			t.Errorf("updated row %d: old_col=%q, want NULL (row-level LWW: the v2 winner replaces the whole row)", i, oldCol.String)
		}
	}

	// Untouched v1 rows keep old_col and their integer-valued score (stored DOUBLE).
	for _, ordinal := range []int{1, 2} {
		row := seed.creates[ordinal]
		n, score, oldCol, newCol := scanMergedEvoRow(ctx, t, env, path, row.RowID)
		if n != 1 || !score.Valid || score.Float64 != float64(ordinal*10) {
			t.Errorf("untouched v1 row %d: n=%d score=%v(valid=%t), want 1 row with score %d", ordinal, n, score.Float64, score.Valid, ordinal*10)
		}
		if !oldCol.Valid || oldCol.String != fmt.Sprintf("old-%04d", ordinal) {
			t.Errorf("untouched v1 row %d: old_col=%q(valid=%t), want %q preserved", ordinal, oldCol.String, oldCol.Valid, fmt.Sprintf("old-%04d", ordinal))
		}
		if newCol.Valid {
			t.Errorf("untouched v1 row %d: new_col=%d, want NULL (attribute never written)", ordinal, newCol.Int64)
		}
	}

	// The deleted v1 row is physically absent.
	if n, _, _, _ := scanMergedEvoRow(ctx, t, env, path, seed.deleted.RowID); n != 0 {
		t.Errorf("deleted row survives the merged base (%d rows), want physically gone", n)
	}
}
```

- [ ] **Step 4: Write the main test (criterion a + manifest + hot-tier guard)**

```go
// TestCompactionMixedGenerationEquivalence covers #257: the real compactor
// over a v1 base + v2 delta (removed old_col, retyped score, added new_col)
// must produce bit-for-bit identical federated results, a union-shaped merged
// base (criterion b, assertMergedBaseUnion), and a still-evolvable schema
// (criterion c, verifyPostCompactionEvolution).
func TestCompactionMixedGenerationEquivalence(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, evoV1Props, evoV1Attrs)
	v2 := writeSimpleSchemaDir(t, evoV2Props, evoV2Attrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	seed := seedMixedGenerationTiers(ctx, t, env, simple, v2)

	// Generation-shape preconditions: without physically divergent parquet
	// shapes the equivalence pass proves nothing about cross-generation merge.
	baseCols := describeParquetCols(ctx, t, env, seed.baseKey)
	requireParquetCols(t, "base (v1)", baseCols, map[string]string{
		"name": "VARCHAR", "value": "DOUBLE", "old_col": "VARCHAR", "score": "INTEGER"})
	forbidParquetCols(t, "base (v1)", baseCols, "new_col")
	deltaCols := describeParquetCols(ctx, t, env, seed.deltaKey)
	requireParquetCols(t, "delta (v2)", deltaCols, map[string]string{
		"score": "DOUBLE", "new_col": "INTEGER"})
	forbidParquetCols(t, "delta (v2)", deltaCols, "old_col")

	// Query-set discrimination preconditions: 11 visible entities (4 base
	// survivors + 4 delta creates + 3 hot); score>=15 excludes exactly the
	// untouched v1 row with score 10; new_col>=0 excludes both untouched v1 rows.
	queries := evolutionEquivalenceQueries(simple)
	full := env.AssertQueryMatches(ctx, queries[0])
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 11 {
		t.Fatalf("full scan total = %d, want 11 (4 base survivors + 4 delta + 3 hot)", full.Total)
	}
	if scored := env.AssertQueryMatches(ctx, queries[2]); scored != nil && scored.Total != 10 {
		t.Fatalf("score >= 15 total = %d, want 10 (only the v1 row with score 10 excluded)", scored.Total)
	}
	if newcol := env.AssertQueryMatches(ctx, queries[3]); newcol != nil && newcol.Total != 9 {
		t.Fatalf("new_col >= 0 total = %d, want 9 (2 updated + 4 delta + 3 hot; untouched v1 rows are NULL)", newcol.Total)
	}

	hotBefore, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows: %v", err)
	}
	if hotBefore == 0 {
		t.Fatal("seed produced no hot rows; the hot-tier-untouched assertion would be vacuous")
	}

	mBefore := loadSchemaManifest(ctx, t, env, simple)
	result := assertCompactionEquivalence(ctx, t, env, simple, queries,
		CompactionOverrides{}, "mixed-generation")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsIn != 12 { // 5 base + delta(2 updates + 1 tombstone + 4 creates)
		t.Errorf("RowsIn = %d, want 12", result.RowsIn)
	}
	if result.RowsOut != 8 { // 5 − 1 deleted + 4 created
		t.Errorf("RowsOut = %d, want 8", result.RowsOut)
	}
	if result.NewBaseKey == "" {
		t.Fatal("RewriteApplied result carries no NewBaseKey")
	}

	mAfter := loadSchemaManifest(ctx, t, env, simple)
	if got := countTier(mAfter, "delta"); got != 0 {
		t.Errorf("delta entries after rewrite = %d, want 0", got)
	}
	if got := countTier(mAfter, "base"); got != 1 {
		t.Errorf("base entries after rewrite = %d, want exactly the merged file", got)
	}
	if mAfter.Version <= mBefore.Version {
		t.Errorf("manifest version %d -> %d, want monotonic advance", mBefore.Version, mAfter.Version)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
	assertManifestMatchesInventory(ctx, t, env, simple)

	hotAfter, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows after rewrite: %v", err)
	}
	if hotAfter != hotBefore {
		t.Errorf("hot change_log rows %d -> %d across rewrite, want untouched", hotBefore, hotAfter)
	}

	assertMergedBaseUnion(ctx, t, env, result.NewBaseKey, seed)
	verifyPostCompactionEvolution(ctx, t, env, simple, seed) // Task 3
}
```

For THIS task's runnable checkpoint, keep the last line commented out (`// verifyPostCompactionEvolution — Task 3`) so the file compiles; Task 3 uncomments it.

- [ ] **Step 5: Run the new test**

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -run 'TestCompactionMixedGenerationEquivalence$' -timeout=10m
```

Expected: PASS (characterization — union_by_name landed in #189). If it fails, diagnose whether the TEST's expectation is wrong (fix the test) or production is broken (STOP, report, file an issue — do not fix production inline).

- [ ] **Step 6: Commit**

```bash
git add internal/e2e_harness/production/compaction_evolution_e2e_test.go
git commit -m "test(e2e): #257 mixed-generation compaction equivalence — union shape + LWW winners"
```

---

### Task 3: Post-compaction evolution continuation (criterion c)

**Files:**
- Modify: `internal/e2e_harness/production/compaction_evolution_e2e_test.go` (append helper, uncomment the call)

**Interfaces:**
- Consumes: `evolutionSeed`, `evolutionEquivalenceQueries`, `describeParquetCols`, `requireParquetCols`, Task 1's `assertCompactionEquivalence`.

- [ ] **Step 1: Write the continuation helper**

The update targets a v2-created row (NOT a v1 survivor) so both untouched v1 rows keep their `old_col` values through the second merge — pinning that the legacy column's DATA survives repeated folds, not just the column.

```go
// verifyPostCompactionEvolution pins #257 criterion (c): after the mixed-
// generation rewrite, a fresh v2 flush layers a delta on the union-shaped
// merged base and queries still resolve; a second compaction pass folds the
// union-typed base again (monotonic healing), keeping the widened types and
// the v1 legacy column data.
func verifyPostCompactionEvolution(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, seed *evolutionSeed) {
	t.Helper()
	update := UpdateEvent(schema, seed.v2Creates[0].RowID, map[string]any{"score": 3000.5, "new_col": float64(600)})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply post-compaction v2 update: %v", err)
	}
	seedGeneration(ctx, t, env, schema, 1, evoV2Profile()) // ordinal 12
	// This flush drains the update + new create + the 3 still-hot seed rows
	// into one fresh v2 delta over the merged base.
	mustFlush(ctx, t, env)

	queries := evolutionEquivalenceQueries(schema)
	full := env.AssertQueryMatches(ctx, queries[0])
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 { // 8 merged + 3 ex-hot + 1 new create
		t.Fatalf("post-compaction full scan total = %d, want 12", full.Total)
	}
	for _, q := range queries[1:] {
		env.AssertQueryMatches(ctx, q)
	}

	second := assertCompactionEquivalence(ctx, t, env, schema, queries,
		CompactionOverrides{}, "second-merge")
	if second.Outcome != compaction.RewriteApplied {
		t.Fatalf("second pass outcome = %s (dirty ratio %.2f), want %s (union-typed base must be re-mergeable)", second.Outcome, second.DirtyRatio, compaction.RewriteApplied)
	}
	if second.RowsIn != 13 { // 8 merged base + delta(1 update + 1 create + 3 ex-hot)
		t.Errorf("second-merge RowsIn = %d, want 13", second.RowsIn)
	}
	if second.RowsOut != 12 {
		t.Errorf("second-merge RowsOut = %d, want 12", second.RowsOut)
	}

	// Monotonic healing: the second merge output keeps the union shape.
	requireParquetCols(t, "second merged base", describeParquetCols(ctx, t, env, second.NewBaseKey),
		map[string]string{"old_col": "VARCHAR", "new_col": "INTEGER", "score": "DOUBLE"})
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(second.NewBaseKey, "/"))
	var oldColSurvivors int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*) FROM read_parquet('%s') WHERE old_col IS NOT NULL", path)).Scan(&oldColSurvivors); err != nil {
		t.Fatalf("scan second merged base %s: %v", second.NewBaseKey, err)
	}
	if oldColSurvivors != 2 {
		t.Errorf("second merged base has %d rows with old_col data, want 2 (untouched v1 rows must survive repeated folds)", oldColSurvivors)
	}
}
```

- [ ] **Step 2: Uncomment the call in `TestCompactionMixedGenerationEquivalence`** (last line of the test becomes `verifyPostCompactionEvolution(ctx, t, env, simple, seed)`).

- [ ] **Step 3: Run the full test**

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production/ -tags=e2e -run 'TestCompactionMixedGenerationEquivalence$' -timeout=10m
```

Expected: PASS. Same triage rule as Task 2 Step 5.

- [ ] **Step 4: File-size check** (coding standard: ≤500 lines/file, ≤100 lines/function)

```bash
wc -l internal/e2e_harness/production/compaction_evolution_e2e_test.go
```

Expected: well under 500. If any function crossed 100 lines, extract a named helper (e.g. split the manifest block of the main test into `assertMixedGenManifest`).

- [ ] **Step 5: Commit**

```bash
git add internal/e2e_harness/production/compaction_evolution_e2e_test.go
git commit -m "test(e2e): #257 post-compaction v2 flush + second merge over union-shaped base"
```

---

### Task 4: Mutation check — prove the test bites

The suite is characterization (green from birth); this step proves it actually guards `union_by_name`.

**Files:**
- Temporarily modify (then restore): `internal/compaction/merge_sql.go:69` and `:88`

- [ ] **Step 1: Remove the union flag from the merge read** (Edit tool — line 69: `FROM read_parquet([%s], union_by_name=true)` → `FROM read_parquet([%s])`; leave line 88's rows-in counter as is — the merge SELECT is the behavior under test)

- [ ] **Step 2: Run the test, expect FAILURE**

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/e2e_harness/production/ -tags=e2e -run 'TestCompactionMixedGenerationEquivalence$' -timeout=10m
```

Expected: FAIL — either a loud DuckDB merge/binder error, or first-file-wins coercion corrupting fractional scores caught by `assertMergedBaseUnion`/`assertResultsIdentical`/oracle. Record WHICH failure mode fired (goes in the PR body as evidence). If it PASSES, the test is vacuous — STOP and re-examine before proceeding.

- [ ] **Step 3: Restore `merge_sql.go` by re-editing the line back** (known trap: do NOT `git checkout`/`git restore` — it can clobber other uncommitted work; reverse the Edit instead). Verify:

```bash
git diff --stat internal/compaction/merge_sql.go
```

Expected: no diff.

---

### Task 5: Full regression + lint + PR

- [ ] **Step 1: Unit tests** (e2e-tagged files are excluded from `make test`, so this guards against accidental fallout only)

```bash
make test
```

Expected: PASS.

- [ ] **Step 2: Full production-harness e2e regression** (covers all 8 re-signed call sites + the new test + the #189 evolution suite; run serially, never alongside unit tests)

```bash
make test-e2e-production
```

Expected: PASS. Known flakes (see Global Constraints) may need a targeted re-run; they are pre-existing (#276), not this branch.

- [ ] **Step 3: Lint**

```bash
make lint
```

Expected: clean (golangci-lint pinned v1.64.8; do not upgrade).

- [ ] **Step 4: Push and open the PR** (do NOT merge — repo rule)

```bash
git push -u origin e2e/257-mixed-gen-compaction-equivalence
gh pr create --repo Lychee-Technology/forma \
  --title "test(e2e): #257 mixed-generation compaction equivalence" \
  --body "$(cat <<'EOF'
Closes #257.

Characterization e2e for the union_by_name mixed-generation merge (#189, PR #254): seeds base under v1 and delta under v2 (removed old_col + retyped score integer→numeric + added new_col), runs the real compactor, and asserts (a) bit-for-bit before/after query equivalence, (b) the merged base is the monotonic column union with widened types and exact row-level LWW winners, (c) a subsequent v2 flush + second compaction over the union-shaped base still resolves.

Harness change: assertCompactionEquivalence now takes the query set as a parameter (the previous hard-coded sort on `count` was e2e_wide-only); 8 existing call sites updated mechanically.

Mutation evidence: removing union_by_name from merge_sql.go makes the suite fail with <RECORDED FAILURE MODE FROM TASK 4>.

Zero production-code changes.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Replace `<RECORDED FAILURE MODE FROM TASK 4>` with the actual observed failure before submitting.

---

## Verification (end-to-end)

1. `make test` — unit suite green.
2. `make test-e2e-production` — full harness green including `TestCompactionMixedGenerationEquivalence` (Docker required).
3. `make lint` — clean.
4. Mutation evidence recorded in the PR body (Task 4).
5. PR open against `main`, linked `Closes #257`, NOT merged.

## Notes for reviewers / assumptions to re-check during execution

- **Expected-count arithmetic** rests on: ordinals contiguous per Env (base 0–4, delta 5–8, hot 9–11, post-compaction create 12), `buildEvolutionProfile` value scheme, and dirty-ratio estimation from manifest row counts. If any count assertion fails on first run, re-derive from the actual seed before touching production code.
- `DefaultSchemaFixtures()[0]` is `e2e_simple`; `NewEnv(t, cluster, WithSchemaDir(v1))` registers the v1 generation.
- `union_by_name` type-widening INTEGER+DOUBLE→DOUBLE is already pinned read-side by `TestSchemaEvolutionChangedType` (#189); this plan extends the same contract across the compaction boundary.
