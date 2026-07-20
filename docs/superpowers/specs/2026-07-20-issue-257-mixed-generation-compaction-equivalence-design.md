# Design: Mixed-Generation Compaction Equivalence E2E (#257)

**Issue:** https://github.com/Lychee-Technology/forma/issues/257
**Date:** 2026-07-20
**Status:** Approved

## Background

#189 (PR #254) added `union_by_name=true` to the compaction merge SQL
(`internal/compaction/merge_sql.go`), so a merge spanning schema generations
physically materializes the column union (NULLs where a generation lacked a
column, numeric widening where types diverge) instead of failing to unify.
Unit-level SQL pins were updated and #188's same-generation equivalence suites
stay green, but no e2e runs the real compactor over a mixed-generation
base+delta set and asserts before/after query equivalence.

This is a **characterization suite**: production code is already correct
(union_by_name landed in #189), so the new test is expected green on first
run. Zero production-code changes.

## Decisions (user-approved)

1. **Single combined fixture** — one v1→v2 evolution step carrying all three
   mutations (added + removed + retyped column), matching the issue's wording.
   The three mutations exercise the same union_by_name mechanism at the merge
   layer; separate scenarios would triple e2e cost for little attribution
   value (#189 already isolates them on the read side).
2. **Rewrite triggered by v2 updates to v1 base rows** — not by a
   `DirtyRatioPct` override. This naturally pushes dirty ratio past the 5%
   default AND forces the merge to fold cross-generation versions of the same
   `row_id` (v1-shaped base copy vs v2-shaped delta winner), the path a
   creates-only delta never covers.

## Fixture

New file `internal/e2e_harness/production/compaction_evolution_e2e_test.go`
(`//go:build e2e`), evolving the `e2e_simple` fixture via the #189 machinery
(`writeSimpleSchemaDir` + `Env.EvolveSchema`).

| attribute | v1                              | v2                               | mutation |
|-----------|---------------------------------|----------------------------------|----------|
| `name`    | text, bound `text_01`, attrID 1 | same                             | stable   |
| `value`   | numeric EAV, attrID 2           | same                             | stable   |
| `old_col` | text EAV, attrID 3              | — (dropped)                      | removed  |
| `score`   | **integer** EAV, attrID 4       | **numeric** EAV, attrID 4        | retyped  |
| `new_col` | —                               | integer EAV, fresh attrID (6)    | added    |

attrID assignments follow the existing evolution fixtures' conventions
(name=1, value=2 fixed; no collisions with the sibling constants in
`schema_evolution_e2e_test.go` is required — each test writes its own
throwaway schema dir).

## Seed recipe

Deliberately NOT `seedEvolutionTiers` (it has no update/delete step); the test
composes the same primitives:

1. Under v1: create 5 rows (`seedGeneration` with a profile writing
   `old_col`/integer `score`) → `runInitBase` → **base parquet** (v1 shape).
2. `env.EvolveSchema(ctx, v2Dir)`.
3. Under v2: `UpdateEvent` on 2 v1 base rows (fractional `score`, a `new_col`
   value), `DeleteEvent` on 1 v1 base row, create 4 new rows → single
   `mustFlush` → **one delta parquet** (v2 shape). Dirty ratio 3/5 = 60% > 5%.
4. Create 3 more v2 rows, left unflushed → **hot tier** (compaction must not
   touch it; assert via `env.countUnflushed` before/after, as in
   `TestCompactionRewriteEquivalence`).
5. Shape preconditions (`describeParquetCols` + `requireParquetCols` / `forbidParquetCols`):
   - base: `old_col` VARCHAR, `score` INTEGER; no `new_col`.
   - delta: `new_col` INTEGER, `score` DOUBLE; no `old_col`.

   Without these the equivalence pass proves nothing about cross-generation
   resolution.

## Harness change: parametrize the equivalence query set

`assertCompactionEquivalence` currently calls `compactionEquivalenceQueries`,
which hard-codes a sort on `count` — an `e2e_wide` attribute the evolution
fixture doesn't have. Targeted improvement:

- Add a `queries []Query` parameter to `assertCompactionEquivalence`.
- Existing call sites (6, in `compaction_e2e_test.go` and
  `compaction_rewrite_e2e_test.go`) pass `compactionEquivalenceQueries(schema)`
  — mechanical update, no behavior change.

Evolution query set (each snapshotted before/after AND oracle-checked, per the
existing helper):

1. Unsorted page (`Limit: 100`) — row-ID-set + value equality.
2. Sort on `value` — order stability on a stable attribute.
3. Filter `score gte 20` spanning both generations — seed values follow the
   #189 pattern (v1: `ordinal*10` integers, v2: `ordinal*10 + 0.5` doubles),
   so the threshold hits v1 INTEGER rows and v2 DOUBLE rows in one numeric
   domain (the #189 widening contract, now across a compaction boundary).
4. Filter on `new_col` — hits only v2-generation rows; v1 rows are NULL.

## Assertions

### (a) Before/after query equivalence

`assertCompactionEquivalence(..., CompactionOverrides{}, ...)` over the query
set above. Result pins:

- `Outcome == RewriteApplied` (with dirty ratio in the failure message).
- `RowsIn == 12` (5 base + 2 updates + 1 tombstone + 4 creates).
- `RowsOut == 8` (5 − 1 deleted + 4 created).
- `NewBaseKey` non-empty.
- Manifest: delta entries 0, base entries 1, version advanced,
  `assertNoDuplicateManifestEntries` + `assertManifestMatchesInventory`.

### (b) Merged base physical schema = union with widened types

`describeParquetCols(ctx, t, env, result.NewBaseKey)`:

- `old_col` VARCHAR **present** (v1 legacy column retained — the union is monotonic; nothing is dropped).
- `new_col` INTEGER present.
- `score` **DOUBLE** (INTEGER widened to the delta's DOUBLE).
- `name` VARCHAR, `value` DOUBLE.

Content assertions (per-row DuckDB scans, `assertRewrittenBase`-style; reuse
it if the shape fits, else a local sibling):

- Exactly 8 rows, zero tombstones, `deleted_at` normalized to 0.
- The 2 updated rows carry the v2 winner payload: fractional `score`,
  `new_col` set, and `old_col` **NULL** — row-level LWW replaces whole rows,
  it does not column-merge (documented in the test comment).
- The 2 untouched v1 rows keep their `old_col` values and integer-valued
  `score` (now stored as DOUBLE).
- The deleted row is physically absent.

### (c) Post-compaction v2 flush + query still resolves

After the equivalence pass:

1. Apply v2 events: update one merged-base row + create one row; `mustFlush`
   → a fresh v2 delta layered on the merged (union-shaped) base.
2. `AssertQueryMatches` on the full query set; `assertUsesDuckDB` on the full
   scan (proves the merged base + new delta were actually read, not served
   hot-only — note the 3 hot rows from the seed are still unflushed, so the
   flush in step 1 also drains them; account for that in expected totals or
   flush them knowingly).
3. Second compaction pass via `assertCompactionEquivalence`: the new delta's
   updates make it another `RewriteApplied` fold over the union-shaped base —
   pinning that a union-typed merged file is itself mergeable (monotonic
   healing). Re-assert `score` stays DOUBLE in the second merge output.

## Files touched

| file | change |
|------|--------|
| `internal/e2e_harness/production/compaction_evolution_e2e_test.go` | new: fixture consts, seed, test(s) |
| `internal/e2e_harness/production/compaction_e2e_test.go` | `assertCompactionEquivalence` gains `queries []Query`; local call sites updated |
| `internal/e2e_harness/production/compaction_rewrite_e2e_test.go` | call sites updated |

Production code: none.

## Verification

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production -tags=e2e -run 'TestCompactionMixedGeneration' -timeout=10m
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v ./internal/e2e_harness/production -tags=e2e -run 'TestCompaction' -timeout=20m   # regression: existing suite over the parametrized helper
make lint
```

Coding-standard constraints that bite here: source files ≤500 lines, test
scenario functions ≤100 lines (split scenarios into named helpers/functions as
in the existing suite), error/failure messages name attribute + expected
state.

## Out of scope

- Promotion path over mixed generations (manifest relabel only, no merge —
  read-side resolution already covered by #189).
- Hot-tier rename asymmetry (documented in #189's
  `TestSchemaEvolutionRenamedColumn`).
- Any production-code change. If the test surfaces a real defect, stop and
  file it rather than fixing inline.
