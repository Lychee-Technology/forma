//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/model"
)

// assertRewrittenBase asserts the merged base parquet holds exactly each
// merged row's newest version: one live row per surviving entity with the
// winning version's changed_at carried verbatim (#210: no re-stamping) and,
// when the winning event pins a title, the winning payload; exactly one
// tombstone per deleted entity, which Protocol V2 keeps so that a flush
// rebased over the compaction cannot resurrect it (#890); and deleted_at
// normalized to 0 on every live survivor (#274: 0 is the canonical live
// encoding on every cold tier).
func assertRewrittenBase(ctx context.Context, t *testing.T, env *Env, key string, winners map[uuid.UUID]*Event, deleted []uuid.UUID) {
	t.Helper()
	path := env.ParquetURI(key)

	var total, tombstones, nullDeleted int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE deleted_at > 0),
		        COUNT(*) FILTER (WHERE deleted_at IS NULL)
		 FROM read_parquet('%s')`, path)).Scan(&total, &tombstones, &nullDeleted); err != nil {
		t.Fatalf("scan rewritten base %s: %v", key, err)
	}
	if total != len(winners)+len(deleted) {
		t.Errorf("rewritten base holds %d rows, want %d LWW winners and %d tombstones", total, len(winners), len(deleted))
	}
	if tombstones != len(deleted) {
		t.Errorf("rewritten base holds %d tombstone rows, want %d (a compaction keeps each deleted row's tombstone)", tombstones, len(deleted))
	}
	if nullDeleted != 0 {
		t.Errorf("rewritten base holds %d rows with deleted_at NULL, want 0 (normalized to 0)", nullDeleted)
	}

	for rowID, ev := range winners {
		var n int
		var changedAt sql.NullInt64
		var title sql.NullString
		if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(*), MAX(changed_at), MAX("title")
			 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ? AND deleted_at = 0`, path),
			rowID.String()).Scan(&n, &changedAt, &title); err != nil {
			t.Fatalf("scan rewritten base row %s: %v", rowID, err)
		}
		if n != 1 {
			t.Errorf("rewritten base holds %d live versions of row %s, want exactly 1", n, rowID)
			continue
		}
		if !changedAt.Valid || changedAt.Int64 != ev.ChangedAt {
			t.Errorf("row %s changed_at = %d (valid=%t), want %d carried verbatim",
				rowID, changedAt.Int64, changedAt.Valid, ev.ChangedAt)
		}
		if wantTitle, ok := ev.Attrs["title"].(string); ok {
			if !title.Valid || title.String != wantTitle {
				t.Errorf("row %s title = %q (valid=%t), want winning payload %q",
					rowID, title.String, title.Valid, wantTitle)
			}
		}
	}
	for _, rowID := range deleted {
		var n, live int
		if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(*), COUNT(*) FILTER (WHERE deleted_at = 0) FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
			rowID.String()).Scan(&n, &live); err != nil {
			t.Fatalf("scan rewritten base for deleted row %s: %v", rowID, err)
		}
		if n != 1 || live != 0 {
			t.Errorf("deleted row %s: rewritten base holds %d versions, %d live; want its tombstone alone", rowID, n, live)
		}
	}
}

// assertManifestMatchesInventory pins the #188 success criteria under the
// #461 retention contract: every manifest entry must exist in S3 (manifest ⊆
// live objects — the read-safety invariant), and every live parquet key NOT
// in the manifest (tmp excluded: swallowed-delete residue is reclaimed by
// manifest-reconcile --gc, #226) must be a retired source — a key that was
// manifest-listed earlier in the test (everListed) and is deliberately
// retained for the in-flight-reader window until manifest-reconcile's GC
// grace reclaims it. A never-listed leftover is still the staged-orphan bug.
func assertManifestMatchesInventory(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, everListed map[string]bool) {
	t.Helper()
	inventory := schemaParquetKeys(ctx, t, env, schema)
	live := make(map[string]bool, len(inventory))
	for _, k := range inventory {
		live[k] = true
	}
	m := loadSchemaManifest(ctx, t, env, schema)
	listed := make(map[string]bool, len(m.Files))
	for _, f := range m.Files {
		listed[f.Path] = true
		if !live[f.Path] {
			t.Errorf("manifest entry %s is missing from S3 (dangling)", f.Path)
		}
	}
	for _, k := range inventory {
		if !listed[k] && !everListed[k] {
			t.Errorf("unlisted object %s in S3 was never manifest-listed; a staged orphan must not linger (#188/#203)", k)
		}
	}
}

// recordListedKeys folds the schema's currently listed manifest paths into
// everListed, so a later assertManifestMatchesInventory can tell a #461
// retired source (was listed, then spliced out) from a staged orphan. Call it
// right before every operation that swaps the manifest.
func recordListedKeys(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, everListed map[string]bool) {
	t.Helper()
	for _, f := range loadSchemaManifest(ctx, t, env, schema).Files {
		everListed[f.Path] = true
	}
}

// TestCompactionRewriteEquivalence covers #188 scenario 2 with hot rows
// present: a dirty-ratio-eligible schema (2 updates + 1 delete over 5 base
// rows) must be rewritten into a single new base parquet holding exactly each
// row's newest version (the deleted row's tombstone included), with the merged
// source objects left out of the next generation but retained in S3 for the
// readers of the generation it replaced (#461, #890), bit-for-bit
// identical federated results, an untouched hot tier, and a Noop second pass
// (idempotency).
func TestCompactionRewriteEquivalence(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 5)
	updates := []*Event{
		UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "rewrite-v2-a"}),
		UpdateEvent(wide, creates[1].RowID, map[string]any{"title": "rewrite-v2-b"}),
	}
	if err := env.ApplyEvents(ctx, updates...); err != nil {
		t.Fatalf("apply updates: %v", err)
	}
	del := DeleteEvent(wide, creates[2].RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	mustFlush(ctx, t, env)

	hot := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}
	hotBefore, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows: %v", err)
	}
	if hotBefore == 0 {
		t.Fatal("seed produced no hot rows; the hot-tier-untouched assertion would be vacuous")
	}

	mBefore := loadSchemaManifest(ctx, t, env, wide)
	mergedFiles := len(mBefore.Files)
	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)

	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "rewrite")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.FilesMerged != mergedFiles {
		t.Errorf("FilesMerged = %d, want %d (full base+delta merge)", result.FilesMerged, mergedFiles)
	}
	if result.RowsIn != 8 { // 5 base + 2 updates + 1 tombstone
		t.Errorf("RowsIn = %d, want 8", result.RowsIn)
	}
	if result.RowsOut != 5 { // 4 live winners + the deleted row's tombstone
		t.Errorf("RowsOut = %d, want 5", result.RowsOut)
	}
	if result.NewBaseKey == "" {
		t.Fatal("RewriteApplied result carries no NewBaseKey")
	}

	mAfter := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(mAfter, "delta"); got != 0 {
		t.Errorf("delta entries after rewrite = %d, want 0", got)
	}
	if got := countTier(mAfter, "base"); got != 1 {
		t.Errorf("base entries after rewrite = %d, want exactly the merged file", got)
	}
	if n := publishesBetween(mBefore, mAfter); n <= 0 {
		t.Errorf("published %d times since the snapshot, want a monotonic advance", n)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)

	winners := map[uuid.UUID]*Event{
		creates[0].RowID: updates[0],
		creates[1].RowID: updates[1],
		creates[3].RowID: creates[3],
		creates[4].RowID: creates[4],
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, []uuid.UUID{del.RowID})

	// The hot tier is not compaction's to touch.
	hotAfter, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows after rewrite: %v", err)
	}
	if hotAfter != hotBefore {
		t.Errorf("hot change_log rows %d -> %d across rewrite, want untouched", hotBefore, hotAfter)
	}

	// Post-rewrite reads must be served from the new base via DuckDB — with
	// manifest-driven reads a stale listed object would fail loudly instead.
	post := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if post == nil || post.Plan == nil || !post.Plan.Routing.UseDuckDB {
		t.Fatalf("post-rewrite query did not route to duckdb")
	}

	// Idempotency: rewriting the rewrite output must be a Noop.
	state := captureState(t, ctx, env, wide)
	second := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "rewrite-idempotency")
	if second.Outcome != compaction.Noop {
		t.Errorf("second pass outcome = %s, want %s (no deltas remain)", second.Outcome, compaction.Noop)
	}
	assertStateUnchanged(t, "rewrite-idempotency", state, captureState(t, ctx, env, wide))
}

// TestCompactionRewriteMultiVersionLWW covers the base/delta fold: a row's
// base copy from cdc-init and a newer update delta must collapse to the
// single newest version, and the untouched row keeps its one copy. Under
// Protocol V2 the init's handoff marks the rows it exported, so the flush
// after it exports nothing and the V1 seed's equal-ver_ts base/delta pair
// (#183/#210) no longer arises. RowsIn counts every copy; RowsOut counts
// entities.
func TestCompactionRewriteMultiVersionLWW(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	// Init exports base copies of v1 and hands their entries off, so the
	// flush that follows has nothing of them to export.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	mustFlush(ctx, t, env)

	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "multi-version-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env) // the delta: v2 of row 0

	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)
	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "multi-version")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsIn != 3 { // 2 base copies + 1 delta-v2
		t.Errorf("RowsIn = %d, want 3", result.RowsIn)
	}
	if result.RowsOut != 2 {
		t.Errorf("RowsOut = %d, want 2 (one row per entity)", result.RowsOut)
	}

	winners := map[uuid.UUID]*Event{
		creates[0].RowID: update,
		creates[1].RowID: creates[1],
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, nil)
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)
	// #256: the merge writer lists the new base's columns from a DESCRIBE of
	// the file it merged into, and stages exactly those bytes, so the entry
	// must equal a DESCRIBE of the published object.
	assertEntriesStampedToByteTruth(ctx, t, env, "compaction rewrite (merged base)",
		TierEntries(loadSchemaManifest(ctx, t, env, wide), "base"))
}

// TestCompactionRewriteAllTombstones covers the schema-empties-out edge:
// deleting every base row and rewriting produces a base object of their
// tombstones alone, which the next generation lists: a compaction keeps every
// merged row's newest version, tombstones included, so that a flush rebased
// over it cannot resurrect a deleted row (#890). Queries return nothing.
func TestCompactionRewriteAllTombstones(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 2)
	deletes := []*Event{
		DeleteEvent(wide, creates[0].RowID),
		DeleteEvent(wide, creates[1].RowID),
	}
	if err := env.ApplyEvents(ctx, deletes...); err != nil {
		t.Fatalf("apply deletes: %v", err)
	}
	mustFlush(ctx, t, env)

	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)
	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "all-tombstones")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsOut != 2 {
		t.Errorf("RowsOut = %d, want 2 (both rows' tombstones)", result.RowsOut)
	}

	m := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(m, "base"); got != 1 {
		t.Errorf("base entries = %d, want the merged tombstones' base listed", got)
	}
	if got := countTier(m, "delta"); got != 0 {
		t.Errorf("delta entries = %d, want 0", got)
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, nil, []uuid.UUID{creates[0].RowID, creates[1].RowID})
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)
	// #256: a base of tombstones alone still lists its full column set.
	assertEntriesStampedToByteTruth(ctx, t, env, "compaction rewrite (tombstone-only merged base)",
		TierEntries(m, "base"))

	empty := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if empty != nil && empty.Total != 0 {
		t.Errorf("post-rewrite total = %d, want 0", empty.Total)
	}
}

// TestCompactionFullLifecycleEquivalence covers #188 scenario 6 and replaces
// the #175 tripwire (assertCompactionPreservesDeletion): seed → flush →
// delete → flush → compact → query. The rewritten base keeps the deleted
// entity's tombstone alone, no live copy of it (Protocol V2 keeps
// tombstones, #890), and the entity stays invisible under every
// tier-preference hint, inheriting the deletion-invisibility half of #175's
// success criteria.
func TestCompactionFullLifecycleEquivalence(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 2)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "lifecycle-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	del := DeleteEvent(wide, creates[1].RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	flush := mustFlush(ctx, t, env)
	assertTombstoneParquet(ctx, t, env, soleParquetKey(t, flush), del)

	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)
	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "full-lifecycle")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}

	winners := map[uuid.UUID]*Event{creates[0].RowID: update}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, []uuid.UUID{del.RowID})
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)

	// Deletion stays invisible under every tier preference; parquet-serving
	// hints must still route through DuckDB (mirrors lifecycle's #175 loop).
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	for _, tiers := range [][]model.DataTier{
		nil,
		{model.DataTierCold},
		{model.DataTierWarm, model.DataTierCold},
	} {
		result := env.AssertQueryMatches(ctx, Query{Schema: wide, PreferredTiers: tiers, Limit: 10})
		if result != nil && !result.Plan.Routing.UseDuckDB {
			t.Errorf("tiers %v did not route to duckdb: %+v", tiers, result.Plan.Routing)
		}
	}
}
