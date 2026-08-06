//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/model"
)

// assertRewrittenBase asserts the merged base parquet holds exactly the LWW
// winners: one live row per surviving entity with the winning version's
// changed_at carried verbatim (#210: no re-stamping) and, when the winning
// event pins a title, the winning payload; zero rows for physically removed
// entities; zero tombstones; and deleted_at normalized to 0 on every survivor
// (#274: 0 is the canonical live encoding on every cold tier).
func assertRewrittenBase(ctx context.Context, t *testing.T, env *Env, key string, winners map[uuid.UUID]*Event, absent []uuid.UUID) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))

	var total, tombstones, nullDeleted int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE deleted_at > 0),
		        COUNT(*) FILTER (WHERE deleted_at IS NULL)
		 FROM read_parquet('%s')`, path)).Scan(&total, &tombstones, &nullDeleted); err != nil {
		t.Fatalf("scan rewritten base %s: %v", key, err)
	}
	if total != len(winners) {
		t.Errorf("rewritten base holds %d rows, want %d LWW winners", total, len(winners))
	}
	if tombstones != 0 {
		t.Errorf("rewritten base holds %d tombstone rows, want 0 (rewrite must drop them)", tombstones)
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
			 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
			rowID.String()).Scan(&n, &changedAt, &title); err != nil {
			t.Fatalf("scan rewritten base row %s: %v", rowID, err)
		}
		if n != 1 {
			t.Errorf("rewritten base holds %d versions of row %s, want exactly 1", n, rowID)
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
	for _, rowID := range absent {
		var n int
		if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(*) FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
			rowID.String()).Scan(&n); err != nil {
			t.Fatalf("scan rewritten base for absent row %s: %v", rowID, err)
		}
		if n != 0 {
			t.Errorf("deleted row %s survives in rewritten base (%d rows), want physically gone", rowID, n)
		}
	}
}

// assertManifestMatchesInventory pins the #188 success criteria "manifest
// correctly reflects object inventory" and "no orphan objects": the schema's
// live parquet keys (tmp excluded: swallowed-delete residue is reclaimed by
// manifest-reconcile --gc, #226) and the manifest's file
// paths must be exactly the same set.
func assertManifestMatchesInventory(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	inventory := schemaParquetKeys(ctx, t, env, schema)
	m := loadSchemaManifest(ctx, t, env, schema)
	listed := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		listed = append(listed, f.Path)
	}
	sort.Strings(listed)
	if !reflect.DeepEqual(inventory, listed) {
		t.Errorf("manifest and S3 inventory diverge:\n s3 objects: %v\n manifest:   %v", inventory, listed)
	}
}

// TestCompactionRewriteEquivalence covers #188 scenario 2 with hot rows
// present: a dirty-ratio-eligible schema (2 updates + 1 delete over 5 base
// rows) must be rewritten into a single new base parquet holding exactly the
// LWW winners, with the merged source objects removed from S3 and the
// manifest, bit-for-bit identical federated results, an untouched hot tier,
// and a Noop second pass (idempotency).
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
	if result.RowsOut != 4 { // 5 created - 1 deleted
		t.Errorf("RowsOut = %d, want 4", result.RowsOut)
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
	if mAfter.Version <= mBefore.Version {
		t.Errorf("manifest version %d -> %d, want monotonic advance", mBefore.Version, mAfter.Version)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
	assertManifestMatchesInventory(ctx, t, env, wide)

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

// TestCompactionRewriteMultiVersionLWW covers the multi-delta fold: three
// copies of one row (an equal-ver_ts base/delta pair from the create→flush→
// init seed, #183/#210, plus a newer update delta) must collapse to the
// single newest version, and the untouched row's tied pair must dedup to one
// row. RowsIn counts every copy; RowsOut counts entities.
func TestCompactionRewriteMultiVersionLWW(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1: v1 of both rows
	// Init now exports base copies of the same versions: an equal-ver_ts
	// base-vs-delta pair per row (#210 seed shape).
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}

	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "multi-version-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env) // delta #2: v2 of row 0

	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "multi-version")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsIn != 5 { // 2 base copies + 2 delta-v1 copies + 1 delta-v2
		t.Errorf("RowsIn = %d, want 5", result.RowsIn)
	}
	if result.RowsOut != 2 {
		t.Errorf("RowsOut = %d, want 2 (one row per entity)", result.RowsOut)
	}

	winners := map[uuid.UUID]*Event{
		creates[0].RowID: update,
		creates[1].RowID: creates[1],
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, nil)
	assertManifestMatchesInventory(ctx, t, env, wide)
	// #256: the merge writer stamps the new base entry from a DESCRIBE of the
	// tmp object it merged into; tmp→final is a byte-identical copy, so the
	// stamp must equal a DESCRIBE of the published object.
	assertEntriesStampedToByteTruth(ctx, t, env, "compaction rewrite (merged base)",
		manifest.FilterByTier(loadSchemaManifest(ctx, t, env, wide), "base"))
}

// TestCompactionRewriteAllTombstones covers the schema-empties-out edge:
// deleting every base row and rewriting must produce a zero-row base parquet
// that STAYS listed in the manifest — with zero entries the manifest-driven
// read source falls back to the legacy glob (manifest.QuerySource), which
// would resurrect any leftover object — and queries must return nothing.
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

	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "all-tombstones")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsOut != 0 {
		t.Errorf("RowsOut = %d, want 0 (everything tombstoned)", result.RowsOut)
	}

	m := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(m, "base"); got != 1 {
		t.Errorf("base entries = %d, want the zero-row base still listed (glob-fallback guard)", got)
	}
	if got := countTier(m, "delta"); got != 0 {
		t.Errorf("delta entries = %d, want 0", got)
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, nil, []uuid.UUID{creates[0].RowID, creates[1].RowID})
	assertManifestMatchesInventory(ctx, t, env, wide)
	// #256 edge: a zero-row merged base still describes to a full column set,
	// so it must still carry a byte-truth stamp rather than fall back to nil.
	assertEntriesStampedToByteTruth(ctx, t, env, "compaction rewrite (zero-row merged base)",
		manifest.FilterByTier(m, "base"))

	empty := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if empty != nil && empty.Total != 0 {
		t.Errorf("post-rewrite total = %d, want 0", empty.Total)
	}
}

// TestCompactionFullLifecycleEquivalence covers #188 scenario 6 and replaces
// the #175 tripwire (assertCompactionPreservesDeletion): seed → flush →
// delete → flush → compact → query. The deleted entity must be PHYSICALLY
// absent from the rewritten base — not merely tombstone-shadowed — and stay
// invisible under every tier-preference hint, inheriting the
// deletion-invisibility half of #175's success criteria.
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

	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{}, "full-lifecycle")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}

	winners := map[uuid.UUID]*Event{creates[0].RowID: update}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, []uuid.UUID{del.RowID})
	assertManifestMatchesInventory(ctx, t, env, wide)

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
