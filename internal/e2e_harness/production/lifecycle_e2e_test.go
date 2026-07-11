//go:build e2e

package production

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/model"
)

// TestEntityLifecycle proves the entity lifecycle is correct across CDC
// boundaries (#175). Each subtest drives one lifecycle chain through the
// real EntityManager, CDC flusher, and federated engine, checking every
// query against the independent oracle plus targeted physical (parquet) and
// change_log assertions. The headline risk is delete resurrection: a
// deletion must stay invisible before the flush (dirty-set anti-join), after
// the flush (parquet LWW over the exported tombstone), and across retries.
func TestEntityLifecycle(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1]   // e2e_wide
	simple := DefaultSchemaFixtures()[0] // e2e_simple

	scenarios := []struct {
		name   string
		schema SchemaRef
		run    func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"create_flush_query", wide, testCreateFlushQuery},
		{"update_lww", wide, testUpdateLWW},
		{"delete_before_flush", wide, testDeleteBeforeFlush},
		{"delete_resurrection", wide, testDeleteResurrection},
		{"restore_after_delete", simple, testRestoreAfterDelete},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), sc.schema)
		})
	}
}

// testCreateFlushQuery is scenario 1: create → flush → query. Exactly one
// visible row with correct values, one live delta version exported.
func testCreateFlushQuery(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply create: %v", err)
	}

	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})

	flush := mustFlush(ctx, t, env)
	if got := countTier(flush.Manifests[wide.ID], "delta"); got != 1 {
		t.Fatalf("manifest holds %d delta files, want 1", got)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if result != nil && !result.Plan.Routing.UseDuckDB {
		t.Errorf("post-flush query did not route to duckdb: %+v", result.Plan.Routing)
	}

	assertLiveParquetRow(ctx, t, env, soleParquetKey(t, flush), creates[0])
}

// testUpdateLWW is scenario 2: create → update → flush → query. The latest
// version wins with no duplicate: the pre-flush change_log upsert coalesces
// both mutations into the single slot-0 row, so exactly one (updated)
// version reaches the delta parquet.
func testUpdateLWW(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	script := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1, Updates: 1})
	if err := env.ApplyEvents(ctx, script...); err != nil {
		t.Fatalf("apply create+update: %v", err)
	}
	update := script[1]
	if update.Kind != EventUpdate {
		t.Fatalf("script[1] is %s, want update", update.Kind)
	}

	var slot0 int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND row_id = $2",
		wide.ID, update.RowID).Scan(&slot0); err != nil {
		t.Fatalf("count change_log rows: %v", err)
	}
	if slot0 != 1 {
		t.Fatalf("change_log holds %d rows for the mutated row, want 1 coalesced slot-0 row", slot0)
	}

	flush := mustFlush(ctx, t, env)

	// The oracle folds create+update by LWW into one row carrying the
	// update's values — a duplicate or stale engine row fails the diff.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	assertLiveParquetRow(ctx, t, env, soleParquetKey(t, flush), update)
}

// testDeleteBeforeFlush is scenario 3: create → delete → query, before any
// flush. The deleted row's only surviving source is the cold base parquet
// (its change_log entry is truncated after init, modeling the production
// onboarding contract), so hiding it rests entirely on the dirty-set
// anti-join: the hard delete leaves no entity_main row for pg_source and the
// unflushed tombstone must evict the base version from s3_source.
func testDeleteBeforeFlush(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	row := creates[0]

	initReport, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if initReport.RowsExported != 1 {
		t.Fatalf("init exported %d rows, want 1", initReport.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1 AND row_id = $2",
		wide.ID, row.RowID)

	// The base file must actually serve the row before the delete —
	// otherwise the post-delete zero-row check could be a glob-mismatch
	// false green.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	if err := env.ApplyEvents(ctx, DeleteEvent(wide, row.RowID)); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	assertTombstoneChangeLog(ctx, t, env, wide, row)
	// Hidden across every tier, pre-flush, and idempotently so.
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
}

// testDeleteResurrection is scenario 4 — the delete-resurrection core:
// create → flush (live versions now in base AND delta parquet) → delete →
// flush. The tombstone must be exported to the delta (deleted_at set, every
// attribute and entity_main column NULL — the hard-deleted row no longer
// joins), and once the dirty set is drained the row must stay invisible on
// parquet LWW alone: the tombstone's later changed_at beats both live
// versions.
func testDeleteResurrection(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	row := creates[0]

	// Live versions in the cold base (RunInit) and, because the create's
	// change_log entry is still unflushed, in a warm delta too — two
	// independent resurrection sources for the tombstone to beat.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	mustFlush(ctx, t, env)
	// Load-bearing positive check: the row must be served from parquet
	// before the delete, or every zero-row assertion below would also pass
	// with a globally broken read path.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	del := DeleteEvent(wide, row.RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	// Pre-flush window: hidden by the dirty-set anti-join.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	flush2 := mustFlush(ctx, t, env)
	assertTombstoneParquet(ctx, t, env, soleParquetKey(t, flush2), del)

	// Post-flush: the dirty set is empty (mustFlush), so invisibility now
	// rests purely on parquet LWW — and no tier-preference hint may
	// resurrect the cold or warm live version.
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

	// Idempotency: a retried flush moves nothing and exports nothing, and
	// the row stays deleted.
	flush3, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if flush3.UnflushedBefore != 0 || flush3.UnflushedAfter != 0 {
		t.Errorf("retry flush saw unflushed %d -> %d, want 0 -> 0", flush3.UnflushedBefore, flush3.UnflushedAfter)
	}
	if len(flush3.NewObjects) != 0 {
		t.Errorf("retry flush created objects %v, want none", flush3.NewObjects)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	assertCompactionPreservesDeletion(ctx, t, env, wide, flush2.Manifests[wide.ID])

	// Row reuse after delete, real-API side: a fresh create through the
	// EntityManager must become the only visible row — the flushed tombstone
	// must not bleed onto new rows. (Same-row_id revival is storage-injected
	// in scenario 5; the public API cannot reuse a row_id.)
	postCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, postCreates...); err != nil {
		t.Fatalf("apply post-delete create: %v", err)
	}
	mustFlush(ctx, t, env)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
}

// testRestoreAfterDelete is scenario 5: create → flush → delete → flush →
// restore → flush → query. Production has no restore API — Update on the
// hard-deleted row must return ErrNotFound — so the revival is injected at
// the storage layer (InjectRestore). The restored version must then beat the
// already flushed parquet tombstone in LWW: one visible row with the
// original attributes and no zombie tombstone hiding it.
func testRestoreAfterDelete(ctx context.Context, t *testing.T, env *Env, simple SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: simple, Creates: 1})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	row := creates[0]
	mustFlush(ctx, t, env)
	// Load-bearing positive check before the delete (see testDeleteResurrection).
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})

	del := DeleteEvent(simple, row.RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	mustFlush(ctx, t, env) // tombstone now lives in a delta parquet
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})

	// Contract pin: the production API cannot revive a hard-deleted row.
	_, err := env.EntityManager().Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: row.RowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"name": "resurrect-attempt"},
	})
	if !errors.Is(err, forma.ErrNotFound) {
		t.Fatalf("update of deleted row returned %v, want ErrNotFound", err)
	}

	restored := env.InjectRestore(ctx, row, del)
	flush3 := mustFlush(ctx, t, env)

	// Visible again everywhere, idempotently so. LWW supersession of the
	// flushed tombstone is proven by these engine-vs-oracle diffs — the
	// tombstone parquet itself is immutable and stays in place; only the
	// merge-on-read outcome changes.
	env.AssertQueryMatches(ctx, Query{Schema: simple, PreferHot: true, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})

	// Physical check: the restore flush exported exactly one live version.
	assertSoleLiveVersion(ctx, t, env, soleParquetKey(t, flush3), restored)
}

// assertCompactionPreservesDeletion runs the real compactor on the schema's
// manifest and pins today's compaction contract on a tombstone-bearing
// dataset (#175 "invisible after compaction"): the dirty ratio — delta rows
// over base rows, here 2/1 — exceeds the rewrite threshold, but the rewrite
// is not implemented, so the compactor must report RewritePending and leave
// the manifest untouched, and the deletion must stay invisible. Promotion
// needs a TargetBaseSizeMB-sized delta tier (≥1 MB, out of reach here), and
// the federated read path never consults the manifest — so a genuine
// before/after equivalence test only becomes possible when #188 implements
// the rewrite. This assertion is #188's tripwire: implementing the rewrite
// flips the outcome and forces that issue to replace it with real coverage.
func assertCompactionPreservesDeletion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, before *manifest.Manifest) {
	t.Helper()
	if before == nil {
		t.Fatal("no manifest recorded before compaction")
	}
	result, err := env.RunCompaction(ctx, wide)
	if err != nil {
		t.Fatalf("run compaction: %v", err)
	}
	if result.Outcome != compaction.RewritePending {
		t.Errorf("compaction outcome = %s (dirty ratio %.2f), want %s — the rewrite landed (#188); replace this tripwire with before/after equivalence coverage",
			result.Outcome, result.DirtyRatio, compaction.RewritePending)
	}
	after, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("reload manifests: %v", err)
	}
	m := after[wide.ID]
	if m == nil || m.Version != before.Version || countTier(m, "delta") != countTier(before, "delta") {
		t.Errorf("compaction mutated the manifest (version %d -> %v), want untouched on RewritePending", before.Version, m)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
}

// allWideAttrsNullSQL is the tombstone NULL-ness predicate over every
// e2e_wide attribute column (17 scalar attributes, both bound and EAV).
const allWideAttrsNullSQL = `"title" IS NULL AND "rank" IS NULL AND "count" IS NULL AND "amount" IS NULL ` +
	`AND "score" IS NULL AND "ref" IS NULL AND "joined" IS NULL AND "touched" IS NULL AND "note" IS NULL ` +
	`AND "active" IS NULL AND "born" IS NULL AND "seen" IS NULL AND "level" IS NULL AND "qty" IS NULL ` +
	`AND "total" IS NULL AND "ratio" IS NULL AND "token" IS NULL`

// assertTombstoneParquet asserts one delta parquet file holds exactly one
// version of the deleted row, shaped as a tombstone: changed_at/deleted_at
// carry the delete's change_log entry, and every attribute column plus the
// entity_main system columns are NULL — the delta export LEFT JOINs
// entity_main precisely so a hard delete still reaches parquet
// (internal/cdc/duckdb_exporter.go).
func assertTombstoneParquet(ctx context.Context, t *testing.T, env *Env, key string, del *Event) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	var n int
	var attrsNull, ltbaseNull sql.NullBool
	var changedAt, deletedAt sql.NullInt64
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*), BOOL_AND(%s),
		        BOOL_AND(ltbase_created_at IS NULL AND ltbase_updated_at IS NULL AND ltbase_deleted_at IS NULL),
		        MAX(changed_at), MAX(deleted_at)
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`,
		allWideAttrsNullSQL, path), del.RowID.String()).Scan(&n, &attrsNull, &ltbaseNull, &changedAt, &deletedAt); err != nil {
		t.Fatalf("scan tombstone parquet %s: %v", key, err)
	}
	if n != 1 {
		t.Fatalf("delta parquet holds %d versions of deleted row %s, want exactly 1 tombstone", n, del.RowID)
	}
	if !attrsNull.Valid || !attrsNull.Bool {
		t.Errorf("tombstone %s carries non-NULL attribute columns", del.RowID)
	}
	if !ltbaseNull.Valid || !ltbaseNull.Bool {
		t.Errorf("tombstone %s carries non-NULL entity_main system columns", del.RowID)
	}
	if !changedAt.Valid || changedAt.Int64 != del.ChangedAt {
		t.Errorf("tombstone %s.changed_at = %d (valid=%t), want %d", del.RowID, changedAt.Int64, changedAt.Valid, del.ChangedAt)
	}
	if !deletedAt.Valid || deletedAt.Int64 != del.DeletedAt || del.DeletedAt <= 0 {
		t.Errorf("tombstone %s.deleted_at = %d (valid=%t), want %d > 0", del.RowID, deletedAt.Int64, deletedAt.Valid, del.DeletedAt)
	}
}

// assertSoleLiveVersion asserts one delta parquet file holds exactly one
// version of the event's row and that it is live: deleted_at NULL and
// changed_at equal to the event's read-back change_log timestamp. Attribute
// columns are not inspected — use assertLiveParquetRow for e2e_wide rows.
func assertSoleLiveVersion(ctx context.Context, t *testing.T, env *Env, key string, ev *Event) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	var n int
	var deletedAt, changedAt sql.NullInt64
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*), MAX(deleted_at), MAX(changed_at)
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
		ev.RowID.String()).Scan(&n, &deletedAt, &changedAt); err != nil {
		t.Fatalf("scan delta parquet %s: %v", key, err)
	}
	if n != 1 || deletedAt.Valid || !changedAt.Valid || changedAt.Int64 != ev.ChangedAt {
		t.Errorf("delta %s: versions=%d deleted_at=(%d,%t) changed_at=(%d,%t), want 1 live version at %d",
			ev.RowID, n, deletedAt.Int64, deletedAt.Valid, changedAt.Int64, changedAt.Valid, ev.ChangedAt)
	}
}

// assertTombstoneChangeLog asserts the hard delete's storage contract: the
// entity_main and eav_data rows are gone and the single unflushed change_log
// slot-0 entry carries the tombstone.
func assertTombstoneChangeLog(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, ev *Event) {
	t.Helper()
	var mainRows, eavRows int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM entity_main WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		schema.ID, ev.RowID).Scan(&mainRows); err != nil {
		t.Fatalf("count entity_main rows: %v", err)
	}
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM eav_data WHERE schema_id = $1 AND row_id = $2",
		schema.ID, ev.RowID).Scan(&eavRows); err != nil {
		t.Fatalf("count eav_data rows: %v", err)
	}
	if mainRows != 0 || eavRows != 0 {
		t.Errorf("hard delete left entity_main=%d eav_data=%d rows, want 0/0", mainRows, eavRows)
	}
	var deletedAt int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COALESCE(deleted_at, 0) FROM change_log WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0",
		schema.ID, ev.RowID).Scan(&deletedAt); err != nil {
		t.Fatalf("load slot-0 tombstone: %v", err)
	}
	if deletedAt <= 0 {
		t.Errorf("slot-0 change_log entry deleted_at = %d, want > 0 (tombstone)", deletedAt)
	}
}

// mustFlush runs a real CDC flush and fails the test unless it drained the
// dirty set completely.
func mustFlush(ctx context.Context, t *testing.T, env *Env) *FlushReport {
	t.Helper()
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	return flush
}

// soleParquetKey returns the single parquet object a flush created, failing
// on any other count (manifest JSON objects are ignored).
func soleParquetKey(t *testing.T, flush *FlushReport) string {
	t.Helper()
	var keys []string
	for _, k := range flush.NewObjects {
		if strings.HasSuffix(k, ".parquet") {
			keys = append(keys, k)
		}
	}
	if len(keys) != 1 {
		t.Fatalf("flush created %d parquet objects %v, want exactly 1", len(keys), keys)
	}
	return keys[0]
}

// assertLiveParquetRow asserts one delta parquet file holds exactly one
// version of the event's row and that it is live: deleted_at NULL (the delta
// exporter emits raw cl.deleted_at), changed_at equal to the event's
// read-back change_log timestamp, and the pinned title/count attributes
// equal to the event payload.
func assertLiveParquetRow(ctx context.Context, t *testing.T, env *Env, key string, ev *Event) {
	t.Helper()
	wantTitle, isStr := ev.Attrs["title"].(string)
	if !isStr {
		t.Fatalf("event %s has no string title attribute (%T)", ev.RowID, ev.Attrs["title"])
	}
	countF, isF64 := ev.Attrs["count"].(float64)
	if !isF64 {
		t.Fatalf("event %s has no float64 count attribute (%T)", ev.RowID, ev.Attrs["count"])
	}
	wantCount := int32(countF)

	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT "title", "count", "changed_at", "deleted_at"
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path), ev.RowID.String())
	if err != nil {
		t.Fatalf("scan delta parquet %s: %v", key, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var title sql.NullString
		var count sql.NullInt32
		var changedAt, deletedAt sql.NullInt64
		if err := rows.Scan(&title, &count, &changedAt, &deletedAt); err != nil {
			t.Fatalf("scan delta parquet row: %v", err)
		}
		if deletedAt.Valid {
			t.Errorf("delta %s.deleted_at = %d, want NULL (live row)", ev.RowID, deletedAt.Int64)
		}
		if !changedAt.Valid || changedAt.Int64 != ev.ChangedAt {
			t.Errorf("delta %s.changed_at = %d (valid=%t), want %d", ev.RowID, changedAt.Int64, changedAt.Valid, ev.ChangedAt)
		}
		if !title.Valid || title.String != wantTitle {
			t.Errorf("delta %s.title = %q (valid=%t), want %q", ev.RowID, title.String, title.Valid, wantTitle)
		}
		if !count.Valid || count.Int32 != wantCount {
			t.Errorf("delta %s.count = %d (valid=%t), want %d", ev.RowID, count.Int32, count.Valid, wantCount)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("delta parquet rows: %v", err)
	}
	if n != 1 {
		t.Errorf("delta parquet holds %d versions of row %s, want exactly 1", n, ev.RowID)
	}
}
