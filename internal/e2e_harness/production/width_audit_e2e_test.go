//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// This file is the #501 acceptance test: the integer-width census finds an
// EAV row whose parquet copy disagrees with Postgres, RequeueForFlush brings
// the federated route back to the Postgres value at once, and the next flush
// and compaction make the repair durable on every tier.
//
// The stale export is staged by flushing the row while its value is in
// range, then rewriting eav_data past the declared width without stamping
// change_log. The warm parquet copy disagrees with Postgres and the row is
// not dirty. That is the state a declared-width TRY_CAST export left behind,
// though the stale bytes differ: history holds NULL or a rounded value, this
// fixture holds the old in-range value. The census, dirty-set routing and
// versioned re-export depend only on the disagreement and the flush state,
// never on the stale bytes, so the fixture exercises the same repair path.

func TestIntegerWidthAuditRequeueRepairsStaleExport(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// qty (attr 14) is the EAV-only integer.
	stale := CreateEvent(wide, map[string]any{"title": "audit-stale", "qty": 42})
	clean := CreateEvent(wide, map[string]any{"title": "audit-clean", "qty": 7})
	mustApplyEvents(ctx, t, env, "width audit creates", stale, clean)
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("initial flush: %v", err)
	}
	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 4294967296 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 14",
		wide.ID, stale.RowID)

	probe := Filter{Attr: "qty", Op: "equals", Value: "4294967296"}
	hot := Query{Schema: wide, PreferHot: true, Limit: 100, Filters: []Filter{probe}}
	duck := Query{Schema: wide, Limit: 100, Filters: []Filter{probe}}
	assertWidthRowSet(t, "staged/hot-pg", mustQuery(ctx, t, env, hot), []*Event{stale})
	assertWidthRowSet(t, "staged/warm-duck", mustQuery(ctx, t, env, duck), nil)

	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}
	targets := widthaudit.Targets(env.Metadata)
	cutover := time.Now().UnixMilli() + 1
	findings, err := widthaudit.Census(ctx, env.Pool, tables, targets)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	if len(findings) != 1 || findings[0].RowID != stale.RowID || findings[0].StoredValue != "4294967296" {
		t.Fatalf("census = %+v, want the one staged row", findings)
	}
	if got := findings[0].Classify(cutover); got != widthaudit.ClassStaleExport {
		t.Fatalf("staged row classified %v, want ClassStaleExport", got)
	}

	repo := internal.NewDBPersistentRecordRepository(env.Pool, env.Metadata)
	if err := repo.RequeueForFlush(ctx, env.Tables, findings[0].SchemaID, stale.RowID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	// The dirty set now routes the row to Postgres before any flush.
	assertWidthRowSet(t, "requeued/warm-duck", mustQuery(ctx, t, env, duck), []*Event{stale})

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("repair flush: %v", err)
	}
	assertWidthRowSet(t, "reflushed/warm-duck", mustQuery(ctx, t, env, duck), []*Event{stale})
	assertWidthAuditClean(ctx, t, env, tables, targets, cutover)

	if _, err := env.RunCompaction(ctx, wide); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	assertWidthRowSet(t, "compacted/cold-duck", mustQuery(ctx, t, env, duck), []*Event{stale})
	assertWidthRowSet(t, "compacted/in-range-row", mustQuery(ctx, t, env,
		Query{Schema: wide, Limit: 100, Filters: []Filter{{Attr: "qty", Op: "equals", Value: "7"}}}), []*Event{clean})
}

// assertWidthAuditClean fails unless every census finding is consistent under
// the cutover: the re-export happened after it, so the repaired row no longer
// counts as stale although its value is still past the declared width.
func assertWidthAuditClean(ctx context.Context, t *testing.T, env *Env, tables widthaudit.Tables,
	targets []widthaudit.Target, cutover int64) {
	t.Helper()
	findings, err := widthaudit.Census(ctx, env.Pool, tables, targets)
	if err != nil {
		t.Fatalf("census after repair: %v", err)
	}
	for _, f := range findings {
		if got := f.Classify(cutover); got != widthaudit.ClassConsistent {
			t.Errorf("after repair row %s classified %v (last_flushed_at=%d, cutover=%d), want ClassConsistent",
				f.RowID, got, f.LastFlushedAt, cutover)
		}
	}
}
