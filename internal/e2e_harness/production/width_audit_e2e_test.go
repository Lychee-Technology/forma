//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// This file pins the integer-width census over an EAV row whose Postgres
// value is out of width and whose parquet copy disagrees with it (#501), as
// #729 changed its repair. The Postgres value is the defect: the OLTP read
// refuses it, so the census reports the row as a failure, never as a stale
// export a re-flush repairs. Rewriting the attribute through the API is the
// repair, and the flush and compaction after it replace the stale copy on
// every tier.
//
// The stale export is staged by flushing the row while its value is in
// range, then rewriting eav_data past the declared width without stamping
// change_log. The warm parquet copy disagrees with Postgres and the row is
// not dirty. That is the state a declared-width TRY_CAST export left behind,
// though the stale bytes differ: history holds NULL or a rounded value, this
// fixture holds the old in-range value. The census and the repair depend
// only on the disagreement and the flush state, never on the stale bytes.

func TestIntegerWidthAuditStaleExportIsRepairedByARewrite(t *testing.T) {
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

	// The warm tier still serves the old copy, which no filter on the
	// Postgres value finds.
	duck := Query{Schema: wide, Limit: 100, Filters: []Filter{{Attr: "qty", Op: "equals", Value: "4294967296"}}}
	assertWidthRowSet(t, "staged/warm-duck", mustQuery(ctx, t, env, duck), nil)

	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}
	targets := widthaudit.TargetsWithRetainedEAV(env.Metadata)
	cutover := time.Now().UnixMilli() + 1
	findings, err := widthaudit.Census(ctx, env.Pool, tables, targets)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	if len(findings) != 1 || findings[0].RowID != stale.RowID || findings[0].StoredValue != "4294967296" {
		t.Fatalf("census = %+v, want the one staged row", findings)
	}
	if got := findings[0].Classify(); got != widthaudit.ClassEAVIntegerOutOfContract || !findings[0].ExportPredates(cutover) {
		t.Fatalf("staged row classified %v (export predates the cutover: %t), want an EAV integer out of contract with a stale copy",
			got, findings[0].ExportPredates(cutover))
	}

	_, err = env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: wide.Name, RowID: &stale.RowID})
	if err == nil || !strings.Contains(err.Error(), "stored integer image 4294967296 in value_numeric does not fit declared type integer") {
		t.Fatalf("OLTP read of the staged row = %v, want the #729 refusal", err)
	}

	mustApplyEvents(ctx, t, env, "rewrite the attribute", UpdateEvent(wide, stale.RowID, map[string]any{"qty": 4242}))
	if findings, err := widthaudit.Census(ctx, env.Pool, tables, targets); err != nil || len(findings) != 0 {
		t.Fatalf("census after the rewrite = %+v, %v; want clean", findings, err)
	}
	rewritten := Query{Schema: wide, Limit: 100, Filters: []Filter{{Attr: "qty", Op: "equals", Value: "4242"}}}
	assertWidthRowSet(t, "rewritten/dirty-duck", mustQuery(ctx, t, env, rewritten), []*Event{stale})

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("repair flush: %v", err)
	}
	assertWidthRowSet(t, "reflushed/warm-duck", mustQuery(ctx, t, env, rewritten), []*Event{stale})
	if _, err := env.RunCompaction(ctx, wide); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	assertWidthRowSet(t, "compacted/cold-duck", mustQuery(ctx, t, env, rewritten), []*Event{stale})
	assertWidthRowSet(t, "compacted/in-range-row", mustQuery(ctx, t, env,
		Query{Schema: wide, Limit: 100, Filters: []Filter{{Attr: "qty", Op: "equals", Value: "7"}}}), []*Event{clean})

	page, err := federatedWideQuery(ctx, env, wide)
	if err != nil {
		t.Fatalf("federated read after the repair: %v", err)
	}
	found := false
	for _, rec := range page.Data {
		if rec.RowID != stale.RowID {
			continue
		}
		found = true
		if rec.Attributes["qty"] != int32(4242) {
			t.Fatalf("federated read of the repaired row = %#v", rec.Attributes)
		}
	}
	if !found {
		t.Fatalf("federated read after the repair has no row %s", stale.RowID)
	}
}
