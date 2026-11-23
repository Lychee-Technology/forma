//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// TestFullyRewrittenInitExportLossFailsClosed (#915, #760 follow-up): losing
// a cdc-init export after every row it exported was rewritten must still fail
// the read. Each exported version is then shadowed by a hot one, so the hot
// leg alone could answer every row; under V1 that left no loss signal and
// the read answered. Under Protocol V2 the read resolves the published
// generation from the record before it looks at the hot leg, so the loss
// fails closed however much of the export is dirty. This pins that against a
// future pass that skips manifest resolution when the dirty set covers the
// whole schema.
func TestFullyRewrittenInitExportLossFailsClosed(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	const rows = 4

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: rows})
	mustApplyEvents(ctx, t, env, "create the rows to export", creates...)
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != rows || report.Manifest == nil || report.Manifest.Kind != "init" {
		t.Fatalf("precondition: init exported %d rows into %+v, want %d rows in an init generation",
			report.RowsExported, report.Manifest, rows)
	}
	base := TierEntries(report.Manifest, "base")
	if len(base) == 0 {
		t.Fatalf("precondition: the init generation lists no base object: %+v", report.Manifest.Files)
	}
	env.MarkOnboarded(ctx, wide)
	handedOff := snapshotPublication(t, ctx, env, wide)

	exported := rewriteEveryRow(ctx, t, env, wide, creates)
	requireEveryRowDirty(ctx, t, env, wide, exported)
	requireInitStillCurrent(ctx, t, env, wide, handedOff, report.Manifest)

	deleteObject(ctx, t, env, report.Manifest.Key)
	for _, f := range base {
		deleteObject(ctx, t, env, f.Path)
	}

	assertLostInitFailsClosed(ctx, t, env, wide)
}

// rewriteEveryRow updates each created row once, so every version the init
// exported is shadowed by a hot one, and answers the rows' IDs.
func rewriteEveryRow(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, creates []*Event) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, len(creates))
	updates := make([]*Event, 0, len(creates))
	for i, ev := range creates {
		ids = append(ids, ev.RowID)
		updates = append(updates, UpdateEvent(schema, ev.RowID, map[string]any{"title": fmt.Sprintf("rewritten-%d", i)}))
	}
	mustApplyEvents(ctx, t, env, "rewrite every exported row", updates...)
	return ids
}

// requireEveryRowDirty fails the test unless each of rows has an unflushed
// change_log entry: the precondition that no exported version is still the
// row's newest.
func requireEveryRowDirty(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rows []uuid.UUID) {
	t.Helper()
	var dirty int
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(DISTINCT row_id) FROM change_log WHERE schema_id = $1 AND row_id = ANY($2) AND flushed_at = 0",
		schema.ID, rows).Scan(&dirty); err != nil {
		t.Fatalf("count the rewritten rows' unflushed entries of schema %d: %v", schema.ID, err)
	}
	if dirty != len(rows) {
		t.Fatalf("precondition: %d of %d exported rows of schema %d are dirty after the rewrite, want all",
			dirty, len(rows), schema.ID)
	}
}

// requireInitStillCurrent fails the test unless rewriting the rows left the
// schema's publication record exactly as the handoff left it, naming the init
// generation: row writes touch change_log and the row tables only.
func requireInitStillCurrent(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, handedOff publicationSnapshot, initGen *Manifest) {
	t.Helper()
	changes, err := publicationChanges(handedOff, snapshotPublication(t, ctx, env, schema))
	if err != nil {
		t.Fatalf("compare schema %d's publication: %v", schema.ID, err)
	}
	if len(changes) != 0 {
		t.Fatalf("rewriting the rows moved schema %d's publication record: %v", schema.ID, changes)
	}
	cur := loadSchemaManifest(ctx, t, env, schema)
	if cur.Kind != "init" || cur.Generation != initGen.Generation || cur.Token != initGen.Token || cur.Key != initGen.Key {
		t.Fatalf("schema %d's current generation is %s %d (%s), want the init generation %d (%s)",
			schema.ID, cur.Kind, cur.Generation, cur.Key, initGen.Generation, initGen.Key)
	}
}

// assertLostInitFailsClosed requires the all-tier and the hot-excluded query,
// each with and without AllowPartialDegradedMode, to fail as a publication
// inconsistency: no answer, and no degradable read failure that the degraded
// mode could answer short from postgres. It calls the engine directly rather
// than through Env.Query, which drops the page whenever the engine errors, so
// a regression that returns partial rows alongside the error is caught.
func assertLostInitFailsClosed(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	eng := env.Engine()
	for _, tiers := range [][]model.DataTier{allTiers, parquetTiers} {
		for _, degraded := range []bool{false, true} {
			fq, err := env.buildFederatedQuery(Query{Schema: schema, Limit: 20, PreferredTiers: tiers})
			if err != nil {
				t.Fatalf("tiers %v, degraded %t: build federated query: %v", tiers, degraded, err)
			}
			page, err := eng.Query(ctx, env.Tables, fq, &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded})
			if err == nil {
				t.Fatalf("tiers %v, degraded %t: query answered %s with the init export lost; want a read failure",
					tiers, degraded, describePage(page))
			}
			if page != nil {
				t.Fatalf("tiers %v, degraded %t: query failed with %v yet answered %s", tiers, degraded, err, describePage(page))
			}
			if !errors.Is(err, fedengine.ErrPublicationInconsistent) {
				t.Fatalf("tiers %v, degraded %t: got %v, want ErrPublicationInconsistent", tiers, degraded, err)
			}
			if errors.Is(err, fedengine.ErrFederatedReadFailed) {
				t.Fatalf("tiers %v, degraded %t: a lost generation is not a degradable read failure: %v", tiers, degraded, err)
			}
		}
	}
}

// describePage summarizes an engine page for a failure message.
func describePage(page *model.PersistentRecordPage) string {
	if page == nil {
		return "a nil page"
	}
	return fmt.Sprintf("a page of %d rows (total %d)", len(page.Records), page.TotalRecords)
}
