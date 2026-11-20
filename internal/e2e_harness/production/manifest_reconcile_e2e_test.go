//go:build e2e

package production

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/reconcile"
)

func runReconcile(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, opts reconcile.Options) reconcile.SchemaReport {
	t.Helper()
	report, err := env.reconciler(t, schema, opts).Run(ctx)
	if err != nil {
		t.Fatalf("manifest-reconcile run: %v", err)
	}
	if len(report.Schemas) != 1 {
		t.Fatalf("manifest-reconcile visited %d schemas, want schema %d alone", len(report.Schemas), schema.ID)
	}
	rep := report.Schemas[0]
	if rep.Err != nil {
		t.Fatalf("manifest-reconcile pass of schema %d failed: %v", schema.ID, rep.Err)
	}
	return rep
}

func schemaObjectKeys(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) []string {
	t.Helper()
	keys, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys: %v", err)
	}
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, buildSchemaKeyPrefix(env, schema)) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func TestManifestReconcile_DiagnosisOfAPublishedSchemaIsCleanAndReadOnly(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]
	creates := seedCompactionBase(ctx, t, env, wide, 5)
	if err := env.ApplyEvents(ctx, UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "reconcile-v2"})); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env)
	m := loadSchemaManifest(ctx, t, env, wide)
	before := schemaObjectKeys(ctx, t, env, wide)

	rep := runReconcile(ctx, t, env, wide, reconcileOptions(false))
	if rep.Residual() || len(rep.Findings) != 0 {
		t.Fatalf("diagnosis of a healthy schema found %+v", rep.Findings)
	}
	if string(rep.State) != "PUBLISHED" || rep.Epoch != m.Epoch || rep.Generation != m.Generation {
		t.Fatalf("diagnosis read %s epoch %d generation %d, record names epoch %d generation %d",
			rep.State, rep.Epoch, rep.Generation, m.Epoch, m.Generation)
	}
	if len(rep.Plan.Objects)+len(rep.Plan.Manifests)+len(rep.Plan.Forget) != 0 {
		t.Fatalf("diagnosis plans deletes on a schema nothing replaced: %+v", rep.Plan)
	}
	if rep.Collected != nil {
		t.Fatal("a diagnosis collected")
	}
	if after := schemaObjectKeys(ctx, t, env, wide); !slices.Equal(before, after) {
		t.Fatalf("diagnosis changed bucket: before %v, after %v", before, after)
	}
}

func TestManifestReconcile_GCCollectsTheGenerationsACompactionReplaced(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]
	creates := seedCompactionBase(ctx, t, env, wide, 5)
	if err := env.ApplyEvents(ctx, UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "gc-v2"})); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env)
	preSwap := loadSchemaManifest(ctx, t, env, wide)
	result, err := env.RunCompaction(ctx, wide)
	if err != nil || result.Outcome != compaction.RewriteApplied {
		t.Fatalf("compaction: outcome %s, err %v", result.Outcome, err)
	}
	post := loadSchemaManifest(ctx, t, env, wide)
	q := Query{Schema: wide, Limit: 100}

	first := runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if !slices.Contains(first.Collected.Retired, preSwap.Generation) {
		t.Fatalf("first collection retired %v, want replaced generation %d among them", first.Collected.Retired, preSwap.Generation)
	}
	if len(first.Collected.Objects)+len(first.Collected.Manifests) != 0 {
		t.Fatalf("first collection deleted %v and %v while retained", first.Collected.Objects, first.Collected.Manifests)
	}
	requireReplacedGenerationRetained(ctx, t, env, wide, preSwap)
	env.AssertQueryMatches(ctx, q)

	time.Sleep(reconcileGrace)
	second := runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if len(second.Findings) != 0 {
		t.Fatalf("second collection found %+v", second.Findings)
	}
	if !slices.Contains(second.Collected.Forgotten, preSwap.Generation) || !slices.Contains(second.Collected.Manifests, preSwap.Key) {
		t.Fatalf("second collection forgot %v and deleted manifests %v: want replaced generation %d forgotten and manifest deleted",
			second.Collected.Forgotten, second.Collected.Manifests, preSwap.Generation)
	}
	want := append(sortedGenerationKeys(post), post.Key)
	slices.Sort(want)
	if got := schemaObjectKeys(ctx, t, env, wide); !slices.Equal(got, want) {
		t.Fatalf("after collection schema holds %v, want compact generation's %v", got, want)
	}
	env.AssertQueryMatches(ctx, q)

	third := runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if len(third.Collected.Objects)+len(third.Collected.Manifests)+len(third.Collected.Forgotten) != 0 {
		t.Fatalf("repeated collection deleted more: %+v", third.Collected)
	}
}

func TestManifestReconcile_ReportsLossAndRecoversThroughAReinit(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]
	creates := seedCompactionBase(ctx, t, env, wide, 5)
	if err := env.ApplyEvents(ctx, UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "loss-v2"})); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env)
	m := loadSchemaManifest(ctx, t, env, wide)
	lost := TierEntries(m, "delta")[0].Path
	deleteObject(ctx, t, env, lost)

	rep := runReconcile(ctx, t, env, wide, reconcileOptions(false))
	if len(rep.Findings) != 1 || rep.Findings[0].Kind != reconcile.LostObject {
		t.Fatalf("findings %+v, want one lost object", rep.Findings)
	}
	f := rep.Findings[0]
	if f.Key != lost || !f.Current || f.Epoch != m.Epoch || f.Generation != m.Generation || !strings.Contains(f.Action, "cdc-init") {
		t.Fatalf("lost-object finding %+v does not name %s of current epoch %d generation %d with re-init action", f, lost, m.Epoch, m.Generation)
	}

	deleteObject(ctx, t, env, m.Key)
	before := schemaObjectKeys(ctx, t, env, wide)
	rep = runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if rep.Refused == "" || rep.Findings[0].Kind != reconcile.MissingGeneration || rep.Findings[0].Generation != m.Generation {
		t.Fatalf("lost manifest must stop collection with missing_generation, got refused %q, findings %+v", rep.Refused, rep.Findings)
	}
	if after := schemaObjectKeys(ctx, t, env, wide); !slices.Equal(before, after) {
		t.Fatalf("refused collection deleted objects: before %v, after %v", before, after)
	}

	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	reinit := loadSchemaManifest(ctx, t, env, wide)
	rep = runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if rep.Refused == "" {
		t.Fatal("lost generation is retained until it expires, so collection stays refused")
	}
	time.Sleep(reconcileGrace)
	rep = runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if rep.Refused != "" || len(rep.Findings) != 0 {
		t.Fatalf("after re-init and grace collection is refused (%q) or finds %+v", rep.Refused, rep.Findings)
	}
	want := append(sortedGenerationKeys(reinit), reinit.Key)
	slices.Sort(want)
	if got := schemaObjectKeys(ctx, t, env, wide); !slices.Equal(got, want) {
		t.Fatalf("after recovery schema holds %v, want init generation's %v", got, want)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
}

func TestManifestReconcile_RecoveryAndCollectionKeepDeletesAndNewestVersions(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]
	creates := seedCompactionBase(ctx, t, env, wide, 5)
	deleted, updated := creates[1].RowID, creates[2].RowID
	apply := func(events ...*Event) {
		t.Helper()
		if err := env.ApplyEvents(ctx, events...); err != nil {
			t.Fatalf("apply events: %v", err)
		}
	}
	apply(UpdateEvent(wide, updated, map[string]any{"title": "recovery-v1"}))
	mustFlush(ctx, t, env)
	apply(DeleteEvent(wide, deleted), UpdateEvent(wide, updated, map[string]any{"title": "recovery-v2"}))
	mustFlush(ctx, t, env)
	lostGen := loadSchemaManifest(ctx, t, env, wide)
	deleteObject(ctx, t, env, lostGen.Key)

	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	reinit := loadSchemaManifest(ctx, t, env, wide)
	assertRecoveredReads(ctx, t, env, wide, deleted, updated)

	runReconcile(ctx, t, env, wide, reconcileOptions(true))
	time.Sleep(reconcileGrace)
	rep := runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if rep.Refused != "" || len(rep.Findings) != 0 {
		t.Fatalf("collection after recovery refused (%q) or finds %+v", rep.Refused, rep.Findings)
	}
	if len(rep.Collected.Objects) == 0 {
		t.Fatal("collection deleted no object of generations re-init replaced")
	}
	want := append(sortedGenerationKeys(reinit), reinit.Key)
	slices.Sort(want)
	if got := schemaObjectKeys(ctx, t, env, wide); !slices.Equal(got, want) {
		t.Fatalf("after collection schema holds %v, want init generation's %v", got, want)
	}
	assertRecoveredReads(ctx, t, env, wide, deleted, updated)

	apply(DeleteEvent(wide, creates[3].RowID), UpdateEvent(wide, updated, map[string]any{"title": "recovery-v3"}))
	mustFlush(ctx, t, env)
	assertRecoveredReads(ctx, t, env, wide, deleted, updated)
	if res := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100}); res != nil && containsRow(res, creates[3].RowID) {
		t.Fatalf("a row deleted over recovered generation is answered")
	}
}

func assertRecoveredReads(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, deleted, updated uuid.UUID) {
	t.Helper()
	for _, tiers := range [][]model.DataTier{nil, {model.DataTierCold}, {model.DataTierWarm, model.DataTierCold}} {
		res := env.AssertQueryMatches(ctx, Query{Schema: schema, PreferredTiers: tiers, Limit: 100})
		if res == nil {
			return
		}
		if containsRow(res, deleted) {
			t.Fatalf("tiers %v: deleted row %s is answered", tiers, deleted)
		}
		if !containsRow(res, updated) {
			t.Fatalf("tiers %v: updated row %s is missing", tiers, updated)
		}
	}
}

func containsRow(res *QueryResult, rowID uuid.UUID) bool {
	return slices.ContainsFunc(res.Records, func(r *model.PersistentRecord) bool { return r.RowID == rowID })
}
