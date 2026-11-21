//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// combinedMetrics captures both circuit-breaker transitions and degraded
// fallback attempts for a single Env.
type combinedMetrics struct {
	mu          sync.Mutex
	transitions []string
	fallbacks   []string
}

func (m *combinedMetrics) EmitMetric(_ context.Context, metric forma.Metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch metric.Name {
	case "duckdb_circuit_breaker_transition_total":
		m.transitions = append(m.transitions, metric.Labels["state"])
	case "federated_degraded_fallback_total":
		m.fallbacks = append(m.fallbacks, metric.Labels["cause"]+"/"+metric.Labels["outcome"])
	}
}

func (m *combinedMetrics) assertClean(t *testing.T, label string) {
	t.Helper()
	m.mu.Lock()
	tr, fb := append([]string(nil), m.transitions...), append([]string(nil), m.fallbacks...)
	m.mu.Unlock()
	if len(tr) != 0 {
		t.Errorf("%s: expected zero breaker transitions, got %v", label, tr)
	}
	if len(fb) != 0 {
		t.Errorf("%s: expected zero degraded fallback metrics, got %v", label, fb)
	}
}

// TestPublicationMatrix_FullLifecycleAndPagination exercises a single schema
// across every Protocol V2 lifecycle state and transition end to end:
// UNPUBLISHED -> flush -> init -> re-init -> compact -> reconcile --gc ->
// DROPPED -> recreated (epoch+2, UNPUBLISHED) -> fresh flush, checking
// filtered, sorted, offset-paginated and keyset-paginated queries against the
// oracle at each stage and verifying no stale or deleted rows resurrect.
func TestPublicationMatrix_FullLifecycleAndPagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))
	wide := DefaultSchemaFixtures()[1]

	// Stage 1: UNPUBLISHED — filtered, sorted, offset and keyset queries
	// answer from PG alone with zero DuckDB statements.
	seed := seedMatrixRows(ctx, t, env, wide, "alpha", 8, 100)
	deletedID := seed[0].RowID
	mustApplyEvents(ctx, t, env, "delete before publish", DeleteEvent(wide, deletedID))
	assertFilteredAndPaginated(ctx, t, env, wide, false)

	// Stage 2: First cdc-flush (UNPUBLISHED -> PUBLISHED, g1) + hot mutations.
	mustFlush(ctx, t, env)
	g1 := loadSchemaManifest(ctx, t, env, wide)
	if g1.Generation != 1 {
		t.Fatalf("first flush generation = %d, want 1", g1.Generation)
	}
	seedMatrixRows(ctx, t, env, wide, "alpha", 4, 200)
	mustApplyEvents(ctx, t, env, "update flushed row", UpdateEvent(wide, seed[1].RowID, map[string]any{"title": "alpha-updated", "count": float64(999)}))
	assertFilteredAndPaginated(ctx, t, env, wide, true)

	// Stage 3: cdc-init (g2) + delta flush (g3) + re-init (g4, new lineage).
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("first cdc-init: %v", err)
	}
	env.MarkOnboarded(ctx, wide)
	g2 := loadSchemaManifest(ctx, t, env, wide)
	seedMatrixRows(ctx, t, env, wide, "beta", 5, 300)
	mustFlush(ctx, t, env)
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	env.MarkOnboarded(ctx, wide)
	g4 := loadSchemaManifest(ctx, t, env, wide)
	if g4.Lineage == g2.Lineage || g4.Generation <= g2.Generation {
		t.Fatalf("re-init must advance generation (%d -> %d) and lineage (%s -> %s)", g2.Generation, g4.Generation, g2.Lineage, g4.Lineage)
	}
	assertFilteredAndPaginated(ctx, t, env, wide, true)

	// Stage 4: Flush delta + compaction + manifest-reconcile --gc.
	seedMatrixRows(ctx, t, env, wide, "alpha", 4, 400)
	mustFlush(ctx, t, env)
	compRes, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if compRes.Generation == 0 {
		t.Fatalf("expected compaction to commit a compact generation, got %+v", compRes)
	}
	// First reconcile retires superseded generations; after the retention grace,
	// a second --gc pass forgets and deletes their expired manifests/objects.
	if _, err := env.reconciler(t, wide, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("manifest-reconcile --gc retire pass: %v", err)
	}
	time.Sleep(reconcileGrace)
	recRep, err := env.reconciler(t, wide, reconcileOptions(true)).Run(ctx)
	if err != nil {
		t.Fatalf("manifest-reconcile --gc collect pass: %v", err)
	}
	if len(recRep.Schemas) != 1 || recRep.Schemas[0].Collected == nil || len(recRep.Schemas[0].Collected.Manifests) == 0 {
		t.Fatalf("expected --gc to delete expired superseded manifests, got %+v", recRep)
	}
	assertFilteredAndPaginated(ctx, t, env, wide, true)
	assertDeletedAbsent(ctx, t, env, wide, deletedID)

	verifyDropAndRecreateLifecycle(ctx, t, env, wide, metrics)
	metrics.assertClean(t, "full lifecycle")
}

// verifyDropAndRecreateLifecycle exercises DROPPED (epoch+1) failing reads and
// writers closed, followed by recreation (UNPUBLISHED at epoch+2) where stale
// process pins still fail closed and a restarted process serves cleanly while
// keeping the generation counter monotonic.
func verifyDropAndRecreateLifecycle(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, metrics *combinedMetrics) {
	t.Helper()
	preDrop := loadSchemaManifest(ctx, t, env, wide)
	droppedEpoch, droppedGen := dropSchemaRecord(ctx, t, env, wide)
	if droppedEpoch != preDrop.Epoch+1 {
		t.Fatalf("dropped epoch = %d, want %d", droppedEpoch, preDrop.Epoch+1)
	}
	assertPublicationFault(ctx, t, env, wide, metrics, false)
	if _, err := env.RunFlush(ctx); err == nil {
		t.Fatal("cdc-flush against DROPPED schema must fail closed")
	}

	recreatedEpoch, recreatedGen := activateDroppedSchemaRecord(ctx, t, env, wide)
	if recreatedEpoch != preDrop.Epoch+2 || recreatedGen != droppedGen {
		t.Fatalf("recreated record = (epoch=%d, gen=%d), want (%d, %d)", recreatedEpoch, recreatedGen, preDrop.Epoch+2, droppedGen)
	}
	// Stale process pin still fails closed even under degraded mode.
	assertPublicationFault(ctx, t, env, wide, metrics, false)

	// Fresh process startup repins the recreated epoch and serves UNPUBLISHED cleanly.
	// In production, drop-schema purges the old schema's rows before Activate;
	// here we seed fresh post-recreation rows (count >= 500) and verify that
	// UNPUBLISHED answers them with zero DuckDB statements, old pre-drop rows
	// are not resurrected into the new epoch, and the first flush of the new
	// epoch keeps the generation counter strictly monotonic across epochs.
	env.RegistryEpochs[wide.ID] = recreatedEpoch
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen process after schema recreate: %v", err)
	}
	seedMatrixRows(ctx, t, env, wide, "reborn", 3, 500)
	rebornUnpub, err := env.Query(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "500"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("query recreated UNPUBLISHED schema: %v", err)
	}
	if len(rebornUnpub.Records) != 3 || planHasEngine(rebornUnpub.Plan, "duckdb") {
		t.Fatalf("recreated UNPUBLISHED query = %d rows (duckdb=%t), want 3 rows with zero DuckDB statements",
			len(rebornUnpub.Records), planHasEngine(rebornUnpub.Plan, "duckdb"))
	}
	mustFlush(ctx, t, env)
	postRecreate := loadSchemaManifest(ctx, t, env, wide)
	if postRecreate.Epoch != recreatedEpoch || postRecreate.Generation <= preDrop.Generation {
		t.Fatalf("post-recreate flush = (epoch=%d, gen=%d), want epoch %d and gen > %d",
			postRecreate.Epoch, postRecreate.Generation, recreatedEpoch, preDrop.Generation)
	}
	rebornPub, err := env.Query(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "500"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("query recreated PUBLISHED schema: %v", err)
	}
	if len(rebornPub.Records) != 3 || !planHasEngine(rebornPub.Plan, "duckdb") {
		t.Fatalf("recreated PUBLISHED query = %d rows (duckdb=%t), want 3 rows via DuckDB",
			len(rebornPub.Records), planHasEngine(rebornPub.Plan, "duckdb"))
	}
}

// TestPublicationMatrix_FaultsFailClosedAndPreserveBreaker verifies that every
// Protocol V2 publication fault (missing/corrupt/stale manifest, forbidden
// empty flush, missing/AWAITING_INIT/DROPPED record, manifest lost between PG
// snapshot and S3 resolution, and 403 AccessDenied on manifest fetch or
// missing-object HeadObject probe) fails closed with ErrPublicationInconsistent,
// never degrades under AllowPartialDegradedMode, and never opens a threshold-1
// circuit breaker.
func TestPublicationMatrix_FaultsFailClosedAndPreserveBreaker(t *testing.T) {
	t.Parallel()

	t.Run("missing_manifest_with_intact_parquets", func(t *testing.T) {
		t.Parallel()
		env, metrics, wide := newPublishedFaultEnv(t)
		m := loadSchemaManifest(context.Background(), t, env, wide)
		deleteObject(context.Background(), t, env, m.Key)
		assertPublicationFault(context.Background(), t, env, wide, metrics, false)
	})

	t.Run("corrupt_manifest_bytes", func(t *testing.T) {
		t.Parallel()
		env, metrics, wide := newPublishedFaultEnv(t)
		m := loadSchemaManifest(context.Background(), t, env, wide)
		overwriteS3Object(context.Background(), t, env, m.Key, []byte(`{"corrupted":true}`))
		assertPublicationFault(context.Background(), t, env, wide, metrics, false)
	})

	t.Run("stale_pointer_identity_mismatch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env, metrics, wide := newPublishedFaultEnv(t)
		g1 := loadSchemaManifest(ctx, t, env, wide)
		seedMatrixRows(ctx, t, env, wide, "second", 3, 200)
		mustFlush(ctx, t, env)
		pointRecordAtStaleManifest(ctx, t, env, wide, g1)
		assertPublicationFault(ctx, t, env, wide, metrics, false)
	})

	t.Run("forbidden_empty_flush_generation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env, metrics, wide := newPublishedFaultEnv(t)
		publishIllicitEmptyFlushManifest(ctx, t, env, wide)
		assertPublicationFault(ctx, t, env, wide, metrics, false)
	})

	t.Run("missing_publication_record", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env, metrics, wide := newPublishedFaultEnv(t)
		deletePublicationRecord(ctx, t, env, wide)
		assertPublicationFault(ctx, t, env, wide, metrics, false)
	})

	t.Run("awaiting_init_record", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env, metrics, wide := newPublishedFaultEnv(t)
		forceAwaitingInitRecord(ctx, t, env, wide)
		assertPublicationFault(ctx, t, env, wide, metrics, false)
	})

	t.Run("dropped_record", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env, metrics, wide := newPublishedFaultEnv(t)
		dropSchemaRecord(ctx, t, env, wide)
		assertPublicationFault(ctx, t, env, wide, metrics, false)
	})

	t.Run("manifest_deleted_between_snapshot_and_resolution", func(t *testing.T) {
		t.Parallel()
		runManifestDeletedMidPass(t)
	})

	t.Run("access_denied_on_manifest_fetch", func(t *testing.T) {
		t.Parallel()
		runAccessDeniedManifestFetch(t)
	})

	t.Run("access_denied_on_missing_object_probe", func(t *testing.T) {
		t.Parallel()
		runAccessDeniedObjectProbe(t)
	})
}

func runManifestDeletedMidPass(t *testing.T) {
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))
	wide := DefaultSchemaFixtures()[1]
	pauser := newPausingPublicationReader()
	pauser.install(env)

	seedMatrixRows(ctx, t, env, wide, "race", 4, 10)
	mustFlush(ctx, t, env)
	m := loadSchemaManifest(ctx, t, env, wide)

	pauser.armed.Store(true)
	errCh := make(chan error, 1)
	go func() {
		_, err := env.Query(ctx, Query{Schema: wide, Limit: 10, AllowPartialDegradedMode: true})
		errCh <- err
	}()
	t.Cleanup(func() { pauser.Release() })
	<-pauser.Reached()
	deleteObject(ctx, t, env, m.Key)
	pauser.Release()

	err := <-errCh
	assertConsistencyError(t, err, wide, false)
	if env.Breaker().IsOpen() {
		t.Fatal("lost manifest between snapshot and resolution must not open circuit breaker")
	}
	metrics.assertClean(t, "lost manifest mid-pass")
}

func runAccessDeniedManifestFetch(t *testing.T) {
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "denied-m", 4, 10)
	mustFlush(ctx, t, env)
	env.PublicationS3Wrap = func(inner PublicationS3Client) PublicationS3Client {
		return &accessDeniedPublicationS3{inner: inner, denyGet: true, keyContains: "/_manifests/"}
	}
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen engine with access-denied S3 wrapper: %v", err)
	}
	assertPublicationFault(ctx, t, env, wide, metrics, true)
}

func runAccessDeniedObjectProbe(t *testing.T) {
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "denied-p", 4, 10)
	mustFlush(ctx, t, env)
	m := loadSchemaManifest(ctx, t, env, wide)
	deleteObject(ctx, t, env, m.Files[0].Path)
	env.PublicationS3Wrap = func(inner PublicationS3Client) PublicationS3Client {
		return &accessDeniedPublicationS3{inner: inner, denyHead: true}
	}
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen engine with access-denied probe wrapper: %v", err)
	}
	assertPublicationFault(ctx, t, env, wide, metrics, true)
}

// newPublishedFaultEnv provisions a fresh Env with a threshold-1 breaker,
// seeds 4 rows and publishes generation 1 so each fault subtest starts on a
// clean published schema with no cached manifest in a fresh engine.
func newPublishedFaultEnv(t *testing.T) (*Env, *combinedMetrics, SchemaRef) {
	t.Helper()
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "fault", 4, 10)
	mustFlush(ctx, t, env)
	// Drop any memoized engine/manifest cache before injecting the fault.
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen engine before fault injection: %v", err)
	}
	return env, metrics, wide
}

func seedMatrixRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, prefix string, n, baseCount int) []*Event {
	t.Helper()
	events := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, CreateEvent(schema, map[string]any{
			"title": fmt.Sprintf("%s-%03d", prefix, i),
			"count": float64(baseCount + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "seed "+prefix, events...)
	return events
}

func assertFilteredAndPaginated(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, expectDuckDB bool) {
	t.Helper()
	// Filtered + sorted offset pagination across pages 1 and 2.
	p1 := env.AssertQueryMatches(ctx, Query{
		Schema:  schema,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   4,
		Offset:  0,
	})
	if expectDuckDB && !planHasEngine(p1.Plan, "duckdb") {
		t.Fatalf("expected published query to scan DuckDB, got sources %+v", p1.Plan.Sources)
	}
	if !expectDuckDB && planHasEngine(p1.Plan, "duckdb") {
		t.Fatalf("expected UNPUBLISHED query to execute zero DuckDB statements, got sources %+v", p1.Plan.Sources)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:  schema,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   4,
		Offset:  4,
	})
	// Keyset pagination on count ascending with required trailing row_id tiebreak:
	// compare directly against the full oracle-checked sorted slice (the harness
	// oracle does not filter by KeysetCursor).
	full := env.AssertQueryMatches(ctx, Query{
		Schema:  schema,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   200,
	})
	if len(full.Records) <= 4 {
		return
	}
	pivot := full.Records[3]
	pivotCount, ok := pivot.Int32Items["integer_01"]
	if !ok {
		t.Fatalf("pivot record %s missing integer_01 count column", pivot.RowID)
	}
	ksPage, err := env.Query(ctx, Query{
		Schema:  schema,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   4,
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "count", Direction: forma.SortOrderAsc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []any{int64(pivotCount), pivot.RowID.String()},
			Mode:   model.KeysetCursorModeAfter,
		},
	})
	if err != nil {
		t.Fatalf("keyset query after (%d, %s): %v", pivotCount, pivot.RowID, err)
	}
	wantSlice := full.Records[4:]
	if len(wantSlice) > 4 {
		wantSlice = wantSlice[:4]
	}
	if len(ksPage.Records) != len(wantSlice) {
		t.Fatalf("keyset page len = %d, want %d", len(ksPage.Records), len(wantSlice))
	}
	for i := range wantSlice {
		if ksPage.Records[i].RowID != wantSlice[i].RowID {
			t.Fatalf("keyset page[%d] = %s, want %s", i, ksPage.Records[i].RowID, wantSlice[i].RowID)
		}
	}
}

func assertDeletedAbsent(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, deletedID any) {
	t.Helper()
	res := env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 200})
	for _, rec := range res.Records {
		if rec.RowID == deletedID {
			t.Fatalf("deleted row %v resurrected in federated query", deletedID)
		}
	}
}

func assertPublicationFault(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, metrics *combinedMetrics, wantUnverifiable bool) {
	t.Helper()
	for _, degraded := range []bool{false, true} {
		_, err := env.Query(ctx, Query{
			Schema:                   schema,
			Limit:                    20,
			AllowPartialDegradedMode: degraded,
		})
		assertConsistencyError(t, err, schema, wantUnverifiable)
	}
	if env.Breaker().IsOpen() {
		t.Fatal("publication fault must not open a threshold-1 circuit breaker")
	}
	metrics.assertClean(t, t.Name())
}

func assertConsistencyError(t *testing.T, err error, schema SchemaRef, wantUnverifiable bool) {
	t.Helper()
	if err == nil {
		t.Fatal("expected fail-closed publication consistency error, got nil")
	}
	if !errors.Is(err, fedengine.ErrPublicationInconsistent) {
		t.Fatalf("want ErrPublicationInconsistent, got %v", err)
	}
	if errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("publication fault must never classify as degradable ErrFederatedReadFailed: %v", err)
	}
	var typed *fedengine.PublicationConsistencyError
	if !errors.As(err, &typed) || typed.SchemaID != schema.ID {
		t.Fatalf("want *PublicationConsistencyError for schema %d, got %v", schema.ID, err)
	}
	if wantUnverifiable {
		if !errors.Is(err, forma.ErrPublishedObjectUnverifiable) {
			t.Fatalf("want ErrPublishedObjectUnverifiable on 403, got %v", err)
		}
		if len(typed.MissingKeys) != 0 {
			t.Fatalf("403 AccessDenied must not claim confirmed missing keys, got %v", typed.MissingKeys)
		}
	}
}
