//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/model"
)

// TestPublicationCrashBoundariesAllWriters injects crashes at every S3
// staging, manifest publish, PG transaction, commit-answer and handoff
// boundary across cdc-flush, cdc-init and compactor. Each crash must leave
// reads oracle-consistent with zero partial visibility, allow a clean follow-up
// to converge, and leave any staged orphans collectable by manifest-reconcile
// --gc.
func TestPublicationCrashBoundariesAllWriters(t *testing.T) {
	t.Parallel()

	t.Run("cdc_flush_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointMarks),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runFlushCrashBoundary(t, point)
			})
		}
	})

	t.Run("cdc_init_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
			string(cdcv2.PointHandoff),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runInitCrashBoundary(t, point)
			})
		}
	})

	t.Run("compactor_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runCompactorCrashBoundary(t, point)
			})
		}
	})
}

func runFlushCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "f1", 3, 10)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)

	seedMatrixRows(ctx, t, env, wide, "f2", 3, 20)
	cfg := env.CDC
	cfg.PublishFailpoint = crashOnceAt(point)
	_, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if point == string(cdcv2.PointCommitAnswer) {
		// Lost answer after a landed COMMIT resolves to committed without error.
		if err != nil {
			t.Fatalf("flush commit-answer resolution failed: %v", err)
		}
		after := loadSchemaManifest(ctx, t, env, wide)
		if after.Generation != before.Generation+1 {
			t.Fatalf("commit-answer generation = %d, want %d", after.Generation, before.Generation+1)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("flush failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed flush at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	// Clean retry converges and leaves oracle-matched state.
	mustFlush(ctx, t, env)
	if after := loadSchemaManifest(ctx, t, env, wide); after.Generation != before.Generation+1 {
		t.Fatalf("retry flush after %s generation = %d, want %d", point, after.Generation, before.Generation+1)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

func runInitCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "i1", 4, 10)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)
	seedMatrixRows(ctx, t, env, wide, "i2", 2, 20)

	cfg := env.CDC
	cfg.PublishFailpoint = crashOnceAt(point)
	_, err := env.RunInitWith(ctx, wide, InitOverrides{Config: &cfg})
	switch point {
	case string(cdcv2.PointCommitAnswer):
		if err != nil {
			t.Fatalf("init commit-answer resolution failed: %v", err)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	case string(cdcv2.PointHandoff):
		// Handoff fails after the init generation committed: reads stay
		// oracle-consistent (dirty rows deduplicate against base), and a
		// follow-up init completes handoff cleanly.
		if err == nil {
			t.Fatal("init failpoint at handoff must fail")
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		if _, err := env.RunInit(ctx, wide); err != nil {
			t.Fatalf("follow-up init after handoff crash: %v", err)
		}
		env.MarkOnboarded(ctx, wide)
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("init failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed init at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("clean retry init after %s: %v", point, err)
	}
	env.MarkOnboarded(ctx, wide)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

func runCompactorCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "c1", 3, 10)
	mustFlush(ctx, t, env)
	seedMatrixRows(ctx, t, env, wide, "c2", 3, 20)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)

	_, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{
		TargetBaseSizeBytes: 1,
		PublishFailpoint:    crashOnceAt(point),
	})
	if point == string(cdcv2.PointCommitAnswer) {
		if err != nil {
			t.Fatalf("compactor commit-answer resolution failed: %v", err)
		}
		after := loadSchemaManifest(ctx, t, env, wide)
		if after.Generation != before.Generation+1 {
			t.Fatalf("compactor commit-answer generation = %d, want %d", after.Generation, before.Generation+1)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("compactor failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed compactor at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	res, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("clean retry compaction after %s: %v", point, err)
	}
	if res.Generation == 0 {
		t.Fatalf("retry compaction after %s did not commit a compact generation: %+v", point, res)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

// TestPublicationConcurrentWritersAndReaders runs cdc-flush, cdc-init, and
// compactor concurrently against the same schema while concurrent filtered and
// paginated federated queries read continuously over real PostgreSQL + RustFS +
// DuckDB. Every racing reader page is validated against the full stable oracle
// (totals, exact row IDs, ordering, and attribute values), and a synchronized
// boundary-straddling reader proves overlap across a committed publication.
func TestPublicationConcurrentWritersAndReaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithDuckMaxConnections(4))
	pauser := newPausingPublicationReader()
	pauser.install(env)
	wide := DefaultSchemaFixtures()[1]

	initial := seedMatrixRows(ctx, t, env, wide, "base", 6, 100)
	deletedID := initial[0].RowID
	mustFlush(ctx, t, env)
	mustApplyEvents(ctx, t, env, "update and delete before race",
		UpdateEvent(wide, initial[1].RowID, map[string]any{"title": "updated-live", "count": float64(777)}),
		DeleteEvent(wide, deletedID),
	)
	mustFlush(ctx, t, env)
	seedMatrixRows(ctx, t, env, wide, "hot", 5, 200)
	preRace := loadSchemaManifest(ctx, t, env, wide)

	// Data is fixed before the race; writers only move rows between tiers, so
	// every racing page must match the exact stable oracle.
	specs := buildRaceReaderSpecs(t, env, wide)
	straddleCh := startBoundaryStraddlingReader(ctx, t, env, pauser, specs[0])

	readCtx, stopReaders := context.WithCancel(ctx)
	var writersActive atomic.Bool
	readsDuringWriters, waitReaders := startConcurrentRaceReaders(readCtx, env, specs, &writersActive)

	writersActive.Store(true)
	errs := runThreeWayRacingWriters(ctx, env, wide)
	writersActive.Store(false)

	stopReaders()
	if err := waitReaders(); err != nil {
		t.Fatalf("concurrent readers saw oracle divergence or error: %v", err)
	}
	if readsDuringWriters.Load() == 0 {
		t.Fatal("continuous readers completed 0 queries while writers were active")
	}
	assertThreeWayWriterOutcomes(t, errs)

	postRace := loadSchemaManifest(ctx, t, env, wide)
	if postRace.Generation <= preRace.Generation {
		t.Fatalf("expected racing writers to advance generation past %d, still at %d", preRace.Generation, postRace.Generation)
	}
	verifyBoundaryStraddlingReader(t, pauser, straddleCh, preRace.Generation)

	// Drain any remaining dirty entries and verify final oracle parity and no resurrection.
	mustFlush(ctx, t, env)
	assertFilteredAndPaginated(ctx, t, env, wide, true)
	assertDeletedAbsent(ctx, t, env, wide, deletedID)
}

type raceReaderSpec struct {
	label    string
	query    Query
	expected *ExpectedResult
	state    *ExpectedState
}

func buildRaceReaderSpecs(t *testing.T, env *Env, wide SchemaRef) []raceReaderSpec {
	t.Helper()
	state, err := env.ExpectedState(wide)
	if err != nil {
		t.Fatalf("build stable race oracle: %v", err)
	}
	offsets := []int{0, 2}
	specs := make([]raceReaderSpec, 0, len(offsets))
	for _, off := range offsets {
		q := Query{
			Schema:  wide,
			Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
			Sorts:   []Sort{{Attr: "count"}},
			Limit:   5,
			Offset:  off,
		}
		exp, err := state.Run(q)
		if err != nil {
			t.Fatalf("run race oracle (offset=%d): %v", off, err)
		}
		specs = append(specs, raceReaderSpec{
			label:    fmt.Sprintf("offset=%d", off),
			query:    q,
			expected: exp,
			state:    state,
		})
	}
	return specs
}

type straddleOutcome struct {
	err error
}

// startBoundaryStraddlingReader launches a reader that pauses immediately after
// taking its REPEATABLE READ snapshot and reading the pre-race publication
// record, proving deterministic overlap with the concurrent writers.
func startBoundaryStraddlingReader(ctx context.Context, t *testing.T, env *Env, pauser *pausingPublicationReader, spec raceReaderSpec) <-chan straddleOutcome {
	t.Helper()
	eng := env.Engine()
	fq, err := env.buildFederatedQuery(spec.query)
	if err != nil {
		t.Fatalf("build straddling query: %v", err)
	}
	qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	ch := make(chan straddleOutcome, 1)
	finished := make(chan struct{})
	pauser.armed.Store(true)
	go func() {
		defer close(finished)
		page, qerr := eng.Query(qctx, env.Tables, fq, nil)
		if qerr != nil {
			ch <- straddleOutcome{err: fmt.Errorf("straddling read: %w", qerr)}
			return
		}
		ch <- straddleOutcome{err: checkRacePageAgainstOracle("straddling "+spec.label, spec, page.Records, page.TotalRecords)}
	}()
	t.Cleanup(func() {
		pauser.Release()
		cancel()
		<-finished
	})
	select {
	case <-pauser.Reached():
	case out := <-ch:
		t.Fatalf("straddling reader finished before pausing at publication boundary: %v", out.err)
	}
	return ch
}

func verifyBoundaryStraddlingReader(t *testing.T, pauser *pausingPublicationReader, ch <-chan straddleOutcome, wantGen int64) {
	t.Helper()
	beforeRelease := len(pauser.Resolved())
	pauser.Release()
	out := <-ch
	if out.err != nil {
		t.Fatalf("boundary-straddling reader failed: %v", out.err)
	}
	resolved := pauser.Resolved()[beforeRelease:]
	if len(resolved) != 1 || resolved[0].Generation != wantGen {
		t.Fatalf("boundary-straddling reader resolved %+v after release, want single pre-race generation %d", resolved, wantGen)
	}
}

func runThreeWayRacingWriters(ctx context.Context, env *Env, wide SchemaRef) []error {
	var writeWG sync.WaitGroup
	errCh := make(chan error, 3)
	writeWG.Add(3)
	go func() {
		defer writeWG.Done()
		_, err := env.RunFlushWith(ctx, FlushOverrides{})
		errCh <- err
	}()
	go func() {
		defer writeWG.Done()
		_, err := env.RunInitWith(ctx, wide, InitOverrides{})
		errCh <- err
	}()
	go func() {
		defer writeWG.Done()
		_, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
		errCh <- err
	}()
	writeWG.Wait()
	close(errCh)
	out := make([]error, 0, 3)
	for err := range errCh {
		out = append(out, err)
	}
	return out
}

func assertThreeWayWriterOutcomes(t *testing.T, errs []error) {
	t.Helper()
	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		if !errors.Is(err, cdcv2.ErrAbandoned) && !errors.Is(err, cdc.ErrSchemaLockContended) {
			t.Fatalf("unexpected writer error in 3-way race: %v", err)
		}
	}
	if succeeded == 0 {
		t.Fatal("expected at least one racing writer to succeed")
	}
}

func startConcurrentRaceReaders(readCtx context.Context, env *Env, specs []raceReaderSpec, writersActive *atomic.Bool) (*atomic.Int64, func() error) {
	eng := env.Engine()
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	var duringWriters atomic.Int64

	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for _, spec := range specs {
		wg.Add(1)
		go func(spec raceReaderSpec) {
			defer wg.Done()
			fq, err := env.buildFederatedQuery(spec.query)
			if err != nil {
				recordErr(err)
				return
			}
			for readCtx.Err() == nil {
				activeAtStart := writersActive.Load()
				page, err := eng.Query(readCtx, env.Tables, fq, nil)
				if err != nil {
					if readCtx.Err() != nil {
						return
					}
					recordErr(fmt.Errorf("concurrent read (%s): %w", spec.label, err))
					return
				}
				if err := checkRacePageAgainstOracle("concurrent "+spec.label, spec, page.Records, page.TotalRecords); err != nil {
					recordErr(err)
					return
				}
				if activeAtStart {
					duringWriters.Add(1)
				}
			}
		}(spec)
	}

	return &duringWriters, func() error {
		wg.Wait()
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}
}

func checkRacePageAgainstOracle(label string, spec raceReaderSpec, records []*model.PersistentRecord, total int64) error {
	actual := &QueryResult{Spec: spec.query, Records: records, Total: total}
	diff := computeDiff(spec.query, spec.expected, actual, spec.state.Cache)
	if !diff.IsEmpty() {
		return fmt.Errorf("%s diverged from stable oracle (expected_total=%d actual_total=%d missing=%v extra=%v dup=%v misordered=%t attrs=%+v)",
			label, diff.ExpectedTotal, diff.ActualTotal, diff.Missing, diff.Extra, diff.Duplicate, diff.Misordered, diff.Attrs)
	}
	return nil
}

func crashOnceAt(point string) func(context.Context, string) error {
	var fired atomic.Bool
	return func(_ context.Context, p string) error {
		if p == point && fired.CompareAndSwap(false, true) {
			return fmt.Errorf("injected crash at %s", point)
		}
		return nil
	}
}

func verifyGCReconcilesCleanly(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	time.Sleep(reconcileGrace)
	if _, err := env.reconciler(t, schema, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("manifest-reconcile --gc after crash recovery: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 50})
}
