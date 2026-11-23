//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/model"
)

// TestPublicationConcurrentWritersAndReaders runs cdc-flush, cdc-init, and
// compactor concurrently against the same schema while concurrent filtered and
// paginated federated queries read continuously over real PostgreSQL + RustFS +
// DuckDB. Every racing reader page is validated against the full stable oracle
// (totals, exact row IDs, ordering, and attribute values), a writer-boundary
// gate deterministically overlaps continuous reads with the active writer
// interval, and a synchronized boundary-straddling reader proves overlap across
// a committed publication.
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

	raceCtx, cancelRace := context.WithTimeout(ctx, pausedFlushTimeout)
	readCtx, stopReaders := context.WithCancel(raceCtx)
	var writersActive atomic.Bool
	writerGate := newWriterBoundaryGate(cdcv2.PointBegin, &writersActive)
	readers := startConcurrentRaceReaders(readCtx, env, specs, &writersActive)
	writersCh, writersDone := startThreeWayRacingWriters(raceCtx, env, wide, writerGate.failpoint)
	t.Cleanup(func() {
		writerGate.Release()
		stopReaders()
		cancelRace()
		<-writersDone
		_ = readers.wait()
	})

	// Hold the first writer at PointBegin until a continuous reader starts
	// while writersActive is true and finishes an oracle-verified query, then
	// release it so all three writers race to completion under live reads.
	select {
	case <-readers.readDone:
	case <-readers.failed:
	case <-writersDone:
	case <-raceCtx.Done():
		t.Fatalf("timed out waiting for continuous reader to complete during active writer interval: %v", raceCtx.Err())
	}
	writerGate.Release()

	errs := <-writersCh
	writersActive.Store(false)
	stopReaders()
	if err := readers.wait(); err != nil {
		t.Fatalf("concurrent readers saw oracle divergence or error: %v", err)
	}
	assertThreeWayWriterOutcomes(t, errs)
	if readers.duringWriters.Load() == 0 {
		t.Fatal("continuous readers completed 0 queries while writers were active")
	}

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
	case <-qctx.Done():
		t.Fatalf("timed out waiting for straddling reader to pause at publication boundary: %v", qctx.Err())
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

// writerBoundaryGate pauses the first concurrent writer that reaches target
// until Release is called, establishing a happens-before barrier so continuous
// readers deterministically start and complete queries while writers are active.
// Subsequent writers and retries pass through without pausing.
type writerBoundaryGate struct {
	target      string
	active      *atomic.Bool
	release     chan struct{}
	reachOnce   sync.Once
	releaseOnce sync.Once
}

func newWriterBoundaryGate(target cdcv2.Point, active *atomic.Bool) *writerBoundaryGate {
	return &writerBoundaryGate{
		target:  string(target),
		active:  active,
		release: make(chan struct{}),
	}
}

func (g *writerBoundaryGate) Release() {
	g.releaseOnce.Do(func() { close(g.release) })
}

func (g *writerBoundaryGate) failpoint(ctx context.Context, point string) error {
	if point != g.target {
		return nil
	}
	first := false
	g.reachOnce.Do(func() {
		first = true
		g.active.Store(true)
	})
	if !first {
		return nil
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func startThreeWayRacingWriters(ctx context.Context, env *Env, wide SchemaRef, failpoint func(context.Context, string) error) (<-chan []error, <-chan struct{}) {
	errResult := make(chan []error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var writeWG sync.WaitGroup
		errCh := make(chan error, 3)
		writeWG.Add(3)
		go func() {
			defer writeWG.Done()
			cfg := env.CDC
			cfg.PublishFailpoint = failpoint
			_, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
			errCh <- err
		}()
		go func() {
			defer writeWG.Done()
			cfg := env.CDC
			cfg.PublishFailpoint = failpoint
			_, err := env.RunInitWith(ctx, wide, InitOverrides{Config: &cfg})
			errCh <- err
		}()
		go func() {
			defer writeWG.Done()
			_, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{
				TargetBaseSizeBytes: 1,
				PublishFailpoint:    failpoint,
			})
			errCh <- err
		}()
		writeWG.Wait()
		close(errCh)
		out := make([]error, 0, 3)
		for err := range errCh {
			out = append(out, err)
		}
		errResult <- out
	}()
	return errResult, done
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

type raceReaders struct {
	duringWriters *atomic.Int64
	readDone      <-chan struct{}
	failed        <-chan struct{}
	wait          func() error
}

func startConcurrentRaceReaders(readCtx context.Context, env *Env, specs []raceReaderSpec, writersActive *atomic.Bool) *raceReaders {
	eng := env.Engine()
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	var duringWriters atomic.Int64
	readDone := make(chan struct{})
	failed := make(chan struct{})
	var readDoneOnce, failedOnce, waitOnce sync.Once

	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		failedOnce.Do(func() { close(failed) })
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
					readDoneOnce.Do(func() { close(readDone) })
				}
			}
		}(spec)
	}

	return &raceReaders{
		duringWriters: &duringWriters,
		readDone:      readDone,
		failed:        failed,
		wait: func() error {
			waitOnce.Do(wg.Wait)
			errMu.Lock()
			defer errMu.Unlock()
			return firstErr
		},
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
