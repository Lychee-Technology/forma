//go:build e2e

package production

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestFlushGraceBackstopServesStaleManifestSnapshot pins the #252 read-side
// grace: even with the append-before-mark ordering, a reader that resolved
// its parquet path set BEFORE a flush's manifest append but runs its dirty
// scan AFTER the mark would see the batch in neither tier. The dirty-barrier
// cutoff anchors at the query's own path-resolution instant (minus the
// configured clock-skew margin), so rows marked after that instant stay
// hot-readable while the resolved path set may predate their delta.
//
// The e2e cannot pause a query between its path resolution and its dirty
// scan, so the race is emulated from the other side: an explicit
// S3ParquetPathTemplate hint (an explicit hint wins over the manifest
// source, #184/#187) lists only the FIRST flush's delta while the second
// flush has already marked and appended, and a large margin stands in for
// "the path set was resolved before that flush". The default-margin probe is
// the counterpart pin: in the steady state (flush completed before the
// query) the exact anchor must NOT widen, so results keep their flushed
// parquet semantics with zero drift.
func TestFlushGraceBackstopServesStaleManifestSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	seedTwoFlushedGenerations := func(t *testing.T, env *Env) (staleHint string) {
		t.Helper()
		wide := DefaultSchemaFixtures()[1] // e2e_wide
		gen1 := buildOpenCreates(wide, 4)
		mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
		first, err := env.RunFlushWith(ctx, FlushOverrides{})
		if err != nil {
			t.Fatalf("first flush: %v", err)
		}
		firstFinals, _ := splitKeys(first.NewObjects)
		if len(firstFinals) != 1 {
			t.Fatalf("first flush must promote exactly one final delta, got %v", firstFinals)
		}
		gen2 := make([]*Event, 0, 4)
		for i := 0; i < 4; i++ {
			gen2 = append(gen2, CreateEvent(wide, map[string]any{
				"title": fmt.Sprintf("fresh-%02d", i),
				"count": float64(300000 + i),
			}))
		}
		mustApplyEvents(ctx, t, env, "seed generation 2", gen2...)
		if _, err := env.RunFlushWith(ctx, FlushOverrides{}); err != nil {
			t.Fatalf("second flush: %v", err)
		}
		// The stale reader's path set: only the first delta, as if the second
		// flush's manifest append had not been observed yet.
		return fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, firstFinals[0])
	}

	queryStale := func(t *testing.T, env *Env, staleHint string) int {
		t.Helper()
		wide := DefaultSchemaFixtures()[1]
		res, err := env.Query(ctx, Query{
			Schema: wide, Limit: 10, S3ParquetPathTemplate: staleHint,
		})
		if err != nil {
			t.Fatalf("stale-snapshot query: %v", err)
		}
		if !res.Plan.Routing.UseDuckDB {
			t.Errorf("stale-snapshot query did not route to duckdb: %+v", res.Plan.Routing)
		}
		return len(res.Records)
	}

	t.Run("MarginServesRowsFlushedAfterSnapshot", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		// A 60s margin emulates a path set resolved before the recent flushes:
		// both generations' flushed_at land after cutoff = resolution - 60s,
		// so the widened barrier serves ALL rows from the hot tier even though
		// the stale path set lists only the first delta.
		env.DuckCfg.FlushVisibilityGraceMs = 60_000
		staleHint := seedTwoFlushedGenerations(t, env)
		if got := queryStale(t, env, staleHint); got != 8 {
			t.Errorf("margined stale-snapshot query saw %d rows, want 8 — rows flushed after the emulated snapshot must stay hot-readable (#252)", got)
		}
	})

	t.Run("ExactAnchorNeverWidensSteadyState", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		staleHint := seedTwoFlushedGenerations(t, env)
		// The inclusive cutoff comparison deliberately widens a mark landing
		// in the SAME millisecond as the query's path stamp; let the clock
		// tick past the flush so this probe pins the steady state, not the
		// boundary.
		waitWallClockTick(t)
		// Default (zero margin): both flushes completed before this query
		// resolved its paths, so nothing is widened — the second generation is
		// invisible through the stale hint exactly as pre-#252. This pins both
		// the zero-drift steady state and that the margin is the serving
		// mechanism in the subtest above.
		if got := queryStale(t, env, staleHint); got != 4 {
			t.Errorf("exact-anchor stale-snapshot query saw %d rows, want 4 (steady state must not widen)", got)
		}
	})
}

// waitWallClockTick blocks until UnixMilli advances, so a subsequent
// timestamp is strictly greater than any stamp taken before the call.
func waitWallClockTick(t *testing.T) {
	t.Helper()
	start := time.Now().UnixMilli()
	for time.Now().UnixMilli() <= start {
		time.Sleep(200 * time.Microsecond)
	}
}

// pausingParquetSource decorates the manifest-driven parquet source: the
// FIRST Paths call resolves the real list, signals Reached, then blocks until
// Release (or ctx cancellation). The caller can therefore run a full flush
// strictly between a query's path resolution and its execution — the #252
// reader race that no S3-level pause can reach.
type pausingParquetSource struct {
	inner       fedengine.ParquetSource
	reached     chan struct{}
	release     chan struct{}
	reachOnce   sync.Once
	releaseOnce sync.Once
}

func newPausingParquetSource() *pausingParquetSource {
	return &pausingParquetSource{reached: make(chan struct{}), release: make(chan struct{})}
}

func (p *pausingParquetSource) Reached() <-chan struct{} { return p.reached }

func (p *pausingParquetSource) Release() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *pausingParquetSource) Paths(ctx context.Context, schemaID int16) ([]string, map[string]map[string]string, error) {
	paths, stamps, err := p.inner.Paths(ctx, schemaID)
	first := false
	p.reachOnce.Do(func() { first = true })
	if first {
		close(p.reached)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}
	return paths, stamps, err
}

func (p *pausingParquetSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	return p.inner.MissingIn(ctx, scanned)
}

// TestFlushGraceExactAnchorCoversInFlightFlush pins the actual #252 reader
// race end to end (review P2): a query stamps its cutoff and resolves its
// parquet path set, THEN a full flush appends and marks a generation the
// resolved set does not list, then the query's dirty scan executes. The
// default exact anchor must keep the raced generation hot-readable; the
// disabled-widening probe proves the anchor is the serving mechanism. This
// also exercises the review-P1 fix — the mark stamp is sampled after the
// append, so it lands at-or-after the paused query's cutoff.
func TestFlushGraceExactAnchorCoversInFlightFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	runRacedQuery := func(t *testing.T, env *Env) int {
		t.Helper()
		// Installed before ANY event: seeding goes through the EntityManager,
		// which assembles the engine on first use — a wrapper set later would
		// be ignored. The wrapper only pauses the first Paths call, and
		// neither writes nor flushes resolve paths, so that first call is
		// exactly the raced query below.
		pauser := newPausingParquetSource()
		env.ParquetSourceWrap = func(src fedengine.ParquetSource) fedengine.ParquetSource {
			pauser.inner = src
			return pauser
		}

		wide := DefaultSchemaFixtures()[1] // e2e_wide
		gen1 := buildOpenCreates(wide, 4)
		mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
		if _, err := env.RunFlushWith(ctx, FlushOverrides{}); err != nil {
			t.Fatalf("first flush: %v", err)
		}
		gen2 := make([]*Event, 0, 4)
		for i := 0; i < 4; i++ {
			gen2 = append(gen2, CreateEvent(wide, map[string]any{
				"title": fmt.Sprintf("raced-%02d", i),
				"count": float64(500000 + i),
			}))
		}
		mustApplyEvents(ctx, t, env, "seed generation 2", gen2...)

		type outcome struct {
			res *QueryResult
			err error
		}
		qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
		done := make(chan outcome, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			res, err := env.Query(qctx, Query{Schema: wide, Limit: 10})
			done <- outcome{res: res, err: err}
		}()
		t.Cleanup(func() {
			pauser.Release()
			cancel()
			<-finished
		})

		select {
		case <-pauser.Reached():
		case out := <-done:
			t.Fatalf("query finished before pausing at path resolution: err=%v", out.err)
		}
		// The query holds a path set WITHOUT generation 2's delta and a cutoff
		// stamped before this flush: append + mark race the suspended query.
		if _, err := env.RunFlushWith(ctx, FlushOverrides{}); err != nil {
			t.Fatalf("racing flush: %v", err)
		}
		pauser.Release()
		out := <-done
		if out.err != nil {
			t.Fatalf("query racing the flush: %v", out.err)
		}
		if !out.res.Plan.Routing.UseDuckDB {
			t.Errorf("raced query did not route to duckdb: %+v", out.res.Plan.Routing)
		}
		return len(out.res.Records)
	}

	t.Run("ExactAnchorServesRacedRows", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		if got := runRacedQuery(t, env); got != 8 {
			t.Errorf("raced query saw %d rows, want 8 — rows marked after the query's path snapshot must stay hot-readable (#252)", got)
		}
	})

	t.Run("DisabledWideningExposesRace", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		env.DuckCfg.FlushVisibilityGraceMs = -1
		if got := runRacedQuery(t, env); got != 4 {
			t.Errorf("disabled-widening raced query saw %d rows, want 4 (the uncovered race)", got)
		}
	})
}
