//go:build e2e

package production

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

const breakerRejection = "circuit breaker open"

// breakerTransitions records the duckdb_circuit_breaker_transition_total
// states an engine emits (#634). The mutex honours the emitter's
// concurrent-use contract.
type breakerTransitions struct {
	mu     sync.Mutex
	states []string
}

func (r *breakerTransitions) EmitMetric(_ context.Context, m forma.Metric) {
	if m.Name != "duckdb_circuit_breaker_transition_total" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, m.Labels["state"])
}

// expect fails the test unless exactly want was emitted since the last call.
func (r *breakerTransitions) expect(t *testing.T, step string, want ...string) {
	t.Helper()
	r.mu.Lock()
	got := r.states
	r.states = nil
	r.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: breaker transitions %v, want %v", step, got, want)
	}
}

// TestCircuitBreaker_OpensAtThresholdAndRecovers covers #185 breaker
// scenarios 1-3 plus the #246 single-probe semantics end to end: exactly N
// real DuckDB failures (a closed database/sql client) open the breaker, an
// open breaker rejects queries before reaching DuckDB (rejection persists
// across a DuckDB rebuild while openDuration lasts), after openDuration the
// single admitted probe succeeds and closes the breaker (failure history
// cleared, so one fresh failure does not reopen it), and a FAILED probe
// re-opens the breaker for a fresh openDuration with no threshold
// re-accumulation (circuit_breaker.go type doc). Each leg also asserts the
// duckdb_circuit_breaker_transition_total samples it emitted (#634).
func TestCircuitBreaker_OpensAtThresholdAndRecovers(t *testing.T) {
	const threshold = 3
	const cooldown = 5 * time.Second

	ctx := context.Background()
	transitions := &breakerTransitions{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(threshold, cooldown), WithMetricEmitter(transitions))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	// Healthy precondition binds the engine — and the breaker the Env caches
	// across DuckDB rebuilds — before the client is closed.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	transitions.expect(t, "healthy precondition")

	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client: %v", err)
	}

	// Scenario 1: the first `threshold` queries reach DuckDB and fail for
	// real; none of them may be breaker rejections.
	for i := 0; i < threshold; i++ {
		_, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
		if err == nil {
			t.Fatalf("failure %d/%d: expected error with duckdb closed, got success", i+1, threshold)
		}
		if strings.Contains(err.Error(), breakerRejection) {
			t.Fatalf("failure %d/%d: breaker opened before the threshold: %v", i+1, threshold, err)
		}
	}
	transitions.expect(t, "threshold reached", "open")
	// ...and the next query is rejected by the breaker, not by DuckDB.
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil || !strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("query after threshold: want breaker rejection, got: %v", err)
	}

	// DuckDB is healthy again, but openDuration has not expired: rejection
	// must come from breaker state, not from the dead client.
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil || !strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("query while open with healthy duckdb: want breaker rejection, got: %v", err)
	}
	transitions.expect(t, "rejections while open")

	// Scenario 2: after openDuration the first query is admitted as the
	// single probe, succeeds, and closes the breaker (oracle-checked result).
	time.Sleep(cooldown + time.Second)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	transitions.expect(t, "successful probe", "half_open", "closed")

	// Scenario 3: the success cleared the failure history — a single fresh
	// failure must NOT reopen the breaker (no half-open accumulation).
	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client again: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil {
		t.Fatal("expected one real failure after re-close, got success")
	} else if strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("single failure after recovery reopened the breaker (history not cleared): %v", err)
	}
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb after single failure: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	transitions.expect(t, "single failure below threshold")

	// Scenario 4 (#246): a failed probe re-opens the breaker for a fresh
	// openDuration without threshold re-accumulation.
	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client for probe-failure leg: %v", err)
	}
	for i := 0; i < threshold; i++ {
		if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil {
			t.Fatalf("probe-failure leg failure %d/%d: expected error with duckdb closed, got success", i+1, threshold)
		} else if strings.Contains(err.Error(), breakerRejection) {
			t.Fatalf("probe-failure leg failure %d/%d: breaker opened before the threshold: %v", i+1, threshold, err)
		}
	}
	transitions.expect(t, "threshold reached again", "open")
	// Breaker is open again; wait out the cooldown with DuckDB STILL closed.
	time.Sleep(cooldown + time.Second)
	// The single admitted probe reaches the dead client and fails for real...
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil {
		t.Fatal("probe against closed duckdb: expected real failure, got success")
	} else if strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("probe against closed duckdb: want a real failure, got breaker rejection: %v", err)
	}
	// ...and that SINGLE failure re-opens the breaker immediately — no
	// threshold re-accumulation while half-open.
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil || !strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("query after failed probe: want breaker rejection, got: %v", err)
	}
	transitions.expect(t, "failed probe", "half_open", "open")
	// Recovery: healthy client + a fresh expired openDuration → the next
	// probe succeeds and closes the breaker.
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb after failed probe: %v", err)
	}
	time.Sleep(cooldown + time.Second)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	transitions.expect(t, "recovery after failed probe", "half_open", "closed")
}

// TestCircuitBreaker_ConcurrentTransitions is #185 breaker scenario 4 under
// the #246 single-probe semantics: goroutines hammer the engine while the
// breaker trips, and again after it recovers. During the outage every
// outcome must be one of the two legal failures — a real DuckDB error or a
// breaker rejection, never a success or a panic. At recovery a concurrent
// burst gets exactly one admitted probe: the only legal outcomes are
// success (the probe, plus stragglers arriving after it closed the breaker)
// or a breaker rejection; once the probe has closed the breaker a full
// concurrent burst must succeed. Engine calls go through eng.Query
// directly: Env.Query records results without a lock and is not safe for
// concurrent use.
//
// Runs on the harness-default DuckDB pool (2 connections): the final
// all-succeed burst is the regression gate for #245 — per-connection init
// must configure every pooled connection, or one of the concurrent queries
// 404s with an empty s3_region.
func TestCircuitBreaker_ConcurrentTransitions(t *testing.T) {
	const threshold = 3
	const cooldown = 3 * time.Second
	const workers = 8
	const queriesPerWorker = 5

	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithBreaker(threshold, cooldown))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})

	eng := env.Engine() // lazily built once, before any concurrency
	runOne := func() error {
		fq, err := env.buildFederatedQuery(Query{Schema: wide, Limit: 20})
		if err != nil {
			return fmt.Errorf("build federated query: %w", err)
		}
		if _, err := eng.Query(ctx, env.Tables, fq, &model.FederatedQueryOptions{}); err != nil {
			return fmt.Errorf("engine query: %w", err)
		}
		return nil
	}

	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client: %v", err)
	}

	// Outage phase: every outcome must be a real failure or a breaker
	// rejection — never a success.
	succeeded, rejected, real := classifyBurstOutcomes(runConcurrentBurst(workers, queriesPerWorker, runOne))
	if succeeded > 0 {
		t.Errorf("%d queries succeeded while duckdb was closed", succeeded)
	}
	if rejected == 0 {
		t.Errorf("no breaker rejections across %d concurrent queries (real failures: %d)", workers*queriesPerWorker, len(real))
	}
	t.Logf("open phase: %d real failures, %d breaker rejections", len(real), rejected)

	// Recovery under single-probe (#246): after openDuration a concurrent
	// burst yields exactly one admitted probe; the rest are rejected until
	// the probe's success closes the breaker. With a healthy client the
	// only legal outcomes are success or breaker rejection.
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb: %v", err)
	}
	eng = env.Engine()
	time.Sleep(cooldown + time.Second)

	succeeded, rejected, real = classifyBurstOutcomes(runConcurrentBurst(workers, 1, runOne))
	for _, err := range real {
		t.Errorf("recovery burst: want success or breaker rejection, got: %v", err)
	}
	if succeeded == 0 {
		t.Errorf("recovery burst: no worker succeeded (breaker rejections: %d)", rejected)
	}
	t.Logf("recovery burst: %d succeeded, %d rejected while the probe was in flight", succeeded, rejected)

	// The probe has closed the breaker: a full concurrent burst must now
	// succeed (this is the #245 pooled-connection regression gate).
	for _, err := range runConcurrentBurst(workers, 1, runOne) {
		if err != nil {
			t.Errorf("post-recovery concurrent query failed: %v", err)
		}
	}
}

// runConcurrentBurst fans runOne out over workers goroutines, each calling
// it queriesPerWorker times, and returns every outcome after all goroutines
// have terminated.
func runConcurrentBurst(workers, queriesPerWorker int, runOne func() error) []error {
	errCh := make(chan error, workers*queriesPerWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < queriesPerWorker; i++ {
				errCh <- runOne()
			}
		}()
	}
	wg.Wait()
	close(errCh)

	outcomes := make([]error, 0, workers*queriesPerWorker)
	for err := range errCh {
		outcomes = append(outcomes, err)
	}
	return outcomes
}

// classifyBurstOutcomes splits burst outcomes into successes, breaker
// rejections, and the remaining real errors.
func classifyBurstOutcomes(outcomes []error) (succeeded, rejected int, real []error) {
	for _, err := range outcomes {
		switch {
		case err == nil:
			succeeded++
		case strings.Contains(err.Error(), breakerRejection):
			rejected++
		default:
			real = append(real, err)
		}
	}
	return succeeded, rejected, real
}
