package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// degradedPlanCase stages one point at which a DuckDB-routed request can fail
// and be absorbed by the degraded fallback.
type degradedPlanCase struct {
	name  string
	setup func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery)
	// cause is a substring of the absorbed error, which the fallback's own
	// note must carry.
	cause string
}

func degradedPlanCases() []degradedPlanCase {
	return []degradedPlanCase{
		{
			// Rejected before any DuckDB work: recordClientUnavailable still
			// writes duckdb_fetch and total.
			name: "breaker rejection",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
				breaker.RecordFailure()
				require.True(t, breaker.IsOpen(), "precondition: the breaker must be open")
				return newSentinelTestEngine(t, &fakeDuckDBExecutor{}, breaker), coldTierQuery()
			},
			cause: "duckdb circuit breaker open",
		},
		{
			// The shape of the #636 review's reproduction ([hot, cold],
			// Limit 2000): the dirty-ID, pushdown-fragment and duckdb sources
			// and translate/duckdb_fetch/total are recorded before duck.Query
			// fails.
			name: "duck.Query failure",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
				return newSentinelTestEngine(t, duck, nil), coldTierQuery()
			},
			cause: "forced duck failure",
		},
		{
			// The page pass succeeds and records a whole DuckDB pass; its page
			// is discarded when the recount fails.
			name: "recount failure after a successful page pass",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &sequencedDuckDBExecutor{
					rows: []duckDBRowsIterator{&emptyDuckDBRows{}},
					errs: []error{nil, fmt.Errorf("forced recount failure")},
				}
				var built []model.FederatedAttributeQuery
				engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck, &built)
				return engine, &model.FederatedAttributeQuery{
					AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 50},
					PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
				}
			},
			cause: "forced recount failure",
		},
		{
			// The #251 retry rewinds the first pass to its own mark; the
			// retry pass then fails too and must not survive either.
			name: "corrupt-parquet retry failure",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &retryFakeDuck{passes: []retryPass{
					{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
					{midStreamFail: true, drainFails: []string{retryCorruptPath2}},
				}}
				engine := newRetryEngine(t, duck,
					[]string{retryKeptPathA, retryKeptPathB, retryCorruptPath1, retryCorruptPath2})
				return engine, coldTierQuery()
			},
			cause: "retry after excluding corrupt parquet",
		},
	}
}

// TestDegradedFallbackPlanDropsTheAbandonedAttempt pins #639: when degraded
// mode absorbs a DuckDB-path failure, the returned plan describes the
// Postgres-only answer alone. Sources and Timings cross the HTTP boundary
// while Notes do not, so a duckdb source or DuckDB timings left by the failed
// attempt would sit next to used_duckdb=false with nothing public to say they
// describe the attempt rather than the answer.
func TestDegradedFallbackPlanDropsTheAbandonedAttempt(t *testing.T) {
	for _, tc := range degradedPlanCases() {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			engine, fq := tc.setup(t)
			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true, IncludeExecutionPlan: true}
			page, err := engine.Query(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, fq, opts)
			require.NoError(t, err, "degraded mode must absorb the failure")

			plan := page.ExecutionPlan
			require.NotNil(t, plan, "the degraded fallback must stitch the plan onto the page")
			require.Same(t, opts.ExecutionPlan, plan)
			require.False(t, plan.Routing.UseDuckDB)
			require.Contains(t, plan.Routing.Reason, "degraded fallback")

			require.Len(t, plan.Sources, 1, "only the fallback's source may survive; sources: %+v", plan.Sources)
			require.False(t, planHasDuckDBSource(plan), "no duckdb source may survive; sources: %+v", plan.Sources)
			requirePlanHasPostgresSource(t, plan, "degraded fallback")
			require.Empty(t, plan.Timings, "the abandoned attempt's timings must not survive")

			// The routing note predates the attempt and belongs to the caller.
			require.Contains(t, plan.Notes, "EvaluateRoutingPolicy")
			// The rewind runs before the fallback records its cause, so the
			// cause is the last note rather than a casualty of the rewind.
			require.NotEmpty(t, plan.Notes)
			last := plan.Notes[len(plan.Notes)-1]
			require.True(t, strings.HasPrefix(last, "degraded fallback to postgres-only: "), "last note: %q", last)
			require.Contains(t, last, tc.cause)
		})
	}
}

func planHasDuckDBSource(plan *model.ExecutionPlan) bool {
	for _, src := range plan.Sources {
		if src.Engine == "duckdb" {
			return true
		}
	}
	return false
}
