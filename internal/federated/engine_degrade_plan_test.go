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
	// breakerRejected marks the case the open breaker turns away before any
	// DuckDB work; its fallback line is logged at Debug (#638).
	breakerRejected bool
	// metricCause is the cause label the fallback's
	// federated_degraded_fallback_total sample must carry (#714).
	metricCause string
}

// degradedPlanCases is the shared table of degrade points the plan, log and
// metric tests (#639, #638, #714) drive through DBFederatedQueryEngine.Query.
func degradedPlanCases() []degradedPlanCase {
	cases := duckDBPassFailureCases()
	cases = append(cases, preDuckDBReadFailureCases()...)
	return append(cases, objectRefusalCases()...)
}

// duckDBPassFailureCases fail the DuckDB pass itself: the breaker turns the
// request away, there is no client, or a statement of the pass fails.
func duckDBPassFailureCases() []degradedPlanCase {
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
			cause:           "duckdb circuit breaker open",
			breakerRejected: true,
			metricCause:     degradeCauseBreakerRejected,
		},
		{
			// The shape of the #636 review's reproduction ([hot, cold],
			// Limit 2000): the dirty-set source is recorded before the parquet
			// leg's statement fails.
			name: "duck.Query failure",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
				return newSentinelTestEngine(t, duck, nil), coldTierQuery()
			},
			cause:       "forced duck failure",
			metricCause: degradeCauseReadFailed,
		},
		{
			// The page's statement succeeds; its page is discarded when the
			// pass's parquet recount (#181) fails.
			name: "recount failure after a successful page pass",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &sequencedDuckDBExecutor{
					rows: []duckDBRowsIterator{&emptyDuckDBRows{}},
					errs: []error{nil, fmt.Errorf("forced recount failure")},
				}
				engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck)
				return engine, &model.FederatedAttributeQuery{
					AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 50},
					PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
				}
			},
			cause:       "forced recount failure",
			metricCause: degradeCauseReadFailed,
		},
		{
			// Without a client the pass fails before the breaker is
			// consulted.
			name: "duckdb client unavailable",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				return newSentinelTestEngine(t, nil, nil), coldTierQuery()
			},
			cause:       "duckdb client not available",
			metricCause: degradeCauseDuckDBUnavailable,
		},
	}
}

// preDuckDBReadFailureCases fail a read the pass makes before DuckDB runs, so
// the failure never counts toward the breaker (#714).
func preDuckDBReadFailureCases() []degradedPlanCase {
	return []degradedPlanCase{
		{
			// The manifest read fails before DuckDB runs, so the failure
			// never counts toward the breaker (#714).
			name: "manifest read failure",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				src := &fakeParquetSource{pathsErr: fmt.Errorf("forced manifest read failure")}
				return newParquetSourceTestEngine(t, &fakeDuckDBExecutor{}, src), coldTierQuery()
			},
			cause:       "forced manifest read failure",
			metricCause: degradeCauseReadFailed,
		},
		{
			// The dirty-ID fetch in the pass's snapshot fails before DuckDB
			// runs (#714).
			name: "dirty-ID fetch failure",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				leg := &fakeHotLeg{changeLogErr: fmt.Errorf("forced dirty-ID failure")}
				return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDuckDBExecutor{}, nil,
					hybridDuckConfig(), testMetadataCacheSchema7(t), withTestParquetPath(), withFakeHotLeg(leg)), coldTierQuery()
			},
			cause:       "forced dirty-ID failure",
			metricCause: degradeCausePostgresReadFailed,
		},
	}
}

// objectRefusalCases refuse a specific Parquet object: confirmed or cached
// corruption (#722) and a scan-guard violation (#351).
func objectRefusalCases() []degradedPlanCase {
	return []degradedPlanCase{
		{
			// Confirmed corruption fails its pass after the dirty-set source
			// is recorded (#722).
			name: "confirmed corrupt object",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				duck := &retryFakeDuck{passes: []retryPass{
					{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
				}}
				return newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1}), coldTierQuery()
			},
			cause:       "confirmed corrupt",
			metricCause: degradeCauseCorruptParquet,
		},
		{
			// A cached verdict refuses at path resolution, before any source
			// is recorded (#722).
			name: "cached corrupt object",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				engine := newRetryEngine(t, &retryFakeDuck{}, []string{retryKeptPathA, retryCorruptPath1})
				engine.corruptPaths.Add([]string{retryCorruptPath1})
				return engine, coldTierQuery()
			},
			cause:       "still cached",
			metricCause: degradeCauseCorruptParquet,
		},
		{
			// The guarded per-file drain names the object the set scan
			// failed on (#351).
			name: "scan-guard violation",
			setup: func(t *testing.T) (*DBFederatedQueryEngine, *model.FederatedAttributeQuery) {
				return newGuardSeamEngine(t, &guardSeamFakeDuck{rogue: guardRoguePath},
					&fakeParquetSource{paths: []string{guardHealthyPath, guardRoguePath}}), coldTierQuery()
			},
			cause:       guardRoguePath,
			metricCause: degradeCauseScanGuardViolation,
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
