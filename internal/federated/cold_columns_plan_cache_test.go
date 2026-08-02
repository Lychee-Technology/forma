package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// coldPlanCachePath is the query's ENTIRE scan set, held constant across every
// run below so the path component of the plan-cache scope key cannot move: the
// only thing that changes between runs is the cold-missing set. (In production
// the path set is typically a glob string, which likewise does not change when
// the first flush lands a column — that is exactly why the missing set has to
// scope the key on its own.)
const coldPlanCachePath = "s3://b/7/base.parquet"

// coldPlanCacheFooter is a v1-generation footer: system columns plus the
// already-flushed `age`. `score` — added to the schema before its first flush
// — is absent, which is the #255 condition.
func coldPlanCacheFooter(withScore bool) map[string]string {
	cols := map[string]string{
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT",
		"age": "INTEGER",
	}
	if withScore {
		cols["score"] = "INTEGER"
	}
	return cols
}

func coldPlanCacheMetadata(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("test", 7, forma.SchemaAttributeCache{
		"age": {AttributeID: 6, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"score": {AttributeID: 9, ValueType: forma.ValueTypeInteger},
	}))
	return mc
}

// runColdPlanCacheQuery drives one full engine request and returns the SQL
// handed to DuckDB plus the execution-plan notes (which carry plan_cache=hit /
// plan_cache=miss).
func runColdPlanCacheQuery(t *testing.T, e *DBFederatedQueryEngine, duck *fakeDuckDBExecutor) (string, []string) {
	t.Helper()
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID:  7,
		Condition: &forma.KvCondition{Attr: "score", Value: "gt:50"},
		Limit:     2000,
	}}
	q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierCold}
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
	tables := model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

	duck.rows = &singleDuckDBRow{rowID: uuid.New()}
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tables, q, q.Limit, 0, nil, opts)
	require.NoError(t, err)
	return duck.lastSQL, opts.ExecutionPlan.Notes
}

// TestEngineColdMissingSetRekeysPlanCache is the engine-seam proof for the
// #255 plan-cache poisoning hazard, at the one seam that can actually poison:
// a real plan cache (as factory.go wires in production) serving repeated
// requests of the SAME shape over the SAME path set.
//
// Run 1 compiles a skeleton while `score` is absent from the whole cold set,
// so the scan source is NULL-augmented. Run 2 changes nothing and is a cache
// HIT — that is what makes the hazard real: the skeleton is genuinely reused,
// byte-for-byte, across requests. Run 3 lands the column in the footer union
// (the first flush) with everything else identical; because the cold-missing
// set participates in the scope key, it MISSES and recompiles without the NULL
// alias. Drop the missing set from duckPlanScopeParts and run 3 turns into a
// hit that keeps projecting NULL over real data.
func TestEngineColdMissingSetRekeysPlanCache(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		coldPlanCacheMetadata(t), "host=x",
		WithPlanCache(queryplan.NewCache(64)),
		WithParquetSource(&fakeParquetSource{paths: []string{coldPlanCachePath}}))

	// Pre-flush: the validator's write-once cache holds the v1 footer, so the
	// probe never reaches the fake executor and the union is complete.
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(false))

	sql1, notes1 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, sql1, "NULL::INTEGER AS score",
		"cold-absent attribute must render as a typed NULL in the scan source")
	require.Contains(t, notes1, "plan_cache=miss", "first request compiles")

	sql2, notes2 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, notes2, "plan_cache=hit",
		"same shape, same paths, same missing set: the skeleton must be reused — this is the reuse that could poison")
	require.Contains(t, sql2, "NULL::INTEGER AS score")

	// The first flush lands `score`. markValidated overwrites the entry, so the
	// union the next request computes carries the column; the path set, query
	// shape, tables, limit and fingerprint are all unchanged.
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(true))

	sql3, notes3 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, notes3, "plan_cache=miss",
		"the missing set alone must re-key the plan cache (#255 poisoning guard)")
	require.NotContains(t, sql3, "NULL::INTEGER AS score",
		"post-flush the real column must be scanned, not a cached NULL projection")
	require.NotContains(t, sql3, "AS cold_scan",
		"no missing columns: the scan source reverts to the unaugmented read_parquet form")
}
