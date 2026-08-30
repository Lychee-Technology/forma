package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
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
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT",
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
	return runColdPlanCacheQueryWithHint(t, e, duck, "")
}

// runColdPlanCacheQueryWithHint is the same request with an optional explicit
// S3ParquetPathTemplate hint (which always wins over the parquet source), so
// the glob variant below can drive the identical query shape over a glob path
// set.
func runColdPlanCacheQueryWithHint(
	t *testing.T, e *DBFederatedQueryEngine, duck *fakeDuckDBExecutor, pathHint string,
) (string, []string) {
	t.Helper()
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID:  7,
		Condition: &forma.KvCondition{Attr: "score", Value: "gt:50"},
		Limit:     2000,
	}}
	q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierCold}
	if pathHint != "" {
		q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: pathHint}
	}
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
		hybridDuckConfig(),
		coldPlanCacheMetadata(t), "host=x",
		WithPlanCache(queryplan.NewCache(64)),
		WithParquetSource(&fakeParquetSource{paths: []string{coldPlanCachePath}}))

	// Pre-flush: the validator's write-once cache holds the v1 footer, so the
	// probe never reaches the fake executor and the union is complete.
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(false), nil)

	sql1, notes1 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, sql1, "NULL::DOUBLE AS score",
		"cold-absent attribute must render as a typed NULL in the scan source")
	require.Contains(t, notes1, "plan_cache=miss", "first request compiles")

	sql2, notes2 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, notes2, "plan_cache=hit",
		"same shape, same paths, same missing set: the skeleton must be reused — this is the reuse that could poison")
	require.Contains(t, sql2, "NULL::DOUBLE AS score")

	// The first flush lands `score`. Overwriting the cached footer is a TEST
	// STAND-IN for "a new file's columns joined the union": flush and
	// compaction always mint fresh keys, so their footers join the union
	// through a NEW path rather than by changing this one. (An init rerun does
	// overwrite in place — that case is re-keyed by the manifest stamp, see
	// parquet_schema_validation_stamps_test.go.) The realistic production
	// trigger — a glob expansion that gained a file — is the sibling test
	// below; this one isolates the re-key with the path set held constant.
	// Either way the union the next request computes carries the column, and
	// the path set, query shape, tables, limit and fingerprint are unchanged.
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(true), nil)

	sql3, notes3 := runColdPlanCacheQuery(t, e, duck)
	require.Contains(t, notes3, "plan_cache=miss",
		"the missing set alone must re-key the plan cache (#255 poisoning guard)")
	require.NotContains(t, sql3, "NULL::DOUBLE AS score",
		"post-flush the real column must be scanned, not a cached NULL projection")
	require.NotContains(t, sql3, ", NULL::",
		"no missing columns: the scan source carries no typed-NULL augmentation at all")
	require.Contains(t, sql3, sqlgen.ParquetNullRowIDMessage,
		"the row_id guard renders in every state, augmented or not (#256)")
}

// coldPlanCacheGlob is a glob path hint: it is the ENTIRE scan-set string the
// plan-cache scope key sees, and — unlike a concrete manifest listing — it is
// byte-identical before and after a flush lands a new object. That is what
// makes it the production-realistic form of the #255 hazard.
const coldPlanCacheGlob = "s3://b/7/*.parquet"

// coldPlanCacheDescribeRows renders a footer in the DESCRIBE row shape the
// validator scans. The system columns must be present or parquetcheck fails
// the invariant before any of this is reached.
func coldPlanCacheDescribeRows(withScore bool) [][2]string {
	rows := [][2]string{
		{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}, {"ltbase_created_at", "BIGINT"},
		{"age", "INTEGER"},
	}
	if withScore {
		rows = append(rows, [2]string{"score", "INTEGER"})
	}
	return rows
}

// TestEngineColdMissingSetRekeysPlanCacheViaGlobExpansion models the trigger
// #255 actually fires on in production, which the sibling test above can only
// stand in for: parquet objects are write-once, so an already-validated
// footer never changes — the column union grows because the scan set's GLOB
// EXPANSION GAINED A FILE (the first flush minting a new delta object).
//
// The glob string itself — the only path component the plan-cache scope key
// sees — is byte-identical across both runs, and so are the query shape,
// tables, limit, offset and fingerprint. Nothing but the cold-missing set can
// therefore explain the run-2 miss: drop the missing set from
// duckPlanScopeParts and run 2 becomes a hit that keeps projecting NULL over
// the freshly flushed column.
func TestEngineColdMissingSetRekeysPlanCacheViaGlobExpansion(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{
		globFiles: []string{"s3://b/7/base.parquet"},
		describeCols: map[string][][2]string{
			"base.parquet": coldPlanCacheDescribeRows(false),
		},
	}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(),
		coldPlanCacheMetadata(t), "host=x",
		WithPlanCache(queryplan.NewCache(64)))

	// Run 1 — pre-flush: the glob expands to the single v1 base object, whose
	// footer has no `score`.
	sql1, notes1 := runColdPlanCacheQueryWithHint(t, e, duck, coldPlanCacheGlob)
	require.Contains(t, sql1, "NULL::DOUBLE AS score",
		"cold-absent attribute must render as a typed NULL in the scan source")
	require.Contains(t, notes1, "plan_cache=miss", "first request compiles")

	// The first flush mints a NEW object carrying `score`. The already-probed
	// base footer is untouched (write-once) and stays served from the
	// validator's cache; only the expansion changed.
	duck.globFiles = []string{"s3://b/7/base.parquet", "s3://b/7/delta1.parquet"}
	duck.describeCols["delta1.parquet"] = coldPlanCacheDescribeRows(true)

	sql2, notes2 := runColdPlanCacheQueryWithHint(t, e, duck, coldPlanCacheGlob)
	require.Contains(t, notes2, "plan_cache=miss",
		"the missing set ALONE must re-key the plan cache: the glob path string never changed (#255)")
	require.NotContains(t, sql2, "NULL::DOUBLE AS score",
		"post-flush the real column must be scanned, not a cached NULL projection")
	require.NotContains(t, sql2, ", NULL::",
		"no missing columns: the scan source carries no typed-NULL augmentation at all")
	require.Contains(t, sql2, sqlgen.ParquetNullRowIDMessage,
		"the row_id guard renders in every state, augmented or not (#256)")
}
