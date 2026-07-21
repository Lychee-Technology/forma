package sqlgen

import (
	"strconv"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// Pins for the #252 read-side grace: in the HasHot form the flushed_at
// barrier widens to (flushed_at = 0 OR flushed_at > cutoff) at BOTH
// change_log scan sites (cutoff = the query's path-resolution instant minus
// the clock-skew margin), hot-excluded shapes keep the strict barrier, the
// cutoff stays out of the compiled skeleton (sentinel splice, cache-key
// stable), and every render path supplies a value — a missing cutoff must
// fall back to FlushGraceCutoffDisabled, never to 0 (which would pull the
// whole flushed history back to hot) or to empty (invalid SQL).

func flushGraceParams(t *testing.T, cutoff any) map[string]any {
	t.Helper()
	m := map[string]any{
		"PG_CONN":              "dbname=forma host=localhost",
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"S3_PATHS":             "['s3://bucket/base/*.parquet']",
		"LOGICAL_WHERE_CLAUSE": "1=1",
	}
	if cutoff != nil {
		m["FlushGraceCutoffMs"] = cutoff
	}
	return injectTestProjection(t, m, 1)
}

func flushGraceQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
	}
}

// requireGraceSites asserts the widened predicate renders at exactly the two
// change_log sites with the given cutoff literal.
func requireGraceSites(t *testing.T, sqlText, cutoff string) {
	t.Helper()
	require.Contains(t, sqlText, "AND (flushed_at = 0 OR flushed_at > "+cutoff+")",
		"dirty_ids must render the widened barrier with the cutoff")
	require.Contains(t, sqlText, "AND (cl.flushed_at = 0 OR cl.flushed_at > "+cutoff+")",
		"pg_source must render the widened barrier with the cutoff")
	require.Equal(t, 2, strings.Count(sqlText, "flushed_at = 0 OR"),
		"the widened barrier must appear at exactly the two change_log sites")
}

// TestFlushGraceCutoffRendersAtBothSites pins the direct (dual-clause) path
// with an engine-supplied cutoff.
func TestFlushGraceCutoffRendersAtBothSites(t *testing.T) {
	const cutoff = int64(1_752_900_000_000)
	dual := &DualClauses{DuckClause: "1=1"}
	sqlText, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		flushGraceParams(t, cutoff), flushGraceQuery(), nil, dual)
	require.NoError(t, err)
	requireGraceSites(t, sqlText, strconv.FormatInt(cutoff, 10))
}

// TestFlushGraceHotExcludedKeepsStrictBarrier pins the HasHot gate: with
// pg_source pruned there is no hot server for grace-discarded rows, so the
// dirty CTE must keep the strict flushed_at = 0 predicate.
func TestFlushGraceHotExcludedKeepsStrictBarrier(t *testing.T) {
	q := flushGraceQuery()
	q.PreferredTiers = []model.DataTier{model.DataTierWarm, model.DataTierCold}
	sqlText, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		flushGraceParams(t, int64(1_752_900_000_000)), q, nil, &DualClauses{DuckClause: "1=1"})
	require.NoError(t, err)
	require.Contains(t, sqlText, "AND (flushed_at = 0)")
	require.NotContains(t, sqlText, "OR flushed_at > ")
}

// TestFlushGraceCutoffDefaultsToDisabled pins the defensive default on both
// the dual and the legacy (dual == nil) render paths: an absent cutoff must
// render FlushGraceCutoffDisabled.
func TestFlushGraceCutoffDefaultsToDisabled(t *testing.T) {
	disabled := strconv.FormatInt(FlushGraceCutoffDisabled, 10)

	dualSQL, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		flushGraceParams(t, nil), flushGraceQuery(), nil, &DualClauses{DuckClause: "1=1"})
	require.NoError(t, err)
	requireGraceSites(t, dualSQL, disabled)

	legacySQL, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		flushGraceParams(t, nil), flushGraceQuery(), nil, nil)
	require.NoError(t, err)
	requireGraceSites(t, legacySQL, disabled)
}

// TestFlushGraceCompiledSkeletonIsCutoffIndependent pins the plan-cache
// contract: the skeleton carries the sentinel (identical across requests with
// different cutoffs), and Bind splices the per-request value at both sites.
func TestFlushGraceCompiledSkeletonIsCutoffIndependent(t *testing.T) {
	cache := dualPlanTestCache()
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID: 7,
		Condition: &forma.KvCondition{Attr: "age", Value: "gt:10"},
	}}
	idx := 0
	dual, err := ToDualClauses(q.Condition, "eav_t", 7, cache, &idx)
	require.NoError(t, err)

	// The engine's per-request param map carries a real cutoff; Compile must
	// still bake the sentinel, not the value.
	params := injectTestProjection(t, map[string]any{
		"EAVTable":           "eav_t",
		"MainTable":          "main_t",
		"ChangeLogTable":     "cl_t",
		"ChangeLogSchema":    "public",
		"ChangeLogScanTable": "cl_t",
		"MainSchema":         "public",
		"MainScanTable":      "main_t",
		"EAVSchema":          "public",
		"EAVScanTable":       "eav_t",
		"SchemaID":           int16(7),
		"Anchor":             map[string]any{"Condition": "1=1"},
		"Limit":              25,
		"Offset":             0,
		"PageSize":           25,
		"FlushGraceCutoffMs": int64(111),
	}, 7)
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, &dual, false)
	require.NoError(t, err)
	require.NotNil(t, compiled)
	require.Contains(t, compiled.Skeleton, flushGraceCutoffSentinel,
		"the skeleton must carry the sentinel so the cache key is cutoff-independent")
	require.NotContains(t, compiled.Skeleton, "flushed_at > 111",
		"a per-request cutoff must never bake into the skeleton")

	sqlA, argsA := compiled.Bind(q, dual, nil, 1000)
	sqlB, argsB := compiled.Bind(q, dual, nil, 2000)
	requireGraceSites(t, sqlA, "1000")
	requireGraceSites(t, sqlB, "2000")
	require.NotContains(t, sqlA, flushGraceCutoffSentinel)
	require.NotContains(t, sqlB, flushGraceCutoffSentinel)
	require.Equal(t, argsA, argsB,
		"the cutoff is a splice, not a bind arg: the interleave must not change")
}
