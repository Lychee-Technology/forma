package sqlgen

import (
	"fmt"
	"testing"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func compiledParityParams(t *testing.T) map[string]any {
	t.Helper()
	return injectTestRenderParams(t, map[string]any{
		"EAVTable":       "eav_t",
		"MainTable":      "main_t",
		"ChangeLogTable": "cl_t",
		"SchemaID":       int16(7),
		"Anchor":         map[string]any{"Condition": "1=1"},
		"Limit":          25,
		"Offset":         0,
		"PageSize":       25,
	}, 7)
}

// coldParityParams is compiledParityParams carrying a #255 cold-missing set,
// so the scan-source augmentation participates in the parity contract.
func coldParityParams(t *testing.T, missing []ScanColumn) map[string]any {
	t.Helper()
	params := compiledParityParams(t)
	if len(missing) > 0 {
		params["ColdMissingColumns"] = missing
	}
	return params
}

// requireCompiledParity is the #142 phase-5 contract: Compile+Bind must
// reproduce BuildDuckDBQuery byte-for-byte (SQL) and deep-equal (args) for
// the production advanced+dual path. missing carries the #255 cold-missing
// set (nil for the unaugmented shapes).
func requireCompiledParity(t *testing.T, q *model.FederatedAttributeQuery, dual DualClauses, dirtyIDs []uuid.UUID, missing []ScanColumn) {
	t.Helper()

	wantSQL, wantArgs, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, coldParityParams(t, missing), q, dirtyIDs, &dual)
	require.NoError(t, err)

	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, coldParityParams(t, missing), q, &dual, len(dirtyIDs) > 0)
	require.NoError(t, err)
	require.NotNil(t, compiled)

	gotSQL, gotArgs, err := compiled.Bind(q, dual, dirtyIDs, FlushGraceCutoffDisabled)
	require.NoError(t, err)
	require.Equal(t, wantSQL, gotSQL)
	require.Equal(t, wantArgs, gotArgs)

	// Non-vacuity: a cold-missing case must actually render the augmentation,
	// otherwise the parity assertion above would hold trivially.
	for _, mc := range missing {
		require.Contains(t, gotSQL, fmt.Sprintf("NULL::%s AS %s", mc.DuckDBType, mc.Name))
	}
}

func parityDual(t *testing.T, cond forma.Condition, cache forma.SchemaAttributeCache) DualClauses {
	t.Helper()
	idx := 0
	dc, err := ToDualClauses(cond, "eav_t", 7, cache, &idx)
	require.NoError(t, err)
	return dc
}

func TestCompiledQueryParity(t *testing.T) {
	cache := dualPlanTestCache()
	mixed := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},
		&forma.KvCondition{Attr: "tag", Value: "equals:x"},
	}}
	dirty := []uuid.UUID{uuid.New(), uuid.New()}
	keysetQ := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}
	keysetQ.KeysetCursor = &model.KeysetCursor{
		Mode:    model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: forma.SortOrderDesc}},
		Values:  []any{int64(1700000000000), "11111111-1111-1111-1111-111111111111"},
	}

	coldOnlyQ := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed},
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	}

	cases := map[string]struct {
		q       *model.FederatedAttributeQuery
		dirty   []uuid.UUID
		missing []ScanColumn
	}{
		"plain":            {&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}, nil, nil},
		"with dirty ids":   {&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}, dirty, nil},
		"with keyset":      {keysetQ, nil, nil},
		"keyset and dirty": {keysetQ, dirty, nil},
		"cold only":        {coldOnlyQ, dirty, nil},
		// #255: the augmented scan source must render identically on the
		// compiled skeleton and the direct build, or a cache-served request
		// would scan a different shape than the one it compiled.
		"cold missing columns": {
			&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}},
			dirty,
			[]ScanColumn{{Name: "score", DuckDBType: "INTEGER"}, {Name: "tags", DuckDBType: "BIGINT[]"}},
		},
		// The augmentation must survive the two shapes that rewrite the
		// surrounding SQL: the keyset predicate (appended after the logical
		// clause, so placeholder order is at stake) and the hot-excluded tier
		// form (which drops the pg_source CTE and its PgMainArgs occurrence).
		"keyset and cold missing": {
			keysetQ,
			dirty,
			[]ScanColumn{{Name: "score", DuckDBType: "INTEGER"}},
		},
		"cold only and cold missing": {
			coldOnlyQ,
			dirty,
			[]ScanColumn{{Name: "score", DuckDBType: "INTEGER"}, {Name: "tags", DuckDBType: "BIGINT[]"}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			requireCompiledParity(t, tc.q, parityDual(t, tc.q.Condition, cache), tc.dirty, tc.missing)
		})
	}
}

// TestCompiledQueryColdOnlyParityOmitsPgSource pins the #184 tier form on the
// compiled path: a hot-excluded shape must compile to a skeleton without the
// pg_source CTE, and Bind must drop the PgMainArgs occurrence so the bind
// list stays aligned with the surviving placeholders.
func TestCompiledQueryColdOnlyParityOmitsPgSource(t *testing.T) {
	cache := dualPlanTestCache()
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: 7,
			Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "age", Value: "gt:10"},
				&forma.KvCondition{Attr: "tag", Value: "equals:x"},
			}},
		},
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	}
	dual := parityDual(t, q.Condition, cache)
	require.NotEmpty(t, dual.PgMainArgs,
		"precondition: the shape must carry pushdown args for the drop to be observable")

	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(t), q, &dual, false)
	require.NoError(t, err)
	require.NotNil(t, compiled)

	gotSQL, gotArgs, err := compiled.Bind(q, dual, nil, FlushGraceCutoffDisabled)
	require.NoError(t, err)
	require.NotContains(t, gotSQL, "pg_source",
		"hot excluded: the compiled skeleton must omit the pg_source CTE")
	wantArgs := append(append([]any{}, dual.DuckArgs...), dual.DuckArgs...)
	require.Equal(t, wantArgs, gotArgs,
		"hot excluded: binds are the two DuckArgs occurrences only")
}

// TestCompiledQueryReuseAcrossRequests pins the cache semantics: one compile,
// two binds with different operands, dirty sets, and cursor values.
func TestCompiledQueryReuseAcrossRequests(t *testing.T) {
	cache := dualPlanTestCache()
	shape := func(age, tag string) *model.FederatedAttributeQuery {
		return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
			SchemaID: 7,
			Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "age", Value: "gt:" + age},
				&forma.KvCondition{Attr: "tag", Value: "equals:" + tag},
			}},
		}}
	}
	qA, qB := shape("10", "x"), shape("77", "zzz")
	dirtyB := []uuid.UUID{uuid.New()}

	dualA := parityDual(t, qA.Condition, cache)
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(t), qA, &dualA, true)
	require.NoError(t, err)

	// Bind request B through the plan compiled from request A's shape.
	plan, err := PlanDualClauses(qB.Condition, "eav_t", 7, cache, 0)
	require.NoError(t, err)
	dualB, err := plan.Bind(qB.Condition, cache, nil)
	require.NoError(t, err)

	gotSQL, gotArgs, err := compiled.Bind(qB, dualB, dirtyB, FlushGraceCutoffDisabled)
	require.NoError(t, err)
	wantSQL, wantArgs, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(t), qB, dirtyB, &dualB)
	require.NoError(t, err)
	require.Equal(t, wantSQL, gotSQL, "request B served from request A's skeleton must equal the direct build")
	require.Equal(t, wantArgs, gotArgs)
}

// TestCompileDuckDBQueryRejectsNonCacheablePaths pins the fallback contract.
func TestCompileDuckDBQueryRejectsNonCacheablePaths(t *testing.T) {
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}
	empty := DualClauses{}
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(t), q, &empty, false)
	require.NoError(t, err)
	require.Nil(t, compiled, "empty dual clause is not cacheable")

	dual := DualClauses{DuckClause: "1=1"}
	generic := template.Must(template.New("generic").Parse("SELECT 1 WHERE {{.Anchor.Condition}}"))
	compiled, err = CompileDuckDBQuery(generic, compiledParityParams(t), q, &dual, false)
	require.NoError(t, err)
	require.Nil(t, compiled, "non-advanced templates are not cacheable")
}

// TestCompiledQueryBindRejectsMisalignedCursor pins the error path #381 item 7
// gave Bind. The plan cache's shape hash covers cursor COLUMNS but deliberately
// not cursor VALUES (internal/queryplan/shape.go, pinned by
// TestShapeHashKeysetValuesDoNotParticipate), so a cached skeleton really can be
// handed a cursor whose values no longer align with its columns. Binding SQL
// NULL for the unfilled arm would answer a silently empty page; Bind must fail
// instead, and must return nothing an unchecked caller could execute.
func TestCompiledQueryBindRejectsMisalignedCursor(t *testing.T) {
	cache := dualPlanTestCache()
	cols := []model.KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "row_id", Direction: forma.SortOrderDesc},
	}
	newQuery := func(values []any) *model.FederatedAttributeQuery {
		q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
			SchemaID: 7,
			Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "age", Value: "gt:10"},
				&forma.KvCondition{Attr: "tag", Value: "equals:x"},
			}},
		}}
		q.KeysetCursor = &model.KeysetCursor{Mode: model.KeysetCursorModeAfter, Columns: cols, Values: values}
		return q
	}

	// Compile from a well-formed cursor: the skeleton is the one a cache hit
	// would serve.
	wellFormed := newQuery([]any{int64(1700000000000), "11111111-1111-1111-1111-111111111111"})
	dual := parityDual(t, wellFormed.Condition, cache)
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(t), wellFormed, &dual, false)
	require.NoError(t, err)
	require.NotNil(t, compiled)

	// Same columns, one value short — the shape the values-blind hash admits.
	sql, args, err := compiled.Bind(newQuery([]any{int64(1700000000000)}), dual, nil, FlushGraceCutoffDisabled)
	require.Error(t, err, "a cursor whose values do not align with its columns must not bind")
	require.Contains(t, err.Error(), "carries 2 column(s) but 1 value(s)",
		"the failure must name the counts, as model.KeysetCursor.ValidateShape does")
	require.Empty(t, sql, "a failed Bind must return no executable SQL")
	require.Empty(t, args, "a failed Bind must return no args")
}
