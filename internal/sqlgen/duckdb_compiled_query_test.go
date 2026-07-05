package sqlgen

import (
	"testing"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func compiledParityParams() map[string]any {
	return map[string]any{
		"EAVTable":       "eav_t",
		"MainTable":      "main_t",
		"ChangeLogTable": "cl_t",
		"SchemaID":       int16(7),
		"Anchor":         map[string]any{"Condition": "1=1"},
		"Limit":          25,
		"Offset":         0,
		"PageSize":       25,
	}
}

// requireCompiledParity is the #142 phase-5 contract: Compile+Bind must
// reproduce BuildDuckDBQuery byte-for-byte (SQL) and deep-equal (args) for
// the production advanced+dual path.
func requireCompiledParity(t *testing.T, q *model.FederatedAttributeQuery, dual DualClauses, dirtyIDs []uuid.UUID) {
	t.Helper()

	wantSQL, wantArgs, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), q, dirtyIDs, &dual)
	require.NoError(t, err)

	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), q, &dual, len(dirtyIDs) > 0)
	require.NoError(t, err)
	require.NotNil(t, compiled)

	gotSQL, gotArgs := compiled.Bind(q, dual, dirtyIDs)
	require.Equal(t, wantSQL, gotSQL)
	require.Equal(t, wantArgs, gotArgs)
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

	cases := map[string]struct {
		q     *model.FederatedAttributeQuery
		dirty []uuid.UUID
	}{
		"plain":            {&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}, nil},
		"with dirty ids":   {&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}, dirty},
		"with keyset":      {keysetQ, nil},
		"keyset and dirty": {keysetQ, dirty},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			requireCompiledParity(t, tc.q, parityDual(t, tc.q.Condition, cache), tc.dirty)
		})
	}
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
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), qA, &dualA, true)
	require.NoError(t, err)

	// Bind request B through the plan compiled from request A's shape.
	plan, err := PlanDualClauses(qB.Condition, "eav_t", 7, cache, 0)
	require.NoError(t, err)
	dualB, err := plan.Bind(qB.Condition, cache, nil)
	require.NoError(t, err)

	gotSQL, gotArgs := compiled.Bind(qB, dualB, dirtyB)
	wantSQL, wantArgs, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), qB, dirtyB, &dualB)
	require.NoError(t, err)
	require.Equal(t, wantSQL, gotSQL, "request B served from request A's skeleton must equal the direct build")
	require.Equal(t, wantArgs, gotArgs)
}

// TestCompileDuckDBQueryRejectsNonCacheablePaths pins the fallback contract.
func TestCompileDuckDBQueryRejectsNonCacheablePaths(t *testing.T) {
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}
	empty := DualClauses{}
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), q, &empty, false)
	require.NoError(t, err)
	require.Nil(t, compiled, "empty dual clause is not cacheable")

	dual := DualClauses{DuckClause: "1=1"}
	generic := template.Must(template.New("generic").Parse("SELECT 1 WHERE {{.Anchor.Condition}}"))
	compiled, err = CompileDuckDBQuery(generic, compiledParityParams(), q, &dual, false)
	require.NoError(t, err)
	require.Nil(t, compiled, "non-advanced templates are not cacheable")
}
