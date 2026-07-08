package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestDuckDBMixedComposite_NoPlaceholderStyleMixing is the #161 regression guard.
//
// A composite `main-column AND pure-EAV` filter is rendered into the DuckDB
// federated template. The DuckClause (s3_source / visible WHERE) uses positional
// "?" placeholders; the PgMainClause (pg_source WHERE) historically used "$n".
// DuckDB cannot mix "?" and "$n" in one statement — the "$1" aliases positional
// param 1, shifting every "?" after it — so the composite silently returned zero
// rows in production and the benchmark. The whole rendered statement must use a
// single placeholder style ("?") so the DuckArgs+PgMainArgs+DuckArgs interleave
// binds positionally.
func TestDuckDBMixedComposite_NoPlaceholderStyleMixing(t *testing.T) {
	cache := dualPlanTestCache() // age -> integer_01 (main), tag -> pure EAV
	mixed := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},
		&forma.KvCondition{Attr: "tag", Value: "equals:x"},
	}}
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Condition: mixed}}

	dual := parityDual(t, mixed, cache)
	// Sanity: the composite really does exercise both a main-column pushdown and
	// an EAV leaf, otherwise the mixing hazard would not be present.
	require.NotEmpty(t, dual.PgMainClause, "expected a main-column pushdown clause")
	require.NotEmpty(t, dual.DuckClause, "expected a DuckDB logical clause")

	sql, args, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, compiledParityParams(), q, nil, &dual)
	require.NoError(t, err)

	// Core guard: no "$n" numbered placeholders anywhere in the DuckDB statement.
	require.NotContains(t, sql, "$", "DuckDB federated SQL must not contain $n placeholders (#161): %s", dual.PgMainClause)

	// The positional "?" count must equal the number of bound args, so every
	// placeholder maps to exactly one argument in appearance order.
	require.Equal(t, strings.Count(sql, "?"), len(args),
		"placeholder count must equal arg count for positional binding")

	// The interleave is DuckArgs + PgMainArgs + DuckArgs (s3 WHERE, pg WHERE,
	// visible WHERE); pin it so a future reordering can't silently mis-bind.
	require.Equal(t, 2*len(dual.DuckArgs)+len(dual.PgMainArgs), len(args))
}
