package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestDuckDBMixedComposite_NoPlaceholderStyleMixing is the #161 regression
// guard, extended to keyset pagination by #212.
//
// A composite `main-column AND pure-EAV` filter is rendered into the DuckDB
// parquet-only template. DuckDB cannot mix "?" and "$n" in one statement (the
// "$1" aliases positional param 1, shifting every "?" after it), so a "$n"
// anywhere in the statement silently mis-binds: pre-#161 the PostgreSQL
// pushdown clause carried "$n", and pre-#212 the keyset cursor rendered "$n"
// ahead of visible's "?" placeholders. The whole rendered statement must use
// a single placeholder style ("?") so the args bind positionally.
func TestDuckDBMixedComposite_NoPlaceholderStyleMixing(t *testing.T) {
	cache := dualPlanTestCache() // age -> integer_01 (main), tag -> pure EAV
	for name, tc := range map[string]struct {
		cursor *model.KeysetCursor
		// tail is the args after the two DuckArgs occurrences: the
		// two-column cursor's 3 occurrence-args, then the window.
		tail int
	}{
		"offset": {nil, 2},
		"keyset": {parquetOnlyTestCursor(), 3 + 1},
	} {
		t.Run(name, func(t *testing.T) {
			q := parquetOnlyTestQuery(tc.cursor)
			dual := parityDual(t, q.Condition, cache)
			// Sanity: the composite really does exercise both a main-column
			// pushdown and an EAV leaf, otherwise the mixing hazard would not
			// be present.
			require.NotEmpty(t, dual.PgMainClause, "expected a main-column pushdown clause")
			require.NotEmpty(t, dual.DuckClause, "expected a DuckDB logical clause")

			sql, args, err := BuildParquetOnlyQuery(parquetOnlyTestParams(t), q, dual, ParquetWindow{Limit: 25})
			require.NoError(t, err)

			// Core guard: no "$n" numbered placeholders anywhere in the
			// DuckDB statement.
			require.NotContains(t, sql, "$", "DuckDB SQL must not contain $n placeholders (#161/#212)")
			// The positional "?" count must equal the number of bound args,
			// so every placeholder maps to exactly one argument in
			// appearance order.
			require.Equal(t, strings.Count(sql, "?"), len(args),
				"placeholder count must equal arg count for positional binding")
			// DuckArgs twice (s3 semijoin + visible), then the keyset args
			// and the window; pin it so a future reordering can't silently
			// mis-bind.
			require.Equal(t, 2*len(dual.DuckArgs)+tc.tail, len(args))
		})
	}
}
