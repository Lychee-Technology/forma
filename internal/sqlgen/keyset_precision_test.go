package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestKeysetArgsPreserveInt64Above2p53 pins the raw-bind contract (#281,
// #205 final review M-2): keyset cursor values bind verbatim — an int64 above
// 2^53 must reach the DuckDB arg list as int64, never float64. DuckDB widens
// `BIGINT col > DOUBLE param` to DOUBLE, so a float64 cursor value at a >2^53
// page boundary rounds and can skip or duplicate one row of the next page.
//
// There is no continuation-token codec in the repo today: model.KeysetCursor
// is in-process only (forma.QueryRequest carries no cursor field), so the M-2
// scenario has zero live instances and this test guards the invariant that
// nothing else does. Any future cursor decoder must produce int64 for integer
// cursor values (json.Number / numutil.Int64Exact — never a plain
// encoding/json float64 path) or this contract breaks silently.
func TestKeysetArgsPreserveInt64Above2p53(t *testing.T) {
	// 2^53+1 is the smallest positive integer float64 cannot represent: a
	// round-trip through float64 lands on 2^53. If this probe value is ever
	// weakened to a float64-safe number the test stops proving anything.
	wantAmount := int64(1)<<53 + 1
	require.NotEqual(t, wantAmount, int64(float64(wantAmount)),
		"probe value must be inexact in float64, else the test cannot detect coercion")

	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "amount", Direction: forma.SortOrderAsc, ExportType: "BIGINT"},
			{Attribute: "row_id", Direction: forma.SortOrderAsc, ExportType: "UUID"},
		},
		Values: []interface{}{wantAmount, "0f000000-0000-0000-0000-000000000000"},
		Mode:   model.KeysetCursorModeAfter,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		KeysetCursor:   cursor,
	}
	dual := DualClauses{
		DuckClause: "age > ?",
		DuckArgs:   []any{int64(25)},
	}

	sql, args, err := BuildParquetOnlyQuery(injectTestRenderParams(t, map[string]any{}, 1), q, dual, ParquetWindow{Limit: 10})
	require.NoError(t, err)

	// [DuckArgs(1), DuckArgs(1), keyset(3), window(1)] — a 2-column cursor
	// repeats its prefix value once per disjunct, so the keyset args are
	// n(n+1)/2 = 3 args: [amount, amount, row_id].
	require.Len(t, args, 6)
	keysetArgs := args[2:5]
	require.Equal(t, "0f000000-0000-0000-0000-000000000000", keysetArgs[2],
		"row_id tiebreak binds last among the keyset args")

	// Every occurrence of the amount value — not just the first — must be a
	// verbatim int64: a coercion that reached only the repeated prefix arg
	// would still corrupt the page boundary.
	occurrences := 0
	for i, arg := range keysetArgs {
		if f, isFloat := arg.(float64); isFloat {
			t.Fatalf("keyset arg %d is float64 %v; cursor values must bind verbatim as int64", i, f)
		}
		v, isInt := arg.(int64)
		if !isInt {
			continue
		}
		require.Equal(t, wantAmount, v,
			"keyset arg %d is int64 %d, want the cursor value %d bound verbatim", i, v, wantAmount)
		occurrences++
	}
	require.Equal(t, 2, occurrences,
		"amount must appear twice as int64 in %v (once per disjunct)", keysetArgs)

	require.NotContains(t, sql, "$", "keyset queries must not reintroduce $n placeholders")
	require.Equal(t, len(args), strings.Count(sql, "?"),
		"placeholder count must equal arg count for positional binding")
}
