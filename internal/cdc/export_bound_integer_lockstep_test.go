package cdc

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// TestCastMainValue_WideIntegerColumnMatchesHotLeg is the #627 lockstep: an
// integer bound to a column wider than its declared type is exported through
// TRY_CAST to that type, and the DuckDB hot leg must read the unflushed row
// through the same expression. Otherwise the row answers differently before
// and after its flush. Before #627 the hot leg read the raw column, and the
// outer select's strict cast failed every query that reached an image the
// type cannot hold.
func TestCastMainValue_WideIntegerColumnMatchesHotLeg(t *testing.T) {
	for _, tc := range []struct {
		vt  forma.ValueType
		col forma.MainColumn
	}{
		{forma.ValueTypeBigInt, forma.MainColumnDouble01},
		{forma.ValueTypeInteger, forma.MainColumnDouble01},
		{forma.ValueTypeInteger, forma.MainColumnBigint01},
		{forma.ValueTypeSmallInt, forma.MainColumnDouble01},
		{forma.ValueTypeSmallInt, forma.MainColumnBigint01},
		{forma.ValueTypeSmallInt, forma.MainColumnInteger01},
	} {
		t.Run(string(tc.vt)+"_in_"+string(tc.col), func(t *testing.T) {
			meta := forma.AttributeMetadata{AttributeID: 1, ValueType: tc.vt,
				ColumnBinding: &forma.MainColumnBinding{ColumnName: tc.col}}
			sp, err := sqlgen.BuildSchemaProjection(7, forma.SchemaAttributeCache{"x": meta})
			require.NoError(t, err)

			export := castMainValue("m."+string(tc.col), meta)
			require.Contains(t, sp.BuildPGSelectNoEAV(), export+" AS x")
			require.Contains(t, sp.PGSourceSelect, "COALESCE(ANY_VALUE(hot_vals.x), "+export+") AS x")
		})
	}
}
