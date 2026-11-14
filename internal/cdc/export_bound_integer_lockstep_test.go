package cdc

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// TestCastMainValue_WideIntegerColumnMatchesPgMainPushdown is the #627
// lockstep: an integer bound to a column wider than its declared type is
// exported through TRY_CAST to that type, and the pg-main pushdown leaf
// sqlgen.ToDualClauses renders must compare the unflushed row through the
// same expression. Otherwise the row answers differently before and after
// its flush.
func TestCastMainValue_WideIntegerColumnMatchesPgMainPushdown(t *testing.T) {
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
			idx := 0
			dc, err := sqlgen.ToDualClauses(&forma.KvCondition{Attr: "x", Value: "equals:1"},
				"eav_data", 7, forma.SchemaAttributeCache{"x": meta}, &idx)
			require.NoError(t, err)

			export := castMainValue("m."+string(tc.col), meta)
			require.True(t, strings.HasPrefix(dc.PgMainClause, export+" "),
				"pg-main clause %q must compare the export image %s", dc.PgMainClause, export)
		})
	}
}
