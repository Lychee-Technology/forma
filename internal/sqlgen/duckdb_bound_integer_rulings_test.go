package sqlgen_test

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// hotLegIntegerRulings gives, for every numeric-family binding
// schemameta.ValidateColumnBinding admits, the type the DuckDB hot leg reads
// the main column at. "" means the raw column. Otherwise the leg reads the
// declared type through TRY_CAST, which only a column wider than the
// declared integer type gets (#627). numeric is DOUBLE on every leg, so it
// always reads raw.
var hotLegIntegerRulings = map[forma.ValueType]map[forma.MainColumnType]string{
	forma.ValueTypeSmallInt: {
		forma.MainColumnTypeSmallint: "",
		forma.MainColumnTypeInteger:  "SMALLINT",
		forma.MainColumnTypeBigint:   "SMALLINT",
		forma.MainColumnTypeDouble:   "SMALLINT",
	},
	forma.ValueTypeInteger: {
		forma.MainColumnTypeSmallint: "",
		forma.MainColumnTypeInteger:  "",
		forma.MainColumnTypeBigint:   "INTEGER",
		forma.MainColumnTypeDouble:   "INTEGER",
	},
	forma.ValueTypeBigInt: {
		forma.MainColumnTypeSmallint: "",
		forma.MainColumnTypeInteger:  "",
		forma.MainColumnTypeBigint:   "",
		forma.MainColumnTypeDouble:   "BIGINT",
	},
	forma.ValueTypeNumeric: {
		forma.MainColumnTypeSmallint: "",
		forma.MainColumnTypeInteger:  "",
		forma.MainColumnTypeBigint:   "",
		forma.MainColumnTypeDouble:   "",
	},
}

// TestHotLegProjections_WideIntegerColumnsReadAtDeclaredType checks each
// ruling on both hot projections. The pairs it walks are not listed here.
// They are every binding of a numeric-family type that registration admits,
// to one column of each type entity_main has. A column type or binding
// admitted later fails this test until it is ruled. Without that check it
// would silently read raw, because sqlgen's width ranks do not know it. This
// is an external test because schemameta imports sqlgen.
func TestHotLegProjections_WideIntegerColumnsReadAtDeclaredType(t *testing.T) {
	walked, ruled := 0, 0
	for vt, rulings := range hotLegIntegerRulings {
		ruled += len(rulings)
		for colType, col := range oneAttributeColumnPerType() {
			meta := forma.AttributeMetadata{AttributeID: 1, ValueType: vt,
				ColumnBinding: &forma.MainColumnBinding{ColumnName: col}}
			declared, hasRuling := rulings[colType]
			if schemameta.ValidateColumnBinding("x", meta) != nil {
				require.False(t, hasRuling, "%s in a %s column is refused at registration but carries a ruling", vt, colType)
				continue
			}
			require.True(t, hasRuling,
				"%s in a %s column is admitted but has no hot-leg ruling: rank the column type in sqlgen's columnWidths and rule it here", vt, colType)
			walked++
			t.Run(fmt.Sprintf("%s_in_%s", vt, colType), func(t *testing.T) {
				read := "m." + string(col)
				if declared != "" {
					read = fmt.Sprintf("TRY_CAST(m.%s AS %s)", col, declared)
				}
				sp, err := sqlgen.BuildSchemaProjection(7, forma.SchemaAttributeCache{"x": meta})
				require.NoError(t, err)
				require.Contains(t, sp.PGSourceSelect, "COALESCE(ANY_VALUE(hot_vals.x), "+read+") AS x")
				require.Contains(t, sp.BuildPGSelectNoEAV(), read+" AS x")
			})
		}
	}
	require.Equal(t, ruled, walked, "a ruling names a column type entity_main does not have")
}

// oneAttributeColumnPerType picks the first non-system column of each column
// type entity_main has.
func oneAttributeColumnPerType() map[forma.MainColumnType]forma.MainColumn {
	cols := make(map[forma.MainColumnType]forma.MainColumn)
	for _, desc := range model.EntityMainColumnDescriptors {
		if model.IsSystemColumn(desc.Name) {
			continue
		}
		col := forma.MainColumn(desc.Name)
		colType := (&forma.MainColumnBinding{ColumnName: col}).ColumnType()
		if _, seen := cols[colType]; !seen {
			cols[colType] = col
		}
	}
	return cols
}
