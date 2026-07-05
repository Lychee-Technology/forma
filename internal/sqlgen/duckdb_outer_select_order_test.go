package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// outerSelectAliases extracts the output column aliases from an outer SELECT
// fragment as produced by buildOuterSelect / BuildBenchmarkOuterSelect (parts
// joined with ",\n\t\t\t").
func outerSelectAliases(t *testing.T, outerSelect string) []string {
	t.Helper()
	parts := strings.Split(outerSelect, ",\n\t\t\t")
	aliases := make([]string, 0, len(parts))
	for _, part := range parts {
		idx := strings.LastIndex(part, " AS ")
		require.GreaterOrEqual(t, idx, 0, "outer select part %q must carry an alias", part)
		aliases = append(aliases, strings.TrimSpace(part[idx+len(" AS "):]))
	}
	return aliases
}

// expectedOuterSelectAliases is the scan contract: streamDuckDBRows scans rows
// positionally in model.EntityMainColumnDescriptors order, then attributes_json.
func expectedOuterSelectAliases() []string {
	expected := make([]string, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, desc := range model.EntityMainColumnDescriptors {
		expected = append(expected, desc.Name)
	}
	return append(expected, "attributes_json")
}

// TestBuildBenchmarkOuterSelect_ColumnOrderMatchesScanContract pins issue #147:
// the outer SELECT column order must match the positional scan order used by
// the DuckDB scan buffers, otherwise attribute values land in wrong columns.
func TestBuildBenchmarkOuterSelect_ColumnOrderMatchesScanContract(t *testing.T) {
	for _, schemaID := range []int16{100, 101, 102} {
		aliases := outerSelectAliases(t, BuildBenchmarkOuterSelect(schemaID))
		require.Equal(t, expectedOuterSelectAliases(), aliases, "schema %d outer select order", schemaID)
	}
}

func TestBuildSchemaProjection_OuterSelectOrderMatchesScanContract(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"customerId": {
			AttributeID: 6,
			ValueType:   forma.ValueTypeUUID,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "uuid_01",
			},
		},
		"symbol": {
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "text_01",
			},
		},
		"quantity": {
			AttributeID: 3,
			ValueType:   forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "bigint_01",
			},
		},
		"exchange": {
			AttributeID: 8,
			ValueType:   forma.ValueTypeText,
		},
	}

	sp, err := BuildSchemaProjection(42, cache)
	require.NoError(t, err)
	aliases := outerSelectAliases(t, sp.OuterSelect)
	require.Equal(t, expectedOuterSelectAliases(), aliases)
}
