package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParquetAttrColumn pins the physical parquet column naming contract.
// The mapping must stay byte-identical with what the CDC exporter has
// always written (formerly internal/cdc safeColumnAlias): existing delta
// and base parquet files on S3 were produced with exactly this folding.
func TestParquetAttrColumn(t *testing.T) {
	cases := map[string]string{
		"name":                 "name",                 // flat names pass through
		"contact.annualIncome": "contact_annualIncome", // dots fold to underscores
		"a.b.c":                "a_b_c",
		"with space":           "with_space",
		"tick`ed":              "ticked",
		"arr[0]":               "arr0",
		"":                     "attr", // empty falls back
	}
	for in, want := range cases {
		require.Equal(t, want, ParquetAttrColumn(in), "ParquetAttrColumn(%q)", in)
	}
}
