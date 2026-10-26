package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

var (
	bigintOnDouble  = forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: "double_01"}}
	bigintOnBigint  = forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_01"}}
	bigintEAV       = forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt}
	integerOnColumn = forma.AttributeMetadata{ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: "integer_01"}}
	numericEAV      = forma.AttributeMetadata{ValueType: forma.ValueTypeNumeric}
	textEAV         = forma.AttributeMetadata{ValueType: forma.ValueTypeText}
	dateEAV         = forma.AttributeMetadata{ValueType: forma.ValueTypeDate}
	boolEAV         = forma.AttributeMetadata{ValueType: forma.ValueTypeBool}
	bigintList      = forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt}
	textList        = forma.AttributeMetadata{ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText}
)

// TestJudgeColumnClassifiesEveryPair pins the four #732 classes for the pairs
// a redeclaration produces. Lossless pairs cost no scan, so a class that
// drifts to lossless is a silent-read regression and one that drifts away
// from it is a scan on every healthy query.
func TestJudgeColumnClassifiesEveryPair(t *testing.T) {
	for name, tc := range map[string]struct {
		fileType  string
		meta      forma.AttributeMetadata
		viaDouble bool
		want      judgementKind
	}{
		"exported type":                    {"BIGINT", bigintOnBigint, false, judgeLossless},
		"smallint widens to integer":       {"SMALLINT", integerOnColumn, false, judgeLossless},
		"integer widens to bigint":         {"INTEGER", bigintEAV, false, judgeLossless},
		"integer into a double-bound":      {"INTEGER", bigintOnDouble, false, judgeLossless},
		"bigint into a double-bound":       {"BIGINT", bigintOnDouble, false, judgeLossless}, // the exported type; #590 bounds it at write
		"double under bigint":              {"DOUBLE", bigintOnDouble, false, judgeNeedsProof},
		"double under integer":             {"DOUBLE", integerOnColumn, false, judgeNeedsProof},
		"bigint under integer":             {"BIGINT", integerOnColumn, false, judgeNeedsProof},
		"text under bigint":                {"VARCHAR", bigintEAV, false, judgeNeedsProof},
		"bigint read through double":       {"BIGINT", bigintEAV, true, judgeNeedsProof},
		"bigint under numeric":             {"BIGINT", numericEAV, false, judgeLossless}, // rounds beyond ±2^53 (#733)
		"integer under numeric":            {"INTEGER", numericEAV, false, judgeLossless},
		"text under numeric":               {"VARCHAR", numericEAV, false, judgeNeedsProof},
		"text under bool":                  {"VARCHAR", boolEAV, false, judgeNeedsProof},
		"bool under bigint":                {"BOOLEAN", bigintEAV, false, judgeIncompatible},
		"number under text":                {"DOUBLE", textEAV, false, judgeIncompatible},
		"double under date":                {"DOUBLE", dateEAV, false, judgeIncompatible},
		"text under date":                  {"VARCHAR", dateEAV, false, judgeIncompatible},
		"scalar under list":                {"BIGINT", bigintList, false, judgeIncompatible},
		"list under scalar":                {"BIGINT[]", bigintEAV, false, judgeIncompatible},
		"list exported element":            {"BIGINT[]", bigintList, false, judgeLossless},
		"list element needing proof":       {"DOUBLE[]", bigintList, false, judgeUnprovable},
		"list element of another family":   {"BIGINT[]", textList, false, judgeIncompatible},
		"list element read through double": {"BIGINT[]", bigintList, true, judgeUnprovable},
	} {
		t.Run(name, func(t *testing.T) {
			got := judgeColumn(`"qty"`, tc.fileType, tc.meta, tc.viaDouble)
			require.Equal(t, tc.want, got.kind)
			if got.kind == judgeNeedsProof {
				require.NotEmpty(t, got.violation)
				require.NotEmpty(t, got.demand)
			}
		})
	}
}

// A bigint bound to a double_* column keeps only a float64 image, so its
// domain is ±2^53 (#590); an EAV or bigint_* bigint has the full int64 range.
func TestJudgeWholeUsesTheDeclaredStorageDomain(t *testing.T) {
	require.Contains(t, judgeColumn(`"qty"`, "DOUBLE", bigintOnDouble, false).demand, "[-9007199254740992, 9007199254740992]")
	require.Contains(t, judgeColumn(`"qty"`, "DOUBLE", bigintEAV, false).demand, "[-9223372036854775808, 9223372036854775807]")
	require.Contains(t, judgeColumn(`"qty"`, "DOUBLE", integerOnColumn, false).demand, "[-2147483648, 2147483647]")
	require.Contains(t, judgeColumn(`"qty"`, "BIGINT", bigintEAV, true).demand, "[-9007199254740992, 9007199254740992]",
		"a BIGINT read through DOUBLE is exact only within ±2^53")
}
