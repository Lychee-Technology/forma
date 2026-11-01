package federated

import (
	"math"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }
func int64Ptr(i int64) *int64     { return &i }

// eavFixture is one record's EAV list: a scalar text, a two-element numeric
// array and a bigint carried with its exact sidecar.
func eavFixture() []model.EAVRecord {
	return []model.EAVRecord{
		{SchemaID: 7, AttrID: 1, ValueText: strPtr("alpha")},
		{SchemaID: 7, AttrID: 2, ArrayIndices: "0", ValueNumeric: floatPtr(1.5)},
		{SchemaID: 7, AttrID: 2, ArrayIndices: "1", ValueNumeric: floatPtr(2.5)},
		{SchemaID: 7, AttrID: 3, ValueNumeric: floatPtr(9e15), ValueInt64: int64Ptr(9_000_000_000_000_000)},
	}
}

func eavComparison(alter func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord)) *ComparisonReport {
	fed, pg := comparisonResult(), comparisonResult()
	fedAttrs, pgAttrs := alter(eavFixture(), eavFixture())
	fed.Records[0].OtherAttributes = fedAttrs
	pg.Records[0].OtherAttributes = pgAttrs
	return (&FederatedTestHarness{}).CompareResults(fed, pg)
}

// TestCompareResultsMatchesEquivalentEAV pins that the EAV comparison follows
// attribute identity, not slice order, and ignores read-path representation
// metadata: each case holds the same values on both sides.
func TestCompareResultsMatchesEquivalentEAV(t *testing.T) {
	cases := []struct {
		name  string
		alter func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord)
	}{
		{
			name: "reversed order",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				for i, j := 0, len(pg)-1; i < j; i, j = i+1, j-1 {
					pg[i], pg[j] = pg[j], pg[i]
				}
				return fed, pg
			},
		},
		{
			name: "array elements swapped",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[1], pg[2] = pg[2], pg[1]
				return fed, pg
			},
		},
		{
			name: "read-path metadata on one side",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[1].ValueNumericRaw = "1.50"
				pg[3].ValueNumericRaw = "9000000000000000"
				pg[3].BoundColumn = "bigint_01"
				return fed, pg
			},
		},
		{
			name: "int64 sidecar on one side only",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[3].ValueInt64 = nil
				return fed, pg
			},
		},
		{
			name: "row id named on one side only",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				for i := range pg {
					pg[i].RowID = comparisonRowIDs[0]
				}
				return fed, pg
			},
		},
		{
			name: "NaN on both sides",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[1].ValueNumeric = floatPtr(math.NaN())
				pg[1].ValueNumeric = floatPtr(math.NaN())
				return fed, pg
			},
		},
		{
			name: "nil and empty lists",
			alter: func(_, _ []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				return nil, []model.EAVRecord{}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if report := eavComparison(tc.alter); !report.Match {
				t.Fatalf("equivalent EAV reported as different: %+v", report.AttributeMismatches)
			}
		})
	}
}

type eavDivergenceCase struct {
	name     string
	alter    func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord)
	wantAttr []string
}

// eavDivergenceCases each change, move, drop or repeat one value of the
// fixture, and name the mismatches the comparison must report, in order.
func eavDivergenceCases() []eavDivergenceCase {
	return []eavDivergenceCase{
		{
			name: "text changed",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[0].ValueText = strPtr("beta")
				return fed, pg
			},
			wantAttr: []string{"eav:7/1[]"},
		},
		{
			name: "array element changed",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[2].ValueNumeric = floatPtr(3.5)
				return fed, pg
			},
			wantAttr: []string{"eav:7/2[1]"},
		},
		{
			name: "array values swapped between indices",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[1].ArrayIndices, pg[2].ArrayIndices = "1", "0"
				return fed, pg
			},
			wantAttr: []string{"eav:7/2[0]", "eav:7/2[1]"},
		},
		{
			name: "index moved",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[2].ArrayIndices = "2"
				return fed, pg
			},
			wantAttr: []string{"eav:7/2[1]", "eav:7/2[2]"},
		},
		{
			name: "exact bigint differs",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				pg[3].ValueInt64 = int64Ptr(9_000_000_000_000_001)
				return fed, pg
			},
			wantAttr: []string{"eav:7/3[]"},
		},
		{
			name: "text against numeric",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[0].ValueText, fed[0].ValueNumeric = nil, floatPtr(0)
				return fed, pg
			},
			wantAttr: []string{"eav:7/1[]"},
		},
		{
			name: "attribute missing in federated",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				return fed[:3], pg
			},
			wantAttr: []string{"eav:7/3[]"},
		},
		{
			name: "attribute missing in postgres",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				return fed, pg[1:]
			},
			wantAttr: []string{"eav:7/1[]"},
		},
		{
			name: "attribute duplicated in postgres",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				return fed, append(pg, pg[0])
			},
			wantAttr: []string{"eav:7/1[]"},
		},
		{
			name: "attribute duplicated on both sides",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				return append(fed, fed[1]), append(pg, pg[1])
			},
			wantAttr: []string{"eav:7/2[0]"},
		},
		{
			name: "attribute under another schema",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[0].SchemaID = 8
				return fed, pg
			},
			wantAttr: []string{"eav:7/1[]", "eav:8/1[]"},
		},
		{
			name: "attribute of another row",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[0].RowID = comparisonRowIDs[1]
				return fed, pg
			},
			wantAttr: []string{"eav:7/1[]", "eav:7/1[]@" + comparisonRowIDs[1].String()},
		},
	}
}

// TestCompareResultsDetectsEAVDivergence pins that keying by attribute
// identity still catches a changed, moved, missing or repeated value.
func TestCompareResultsDetectsEAVDivergence(t *testing.T) {
	for _, tc := range eavDivergenceCases() {
		t.Run(tc.name, func(t *testing.T) {
			report := eavComparison(tc.alter)
			if report.Match {
				t.Fatal("divergent EAV reported as matching")
			}
			got := make([]string, 0, len(report.AttributeMismatches))
			for _, m := range report.AttributeMismatches {
				got = append(got, m.AttributeName)
			}
			if len(got) != len(tc.wantAttr) {
				t.Fatalf("mismatches on %v, want %v", got, tc.wantAttr)
			}
			for i := range got {
				if got[i] != tc.wantAttr[i] {
					t.Fatalf("mismatches on %v, want %v", got, tc.wantAttr)
				}
			}
		})
	}
}
