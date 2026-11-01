package federated

import (
	"math"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// 2^53 is the float64 exact-integer boundary: 2^53+1 has no float64 of its
// own and rounds to 2^53.
const (
	pow53     = int64(1) << 53
	pow53Plus = pow53 + 1
)

func imageOnly(f float64) model.EAVRecord { return model.EAVRecord{ValueNumeric: floatPtr(f)} }

func withSidecar(i int64) model.EAVRecord {
	return model.EAVRecord{ValueNumeric: floatPtr(float64(i)), ValueInt64: int64Ptr(i)}
}

// withToken mirrors ParseAttributesJSON: the token and its float64 image.
func withToken(token string, image float64) model.EAVRecord {
	return model.EAVRecord{ValueNumeric: floatPtr(image), ValueNumericRaw: token}
}

// TestCompareNumbersVerdicts pins that a numeric reading is compared at the
// precision the read path delivered it: exactly when both sides know their
// value, and as ambiguous, never equal, when one side has only a float64
// image the other's exact value rounds to.
func TestCompareNumbersVerdicts(t *testing.T) {
	cases := []struct {
		name string
		a, b model.EAVRecord
		want eavVerdict
	}{
		{"image against same image", imageOnly(1.5), imageOnly(1.5), eavSame},
		{"NaN image against NaN token", imageOnly(math.NaN()), withToken("NaN", math.NaN()), eavSame},
		{"sidecar against image holding it", withSidecar(9e15), imageOnly(9e15), eavSame},
		{"sidecar at 2^53 against its image", withSidecar(pow53), imageOnly(float64(pow53)), eavSame},
		{"sidecar against token of it", withSidecar(pow53Plus), withToken("9007199254740993", float64(pow53Plus)), eavSame},
		{"sidecar against decimal token", withSidecar(1704067200123), withToken("1704067200123.0", 1704067200123), eavSame},
		{"token against equal token", withToken("1.50", 1.5), withToken("1.5", 1.5), eavSame},
		{"shortest token against its image", withToken("0.1", 0.1), imageOnly(0.1), eavSame},
		{"exact token against its image", withToken("1.50", 1.5), imageOnly(1.5), eavSame},
		{"sidecars past 2^53 differ", withSidecar(pow53Plus), withSidecar(pow53), eavDiffer},
		{"tokens past 2^53 differ", withToken("9007199254740993", float64(pow53)), withToken("9007199254740992", float64(pow53)), eavDiffer},
		{"sidecar against fractional token", withSidecar(1704067200123), withToken("1704067200123.0000001", 1704067200123), eavDiffer},
		{"sidecar against another image", withSidecar(42), imageOnly(43), eavDiffer},
		{"sidecar against NaN image", withSidecar(42), imageOnly(math.NaN()), eavDiffer},
		{"number against none", withSidecar(42), model.EAVRecord{}, eavDiffer},
		{"sidecar past 2^53 against rounded image", withSidecar(pow53Plus), imageOnly(float64(pow53)), eavAmbiguous},
		{"token past 2^53 against rounded image", withToken("9007199254740993", float64(pow53)), imageOnly(float64(pow53)), eavAmbiguous},
		{"fractional token against whole image", withToken("1001.0000000000000001", 1001), imageOnly(1001), eavAmbiguous},
		{"token past float64 against infinity", withToken("1e400", math.Inf(1)), imageOnly(math.Inf(1)), eavAmbiguous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := newEAVNumber(tc.a), newEAVNumber(tc.b)
			if got := compareNumbers(a, b); got != tc.want {
				t.Fatalf("compareNumbers(%v, %v) = %d, want %d", a, b, got, tc.want)
			}
			if got := compareNumbers(b, a); got != tc.want {
				t.Fatalf("compareNumbers(%v, %v) = %d, want %d (reversed)", b, a, got, tc.want)
			}
		})
	}
}

// TestCompareResultsReportsLossyEAVImage pins that a bigint past 2^53 that one
// engine reads exactly and the other only as its rounded float64 image fails
// the comparison, naming the attribute and marking the lossy side.
func TestCompareResultsReportsLossyEAVImage(t *testing.T) {
	report := eavComparison(func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
		fed[3] = model.EAVRecord{SchemaID: 7, AttrID: 3, ValueNumeric: floatPtr(float64(pow53))}
		pg[3] = model.EAVRecord{SchemaID: 7, AttrID: 3, ValueNumeric: floatPtr(float64(pow53Plus)), ValueInt64: int64Ptr(pow53Plus)}
		return fed, pg
	})
	if report.Match || len(report.AttributeMismatches) != 1 {
		t.Fatalf("want one mismatch, got match=%v %+v", report.Match, report.AttributeMismatches)
	}
	m := report.AttributeMismatches[0]
	if m.AttributeName != "eav:7/3[]" {
		t.Fatalf("mismatch on %q, want eav:7/3[]", m.AttributeName)
	}
	fedVal, pgVal := m.FederatedVal.(eavValue), m.PostgresVal.(eavValue)
	if !fedVal.ambiguous || pgVal.ambiguous {
		t.Fatalf("want only the federated image marked ambiguous: fed=%v pg=%v", fedVal, pgVal)
	}
	if !strings.Contains(pgVal.String(), "9007199254740993 (int64)") {
		t.Fatalf("postgres side does not show its exact value: %v", pgVal)
	}
}

// eavNumericDivergenceCases change the fixture's bigint past the float64
// exact-integer boundary, or below its token's precision, where the float64
// images of both sides agree: only exact comparison tells them apart.
func eavNumericDivergenceCases() []eavDivergenceCase {
	return []eavDivergenceCase{
		{
			name: "exact bigint past 2^53 differs only exactly",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[3].ValueNumeric, fed[3].ValueInt64 = floatPtr(9007199254740992), int64Ptr(9007199254740993)
				pg[3].ValueNumeric, pg[3].ValueInt64 = floatPtr(9007199254740992), int64Ptr(9007199254740992)
				return fed, pg
			},
			wantAttr: []string{"eav:7/3[]"},
		},
		{
			name: "exact bigint past 2^53 against its rounded image",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[3].ValueNumeric, fed[3].ValueInt64 = floatPtr(9007199254740992), nil
				pg[3].ValueNumeric, pg[3].ValueInt64 = floatPtr(9007199254740992), int64Ptr(9007199254740993)
				return fed, pg
			},
			wantAttr: []string{"eav:7/3[]"},
		},
		{
			name: "raw token beyond its rounded image",
			alter: func(fed, pg []model.EAVRecord) ([]model.EAVRecord, []model.EAVRecord) {
				fed[3].ValueInt64 = int64Ptr(1704067200123)
				fed[3].ValueNumeric = floatPtr(1704067200123)
				pg[3].ValueInt64, pg[3].ValueNumericRaw = nil, "1704067200123.0000001"
				pg[3].ValueNumeric = floatPtr(1704067200123)
				return fed, pg
			},
			wantAttr: []string{"eav:7/3[]"},
		},
	}
}
