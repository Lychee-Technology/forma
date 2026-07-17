package transform

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func newBigintPrecisionRegistry() *stubSchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   202,
		schemaName: "bigint_precision_test",
		cache: forma.SchemaAttributeCache{
			"amount": {
				AttributeID: 1,
				ValueType:   forma.ValueTypeBigInt,
				ColumnBinding: &forma.MainColumnBinding{
					ColumnName: forma.MainColumnBigint01,
					Encoding:   forma.MainColumnEncodingDefault,
				},
			},
		},
	}
}

// TestToPersistentRecordPreservesBoundBigintExactly pins the #205 Hop-1 fix:
// an int64-carrying payload must reach the bound BIGINT main column without a
// float64 hop. Pre-#205, -MaxInt64 landed as MinInt64 (deterministic
// off-by-one) and +MaxInt64 survived only by platform-dependent saturation.
func TestToPersistentRecordPreservesBoundBigintExactly(t *testing.T) {
	tr := NewPersistentRecordTransformer(newBigintPrecisionRegistry())
	cases := []struct {
		name string
		in   any
		want int64
	}{
		{"max_int64", int64(math.MaxInt64), math.MaxInt64},
		{"neg_max_int64", int64(-math.MaxInt64), -math.MaxInt64},
		{"above_2_53", int64(1<<53 + 1), 1<<53 + 1},
		{"json_number", json.Number("9223372036854775807"), math.MaxInt64},
		{"string", "9223372036854775807", math.MaxInt64},
		{"float64_fallback", float64(42), 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := tr.ToPersistentRecord(context.Background(), 202, uuid.New(),
				map[string]any{"amount": tc.in})
			require.NoError(t, err)
			got, ok := rec.Int64Items["bigint_01"]
			require.True(t, ok, "bound bigint column absent")
			require.Equal(t, tc.want, got)
		})
	}
}

// TestFromPersistentRecordReturnsBoundBigintExactly pins the read-back leg: a
// stored bigint (legal Postgres BIGINT, e.g. written by external tooling)
// must surface exactly through the JSON transform, not via int64→float64→int64.
// Pre-#205, the read path contains a float64 hop (persistent_record.go:402
// `f := float64(val)` + attribute_converter.go:496 `int64(*record.ValueNumeric)`).
// MaxInt64 case: saturation on arm64 (implementation-defined); kept for symmetry.
// NegMaxInt64 and Above2_53: deterministic on all platforms pre-fix. They must red.
func TestFromPersistentRecordReturnsBoundBigintExactly(t *testing.T) {
	tr := NewPersistentRecordTransformer(newBigintPrecisionRegistry())
	cases := []struct {
		name string
		val  int64
	}{
		{"max_int64", math.MaxInt64},
		{"neg_max_int64", -math.MaxInt64},
		{"above_2_53", 1<<53 + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &model.PersistentRecord{
				SchemaID:     202,
				RowID:        uuid.New(),
				TextItems:    map[string]string{},
				Int16Items:   map[string]int16{},
				Int32Items:   map[string]int32{},
				Int64Items:   map[string]int64{"bigint_01": tc.val},
				Float64Items: map[string]float64{},
				UUIDItems:    map[string]uuid.UUID{},
			}
			out, err := tr.FromPersistentRecord(context.Background(), rec)
			require.NoError(t, err)
			require.EqualValues(t, tc.val, out["amount"],
				"read-back must not round through float64")
		})
	}
}
