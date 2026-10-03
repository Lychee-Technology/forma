package model

import (
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// parseOneValueNumeric runs one attributes_json element carrying token as its
// value_numeric through ParseAttributesJSON.
func parseOneValueNumeric(t *testing.T, token string) (EAVRecord, error) {
	t.Helper()
	attrsJSON := `[{"schema_id":100,"attr_id":7,"row_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","array_indices":"","value_text":null,"value_numeric":` + token + `}]`
	var record PersistentRecord
	if err := ParseAttributesJSON([]byte(attrsJSON), &record); err != nil {
		return EAVRecord{}, err
	}
	require.Len(t, record.OtherAttributes, 1)
	return record.OtherAttributes[0], nil
}

// #592: the parser preserves the token the read query emitted beside its
// float64 image and applies no value-type rule. A float64 decode rounded
// 2^53+1 to 2^53 before any contract could judge it, and a token past
// float64 failed the whole row's decode.
func TestParseAttributesJSON_KeepsTheEmittedToken(t *testing.T) {
	cases := []struct {
		token string
		image float64
	}{
		{"0", 0},
		{"-1", -1},
		{"1704067200123", 1704067200123},
		{"9007199254740993", 9007199254740992},
		{"-9007199254740993", -9007199254740992},
		{"9223372036854775807", 9223372036854775807},
		{"1704067200123.0", 1704067200123},
		{"1704067200123.000", 1704067200123},
		{"9.007199254740992e+15", 9007199254740992},
		{"1000.5", 1000.5},
		{"1" + strings.Repeat("0", 300), 1e300},
		{"1e400", math.Inf(1)},
		{"-1e400", math.Inf(-1)},
	}
	for _, tc := range cases {
		t.Run(tc.token[:min(len(tc.token), 24)], func(t *testing.T) {
			attr, err := parseOneValueNumeric(t, tc.token)
			require.NoError(t, err)
			require.NotNil(t, attr.ValueNumeric)
			require.Equal(t, tc.image, *attr.ValueNumeric)
			require.Equal(t, tc.token, attr.ValueNumericRaw)
			require.Nil(t, attr.ValueInt64, "the parser never invents the exact sidecar")
		})
	}
}

// Postgres JSON_BUILD_OBJECT renders a NUMERIC or DOUBLE PRECISION NaN or
// infinity as a JSON string. It used to read as an absent attribute, so the
// client saw null and an update erased the stored value; it now reaches
// the transform layer as the float64 special with its token (#592).
func TestParseAttributesJSON_NonFiniteStringsAreValues(t *testing.T) {
	for token, check := range map[string]func(float64) bool{
		"NaN":       math.IsNaN,
		"Infinity":  func(v float64) bool { return math.IsInf(v, 1) },
		"-Infinity": func(v float64) bool { return math.IsInf(v, -1) },
	} {
		t.Run(token, func(t *testing.T) {
			attr, err := parseOneValueNumeric(t, `"`+token+`"`)
			require.NoError(t, err)
			require.NotNil(t, attr.ValueNumeric)
			require.True(t, check(*attr.ValueNumeric), "%v", *attr.ValueNumeric)
			require.Equal(t, token, attr.ValueNumericRaw)
		})
	}
}

// A value_numeric that names no number is a malformed row, reported with
// the attribute and the row, never read as absent.
func TestParseAttributesJSON_RefusesAValueNumericThatIsNoNumber(t *testing.T) {
	for token, want := range map[string]string{
		`"abc"`:   `value_numeric is the string "abc", which names no number`,
		`"nan"`:   `value_numeric is the string "nan", which names no number`,
		`"12"`:    `value_numeric is the string "12", which names no number`,
		`true`:    "value_numeric is a bool, not a number",
		`[1]`:     "value_numeric is a []interface {}, not a number",
		`{"a":1}`: "value_numeric is a map[string]interface {}, not a number",
	} {
		t.Run(token, func(t *testing.T) {
			_, err := parseOneValueNumeric(t, token)
			require.Error(t, err)
			require.Contains(t, err.Error(), want)
			require.Contains(t, err.Error(), "attr_id 7 of schema 100 (row aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa)")
		})
	}
}

func TestParseAttributesJSON_NullValueNumericIsAbsent(t *testing.T) {
	attr, err := parseOneValueNumeric(t, "null")
	require.NoError(t, err)
	require.Nil(t, attr.ValueNumeric)
	require.Empty(t, attr.ValueNumericRaw)
}

// The decoder reads one array; anything after it is malformed input that
// json.Unmarshal refused and Decoder.Decode alone would ignore.
func TestParseAttributesJSON_RefusesTrailingData(t *testing.T) {
	var record PersistentRecord
	err := ParseAttributesJSON([]byte(`[{"schema_id":1,"attr_id":2}] [{"schema_id":1,"attr_id":3}]`), &record)
	require.ErrorContains(t, err, "unexpected data after the attributes array")

	err = ParseAttributesJSON([]byte(`[{"schema_id":1,"attr_id":2}] x`), &record)
	require.Error(t, err)

	require.NoError(t, ParseAttributesJSON([]byte(" [{\"schema_id\":1,\"attr_id\":2}] \n"), &record))
	require.Len(t, record.OtherAttributes, 1)
}

// ParseEAVAttribute takes the ids as the json.Number the decoder yields and
// as the float64 of a caller-built map; a float64 value_numeric carries no
// token.
func TestParseEAVAttribute_AcceptsDecodedAndBuiltNumbers(t *testing.T) {
	rowID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	var record PersistentRecord
	require.NoError(t, ParseAttributesJSON([]byte(`[{"schema_id":12,"attr_id":34,"row_id":"`+rowID.String()+`","array_indices":"0","value_text":"x"}]`), &record))
	require.Equal(t, EAVRecord{SchemaID: 12, AttrID: 34, RowID: rowID, ArrayIndices: "0", ValueText: ptr("x")}, record.OtherAttributes[0])

	image := 1.5
	attr, err := ParseEAVAttribute(map[string]any{"schema_id": float64(12), "attr_id": float64(34), "value_numeric": image})
	require.NoError(t, err)
	require.Equal(t, &image, attr.ValueNumeric)
	require.Empty(t, attr.ValueNumericRaw)

	_, err = ParseEAVAttribute(map[string]any{"schema_id": "12", "attr_id": float64(34)})
	require.ErrorContains(t, err, "schema_id is missing or not a number")
}

func ptr[T any](v T) *T { return &v }
