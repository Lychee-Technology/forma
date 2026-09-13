package transform

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// boolTruthRow is one row of the #404 ruling: what the write side accepts as
// a bool. want is meaningful only when reject is false.
type boolTruthRow struct {
	input  any
	want   bool
	reject bool
}

// boolWriteTruthTable is the single write-side truth table both funnels must
// spell (#404). Numbers are accepted only as the exact images 0 and 1 — the
// read-side threshold is a tolerance for persisted data, not an acceptance
// rule, so fractional and out-of-range images are rejected rather than
// guessed. Strings go through strconv.ParseBool; anything else is rejected.
//
// The numeric type set is the write path's (numutil.Float64): int8 and the
// unsigned widths are rejected on both funnels, as the numeric funnel rejects
// them, until #566 widens every funnel together. json.Number is decided on
// its decimal text, so the rows around 0 and 1 pin that a literal which only
// rounds to an image is still rejected.
func boolWriteTruthTable() map[string]boolTruthRow {
	one, zero, third := 1.0, 0.0, 0.3
	trueStr, negOne := "TRUE", -1
	int8One, uint8One, uint64One := int8(1), uint8(1), uint64(1)
	return map[string]boolTruthRow{
		"bool true":         {input: true, want: true},
		"bool false":        {input: false, want: false},
		"string true":       {input: "true", want: true},
		"string TRUE":       {input: "TRUE", want: true},
		"string t":          {input: "t", want: true},
		"string 1":          {input: "1", want: true},
		"string 0":          {input: "0", want: false},
		"string F":          {input: "F", want: false},
		"string banana":     {input: "banana", reject: true},
		"string NaN":        {input: "NaN", reject: true},
		"string empty":      {input: "", reject: true},
		"*string TRUE":      {input: &trueStr, want: true},
		"int 1":             {input: int(1), want: true},
		"int 0":             {input: int(0), want: false},
		"int -1":            {input: int(-1), reject: true},
		"*int -1":           {input: &negOne, reject: true},
		"int16 1":           {input: int16(1), want: true},
		"int32 -1":          {input: int32(-1), reject: true},
		"int64 5":           {input: int64(5), reject: true},
		"int64 0":           {input: int64(0), want: false},
		"float32 1":         {input: float32(1), want: true},
		"float32 0.5":       {input: float32(0.5), reject: true},
		"float64 1":         {input: float64(1), want: true},
		"float64 0":         {input: float64(0), want: false},
		"float64 0.3":       {input: float64(0.3), reject: true},
		"float64 0.7":       {input: float64(0.7), reject: true},
		"float64 -1":        {input: float64(-1), reject: true},
		"float64 2":         {input: float64(2), reject: true},
		"*float64 1":        {input: &one, want: true},
		"*float64 0":        {input: &zero, want: false},
		"*float64 0.3":      {input: &third, reject: true},
		"int8 1":            {input: int8(1), reject: true},
		"*int8 1":           {input: &int8One, reject: true},
		"uint 1":            {input: uint(1), reject: true},
		"uint8 1":           {input: uint8(1), reject: true},
		"*uint8 1":          {input: &uint8One, reject: true},
		"uint16 0":          {input: uint16(0), reject: true},
		"uint32 1":          {input: uint32(1), reject: true},
		"uint64 1":          {input: uint64(1), reject: true},
		"*uint64 1":         {input: &uint64One, reject: true},
		"json.Number 1":     {input: json.Number("1"), want: true},
		"json.Number 0":     {input: json.Number("0"), want: false},
		"json.Number -0":    {input: json.Number("-0"), want: false},
		"json.Number 1.0":   {input: json.Number("1.0"), want: true},
		"json.Number 0.0":   {input: json.Number("0.0"), want: false},
		"json.Number 1e0":   {input: json.Number("1e0"), want: true},
		"json.Number 10e-1": {input: json.Number("10e-1"), want: true},
		"json.Number 2":     {input: json.Number("2"), reject: true},
		"json.Number -1":    {input: json.Number("-1"), reject: true},
		"json.Number 0.5":   {input: json.Number("0.5"), reject: true},
		"json.Number abc":   {input: json.Number("abc"), reject: true},
		"json.Number empty": {input: json.Number(""), reject: true},
		"json.Number NaN":   {input: json.Number("NaN"), reject: true},
		"json.Number Inf":   {input: json.Number("Inf"), reject: true},
		"json.Number 1e400": {input: json.Number("1e400"), reject: true},
		// Literals that round to the images 0/1 in float64 but are not them.
		"json.Number just above 1":     {input: json.Number("1.0000000000000001"), reject: true},
		"json.Number just below 1":     {input: json.Number("0.99999999999999999"), reject: true},
		"json.Number just above 0":     {input: json.Number("0.0000000000000000001"), reject: true},
		"json.Number just below 0":     {input: json.Number("-0.0000000000000000001"), reject: true},
		"json.Number underflow to 0":   {input: json.Number("1e-400"), reject: true},
		"json.Number 2p53 plus 1":      {input: json.Number("9007199254740993"), reject: true},
		"json.Number integer past i64": {input: json.Number("99999999999999999999"), reject: true},
		"nil *bool":                    {input: (*bool)(nil), reject: true},
		"nil *float64":                 {input: (*float64)(nil), reject: true},
		"slice":                        {input: []int{1}, reject: true},
		"map":                          {input: map[string]any{}, reject: true},
		"time-shaped string":           {input: "2024-01-01", reject: true},
	}
}

// TestBoolWriteFunnelsShareOneTruthTable is the #404 acceptance criterion:
// populateTypedValue and ToEAVRecord must reach the same verdict on every
// input, and that verdict must be the ruled one. Before this, int(-1) was
// true on one funnel and false on the other, float64(0.3) likewise, and
// "banana" was an error on one and a silent false on the other.
func TestBoolWriteFunnelsShareOneTruthTable(t *testing.T) {
	meta := forma.AttributeMetadata{AttributeID: 3, ValueType: forma.ValueTypeBool}
	converter := NewAttributeConverter(nil)
	for name, row := range boolWriteTruthTable() {
		t.Run(name, func(t *testing.T) {
			var typed model.EAVRecord
			_, typedErr := populateTypedValue(&typed, "active", row.input, meta)
			eav, eavErr := converter.ToEAVRecord(model.EntityAttribute{
				SchemaID: 1, AttrID: 3, ValueType: forma.ValueTypeBool, Value: row.input,
			}, uuid.New())

			if row.reject {
				require.ErrorIs(t, typedErr, forma.ErrInvalidInput, "populateTypedValue must reject %#v", row.input)
				require.Error(t, eavErr, "ToEAVRecord must reject %#v", row.input)
				require.Nil(t, typed.ValueNumeric, "nothing may be staged for storage")
				require.Nil(t, eav.ValueNumeric, "nothing may be staged for storage")
				return
			}
			require.NoError(t, typedErr)
			require.NoError(t, eavErr)
			require.NotNil(t, typed.ValueNumeric)
			require.NotNil(t, eav.ValueNumeric)
			require.Equal(t, boolToFloat64(row.want), *typed.ValueNumeric, "populateTypedValue verdict")
			require.Equal(t, *typed.ValueNumeric, *eav.ValueNumeric,
				"both write funnels must persist the same image for %#v", row.input)
		})
	}
}

// TestBoolRejectionNamesTheImageRule pins the prose: a finite number that is
// not 0 or 1 is rejected as not a boolean image, not with the non-finite
// guard's "has no truth value" nor the numeric guard's "not storable".
func TestBoolRejectionNamesTheImageRule(t *testing.T) {
	meta := forma.AttributeMetadata{AttributeID: 3, ValueType: forma.ValueTypeBool}
	reject := func(t *testing.T, input any, fragments ...string) {
		t.Helper()
		var attr model.EAVRecord
		_, err := populateTypedValue(&attr, "active", input, meta)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.Contains(t, err.Error(), "active")
		for _, fragment := range fragments {
			require.Contains(t, err.Error(), fragment)
		}
	}
	t.Run("fraction", func(t *testing.T) {
		reject(t, 0.3, "0.3", "must be 0 or 1")
	})
	// A json.Number is rejected by its own text, not by its float64 image —
	// "value 1 is not a boolean image" would contradict the caller's eyes.
	t.Run("json.Number rounding to an image", func(t *testing.T) {
		reject(t, json.Number("1.0000000000000001"), "1.0000000000000001", "must be 0 or 1")
	})
	// The cause and the offending value both survive into the published
	// message (coding-standard: wrap, never discard).
	t.Run("json.Number not a number", func(t *testing.T) {
		reject(t, json.Number("abc"), `"abc"`, "invalid syntax")
	})
	t.Run("json.Number out of range", func(t *testing.T) {
		reject(t, json.Number("1e400"), `"1e400"`, "out of range")
	})
	t.Run("unsupported width", func(t *testing.T) {
		reject(t, uint8(1), "uint8", "value 1", "cannot convert uint8 to float64")
	})
}

// TestExtractValueFromEAVRecordReadsBoolByThreshold pins the read side of the
// ruling: a persisted numeric image is read as the nearest of 0/1
// (float64ToBool's > 0.5), never through the strict write funnel. A stored
// 0.3 is a tolerated float-noise zero, not a storage error.
func TestExtractValueFromEAVRecordReadsBoolByThreshold(t *testing.T) {
	for name, tc := range map[string]struct {
		stored float64
		want   bool
	}{
		"exact 1":     {stored: 1, want: true},
		"exact 0":     {stored: 0, want: false},
		"noise 0.3":   {stored: 0.3, want: false},
		"noise 0.7":   {stored: 0.7, want: true},
		"stored 2":    {stored: 2, want: true},
		"stored -1":   {stored: -1, want: false},
		"exactly 0.5": {stored: 0.5, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			stored := tc.stored
			got, err := extractValueFromEAVRecord(model.EAVRecord{ValueNumeric: &stored}, forma.ValueTypeBool)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestBoolTextReadRejectsOffContractText pins the bool_text main-column
// funnel (#404, second comment). The contract is "1"/"0"; a stored "true"
// used to read back as false and be rewritten as "0" by the next partial
// update. It is now a storage consistency error naming the column.
func TestBoolTextReadRejectsOffContractText(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)

	read := func(t *testing.T, stored string) (map[string]any, error) {
		t.Helper()
		return transformer.FromPersistentRecord(ctx, &model.PersistentRecord{
			RowID:     uuid.Must(uuid.NewV7()),
			SchemaID:  schemaID,
			TextItems: map[string]string{string(forma.MainColumnText02): stored},
		})
	}

	t.Run("1 reads true", func(t *testing.T) {
		got, err := read(t, "1")
		require.NoError(t, err)
		require.Equal(t, true, got["isActiveText"])
	})
	t.Run("0 reads false", func(t *testing.T) {
		got, err := read(t, "0")
		require.NoError(t, err)
		require.Equal(t, false, got["isActiveText"])
	})
	for _, stored := range []string{"true", "TRUE", "t", "", "banana"} {
		t.Run("off-contract "+stored, func(t *testing.T) {
			_, err := read(t, stored)
			require.Error(t, err)
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "a read-path consistency error is not caller input")
			require.Contains(t, err.Error(), "text_02")
			require.Contains(t, err.Error(), "bool_text")
		})
	}
}
