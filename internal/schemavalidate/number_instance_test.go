package schemavalidate

import (
	"encoding/json"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestValidateChecksBoundsAbove2p53 guards #282: the validation instance must
// carry integers exactly. Pre-#282 the round-trip decode rode float64, so
// 2^53+1 rounded to exactly 2^53 and slipped past a maximum of 2^53 even
// though the stored value (exact via the transform sidecar) violated it.
func TestValidateChecksBoundsAbove2p53(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"amount":{"type":"integer","maximum":9007199254740992}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"amount": json.Number("9007199254740992")}))

	err = v.Validate(3, map[string]any{"amount": json.Number("9007199254740993")})
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateNumberInstanceTypeChecks pins that the exact-number rewrite
// keeps jsonschema type checking intact: a raw json.Number would classify as
// "string" if it leaked through (jsonType has no json.Number case), and a
// fractional literal must satisfy "number" but not "integer".
func TestValidateNumberInstanceTypeChecks(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"i":{"type":"integer"},"f":{"type":"number"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"i": json.Number("9223372036854775807")}))
	require.NoError(t, v.Validate(3, map[string]any{"f": json.Number("1.5")}))

	err = v.Validate(3, map[string]any{"i": json.Number("1.5")})
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateNumericEnumEquality pins enum comparison across the int64
// rewrite: jsonschema-go compares numbers via big.Rat, so an int64 instance
// must still match a float64-decoded schema enum literal.
func TestValidateNumericEnumEquality(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"level":{"enum":[1,2,3]}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"level": json.Number("2")}))

	err = v.Validate(3, map[string]any{"level": json.Number("4")})
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateNestedNumbersAbove2p53 pins that the rewrite walks nested
// objects and arrays, not just top-level properties.
func TestValidateNestedNumbersAbove2p53(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"o":{"type":"object","properties":{"xs":{"type":"array","items":{"type":"integer","maximum":9007199254740992}}}}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	doc := map[string]any{"o": map[string]any{"xs": []any{json.Number("9007199254740993")}}}
	err = v.Validate(3, doc)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
