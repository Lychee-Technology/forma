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

// TestValidateClassifiesOutOfRangeLiteralAsInvalidInput is issue #402's
// headline: a literal that fits neither int64 nor float64 — {"score": 1e400},
// which reaches Validate intact because httpapi decodes with UseNumber and
// json.Marshal re-emits a json.Number verbatim — is the caller's own value
// and must carry the sentinel rather than answer a redacted 500. The
// published message names the attribute path and the literal, and the stdlib
// range text stays operator-only (#453's toolchain-drift ruling).
func TestValidateClassifiesOutOfRangeLiteralAsInvalidInput(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"score":{"type":"number"},"o":{"type":"object"},"xs":{"type":"array"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		doc      map[string]any
		wantPath string
		wantLit  string
	}{
		"top level": {
			doc: map[string]any{"score": json.Number("1e400")}, wantPath: "score", wantLit: "1e400"},
		"negative": {
			doc: map[string]any{"score": json.Number("-1e400")}, wantPath: "score", wantLit: "-1e400"},
		"nested object": {
			doc:      map[string]any{"o": map[string]any{"deep": json.Number("1e400")}},
			wantPath: "o.deep", wantLit: "1e400"},
		"array index": {
			doc:      map[string]any{"xs": []any{json.Number("1"), json.Number("1e400")}},
			wantPath: "xs[1]", wantLit: "1e400"},
	} {
		t.Run(name, func(t *testing.T) {
			err := v.Validate(3, tc.doc)
			require.ErrorIs(t, err, forma.ErrInvalidInput)

			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "the carrier must publish, not earn a redacted body (#313)")
			require.Contains(t, msg, `attribute "`+tc.wantPath+`"`)
			require.Contains(t, msg, `"`+tc.wantLit+`"`)
			require.NotContains(t, msg, "strconv", "stdlib text is operator detail, never published")
			require.NotContains(t, msg, "value out of range", "stdlib text is operator detail, never published")

			require.True(t, forma.HasOperatorDetail(err))
			require.Contains(t, err.Error(), "value out of range", "the log keeps the stdlib text")
		})
	}
}

// TestExactNumberInstanceRootLiteralHasNoAttribute mirrors the marshal walk's
// root rule: a literal at the document root has no attribute to name, so the
// published message carries the literal alone.
func TestExactNumberInstanceRootLiteralHasNoAttribute(t *testing.T) {
	_, err := exactNumberInstance(json.Number("1e400"))
	require.ErrorIs(t, err, forma.ErrInvalidInput)

	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok)
	require.NotContains(t, msg, "attribute")
	require.Contains(t, msg, `"1e400"`)
}

// TestValidateRedecodeFailureStaysPlain pins the split #402 asked for: the
// out-of-range carrier must not be bought by blanket-classifying the decode
// wrap. The rewrite site and the redecode site must carry distinct wrap text
// so an operator reading a log can tell the internal fault from caller input.
func TestValidateRewriteWrapIsDistinctFromDecodeWrap(t *testing.T) {
	dir := shippedSchemaDir(t)
	schema := `{"type":"object","properties":{"score":{"type":"number"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{"score": json.Number("1e400")})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "failed to decode payload",
		"the redecode wrap is reserved for the genuinely internal branch")
}
