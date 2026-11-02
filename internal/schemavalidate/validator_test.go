package schemavalidate

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestValidateRejectsEnumViolation is the issue's own example: lead.json
// declares status as a four-value enum and "banana" is not one of them.
//
// The substring assertion couples to jsonschema-go's message prose
// ("enum: banana does not equal any of: [...]"). That is deliberate — it proves
// the offending value reaches the operator — but it will need updating if the
// library rewords its enum failure.
func TestValidateRejectsEnumViolation(t *testing.T) {
	dir := shippedSchemaDir(t)
	resolved, err := resolveSchemaFile(dir, "lead.json")
	require.NoError(t, err)

	err = resolved.Validate(map[string]any{"status": "banana"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

// TestValidateRoundTripsNativeValues pins that Validate marshals before
// validating. time.Time in a native map has Go type "object" to the validator
// and fails a "type":"string" property until it is round-tripped; two real
// call sites pass time.Now() for string-typed properties.
func TestValidateRoundTripsNativeValues(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"at":{"type":"string"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"at": time.Now()}))
}

// TestValidateWrapsErrInvalidInput pins the error class: a schema violation is
// caller input, so it must surface as 4xx rather than a redacted 500 (#307).
func TestValidateWrapsErrInvalidInput(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"status":{"enum":["open","won"]}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{"status": "banana"})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "banana")
}

// TestValidateUnknownSchemaIDIsNotClientError pins that a missing resolved
// schema is an operator problem, not the caller's: it must NOT wrap
// ErrInvalidInput, or a server misconfiguration would answer 400.
func TestValidateUnknownSchemaIDIsNotClientError(t *testing.T) {
	dir := t.TempDir()
	v, err := New(registryWith(t, "ev", `{"type":"object"}`, 3), dir)
	require.NoError(t, err)

	err = v.Validate(99, map[string]any{})
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateClassifiesUnmarshallablePayloadAsInvalidInput pins #322: a
// payload the caller built that cannot be encoded as JSON — math.NaN()/
// math.Inf(), reachable only from a Go embedder, since encoding/json cannot
// decode those literals from an HTTP body — is the caller's fault, not the
// operator's. It must carry forma.ErrInvalidInput and publish a message naming
// the offending value — since #403 that message is synthesized, not the library's.
func TestValidateClassifiesUnmarshallablePayloadAsInvalidInput(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"score":{"type":"number"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	for name, value := range map[string]float64{
		"NaN":  math.NaN(),
		"+Inf": math.Inf(1),
		"-Inf": math.Inf(-1),
	} {
		t.Run(name, func(t *testing.T) {
			err := v.Validate(3, map[string]any{"score": value})
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "the carrier must publish, not earn a redacted body (#313)")
			require.Contains(t, msg, name, "the published message names the offending value on either marshalRefusalError branch")
		})
	}
}

// TestValidateDoesNotReResolve pins that Validate reuses the schema resolved by
// New. Resolving is ~250x the cost of validating, so a regression here is a
// throughput cliff rather than a correctness bug.
//
// The mechanism matters: an earlier version of this test compared
// v.resolved[id] before and after, which can only fail if something *writes*
// the map — nothing ever does, so a Validate that re-resolved on every call
// still passed. This version removes the file the schema's $ref needs once New
// has returned. A correct Validate is unaffected because resolution is already
// complete; a Validate that re-resolves must invoke the loader, which can no
// longer read the file.
func TestValidateDoesNotReResolve(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, dir, "target.json", `{"type":"string","maxLength":3}`)
	const root = `{"type":"object","properties":{"x":{"$ref":"target.json"}}}`
	writeSchemaFile(t, dir, "root.json", root)

	v, err := New(registryWith(t, "ev", root, 3), dir)
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, "target.json")))

	// Prove the removal is load-bearing: resolving this schema now genuinely
	// fails, so the assertions below would catch a re-resolving Validate.
	_, err = resolveSchemaFile(dir, "root.json")
	require.Error(t, err, "removing the $ref target must make resolution fail")

	require.NoError(t, v.Validate(3, map[string]any{"x": "ok"}))
	// The ref target's own constraint is still enforced, so the cached schema is
	// the fully resolved one rather than a degraded copy that skipped the $ref.
	require.ErrorIs(t, v.Validate(3, map[string]any{"x": "far too long"}), forma.ErrInvalidInput)
}

// TestValidateOnNilValidatorIsOperatorError pins that a nil *Validator returns
// the plain operator error rather than panicking. Task 4 threads a *Validator
// that is nil when validation is unconfigured; Task 5 nil-checks before
// calling, so this is insurance against a future caller that does not.
func TestValidateOnNilValidatorIsOperatorError(t *testing.T) {
	var v *Validator
	err := v.Validate(3, map[string]any{})
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}
