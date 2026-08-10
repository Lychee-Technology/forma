package internal

import (
	"testing"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/stretchr/testify/require"
)

// validateFixtureChild resolves a fixture's child.json through the same
// constructor production uses and validates one payload against it.
//
// The guard's whole job is to predict this function's answer for the payloads a
// caller can still send once StripComputedFields has removed the relation root.
// Where a comment in relation_required.go says a conditional keyword does or
// does not make a property mandatory, this is what makes that claim checkable
// rather than remembered.
func validateFixtureChild(t *testing.T, fixture string, payload map[string]any) error {
	t.Helper()
	dir := relationFixtureDir(fixture)
	registry := relationFixtureRegistry(t, fixture)

	validator, err := schemavalidate.New(registry, dir)
	require.NoError(t, err, "fixture %s must resolve", fixture)

	childID, _, err := registry.GetSchemaByName("child")
	require.NoError(t, err)

	return validator.Validate(childID, payload)
}

// TestConditionalFixturesAgainstTheValidator is the ground truth the
// if/then/else half of the guard is judged against. Each payload here is one a
// caller can still send after the strip has removed "contactSnapshot" — that is
// the only kind of payload the guard's question is about.
//
// "if" contributes no assertions of its own; only "then" and "else" do. So a
// schema whose "if" turns on a stripped property has an "if" that fails on every
// reachable payload, its "then" never applies, and it demands nothing extra —
// relation_required_if_only, whose "then" asks for "fallback", accepts both
// payloads below. A schema whose "then" asks for the relation root itself does
// reject the documents that take that branch — relation_required_ifthen. And
// "then" without an "if" is inert, which is what lets the guard skip the trio
// entirely in that case — relation_ok_then_without_if.
func TestConditionalFixturesAgainstTheValidator(t *testing.T) {
	t.Run("if_only demands nothing once the relation root is stripped", func(t *testing.T) {
		require.NoError(t, validateFixtureChild(t, "relation_required_if_only",
			map[string]any{"id": "x", "parentId": "p"}))
		require.NoError(t, validateFixtureChild(t, "relation_required_if_only",
			map[string]any{"id": "x", "parentId": "p", "fallback": "f"}))
	})

	t.Run("ifthen demands the relation root on the documents that take then", func(t *testing.T) {
		err := validateFixtureChild(t, "relation_required_ifthen",
			map[string]any{"id": "x", "parentId": "p", "kind": "snapshot"})

		require.Error(t, err)
		require.ErrorContains(t, err, "contactSnapshot")
	})

	t.Run("then without if is inert", func(t *testing.T) {
		require.NoError(t, validateFixtureChild(t, "relation_ok_then_without_if",
			map[string]any{"id": "x", "parentId": "p"}))
	})
}
