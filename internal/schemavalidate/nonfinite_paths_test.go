package schemavalidate

import (
	"math"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestNonFinitePaths(t *testing.T) {
	t.Run("flat map", func(t *testing.T) {
		found := nonFinitePaths(map[string]any{"name": "x", "score": math.NaN()})
		require.Len(t, found, 1)
		require.Equal(t, "score", found[0].path)
		require.True(t, math.IsNaN(found[0].value))
	})

	t.Run("nested map is dotted", func(t *testing.T) {
		found := nonFinitePaths(map[string]any{"a": map[string]any{"b": math.Inf(1)}})
		require.Len(t, found, 1)
		require.Equal(t, "a.b", found[0].path)
		require.Equal(t, math.Inf(1), found[0].value)
	})

	t.Run("array element is indexed", func(t *testing.T) {
		found := nonFinitePaths(map[string]any{"scores": []any{1.0, math.Inf(-1)}})
		require.Len(t, found, 1)
		require.Equal(t, "scores[1]", found[0].path)
	})

	t.Run("float32 counts", func(t *testing.T) {
		found := nonFinitePaths(map[string]any{"ratio": float32(math.Inf(1))})
		require.Len(t, found, 1)
		require.Equal(t, "ratio", found[0].path)
	})

	t.Run("finite payload yields nothing", func(t *testing.T) {
		require.Empty(t, nonFinitePaths(map[string]any{
			"a": 1.5, "b": []any{2.0, map[string]any{"c": float32(3)}}, "d": "NaN",
		}))
	})

	// Sorted, not map-iteration order: the same payload must always name the
	// same attribute, or the published message is nondeterministic.
	t.Run("multiple offenders come back sorted", func(t *testing.T) {
		found := nonFinitePaths(map[string]any{
			"zeta": math.NaN(), "alpha": math.Inf(1), "mid": map[string]any{"x": math.NaN()},
		})
		require.Len(t, found, 3)
		require.Equal(t, []string{"alpha", "mid.x", "zeta"},
			[]string{found[0].path, found[1].path, found[2].path})
	})
}

// TestValidateNamesNonFiniteAttribute pins PR #403's P2: the published message
// for a marshal refusal must name the attribute, the value, and the expected
// state. encoding/json carries no path, so the path is derived by walking the
// payload — this test is the reason that walk exists.
func TestValidateNamesNonFiniteAttribute(t *testing.T) {
	const schema = `{"type":"object","properties":{"score":{"type":"number"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), t.TempDir())
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		doc      map[string]any
		wantPath string
		wantMore bool
	}{
		"flat":     {doc: map[string]any{"score": math.NaN()}, wantPath: `"score"`},
		"nested":   {doc: map[string]any{"a": map[string]any{"b": math.Inf(1)}}, wantPath: `"a.b"`},
		"multiple": {doc: map[string]any{"score": math.NaN(), "other": math.NaN()}, wantPath: `"other"`, wantMore: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := v.Validate(3, tc.doc)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "the carrier must publish, not earn a redacted body (#313)")
			require.Contains(t, msg, "payload cannot be encoded as JSON")
			require.Contains(t, msg, tc.wantPath, "the offending attribute must be named")
			require.Contains(t, msg, "a finite number is required", "the expected state must be stated")
			if tc.wantMore {
				require.Contains(t, msg, "(and 1 more)")
			} else {
				require.NotContains(t, msg, "more)")
			}
		})
	}
}

// TestValidateFallsBackToLibraryTextWithoutNonFinite pins the other half: a
// marshal refusal the walk cannot explain must still publish encoding/json's
// text, which is then the only description of the fault that exists.
func TestValidateFallsBackToLibraryTextWithoutNonFinite(t *testing.T) {
	const schema = `{"type":"object","properties":{"score":{"type":"number"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), t.TempDir())
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{"x": make(chan int)})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "the carrier must publish, not earn a redacted body (#313)")
	require.Contains(t, msg, "payload cannot be encoded as JSON")
	require.Contains(t, msg, "unsupported type")
	require.NotContains(t, msg, "non-finite")
}
