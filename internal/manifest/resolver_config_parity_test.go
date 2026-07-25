package manifest

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The config surface validates manifest templates (forma.DuckDBConfig
// ValidateManifestRead) while PathResolver.Resolve is what actually renders
// them at read time. #300 added an Execute-probe to the validator, which only
// helps if the two agree: the validator probes a map[string]any keyed
// "SchemaID" because that is exactly what Resolve executes against
// (resolver.go), and it sets missingkey=error while Resolve does not — making
// the validator strictly stricter, never differently strict.
//
// These tests are the lockstep guard. If Resolve's data shape ever changes
// (to a struct, or a renamed key), the validator's probe must change with it
// or a template that boots will resolve nonsense at runtime.

func TestManifestTemplate_AcceptedByConfigRendersInResolver(t *testing.T) {
	t.Parallel()

	accepted := []string{
		"manifest/{{.SchemaID}}.json",
		"lake/v2/schema-{{.SchemaID}}/manifest.json",
		"{{.SchemaID}}",
		"a{{.SchemaID}}b{{.SchemaID}}c",
	}

	for _, tmpl := range accepted {
		t.Run(tmpl, func(t *testing.T) {
			t.Parallel()

			cfg := forma.DuckDBConfig{S3Bucket: "forma-lake", ManifestTemplate: tmpl}
			require.NoError(t, cfg.ValidateManifestRead(),
				"config must accept this template for the parity claim to mean anything")

			// The resolver renders it, varies by schema, and never leaks the
			// unbound-parameter sentinel the validator screens for.
			first, err := PathResolver{PathTemplate: tmpl}.Resolve(1)
			require.NoError(t, err)
			second, err := PathResolver{PathTemplate: tmpl}.Resolve(2)
			require.NoError(t, err)

			require.NotEmpty(t, first)
			require.NotContains(t, first, "<no value>")
			require.NotEqual(t, first, second,
				"validator promised per-schema addressing; resolver must deliver it")
		})
	}
}

// TestManifestTemplate_RejectedByConfigWouldMisresolve is the other half: for
// every shape the validator rejects, show what Resolve would have done with
// it. This is what makes the rejection load-bearing rather than fussy.
func TestManifestTemplate_RejectedByConfigWouldMisresolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		symptom  string // "novalue" | "collapsed"
	}{
		{"misspelled field", "manifest/{{.SchemaId}}.json", "novalue"},
		{"constant path", "manifest/all.json", "collapsed"},
		{"printf verb", "manifest/%d.json", "collapsed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := forma.DuckDBConfig{S3Bucket: "forma-lake", ManifestTemplate: tt.template}
			require.Error(t, cfg.ValidateManifestRead(), "config must reject this template")

			// Resolve does not fail on any of these — that silence is the whole
			// problem, and why the check has to live in config validation.
			first, err := PathResolver{PathTemplate: tt.template}.Resolve(1)
			require.NoError(t, err)
			second, err := PathResolver{PathTemplate: tt.template}.Resolve(2)
			require.NoError(t, err)

			switch tt.symptom {
			case "novalue":
				require.Contains(t, first, "<no value>",
					"resolver renders the sentinel the validator screens for")
			case "collapsed":
				require.NotContains(t, first, "<no value>")
			}
			require.Equal(t, first, second,
				"every schema would resolve to one manifest object: %s", first)
		})
	}
}

// TestManifestTemplate_ResolverDataShapeIsAMap pins the assumption the probe
// depends on. Resolve executes against a map, so a missing key renders
// "<no value>" instead of erroring — which is precisely why parsing alone
// could not catch a typo and the probe has to render.
func TestManifestTemplate_ResolverDataShapeIsAMap(t *testing.T) {
	t.Parallel()

	path, err := PathResolver{PathTemplate: "m/{{.NotAField}}.json"}.Resolve(7)
	require.NoError(t, err, "map data yields no-value rendering, not an execution error")
	require.True(t, strings.Contains(path, "<no value>"),
		"if this ever errors instead, the validator's probe shape must be revisited")
}
