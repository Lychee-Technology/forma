package forma

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateDuckDBConfig_ManifestTemplateProbe covers the #300 Execute-probe:
// parsing a template is not enough, because the resolver executes it against a
// map, so a template can parse cleanly and still address the wrong object — or
// the same object — for every schema. Split from
// TestValidateDuckDBConfig_ManifestRead, which was over the 100-line function
// cap (coding-standard.md).
func TestValidateDuckDBConfig_ManifestTemplateProbe(t *testing.T) {
	t.Parallel()

	runManifestReadCases(t, []manifestReadCase{
		{
			// #300: a case-typo field name parses fine but renders "<no value>"
			// against the resolver's map, so every schema resolves to the same
			// bogus manifest key, every manifest loads empty, and reads degrade
			// silently — the exact loss shape manifest reads exist to prevent,
			// one typo away.
			name: "template with a misspelled field renders no value",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/{{.SchemaId}}.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Same collapse, different cause: a template that ignores the schema
			// entirely points every schema at one object.
			name: "template that does not vary by schema",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/all.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "template rendering to nothing",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "{{if false}}manifest/{{.SchemaID}}.json{{end}}",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Printf-style verbs are a plausible slip for someone who has not
			// used text/template: they render literally and never vary.
			name: "printf style placeholder instead of template action",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "manifest/%d.json",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Review P1: only the first probe output was screened, so a template
			// that renders for schema 1 and nothing for schema 2 passed — the two
			// outputs differ, and the empty one was never checked. Those schemas
			// would resolve an empty manifest path at runtime.
			name: "template rendering empty for only some schemas",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "{{if eq .SchemaID 1}}manifest/1.json{{end}}",
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			// Same gap, reached differently: index bypasses missingkey=error, so
			// only an output check catches the "<no value>" it renders.
			name: "template rendering no value for only some schemas",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: `{{if eq .SchemaID 1}}{{.SchemaID}}{{else}}{{index . "SchemaId"}}{{end}}`,
			},
			errorField: "duckdb.manifestTemplate",
		},
		{
			name: "schema id embedded in a longer path is valid",
			duckDB: DuckDBConfig{
				S3Bucket:         "forma-lake",
				ManifestTemplate: "lake/v2/schema-{{.SchemaID}}/manifest.json",
			},
			errorField: "",
		},
	})
}

// ValidateManifestPathTemplate carries the ValidateManifestRead template
// rules to templates supplied outside the server config (cdc-init's
// --manifest-template, #371 review), naming the caller's field.
func TestValidateManifestPathTemplate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, tmpl, wantMsg string
	}{
		{"empty", "", "must not be empty"},
		// #371 review P2: the resolver renders the raw value, so a padded
		// template would publish under a key the server-side
		// ValidateManifestRead refuses to configure. Same rule, same message.
		{"whitespace only", "  ", "must not have leading or trailing whitespace"},
		{"leading space", " manifest/{{.SchemaID}}.json", "must not have leading or trailing whitespace"},
		{"trailing newline", "manifest/{{.SchemaID}}.json\n", "must not have leading or trailing whitespace"},
		{"unparseable", "manifest/{{", "must be a valid text/template path template"},
		{"constant path", "manifest/all.json", "must vary by schema"},
		{"misspelled field", "manifest/{{.SchemaId}}.json", "must render against the manifest resolver's data"},
		{"valid", "lake/v2/schema-{{.SchemaID}}/manifest.json", ""},
	}
	for _, c := range cases {
		err := ValidateManifestPathTemplate("manifest-template", c.tmpl)
		if c.wantMsg == "" {
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", c.name, err)
			}
			continue
		}
		var cfgErr *ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("%s: err = %v, want *ConfigError", c.name, err)
		}
		if cfgErr.Field != "manifest-template" {
			t.Fatalf("%s: field = %q, want the caller's field", c.name, cfgErr.Field)
		}
		if !strings.Contains(cfgErr.Message, c.wantMsg) {
			t.Fatalf("%s: message %q does not contain %q", c.name, cfgErr.Message, c.wantMsg)
		}
	}
}
