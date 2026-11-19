package forma

import (
	"errors"
	"strings"
	"testing"
)

// ValidateManifestPathTemplate applies the manifest template rules to a
// template a tool is given (cdc-init's --manifest-template, #371 review),
// naming the caller's field. The probe (#300) renders it against the
// resolver's data for two schemas: parsing alone would pass a typo that
// renders the same bogus key for every schema.
func TestValidateManifestPathTemplate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, tmpl, wantMsg string
	}{
		{"empty", "", "must not be empty"},
		// #371 review P2: the resolver renders the raw value, so a padded
		// template would publish under a key other than the one an
		// operator reads in the config.
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
