package manifest

import (
	"bytes"
	"fmt"
	"text/template"
)

// PathResolver builds manifest paths per schema using a template.
// Defaults to "manifest/{{.SchemaID}}.json" under an optional prefix.
type PathResolver struct {
	Prefix       string
	PathTemplate string
}

// Resolve returns the full path for a schema manifest.
func (r PathResolver) Resolve(schemaID int16) (string, error) {
	tmpl := r.PathTemplate
	if tmpl == "" {
		tmpl = "manifest/{{.SchemaID}}.json"
	}
	t, err := template.New("manifestPath").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse manifest template: %w", err)
	}
	var buf bytes.Buffer
	data := map[string]any{"SchemaID": schemaID}
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute manifest template: %w", err)
	}
	path := buf.String()
	if r.Prefix != "" {
		path = fmt.Sprintf("%s/%s", trimSlash(r.Prefix), path)
	}
	return path, nil
}

func trimSlash(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
