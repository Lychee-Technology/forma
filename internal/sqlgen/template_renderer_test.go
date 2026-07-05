package sqlgen

import (
	"strings"
	"testing"
	"text/template"
)

func TestSQLRendererIdent(t *testing.T) {
	renderer := NewSQLRenderer()

	got, err := renderer.Ident("valid_name_01")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != `"valid_name_01"` {
		t.Fatalf("expected quoted identifier, got %q", got)
	}
}

func TestSQLRendererIdentInvalid(t *testing.T) {
	renderer := NewSQLRenderer()

	_, err := renderer.Ident(`bad-name`)
	if err == nil {
		t.Fatalf("expected error for invalid identifier")
	}
	if !strings.Contains(err.Error(), "invalid SQL identifier") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRenderSQLTemplate_InvalidIdentReturnsError(t *testing.T) {
	// Add a placeholder ident func for parse-time registration.
	tpl := template.Must(template.New("sql").Funcs(template.FuncMap{
		"ident": func(name string) string { return name },
	}).Parse(`SELECT * FROM {{ident .Table}}`))

	_, _, err := RenderSQLTemplate(tpl, map[string]any{
		"Table": "bad-name",
	})
	if err == nil {
		t.Fatalf("expected render error for invalid identifier")
	}
	if !strings.Contains(err.Error(), "invalid SQL identifier") {
		t.Fatalf("unexpected error: %v", err)
	}
}
