package internal

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"
)

// SQLRenderer renders text/template SQL templates while collecting parameter
// values and providing a safe identifier helper to avoid SQL injection.
type SQLRenderer struct {
	args []any
}

func NewSQLRenderer() *SQLRenderer {
	return &SQLRenderer{
		args: make([]any, 0),
	}
}

// Param appends a value to the renderer's args and returns a "?" placeholder
// to be inserted into the template.
func (r *SQLRenderer) Param(v any) string {
	r.args = append(r.args, v)
	return "?"
}

var identRegex = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Ident validates a SQL identifier (table/column) and returns it quoted.
// If the identifier is invalid, Ident panics (templates should not pass
// untrusted strings to Ident; callers can recover or validate before use).
func (r *SQLRenderer) Ident(name string) string {
	if !identRegex.MatchString(name) {
		panic(fmt.Sprintf("invalid SQL identifier: %q", name))
	}
	// Double-quote to be safe for SQL identifiers.
	return `"` + name + `"`
}

// Render executes tpl with data while providing the template functions:
//   - param: adds a param and returns "?" placeholder
//   - ident: validates and returns a quoted identifier
//
// It returns the rendered SQL and the collected args slice.
func (r *SQLRenderer) Render(tpl *template.Template, data any) (string, []any, error) {
	// Clone template to avoid mutating shared FuncMap state.
	tplClone, err := tpl.Clone()
	if err != nil {
		return "", nil, fmt.Errorf("clone template: %w", err)
	}

	funcs := template.FuncMap{
		"param": func(v any) string { return r.Param(v) },
		"ident": func(s string) string { return r.Ident(s) },
	}
	tplClone = tplClone.Funcs(funcs)

	var buf bytes.Buffer
	if err := tplClone.Execute(&buf, data); err != nil {
		return "", nil, fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), r.args, nil
}

// Convenience helper: one-shot render
func RenderSQLTemplate(tpl *template.Template, data any) (string, []any, error) {
	r := NewSQLRenderer()
	return r.Render(tpl, data)
}
