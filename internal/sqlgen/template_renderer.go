package sqlgen

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/lychee-technology/forma"
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

func valueTypeFromTemplateTypeName(typeName string) forma.ValueType {
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "uuid":
		return forma.ValueTypeUUID
	case "smallint":
		return forma.ValueTypeSmallInt
	case "integer", "int":
		return forma.ValueTypeInteger
	case "bigint":
		return forma.ValueTypeBigInt
	case "numeric", "double":
		return forma.ValueTypeNumeric
	case "date":
		return forma.ValueTypeDate
	case "datetime", "timestamp":
		return forma.ValueTypeDateTime
	case "bool", "boolean":
		return forma.ValueTypeBool
	case "text", "varchar", "value_text":
		return forma.ValueTypeText
	default:
		return forma.ValueTypeText
	}
}

// Ident validates a SQL identifier (table/column) and returns it quoted.
func (r *SQLRenderer) Ident(name string) (string, error) {
	if !identRegex.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %q", name)
	}
	// Double-quote to be safe for SQL identifiers.
	return `"` + name + `"`, nil
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
		"ident": func(s string) (string, error) { return r.Ident(s) },
		"cast": func(col string, typeName string) string {
			valueType := valueTypeFromTemplateTypeName(typeName)
			return CastExpression(col, valueType)
		},
		"param_cast": func(v any, typeName string) string {
			valueType := valueTypeFromTemplateTypeName(typeName)
			conv, err := ToDuckDBParam(v, valueType)
			if err != nil {
				return r.Param(v)
			}
			return r.Param(conv)
		},
		"duck_type": func(typeName string) string {
			valueType := valueTypeFromTemplateTypeName(typeName)
			return MapValueTypeToDuckDBType(valueType)
		},
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
