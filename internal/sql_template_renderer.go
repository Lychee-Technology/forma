package internal

import (
	"bytes"
	"fmt"
	"regexp"
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
		"cast": func(col string, typeName string) string {
			// Map common type name strings to DuckDB types.
			var mapType = func(n string) string {
				switch n {
				case "text", "Text", "varchar", "VARCHAR", "value_text", "VALUE_TEXT":
					return "VARCHAR"
				case "uuid", "UUID":
					return "VARCHAR"
				case "smallint", "SmallInt":
					return "SMALLINT"
				case "integer", "Integer", "int", "INT":
					return "INTEGER"
				case "bigint", "BigInt":
					return "BIGINT"
				case "numeric", "Numeric", "double", "DOUBLE":
					return "DOUBLE"
				case "date", "datetime", "Date", "DateTime", "timestamp":
					return "TIMESTAMP"
				case "bool", "boolean", "Bool", "BOOLEAN":
					return "BOOLEAN"
				default:
					return "VARCHAR"
				}
			}
			return fmt.Sprintf("CAST(%s AS %s)", col, mapType(typeName))
		},
		"param_cast": func(v any, typeName string) string {
			// Convert value according to typeName then append as param, returning "?".
			var toType = func(val any, n string) (any, error) {
				switch n {
				case "uuid", "UUID":
					return ToDuckDBParam(val, forma.ValueTypeUUID)
				case "smallint", "SmallInt":
					return ToDuckDBParam(val, forma.ValueTypeSmallInt)
				case "integer", "Integer", "int", "INT":
					return ToDuckDBParam(val, forma.ValueTypeInteger)
				case "bigint", "BigInt":
					return ToDuckDBParam(val, forma.ValueTypeBigInt)
				case "numeric", "Numeric", "double", "DOUBLE":
					return ToDuckDBParam(val, forma.ValueTypeNumeric)
				case "date":
					return ToDuckDBParam(val, forma.ValueTypeDate)
				case "datetime", "timestamp":
					return ToDuckDBParam(val, forma.ValueTypeDateTime)
				case "bool", "boolean", "Bool", "BOOLEAN":
					return ToDuckDBParam(val, forma.ValueTypeBool)
				case "text", "Text":
					return ToDuckDBParam(val, forma.ValueTypeText)
				default:
					return ToDuckDBParam(val, forma.ValueTypeText)
				}
			}
			conv, err := toType(v, typeName)
			if err != nil {
				return r.Param(v)
			}
			return r.Param(conv)
		},
		"duck_type": func(typeName string) string {
			switch typeName {
			case "text":
				return "VARCHAR"
			case "uuid":
				return "VARCHAR"
			case "smallint":
				return "SMALLINT"
			case "integer":
				return "INTEGER"
			case "bigint":
				return "BIGINT"
			case "numeric":
				return "DOUBLE"
			case "date", "datetime", "timestamp":
				return "TIMESTAMP"
			case "bool", "boolean":
				return "BOOLEAN"
			default:
				return "VARCHAR"
			}
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
