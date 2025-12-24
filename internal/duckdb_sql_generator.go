package internal

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// RenderS3ParquetPath interpolates a simple Go template for parquet path rendering.
// Example template: "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
func RenderS3ParquetPath(tmpl string, schemaID int16) (string, error) {
	if tmpl == "" {
		return "", fmt.Errorf("template string is empty")
	}
	t, err := template.New("s3path").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	data := map[string]any{
		"SchemaID": schemaID,
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}

// GenerateDuckDBWhereClause produces a minimal DuckDB WHERE clause for a FederatedAttributeQuery.
// This is an intentionally small helper for the initial integration: it supports CompositeCondition
// with KvCondition children and translates simple operators. It returns the clause and a list of args
// suitable for use with database/sql parameter placeholders ($1, $2 style are left for later templating).
//
// NOTE: This is a minimal implementation to allow compilation and unit testing of rendering logic.
// Full query translation (including EAV-to-column mapping and proper parameter indexing) will be
// implemented in follow-up tasks.
func GenerateDuckDBWhereClause(q *FederatedAttributeQuery) (string, []any, error) {
	if q == nil || q.Condition == nil {
		return "1=1", nil, nil
	}

	var build func(c forma.Condition) (string, []any, error)

	build = func(c forma.Condition) (string, []any, error) {
		switch cond := c.(type) {
		case *forma.CompositeCondition:
			if len(cond.Conditions) == 0 {
				return "1=1", nil, nil
			}
			parts := make([]string, 0, len(cond.Conditions))
			args := []any{}
			joiner := " AND "
			if cond.Logic == forma.LogicOr {
				joiner = " OR "
			}
			for _, child := range cond.Conditions {
				p, a, err := build(child)
				if err != nil {
					return "", nil, err
				}
				if p != "" {
					parts = append(parts, fmt.Sprintf("(%s)", p))
					args = append(args, a...)
				}
			}
			if len(parts) == 0 {
				return "1=1", nil, nil
			}
			return fmt.Sprintf("%s", joinStrings(parts, joiner)), args, nil
		case *forma.KvCondition:
			// kv format: "op:value" or "value" (defaults to equals)
			opPart, valPart := splitOnce(cond.Value, ":")
			opStr := "equals"
			valStr := cond.Value
			if opPart != "" && valPart != "" {
				opStr = opPart
				valStr = valPart
			}
			var sqlOp string
			switch opStr {
			case "equals":
				sqlOp = "="
			case "gt":
				sqlOp = ">"
			case "gte":
				sqlOp = ">="
			case "lt":
				sqlOp = "<"
			case "lte":
				sqlOp = "<="
			case "not_equals":
				sqlOp = "!="
			case "starts_with":
				sqlOp = "LIKE"
				valStr = valStr + "%"
			case "contains":
				sqlOp = "LIKE"
				valStr = "%" + valStr + "%"
			default:
				return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
			}

			// For initial integration, reference attribute by name directly.
			// Proper column binding and EAV expansion will be done later.
			clause := fmt.Sprintf("%s %s ?", cond.Attr, sqlOp)
			return clause, []any{valStr}, nil
		default:
			return "", nil, fmt.Errorf("unsupported condition type %T", c)
		}
	}

	where, args, err := build(q.Condition)
	if err != nil {
		return "", nil, err
	}
	return where, args, nil
}

// helper: join strings
func joinStrings(parts []string, joiner string) string {
	var buf bytes.Buffer
	for i, p := range parts {
		if i > 0 {
			buf.WriteString(joiner)
		}
		buf.WriteString(p)
	}
	return buf.String()
}

// helper: splitOnce returns two strings; if sep not present, second is empty
func splitOnce(s, sep string) (string, string) {
	idx := -1
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			idx = i
			break
		}
	}
	if idx == -1 {
		return "", ""
	}
	return s[:idx], s[idx+len(sep):]
}

// AppendDirtyExclusion adds a NOT IN clause excluding dirty row ids.
// dirtyIDs are converted to strings for DuckDB parameterization using ? placeholders.
func AppendDirtyExclusion(baseClause string, dirtyIDs []uuid.UUID) (string, []any) {
	if len(dirtyIDs) == 0 {
		return baseClause, nil
	}
	placeholders := make([]string, len(dirtyIDs))
	args := make([]any, len(dirtyIDs))
	for i, id := range dirtyIDs {
		placeholders[i] = "?"
		args[i] = id.String()
	}
	excl := fmt.Sprintf("%s AND row_id NOT IN (%s)", baseClause, joinStrings(placeholders, ","))
	return excl, args
}

// GenerateDuckDBWhereClauseWithExclusions builds a DuckDB WHERE clause for the query
// and appends an exclusion for dirty row ids (to be used as an anti-join).
func GenerateDuckDBWhereClauseWithExclusions(q *FederatedAttributeQuery, dirtyIDs []uuid.UUID) (string, []any, error) {
	where, args, err := GenerateDuckDBWhereClause(q)
	if err != nil {
		return "", nil, err
	}
	clause, exclArgs := AppendDirtyExclusion(where, dirtyIDs)
	// Combine args: WHERE args first, then exclusion args
	combined := make([]any, 0, len(args)+len(exclArgs))
	combined = append(combined, args...)
	combined = append(combined, exclArgs...)
	return clause, combined, nil
}
