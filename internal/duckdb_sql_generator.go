package internal

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

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
			// Enhance by emitting explicit CASTs for non-text comparisons where detectable.
			detectValueTypeFromString := func(s string) forma.ValueType {
				// Try UUID
				if _, err := uuid.Parse(s); err == nil {
					return forma.ValueTypeUUID
				}
				// Try bool
				ls := strings.ToLower(s)
				if ls == "true" || ls == "false" || ls == "1" || ls == "0" {
					return forma.ValueTypeBool
				}
				// Try numeric
				if _, err := strconv.ParseFloat(s, 64); err == nil {
					return forma.ValueTypeNumeric
				}
				// Try timestamp (RFC3339 or unix millis)
				if _, err := time.Parse(time.RFC3339Nano, s); err == nil {
					return forma.ValueTypeDateTime
				}
				if _, err := strconv.ParseInt(s, 10, 64); err == nil {
					// ambiguous integer: treat as numeric/bigint; choose numeric for comparisons
					return forma.ValueTypeNumeric
				}
				return forma.ValueTypeText
			}

			parseParam := func(s string, vt forma.ValueType) any {
				switch vt {
				case forma.ValueTypeUUID:
					return s
				case forma.ValueTypeBool:
					b, err := strconv.ParseBool(strings.ToLower(s))
					if err == nil {
						return b
					}
					if s == "1" {
						return true
					}
					if s == "0" {
						return false
					}
					return s
				case forma.ValueTypeNumeric:
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						return f
					}
					return s
				case forma.ValueTypeDate, forma.ValueTypeDateTime:
					if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
						return t.UTC()
					}
					if i, err := strconv.ParseInt(s, 10, 64); err == nil {
						// assume epoch millis
						return time.UnixMilli(i).UTC()
					}
					return s
				default:
					return s
				}
			}

			// For LIKE operators, keep text comparison
			if sqlOp == "LIKE" {
				clause := fmt.Sprintf("%s %s ?", cond.Attr, sqlOp)
				return clause, []any{valStr}, nil
			}

			// Detect type and emit CAST on the parameter
			valueType := detectValueTypeFromString(valStr)
			duckType := MapValueTypeToDuckDBType(valueType)
			var clause string
			if duckType == "VARCHAR" {
				clause = fmt.Sprintf("%s %s ?", cond.Attr, sqlOp)
			} else {
				clause = fmt.Sprintf("%s %s CAST(? AS %s)", cond.Attr, sqlOp, duckType)
			}
			param := parseParam(valStr, valueType)
			return clause, []any{param}, nil
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
