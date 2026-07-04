package internal

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ErrListInOrderBy is returned when a LIST type attribute is used in ORDER BY.
var ErrListInOrderBy = fmt.Errorf("LIST type attributes cannot be used in ORDER BY")

// ListOperatorMapping defines how DSL operators map to DuckDB LIST functions.
// DSL operators for LIST columns use list_contains or list_any_match patterns.
type ListOperatorMapping struct {
	// Operator is the DSL operator name (equals, contains, starts_with, gt, etc.)
	Operator string
	// DuckDBExpr is a template for the DuckDB expression.
	// Placeholders: {{.Column}} for the column name, {{.Value}} for the parameter.
	DuckDBExpr string
	// RequiresLambda indicates if the expression uses a lambda (x -> predicate)
	RequiresLambda bool
}

// BuildListPredicate generates a DuckDB predicate for LIST column operations.
// Supported operators:
//   - equals: list_contains(col, value) - checks if value is in the list
//   - not_equals: NOT list_contains(col, value)
//   - contains: list_any_match(col, x -> x LIKE '%value%')
//   - starts_with: list_any_match(col, x -> x LIKE 'value%')
//   - gt/gte/lt/lte: list_any_match(col, x -> x OP value)
//
// Returns the SQL fragment and the parameter value to bind.
func BuildListPredicate(column, operator, value string, elementType forma.ValueType) (string, any, error) {
	// Validate operator is allowed for LIST
	switch operator {
	case "equals":
		// list_contains checks if the value exists in the list
		duckType := MapValueTypeToDuckDBType(elementType)
		if duckType == "VARCHAR" {
			return fmt.Sprintf("list_contains(%s, ?)", column), value, nil
		}
		return fmt.Sprintf("list_contains(%s, CAST(? AS %s))", column, duckType), value, nil

	case "not_equals":
		duckType := MapValueTypeToDuckDBType(elementType)
		if duckType == "VARCHAR" {
			return fmt.Sprintf("NOT list_contains(%s, ?)", column), value, nil
		}
		return fmt.Sprintf("NOT list_contains(%s, CAST(? AS %s))", column, duckType), value, nil

	case "contains":
		// list_any_match with LIKE for substring matching
		return fmt.Sprintf("list_any_match(%s, x -> x LIKE ?)", column), "%" + value + "%", nil

	case "starts_with":
		return fmt.Sprintf("list_any_match(%s, x -> x LIKE ?)", column), value + "%", nil

	case "gt":
		duckType := MapValueTypeToDuckDBType(elementType)
		return fmt.Sprintf("list_any_match(%s, x -> x > CAST(? AS %s))", column, duckType), value, nil

	case "gte":
		duckType := MapValueTypeToDuckDBType(elementType)
		return fmt.Sprintf("list_any_match(%s, x -> x >= CAST(? AS %s))", column, duckType), value, nil

	case "lt":
		duckType := MapValueTypeToDuckDBType(elementType)
		return fmt.Sprintf("list_any_match(%s, x -> x < CAST(? AS %s))", column, duckType), value, nil

	case "lte":
		duckType := MapValueTypeToDuckDBType(elementType)
		return fmt.Sprintf("list_any_match(%s, x -> x <= CAST(? AS %s))", column, duckType), value, nil

	default:
		return "", nil, fmt.Errorf("unsupported operator '%s' for LIST column", operator)
	}
}

// ValidateOrderByForListTypes checks if any ORDER BY attributes are LIST types.
// Returns an error if a LIST type is found (LIST columns cannot be used in ORDER BY).
// Uses forma.OrderBy (the DSL type with Attribute string field).
func ValidateOrderByForListTypes(orderBy []forma.OrderBy, getValueType func(attrName string) (forma.ValueType, bool)) error {
	for _, o := range orderBy {
		if vt, ok := getValueType(o.Attribute); ok && vt == forma.ValueTypeList {
			return fmt.Errorf("%w: attribute '%s'", ErrListInOrderBy, o.Attribute)
		}
	}
	return nil
}

// ValidateOrderByAttributesForListTypes checks if resolved AttributeOrder entries are LIST types.
// This variant works with the internal AttributeOrder type used after attribute resolution.
func ValidateOrderByAttributesForListTypes(orderBy []AttributeOrder) error {
	for _, o := range orderBy {
		if o.ValueType == forma.ValueTypeList {
			return fmt.Errorf("%w: attribute ID %d", ErrListInOrderBy, o.AttrID)
		}
	}
	return nil
}

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
	excl := fmt.Sprintf("%s AND row_id NOT IN (%s)", baseClause, strings.Join(placeholders, ","))
	return excl, args
}
