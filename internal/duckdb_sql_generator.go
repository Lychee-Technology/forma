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
	"github.com/lychee-technology/forma/internal/conditionexpr"
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
	return generateDuckDBCondition(q.Condition)
}

// generateDuckDBCondition recursively builds DuckDB WHERE clause from a condition tree.
func generateDuckDBCondition(c forma.Condition) (string, []any, error) {
	switch cond := c.(type) {
	case *forma.CompositeCondition:
		return generateDuckDBCompositeCondition(cond)
	case *forma.KvCondition:
		return generateDuckDBKvCondition(cond)
	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", c)
	}
}

// generateDuckDBCompositeCondition handles CompositeCondition for DuckDB WHERE generation.
func generateDuckDBCompositeCondition(cond *forma.CompositeCondition) (string, []any, error) {
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
		p, a, err := generateDuckDBCondition(child)
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
	return joinStrings(parts, joiner), args, nil
}

// generateDuckDBKvCondition handles KvCondition for DuckDB WHERE generation.
func generateDuckDBKvCondition(cond *forma.KvCondition) (string, []any, error) {
	parsed := conditionexpr.ParseOperatorValueLenient(cond.Value)
	opStr := parsed.Operator
	valStr := parsed.Value

	// Convert to SQL operator
	sqlOp, valStr, err := duckDBSQLOperator(opStr, valStr)
	if err != nil {
		return "", nil, err
	}

	// For LIKE operators, keep text comparison
	if sqlOp == "LIKE" {
		clause := fmt.Sprintf("%s %s ?", cond.Attr, sqlOp)
		return clause, []any{valStr}, nil
	}

	// Detect type and emit CAST on the parameter
	valueType := detectDuckDBValueType(valStr)
	duckType := MapValueTypeToDuckDBType(valueType)

	var clause string
	if duckType == "VARCHAR" {
		clause = fmt.Sprintf("%s %s ?", cond.Attr, sqlOp)
	} else {
		clause = fmt.Sprintf("%s %s CAST(? AS %s)", cond.Attr, sqlOp, duckType)
	}

	param := parseDuckDBParamValue(valStr, valueType)
	return clause, []any{param}, nil
}

// duckDBSQLOperator converts a DSL operator to SQL operator and modifies value for LIKE patterns.
func duckDBSQLOperator(op, value string) (string, string, error) {
	switch op {
	case "equals":
		return "=", value, nil
	case "gt":
		return ">", value, nil
	case "gte":
		return ">=", value, nil
	case "lt":
		return "<", value, nil
	case "lte":
		return "<=", value, nil
	case "not_equals":
		return "!=", value, nil
	case "starts_with":
		return "LIKE", value + "%", nil
	case "contains":
		return "LIKE", "%" + value + "%", nil
	default:
		return "", "", fmt.Errorf("unsupported operator: %s", op)
	}
}

// detectDuckDBValueType infers the forma.ValueType from a string literal.
func detectDuckDBValueType(s string) forma.ValueType {
	// Try UUID
	if _, err := uuid.Parse(s); err == nil {
		return forma.ValueTypeUUID
	}
	// Try bool
	ls := strings.ToLower(s)
	if ls == "true" || ls == "false" || ls == "1" || ls == "0" {
		return forma.ValueTypeBool
	}
	// Try timestamp (RFC3339 or unix millis)
	if _, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return forma.ValueTypeDateTime
	}
	trimmed := strings.TrimPrefix(s, "-")
	if len(trimmed) >= 12 {
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			return forma.ValueTypeDateTime
		}
	}
	// Try numeric
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return forma.ValueTypeNumeric
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		// ambiguous integer: treat as numeric/bigint; choose numeric for comparisons
		return forma.ValueTypeNumeric
	}
	return forma.ValueTypeText
}

// parseDuckDBParamValue parses a string value into a typed Go value for DuckDB parameters.
func parseDuckDBParamValue(s string, vt forma.ValueType) any {
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
		if t, err := conditionexpr.ParseRFC3339OrUnixMs(s); err == nil {
			return t.UTC()
		}
		return s
	default:
		return s
	}
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
