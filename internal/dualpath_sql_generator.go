package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// DualClauses contains SQL fragments and argument lists for both Postgres and DuckDB.
type DualClauses struct {
	PgClause     string // existing EAV-based clause (EXISTS...)
	PgArgs       []any
	PgMainClause string // predicates that can be pushed into entity_main (m.*)
	PgMainArgs   []any

	DuckClause string
	DuckArgs   []any
}

// ToDualClauses generates Postgres and DuckDB WHERE fragments for the given condition.
// - PgClause reuses existing SQLGenerator (EAV-based EXISTS expressions).
// - PgMainClause contains predicates suitable for entity_main pushdown.
// - DuckClause maps attributes to column names when available and emits a simple DuckDB-style clause.
// Note: DuckDB placeholders are "?" and args are returned in order. Postgres uses $n placeholders.
func ToDualClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (DualClauses, error) {
	// Build pushdown-capable main table predicates first so placeholders ($n) align.
	pgMainClause, pgMainArgs, err := buildPgMainClause(condition, cache, paramIndex)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg main generation: %w", err)
	}

	// Postgres EAV side: reuse existing SQL generator for full condition
	pgGen := NewSQLGenerator()
	pgClause, pgArgs, err := pgGen.ToSQLClauses(condition, eavTable, schemaID, cache, paramIndex)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg sql generation: %w", err)
	}

	// DuckDB side: generate simple column-based predicates using attribute metadata
	duckClause, duckArgs, err := buildDuckClause(condition, cache)
	if err != nil {
		return DualClauses{}, fmt.Errorf("duck sql generation: %w", err)
	}

	return DualClauses{
		PgClause:     pgClause,
		PgArgs:       pgArgs,
		PgMainClause: pgMainClause,
		PgMainArgs:   pgMainArgs,
		DuckClause:   duckClause,
		DuckArgs:     duckArgs,
	}, nil
}

// classifyPredicate returns whether a KvCondition can be pushed to main table based on metadata.
func classifyPredicate(kv *forma.KvCondition, meta forma.AttributeMetadata) (bool, string) {
	if meta.ColumnBinding == nil {
		return false, "no column binding"
	}

	// Simple operator extraction
	opPart := ""
	valPart := ""
	if idx := strings.Index(kv.Value, ":"); idx >= 0 {
		opPart = kv.Value[:idx]
		valPart = kv.Value[idx+1:]
	}
	opStr := "equals"
	if opPart != "" && valPart != "" {
		opStr = opPart
	}

	// Decide based on value type and operator
	switch meta.ValueType {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		// Text supports equals, starts_with, contains
		if opStr == "equals" || opStr == "starts_with" || opStr == "contains" {
			return true, "text supported"
		}
		return false, "text operator not supported"
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		// numeric supports comparison
		switch opStr {
		case "equals", "gt", "gte", "lt", "lte", "not_equals":
			return true, "numeric supported"
		default:
			return false, "numeric operator not supported"
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// date comparisons allowed; assume main column encoding supports it
		switch opStr {
		case "equals", "gt", "gte", "lt", "lte", "not_equals":
			return true, "date supported"
		default:
			return false, "date operator not supported"
		}
	case forma.ValueTypeBool:
		if opStr == "equals" || opStr == "not_equals" {
			return true, "bool supported"
		}
		return false, "bool operator not supported"
	default:
		return false, "unknown value type"
	}
}

// buildPgMainClause traverses the condition tree and emits a WHERE fragment targeting entity_main (m.*)
// It returns the clause string (with $n placeholders) and args slice, advancing paramIndex as needed.
func buildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	if cond == nil {
		return "", nil, nil
	}

	switch c := cond.(type) {
	case *forma.CompositeCondition:
		return buildPgMainCompositeClause(c, cache, paramIndex)

	case *forma.KvCondition:
		return buildPgMainKvClause(c, cache, paramIndex)

	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}

// isFullyPushableToMain returns true iff every leaf KvCondition in the tree
// has a column binding and a supported operator, so the whole tree can be
// pushed to the Postgres entity_main table. Used to guard OR composites:
// pushing a partial OR would silently drop rows matched only by the
// non-pushable branch, so the entire OR must be skipped when any child fails.
func isFullyPushableToMain(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		for _, child := range c.Conditions {
			if !isFullyPushableToMain(child, cache) {
				return false
			}
		}
		return true
	case *forma.KvCondition:
		meta, ok := cache[c.Attr]
		if !ok || meta.ColumnBinding == nil {
			return false
		}
		pushable, _ := classifyPredicate(c, meta)
		return pushable
	default:
		return false
	}
}

// buildPgMainCompositeClause handles CompositeCondition for Postgres main table.
func buildPgMainCompositeClause(c *forma.CompositeCondition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	if len(c.Conditions) == 0 {
		return "", nil, nil
	}

	// For OR logic every branch must be pushable to main table. If any branch
	// cannot be pushed we must skip the entire OR: emitting a partial OR would
	// silently drop rows that are matched only by the non-pushable branch.
	if c.Logic == forma.LogicOr && !isFullyPushableToMain(c, cache) {
		return "", nil, nil
	}

	parts := make([]string, 0, len(c.Conditions))
	args := []any{}
	joiner := " AND "
	if c.Logic == forma.LogicOr {
		joiner = " OR "
	}

	for _, child := range c.Conditions {
		p, a, err := buildPgMainClause(child, cache, paramIndex)
		if err != nil {
			return "", nil, err
		}
		if p != "" {
			parts = append(parts, fmt.Sprintf("(%s)", p))
			args = append(args, a...)
		}
	}

	if len(parts) == 0 {
		return "", nil, nil
	}
	if len(parts) == 1 {
		return parts[0], args, nil
	}
	return "(" + strings.Join(parts, joiner) + ")", args, nil
}

// buildPgMainKvClause handles KvCondition for Postgres main table pushdown.
func buildPgMainKvClause(c *forma.KvCondition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	meta, ok := cache[c.Attr]
	if !ok {
		// unknown attribute -> cannot push
		return "", nil, nil
	}

	// Parse operator and value
	opVal := parseOperatorValue(c.Value)

	// Check if we can push to main table
	useMain, _ := classifyPredicate(c, meta)
	if !useMain {
		if meta.ColumnBinding == nil {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("unsupported operator: %s", opVal.op)
	}
	if meta.ColumnBinding == nil {
		return "", nil, nil
	}

	// Convert operator to SQL
	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
	if err != nil {
		return "", nil, err
	}

	// Resolve column name
	colName := resolveMainTableColumn(c.Attr, meta)

	// Convert value based on metadata
	parsedValue, err := convertPgMainValue(sqlOpResult.value, c.Attr, meta)
	if err != nil {
		return "", nil, err
	}

	// Create placeholder and clause
	*paramIndex++
	ph := fmt.Sprintf("$%d", *paramIndex)
	sql := fmt.Sprintf("%s %s %s", colName, sqlOpResult.sqlOp, ph)
	return sql, []any{parsedValue}, nil
}

// buildDuckClause traverses the condition tree and produces a DuckDB-compatible WHERE clause.
// This mirrors GenerateDuckDBWhereClause but uses attribute metadata to resolve column bindings.
func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	if cond == nil {
		return "1=1", nil, nil
	}

	switch c := cond.(type) {
	case *forma.CompositeCondition:
		return buildDuckCompositeClause(c, cache)

	case *forma.KvCondition:
		return buildDuckKvClause(c, cache)

	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}

// buildDuckCompositeClause handles CompositeCondition for DuckDB.
func buildDuckCompositeClause(c *forma.CompositeCondition, cache forma.SchemaAttributeCache) (string, []any, error) {
	if len(c.Conditions) == 0 {
		return "1=1", nil, nil
	}

	parts := make([]string, 0, len(c.Conditions))
	args := []any{}
	joiner := " AND "
	if c.Logic == forma.LogicOr {
		joiner = " OR "
	}

	for _, child := range c.Conditions {
		p, a, err := buildDuckClause(child, cache)
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
	return strings.Join(parts, joiner), args, nil
}

// buildDuckKvClause handles KvCondition for DuckDB.
func buildDuckKvClause(c *forma.KvCondition, cache forma.SchemaAttributeCache) (string, []any, error) {
	// Parse operator and value
	opVal := parseOperatorValue(c.Value)

	// Convert operator to SQL
	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
	if err != nil {
		return "", nil, err
	}

	// Resolve column name using metadata
	colName := resolveDuckDBColumn(c.Attr, cache)

	// Get metadata and determine value type
	var meta forma.AttributeMetadata
	var hasMeta bool
	if m, ok := cache[c.Attr]; ok {
		meta = m
		hasMeta = true
	}

	// If LIKE operator, keep simple text comparison
	if sqlOpResult.sqlOp == "LIKE" {
		clause := fmt.Sprintf("%s %s ?", colName, sqlOpResult.sqlOp)
		return clause, []any{sqlOpResult.value}, nil
	}

	// Determine value type
	var valueType forma.ValueType
	if hasMeta {
		valueType = meta.ValueType
	} else {
		valueType = detectValueType(sqlOpResult.value)
	}

	// For text type, simple comparison
	if valueType == forma.ValueTypeText {
		clause := fmt.Sprintf("%s %s ?", colName, sqlOpResult.sqlOp)
		return clause, []any{sqlOpResult.value}, nil
	}

	// Use CastExpression to create CAST(? AS TYPE)
	castExpr := CastExpression("?", valueType)
	clause := fmt.Sprintf("%s %s %s", colName, sqlOpResult.sqlOp, castExpr)

	// Parse param into typed form
	rawParam, err := parseDuckDBRawParam(sqlOpResult.value, c.Attr, valueType)
	if err != nil {
		return "", nil, err
	}

	// Normalize for DuckDB
	param, err := ToDuckDBParam(rawParam, valueType)
	if err != nil {
		return "", nil, fmt.Errorf("to duckdb param: %w", err)
	}

	return clause, []any{param}, nil
}
