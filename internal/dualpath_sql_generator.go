package internal

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
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
	pgClause, pgArgs, err := pgGen.ToSqlClauses(condition, eavTable, schemaID, cache, paramIndex)
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
		if len(c.Conditions) == 0 {
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

	case *forma.KvCondition:
		meta, ok := cache[c.Attr]
		if !ok {
			// unknown attribute -> cannot push
			return "", nil, nil
		}
		// parse operator and value up-front so we can return an error for unsupported operators
		opPart := ""
		valPart := ""
		if idx := strings.Index(c.Value, ":"); idx >= 0 {
			opPart = c.Value[:idx]
			valPart = c.Value[idx+1:]
		}
		opStr := "equals"
		valStr := c.Value
		if opPart != "" && valPart != "" {
			opStr = opPart
			valStr = valPart
		}

		useMain, _ := classifyPredicate(c, meta)
		if !useMain {
			if meta.ColumnBinding == nil {
				// cannot push due to missing column binding, not an error
				return "", nil, nil
			}
			return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
		}
		if meta.ColumnBinding == nil {
			return "", nil, nil
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

		// Resolve column name relative to main table (prefix with m.)
		colName := "m." + c.Attr
		if meta.ColumnBinding != nil {
			colName = "m." + string(meta.ColumnBinding.ColumnName)
		}

		// Convert value based on metadata similar to sql_generator.buildKv
		var parsedValue any
		switch meta.ValueType {
		case forma.ValueTypeText, forma.ValueTypeUUID:
			parsedValue = valStr
		case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
			parsed := tryParseNumber(valStr)
			switch v := parsed.(type) {
			case int64:
				parsedValue = float64(v)
			case float64:
				parsedValue = v
			default:
				return "", nil, fmt.Errorf("invalid numeric value for '%s': %s", c.Attr, valStr)
			}
		case forma.ValueTypeDate, forma.ValueTypeDateTime:
			var err error
			parsedValue, err = parseDateValue(valStr, meta)
			if err != nil {
				return "", nil, fmt.Errorf("invalid date value for '%s': %w", c.Attr, err)
			}
		case forma.ValueTypeBool:
			parsedInt, err := strconv.Atoi(valStr)
			if err != nil {
				return "", nil, fmt.Errorf("invalid boolean value for '%s': %s", c.Attr, valStr)
			}
			// respect column encoding
			if meta.ColumnBinding != nil {
				switch meta.ColumnBinding.Encoding {
				case forma.MainColumnEncodingBoolInt:
					if parsedInt > 0 {
						parsedValue = int64(1)
					} else {
						parsedValue = int64(0)
					}
				case forma.MainColumnEncodingBoolText:
					if parsedInt > 0 {
						parsedValue = "1"
					} else {
						parsedValue = "0"
					}
				default:
					// default to text "1"/"0"
					if parsedInt > 0 {
						parsedValue = "1"
					} else {
						parsedValue = "0"
					}
				}
			} else {
				if parsedInt > 0 {
					parsedValue = "1"
				} else {
					parsedValue = "0"
				}
			}
		default:
			return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, c.Attr)
		}

		// create placeholder
		*paramIndex++
		ph := fmt.Sprintf("$%d", *paramIndex)
		sql := fmt.Sprintf("%s %s %s", colName, sqlOp, ph)
		return sql, []any{parsedValue}, nil

	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}

// buildDuckClause traverses the condition tree and produces a DuckDB-compatible WHERE clause.
// This mirrors GenerateDuckDBWhereClause but uses attribute metadata to resolve column bindings.
func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	if cond == nil {
		return "1=1", nil, nil
	}

	switch c := cond.(type) {
	case *forma.CompositeCondition:
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

	case *forma.KvCondition:
		// parse op:value or default equals
		opPart := ""
		valPart := ""
		if idx := strings.Index(c.Value, ":"); idx >= 0 {
			opPart = c.Value[:idx]
			valPart = c.Value[idx+1:]
		}
		opStr := "equals"
		valStr := c.Value
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

		// Resolve column name using metadata; fallback to attribute name
		colName := c.Attr
		var meta forma.AttributeMetadata
		var hasMeta bool
		if m, ok := cache[c.Attr]; ok {
			meta = m
			hasMeta = true
			if meta.ColumnBinding != nil {
				colName = string(meta.ColumnBinding.ColumnName)
			}
		}

		// If LIKE operator keep simple text comparison
		if sqlOp == "LIKE" {
			clause := fmt.Sprintf("%s %s ?", colName, sqlOp)
			return clause, []any{valStr}, nil
		}

		// Determine value type: prefer metadata when present, otherwise detect from literal
		valueType := forma.ValueTypeText
		if hasMeta {
			valueType = meta.ValueType
		} else {
			// detect from literal
			if _, err := uuid.Parse(valStr); err == nil {
				valueType = forma.ValueTypeUUID
			} else if ls := strings.ToLower(valStr); ls == "true" || ls == "false" || ls == "1" || ls == "0" {
				valueType = forma.ValueTypeBool
			} else if _, err := strconv.ParseFloat(valStr, 64); err == nil {
				valueType = forma.ValueTypeNumeric
			} else if _, err := time.Parse(time.RFC3339Nano, valStr); err == nil {
				valueType = forma.ValueTypeDateTime
			} else if _, err := strconv.ParseInt(valStr, 10, 64); err == nil {
				valueType = forma.ValueTypeNumeric
			} else {
				valueType = forma.ValueTypeText
			}
		}

		// Build clause using explicit CAST when type is not text
		if valueType == forma.ValueTypeText {
			clause := fmt.Sprintf("%s %s ?", colName, sqlOp)
			return clause, []any{valStr}, nil
		}

		// Use CastExpression to create CAST(? AS TYPE)
		castExpr := CastExpression("?", valueType)
		clause := fmt.Sprintf("%s %s %s", colName, sqlOp, castExpr)

		// Parse param into typed form and normalize for DuckDB
		var rawParam any = valStr
		var err error
		switch valueType {
		case forma.ValueTypeUUID:
			rawParam = valStr
		case forma.ValueTypeBool:
			if b, e := strconv.ParseBool(strings.ToLower(valStr)); e == nil {
				rawParam = b
			} else if valStr == "1" {
				rawParam = true
			} else if valStr == "0" {
				rawParam = false
			}
		case forma.ValueTypeNumeric, forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
			if f, e := strconv.ParseFloat(valStr, 64); e == nil {
				rawParam = f
			} else {
				return "", nil, fmt.Errorf("invalid numeric literal for %s: %s", c.Attr, valStr)
			}
		case forma.ValueTypeDate, forma.ValueTypeDateTime:
			if t, e := time.Parse(time.RFC3339Nano, valStr); e == nil {
				rawParam = t.UTC()
			} else if i, e := strconv.ParseInt(valStr, 10, 64); e == nil {
				rawParam = time.UnixMilli(i).UTC()
			} else {
				return "", nil, fmt.Errorf("invalid date literal for %s: %s", c.Attr, valStr)
			}
		default:
			rawParam = valStr
		}

		param, err := ToDuckDBParam(rawParam, valueType)
		if err != nil {
			return "", nil, fmt.Errorf("to duckdb param: %w", err)
		}

		return clause, []any{param}, nil

	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}
