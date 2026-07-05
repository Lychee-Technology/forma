package sqlgen

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
		if opStr == "equals" || opStr == "starts_with" || opStr == "contains" {
			return true, "text supported"
		}
		return false, "text operator not supported"
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		switch opStr {
		case "equals", "gt", "gte", "lt", "lte", "not_equals":
			return true, "numeric supported"
		default:
			return false, "numeric operator not supported"
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
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

// pgMainGuard implements compositeGuard with OR veto for pg-main.
type pgMainGuard struct {
	cache forma.SchemaAttributeCache
}

func (g *pgMainGuard) SkipComposite(c *forma.CompositeCondition) bool {
	if c.Logic == forma.LogicOr {
		return !isFullyPushableToMain(c, g.cache)
	}
	return false
}

// pgMainLeafEmitter renders KvCondition leaves for PG main table pushdown.
type pgMainLeafEmitter struct {
	cache      forma.SchemaAttributeCache
	paramIndex *int
}

// pgMainLeafValue is the value core shared by the string emitter and the
// plan binder (#142): every pushability decision and conversion lives here
// exactly once so bound args cannot diverge from emitted clauses.
func pgMainLeafValue(c *forma.KvCondition, cache forma.SchemaAttributeCache) (any, bool, error) {
	meta, ok := cache[c.Attr]
	if !ok {
		// unknown attribute -> cannot push
		return nil, false, nil
	}

	opVal := parseOperatorValue(c.Value)

	// Check if we can push to main table
	useMain, _ := classifyPredicate(c, meta)
	if !useMain {
		if meta.ColumnBinding == nil {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unsupported operator: %s", opVal.op)
	}
	if meta.ColumnBinding == nil {
		return nil, false, nil
	}

	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
	if err != nil {
		return nil, false, err
	}

	parsedValue, err := ConvertPgMainValue(sqlOpResult.value, c.Attr, meta)
	if err != nil {
		return nil, false, err
	}
	return parsedValue, true, nil
}

func (e *pgMainLeafEmitter) EmitLeaf(c *forma.KvCondition) (string, []any, error) {
	parsedValue, emit, err := pgMainLeafValue(c, e.cache)
	if err != nil || !emit {
		return "", nil, err
	}

	meta := e.cache[c.Attr]
	opVal := parseOperatorValue(c.Value)
	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
	if err != nil {
		return "", nil, err
	}
	colName := resolveMainTableColumn(c.Attr, meta)

	*e.paramIndex++
	ph := fmt.Sprintf("$%d", *e.paramIndex)
	sql := fmt.Sprintf("%s %s %s", colName, sqlOpResult.sqlOp, ph)
	return sql, []any{parsedValue}, nil
}

// buildPgMainClause traverses the condition tree and emits a WHERE fragment targeting entity_main (m.*)
// It returns the clause string (with $n placeholders) and args slice, advancing paramIndex as needed.
func buildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	if cond == nil {
		return "", nil, nil
	}
	guard := &pgMainGuard{cache: cache}
	emitter := &pgMainLeafEmitter{cache: cache, paramIndex: paramIndex}
	return walkCondition(cond, pgMainStyle, guard, emitter)
}

func BuildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	return buildPgMainClause(cond, cache, paramIndex)
}

// duckLeafEmitter renders KvCondition leaves for DuckDB with ? placeholders and CAST expressions.
type duckLeafEmitter struct {
	cache forma.SchemaAttributeCache
}

// duckLeafParts is the shape/value decomposition of a DuckDB leaf shared by
// the string emitter and the plan binder (#142).
type duckLeafParts struct {
	sqlOp     string
	valueType forma.ValueType
	textLike  bool // emit `col op ?` with the string value (LIKE or text type)
	param     any
}

// duckLeafValue is the value core shared by the string emitter and the plan
// binder: operator/type/param decisions live here exactly once.
func duckLeafValue(c *forma.KvCondition, cache forma.SchemaAttributeCache) (duckLeafParts, error) {
	opVal := parseOperatorValue(c.Value)

	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
	if err != nil {
		return duckLeafParts{}, err
	}

	var meta forma.AttributeMetadata
	var hasMeta bool
	if m, ok := cache[c.Attr]; ok {
		meta = m
		hasMeta = true
	}

	if sqlOpResult.sqlOp == "LIKE" {
		return duckLeafParts{sqlOp: sqlOpResult.sqlOp, textLike: true, param: sqlOpResult.value}, nil
	}

	var valueType forma.ValueType
	if hasMeta {
		valueType = meta.ValueType
	} else {
		valueType = detectValueType(sqlOpResult.value)
	}

	if valueType == forma.ValueTypeText {
		return duckLeafParts{sqlOp: sqlOpResult.sqlOp, valueType: valueType, textLike: true, param: sqlOpResult.value}, nil
	}

	rawParam, err := parseDuckDBRawParam(sqlOpResult.value, c.Attr, valueType)
	if err != nil {
		return duckLeafParts{}, err
	}
	param, err := ToDuckDBParam(rawParam, valueType)
	if err != nil {
		return duckLeafParts{}, fmt.Errorf("to duckdb param: %w", err)
	}
	return duckLeafParts{sqlOp: sqlOpResult.sqlOp, valueType: valueType, param: param}, nil
}

func (e *duckLeafEmitter) EmitLeaf(c *forma.KvCondition) (string, []any, error) {
	parts, err := duckLeafValue(c, e.cache)
	if err != nil {
		return "", nil, err
	}

	colName := resolveDuckDBColumn(c.Attr, e.cache)

	if parts.textLike {
		clause := fmt.Sprintf("%s %s ?", colName, parts.sqlOp)
		return clause, []any{parts.param}, nil
	}

	castExpr := CastExpression("?", parts.valueType)
	clause := fmt.Sprintf("%s %s %s", colName, parts.sqlOp, castExpr)
	return clause, []any{parts.param}, nil
}

// buildDuckClause traverses the condition tree and produces a DuckDB-compatible WHERE clause.
func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	if cond == nil {
		return "1=1", nil, nil
	}
	emitter := &duckLeafEmitter{cache: cache}
	return walkCondition(cond, duckStyle, nil, emitter)
}

func BuildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	return buildDuckClause(cond, cache)
}
