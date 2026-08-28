package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

// normalizeTargets selects which per-target payloads normalizePredicates
// computes, so standalone entrypoints do not pay for backends they never
// emit.
type normalizeTargets uint8

const (
	targetPgEav normalizeTargets = 1 << iota
	targetPgMain
	targetDuck
	targetHybrid

	targetsNone normalizeTargets = 0
	targetsAll                   = targetPgEav | targetPgMain | targetDuck
)

// normalizePredicates converts a forma.Condition into the typed predicate
// tree consumed by walkPredicate. It is the parse-once layer of #143: every
// "op:value" parse, metadata lookup, operator whitelist check, and target
// value conversion happens here exactly once per leaf. Structural oddities
// (nil nodes, unknown condition types) are preserved as dedicated nodes so
// each walk style keeps its lazy, per-target semantics.
func normalizePredicates(cond forma.Condition, cache forma.SchemaAttributeCache, targets normalizeTargets) PredicateNode {
	if cond == nil {
		return predicateNilNode{}
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		group := &PredicateGroup{Logic: c.Logic, Source: c, FullyPushable: true}
		for _, child := range c.Conditions {
			node := normalizePredicates(child, cache, targets)
			group.Children = append(group.Children, node)
			if !nodeFullyPushable(node) {
				group.FullyPushable = false
			}
		}
		return group
	case *forma.KvCondition:
		return normalizeLeaf(c, cache, targets)
	default:
		return predicateInvalidNode{err: fmt.Errorf("unsupported condition type %T", cond)}
	}
}

func nodeFullyPushable(node PredicateNode) bool {
	switch n := node.(type) {
	case *PredicateGroup:
		return n.FullyPushable
	case *PredicateLeaf:
		return n.Pushable
	default:
		return false
	}
}

func normalizeLeaf(kv *forma.KvCondition, cache forma.SchemaAttributeCache, targets normalizeTargets) *PredicateLeaf {
	leaf := &PredicateLeaf{Attr: kv.Attr, Source: kv}

	meta, hasMeta := cache[kv.Attr]
	leaf.HasMeta = hasMeta
	leaf.Meta = meta
	if hasMeta && meta.ColumnBinding != nil {
		leaf.Storage = PredicateStorageMain
	}

	lenient := conditionexpr.ParseOperatorValueLenient(kv.Value)
	leaf.Operator = PredicateOperator(conditionexpr.CanonicalOperator(lenient.Operator))
	lenientSQL, lenientSQLErr := conditionexpr.ToSQLOperator(lenient.Operator, lenient.Value)
	if lenientSQLErr == nil {
		leaf.SQLOperator = lenientSQL.SQLOperator
		switch leaf.Operator {
		case PredicateOpStartsWith:
			leaf.Pattern = PatternKindPrefix
		case PredicateOpContains:
			leaf.Pattern = PatternKindContains
		}
	}

	var classifyOK bool
	if hasMeta {
		classifyOK, _ = classifyPredicate(kv, meta)
	}
	leaf.Pushable = hasMeta && meta.ColumnBinding != nil && classifyOK

	if targets&targetPgEav != 0 {
		leaf.PgEav = normalizePgEavPayload(kv, meta, hasMeta)
	}
	if targets&targetPgMain != 0 {
		leaf.PgMain = normalizePgMainPayload(kv, meta, hasMeta, classifyOK, lenient, lenientSQL, lenientSQLErr)
	}
	if targets&targetDuck != 0 {
		leaf.Duck = normalizeDuckPayload(kv, meta, hasMeta, lenientSQL, lenientSQLErr)
	}
	if targets&targetHybrid != 0 {
		leaf.Hybrid = normalizeHybridPayload(kv, meta, hasMeta, lenientSQL, lenientSQLErr)
	}
	return leaf
}

// normalizeHybridPayload converts a leaf for the hybrid condition builder.
// Routing mirrors the retired hybridConditionBuilder.resolveColumnName: a raw
// entity_main column (IsMainTableColumn) or a cache-bound attribute emits
// against the main table; everything else falls to the EAV EXISTS form. For
// the main branch the operator parse error is surfaced before the
// descriptor-validation error, matching the retired buildMainTableCondition
// ordering. The EAV branch reuses the strict-parse PgEav payload verbatim.
func normalizeHybridPayload(
	kv *forma.KvCondition,
	meta forma.AttributeMetadata,
	hasMeta bool,
	lenientSQL conditionexpr.SQLOperatorResult,
	lenientSQLErr error,
) HybridLeafPayload {
	colName := ""
	if model.IsMainTableColumn(kv.Attr) {
		colName = kv.Attr
	} else if hasMeta && meta.ColumnBinding != nil {
		colName = string(meta.ColumnBinding.ColumnName)
	}

	if colName == "" {
		return HybridLeafPayload{IsMain: false, Eav: normalizePgEavPayload(kv, meta, hasMeta)}
	}

	// Main-table branch. Operator parse error first (pre-#154 ordering).
	if lenientSQLErr != nil {
		return HybridLeafPayload{IsMain: true, Err: lenientSQLErr}
	}
	leafMeta, err := resolveHybridLeafMeta(colName, meta, hasMeta)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	value, err := ConvertPgMainValue(lenientSQL.Value, kv.Attr, leafMeta)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: err}
	}
	return HybridLeafPayload{
		IsMain:     true,
		MainColumn: colName,
		MainSQLOp:  lenientSQL.SQLOperator,
		MainValue:  value,
	}
}

// resolveHybridLeafMeta returns the attribute metadata driving value
// conversion for a main-table hybrid leaf. The physical column is always
// validated against the entity_main descriptors (preserving the pre-#140
// "unknown main table column" contract even for cache-bound attributes);
// cache-bound attributes that omit value_type derive it from the descriptor
// kind, and raw column references without metadata derive a minimal metadata.
func resolveHybridLeafMeta(colName string, meta forma.AttributeMetadata, hasMeta bool) (forma.AttributeMetadata, error) {
	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return forma.AttributeMetadata{}, fmt.Errorf("unknown main table column: %s", colName)
	}
	if hasMeta {
		if meta.ValueType == "" {
			meta.ValueType = model.ColumnKindToValueType(desc.Kind)
		}
		return meta, nil
	}
	return forma.AttributeMetadata{ValueType: model.ColumnKindToValueType(desc.Kind)}, nil
}

// classifyPredicate returns whether a KvCondition can be pushed to main table based on metadata.
// Note it splits the raw value at the first colon itself (no lenient date
// fallback, no alias canonicalization) — bare ISO timestamps and alias
// operators like "neq" therefore classify as unpushable.
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

// normalizePgEavPayload converts a leaf for the EAV EXISTS emitter: strict
// parse, EAV value-column selection, type conversion, and the text/bool
// operator whitelist.
func normalizePgEavPayload(kv *forma.KvCondition, meta forma.AttributeMetadata, hasMeta bool) PgEavLeafPayload {
	if !hasMeta {
		return PgEavLeafPayload{Err: forma.InvalidInputf("attribute not found in cache: %s", kv.Attr)}
	}

	operator, err := conditionexpr.ParseOperatorValueStrict(kv.Value)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}
	opStr := operator.Operator
	valStr := operator.Value

	valueColumn, parsedValue, err := parsePgEavValue(kv.Attr, meta, valStr)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}

	sqlOperator, err := conditionexpr.ToSQLOperator(opStr, valStr)
	if err != nil {
		return PgEavLeafPayload{Err: err}
	}
	sqlOp := sqlOperator.SQLOperator
	if sqlOp == "LIKE" {
		parsedValue = sqlOperator.Value
	}

	// Both operator-whitelist rejections wrap forma.ErrInvalidInput: the operator
	// is caller-supplied in the advanced_query condition DSL, and since #301 the
	// HTTP boundary classifies on sentinel evidence alone. Without the sentinel,
	// starts_with on a UUID column answered an opaque 500 instead of a 400 naming
	// the operator and the type (#307 round-4 Finding 4).
	//
	// The `unsupported value_type` default in parsePgEavValue deliberately stays
	// plain: that names the schema's declared type, not anything the caller sent.
	if meta.ValueType != forma.ValueTypeText && sqlOp == "LIKE" {
		return PgEavLeafPayload{Err: forma.InvalidInputf("operator '%s' only supported for text attributes, not '%s'", opStr, meta.ValueType)}
	}
	if meta.ValueType == forma.ValueTypeBool && sqlOp != "=" && sqlOp != "!=" {
		return PgEavLeafPayload{Err: forma.InvalidInputf("operator '%s' not supported for boolean attributes", opStr)}
	}

	return PgEavLeafPayload{
		AttrID:      meta.AttributeID,
		ValueColumn: valueColumn,
		SQLOp:       sqlOp,
		Value:       parsedValue,
		Truthy:      meta.ValueType == forma.ValueTypeBool,
	}
}

// parsePgEavValue picks the eav_data value column for the attribute's declared
// type and converts the literal into the Go value bound against it. Extracted
// from normalizePgEavPayload (#357) to keep that function under the size limit;
// behavior is unchanged, and the characterization matrix guards it.
func parsePgEavValue(attr string, meta forma.AttributeMetadata, valStr string) (valueColumn string, parsedValue any, err error) {
	switch meta.ValueType {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return "value_text", valStr, nil

	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		switch v := numutil.TryParseNumber(valStr).(type) {
		case int64:
			// Exact bind (#281, #357): pgx encodes int64 into the NUMERIC
			// comparison losslessly, in every integral spelling. Mirrors
			// ConvertPgMainValue on the main-table path.
			return "value_numeric", v, nil
		case float64:
			return "value_numeric", v, nil
		default:
			return "", nil, forma.InvalidInputf("invalid numeric value for '%s': %s", attr, valStr)
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		parsed, dateErr := parseDateValue(valStr, meta)
		if dateErr != nil {
			return "", nil, fmt.Errorf("invalid date value for '%s': %w", attr, dateErr)
		}
		return "value_numeric", parsed, nil

	case forma.ValueTypeBool:
		// The bind is a Go bool compared against the (value_numeric <> 0)
		// truthiness expression, not the raw column — the payload's Truthy
		// flag drives the emitters. The operand parse is the engine-shared
		// parseBoolOperand rule (#384 P2b).
		parsed, ok := parseBoolOperand(valStr)
		if !ok {
			return "", nil, forma.InvalidInputf("invalid boolean value for '%s': %s", attr, valStr)
		}
		return "value_numeric", parsed, nil

	default:
		return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, attr)
	}
}

// normalizePgMainPayload converts a leaf for entity_main pushdown: unknown
// attributes and unbound columns skip silently, bound-but-unclassifiable
// operators error, and values convert per column encoding.
func normalizePgMainPayload(
	kv *forma.KvCondition,
	meta forma.AttributeMetadata,
	hasMeta bool,
	classifyOK bool,
	lenient conditionexpr.OperatorValue,
	lenientSQL conditionexpr.SQLOperatorResult,
	lenientSQLErr error,
) PgMainLeafPayload {
	if !hasMeta {
		// unknown attribute -> cannot push
		return PgMainLeafPayload{Skip: true}
	}
	if !classifyOK {
		if meta.ColumnBinding == nil {
			return PgMainLeafPayload{Skip: true}
		}
		return PgMainLeafPayload{Err: forma.InvalidInputf("unsupported operator: %s", lenient.Operator)}
	}
	if lenientSQLErr != nil {
		return PgMainLeafPayload{Err: lenientSQLErr}
	}

	parsedValue, err := ConvertPgMainValue(lenientSQL.Value, kv.Attr, meta)
	if err != nil {
		return PgMainLeafPayload{Err: err}
	}
	return PgMainLeafPayload{
		Column: resolveMainTableColumn(kv.Attr, meta),
		SQLOp:  lenientSQL.SQLOperator,
		Value:  parsedValue,
	}
}

// normalizeDuckPayload converts a leaf for the DuckDB emitter, keeping the
// no-metadata fallback inference (detectValueType) and the text/LIKE
// no-cast emission.
func normalizeDuckPayload(
	kv *forma.KvCondition,
	meta forma.AttributeMetadata,
	hasMeta bool,
	lenientSQL conditionexpr.SQLOperatorResult,
	lenientSQLErr error,
) DuckLeafPayload {
	if lenientSQLErr != nil {
		return DuckLeafPayload{Err: lenientSQLErr}
	}

	column := resolveDuckDBColumn(kv.Attr)

	if lenientSQL.SQLOperator == "LIKE" {
		return DuckLeafPayload{Column: column, SQLOp: lenientSQL.SQLOperator, TextLike: true, Param: lenientSQL.Value}
	}

	var valueType forma.ValueType
	if hasMeta {
		valueType = meta.ValueType
	} else {
		valueType = detectValueType(lenientSQL.Value)
	}

	if valueType == forma.ValueTypeText {
		return DuckLeafPayload{Column: column, SQLOp: lenientSQL.SQLOperator, ValueType: valueType, TextLike: true, Param: lenientSQL.Value}
	}

	rawParam, err := parseDuckDBRawParam(lenientSQL.Value, kv.Attr, valueType)
	if err != nil {
		return DuckLeafPayload{Err: err}
	}
	param, err := ToDuckDBParam(rawParam, valueType)
	if err != nil {
		return DuckLeafPayload{Err: fmt.Errorf("to duckdb param: %w", err)}
	}
	return DuckLeafPayload{Column: column, SQLOp: lenientSQL.SQLOperator, ValueType: valueType, Param: param}
}
